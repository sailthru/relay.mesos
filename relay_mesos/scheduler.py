from __future__ import division
import random
import sys

import mesos.interface
from mesos.interface import mesos_pb2

from relay_mesos import log
from relay_mesos.util import catch


# Resource types supported by Mesos
SCALAR_KEYS = {'cpus': float, 'mem': int, 'disk': int}
RANGE_KEYS = {'ports': int}
SET_KEYS = {'disks': str}


# TODO: remove this
TOTAL_TASKS = 10
MAX_FAILURES = 10000


class MaxFailuresReached(Exception):
    pass


def filter_offers(offers, task_resources):
    """
    Determine which offers are usable

    `offers` a list of mesos Offer instances
    `task_resources` the stuff a task would consume:
        {
            "cpus": 10,
            "mem": 1,
            "disk": 12,
            "ports": [(20, 34), (35, 35)],
            "disks": ["sda1"]
        }
    """
    available_offers = []
    decline_offers = []
    for offer in offers:
        ntasks = calc_tasks_per_offer(offer, task_resources)
        if ntasks == 0:
            decline_offers.append(offer)
            continue
        available_offers.append((offer, ntasks))
    return available_offers, decline_offers


def calc_tasks_per_offer(offer, task_resources):
    """
    Decide how many tasks a given mesos Offer can contain.

    `offer` a mesos Offer instance
    `resources` the stuff a task would consume:
        {
            "cpus": 10,
            "mem": 1,
            "disk": 12,
            "ports": [(20, 34), (35, 35)],
            "disks": ["sda1"]
        }
    """
    num_tasks = float('inf')
    for res in offer.resources:
        if res.name not in task_resources:
            continue  # we don't care about this resource
        if res.name in SCALAR_KEYS:
            oval = float(res.scalar.value)
            reqval = float(task_resources.get(res.name))
            if reqval <= oval:
                num_tasks = min(num_tasks, int(oval / reqval))
            else:
                num_tasks = 0
                break
        elif res.name in RANGE_KEYS:
            raise NotImplementedError("TODO ... not sure how to handle this")
            # what does reqval look like?
            # reqval = float(task_resources.get(res.name))
            # if any(start <= reqval <= stop
            #        for start, stop in res.ranges.range):
            #     num_tasks = 1
            # else:
            # val = list(val) ???
        elif res.name in SET_KEYS:
            raise NotImplementedError("TODO ... not sure how to handle this")
        else:
            raise NotImplementedError((
                "Unrecognized mesos resource: %s.  You should figure out how"
                " to support this") % res.name)
    if num_tasks == float('inf'):
        return 0
    else:
        return num_tasks


def create_tasks(MV, available_offers, driver, executor, task_resources,
                 command):
    """
    Launch up to `MV` mesos tasks, depending on availability of mesos
    resources.

    `MV` max number of mesos tasks to spin up.  Relay chooses this number
    `available_offers` a dict of mesos offers and num tasks they can support
    `driver` a mesos driver instance
    `executor` a mesos executor instance
    `task_resources` the stuff a task would consume:
        {
            "cpus": 10,
            "mem": 1,
            "disk": 12,
            "ports": [(20, 34), (35, 35)],
            "disks": ["sda1"]
        }
    """
    n_fulfilled = 0
    for offer, ntasks in available_offers:
        if n_fulfilled >= MV:
            driver.declineOffer(offer.id)
            continue
        tasks = []
        for ID in range(ntasks):
            if n_fulfilled >= MV:
                break
            n_fulfilled += 1

            tid = "%s.%s.%s" % (
                ID, offer.id.value, random.randint(1, sys.maxint))
            log.debug(
                "Accepting offer to start a task",
                extra=dict(offer_host=offer.hostname, task_id=tid))
            task = _create_task(
                tid, offer, executor, task_resources, command)
            tasks.append(task)
        driver.launchTasks(offer.id, tasks)
    return n_fulfilled


def _create_task(tid, offer, executor, task_resources, command):
    """
    `tid` (str) task id
    `offer` a mesos Offer instance
    `executor` a mesos Executor instance
    `task_resources` the stuff a task would consume:
        {
            "cpus": 10,
            "mem": 1,
            "disk": 12,
            "ports": [(20, 34), (35, 35)],
            "disks": ["sda1"]
        }
    """
    task = mesos_pb2.TaskInfo()
    task.task_id.value = tid
    task.slave_id.value = offer.slave_id.value
    task.name = "task %s" % tid
    # task.executor.MergeFrom(executor)  # TODO: support docker containers
    task.command.value = command
    seen = set()
    for key in set(SCALAR_KEYS).intersection(task_resources):
        seen.add(key)
        resource = task.resources.add()
        resource.name = key
        resource.type = mesos_pb2.Value.SCALAR
        typecast = SCALAR_KEYS[key]
        resource.scalar.value = typecast(task_resources[key])

    for key in set(RANGE_KEYS).intersection(task_resources):
        seen.add(key)
        resource = task.resources.add()
        resource.name = key
        resource.type = mesos_pb2.Value.RANGES
        for range_data in task_resources[key]:
            inst = resource.ranges.range.add()
            typecast = RANGE_KEYS[key]
            inst.begin = typecast(range_data[0])
            inst.end = typecast(range_data[1])

    for key in set(SET_KEYS).intersection(task_resources):
        typecast = SET_KEYS[key]
        seen.add(key)
        resource = task.resources.add()
        resource = task.resources.add()
        resource.name = key
        resource.type = mesos_pb2.Value.SET
        for elem in task_resources[key]:
            resource.set.item.append(typecast(elem))
    unrecognized_keys = set(task_resources).difference(seen)
    if unrecognized_keys:
        msg = "Unrecognized keys in task_resources!"
        log.error(msg, extra=dict(unrecognized_keys=unrecognized_keys))
        raise UserWarning(
            "%s unrecognized_keys: %s" % (msg, unrecognized_keys))
    return task


class Scheduler(mesos.interface.Scheduler):
    def __init__(self, executor, MV, task_resources, exception_sender,
                 warmer, cooler):
        self.warmer = warmer
        self.cooler = cooler
        self.executor = executor
        self.task_resources = task_resources
        self.MV = MV
        self.exception_sender = exception_sender
        self.failures = 0

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos
        master.  This is only called when the scheduler has previously been
        registered.

        MasterInfo containing the updated information about the elected master
        is provided as an argument.
        """
        log.info(
            "Registered with framework",
            extra=dict(framework_id=frameworkId.value))

    def resourceOffers(self, driver, offers):
        catch(self._resourceOffers, self.exception_sender)(
            driver, offers)

    def _resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework. A single
        offer will only contain resources from a single slave.  Resources
        associated with an offer will not be re-offered to _this_ framework
        until either (a) this framework has rejected those resources (see
        SchedulerDriver.launchTasks) or (b) those resources have been
        rescinded (see Scheduler.offerRescinded).  Note that resources may be
        concurrently offered to more than one framework at a time (depending
        on the allocator being used).  In that case, the first framework to
        launch tasks using those resources will be able to use them while the
        other frameworks will have those resources rescinded (or if a
        framework has already launched tasks with those resources then those
        tasks will fail with a TASK_LOST status and a message saying as much).
        """
        log.debug("Got resource offers", extra=dict(num_offers=len(offers)))
        available_offers, decline_offers = filter_offers(
            offers, self.task_resources)
        for offer in decline_offers:
            driver.declineOffer(offer.id)
        if not available_offers:
            log.debug('None of the mesos offers had enough relevant resources')
            return
        log.debug(
            'Mesos has offers available', extra=dict(
                available_offers=len(available_offers),
                max_runnable_tasks=sum(x[1] for x in available_offers)))
        # get num tasks I should create from relay.  wait indefinitely.
        with self.MV.get_lock():
            MV = self.MV.value
            self.MV.value = 0
        if MV == 0:
            log.debug('mesos scheduler has received no requests from relay')
            for offer, _ in available_offers:
                driver.declineOffer(offer.id)
            return
        # create tasks that fulfill relay's requests
        if MV > 0:
            command = self.warmer
        elif MV < 0:
            command = self.cooler
        create_tasks(
            MV=abs(MV), available_offers=available_offers,
            driver=driver, executor=self.executor,
            task_resources=self.task_resources, command=command)
        # TODO: count how many were fulfilled?  make relay block?
        # n_fulfilled = create_tasks(...)
        # self.relay_channel.send_pyobj(n_fulfilled)

    def statusUpdate(self, driver, update):
        catch(self._statusUpdate, self.exception_sender)(driver, update)

    def _statusUpdate(self, driver, update):
        log.debug('task status update: %s' % str(update.message), extra={
            'task_id': update.task_id.value, 'state': update.state,
            'slave_id': update.slave_id.value, 'timestamp': update.timestamp})
        # TODO: figure out how to identify these
        # toEnum 0 = Starting
        # toEnum 1 = TaskRunning
        # toEnum 2 = Finished
        # toEnum 3 = Failed
        # toEnum 4 = Killed
        # toEnum 5 = Lost
        # toEnum 6 = Staging
        if update.state in [3, 4, 5]:
            self.failures += 1
        elif update.state in [2, 1]:  # TODO are these choices acceptable?
            self.failures = max(self.failures - 1, 0)
        if self.failures >= MAX_FAILURES:
            log.error(
                "Max allowable number of failures reached",
                extra=dict(max_failures=self.failures))
            driver.stop()
            raise MaxFailuresReached(self.failures)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        pass

        # self.messagesReceived += 1

        # The message bounced back as expected.
        # if message != "data with a \0 byte":
        #     print "The returned message data did not match!"
        #     print "  Expected: 'data with a \\x00 byte'"
        #     print "  Actual:  ", repr(str(message))
        #     sys.exit(1)
        # print "Received message:", repr(str(message))

        # if self.messagesReceived == TOTAL_TASKS:
        #     if self.messagesReceived != self.messagesSent:
        #         print "Sent", self.messagesSent,
        #         print "but received", self.messagesReceived
        #         sys.exit(1)
        #     print "All tasks done, and all messages received, exiting"
        #     driver.stop()
