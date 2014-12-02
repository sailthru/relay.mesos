from __future__ import division
import random
import sys
import zmq

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2

from relay import log


# Resource types supported by Mesos
SCALAR_KEYS = {'cpus': float, 'mem': int, 'disk': int}
RANGE_KEYS = {'ports': int}
SET_KEYS = {'disks': str}


# TODO: remove this
TOTAL_TASKS = 10


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


def create_tasks(reqtasks, available_offers, driver, executor, task_resources):
    """
    Launch up to `reqtasks` mesos tasks, depending on availability of mesos
    resources.

    `reqtasks` max number of mesos tasks to spin up.  Relay chooses this number
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
        if n_fulfilled >= reqtasks:
            driver.declineOffer(offer.id)
            continue
        tasks = []
        for ID in range(ntasks):
            if n_fulfilled >= reqtasks:
                break
            n_fulfilled += 1

            tid = "%s.%s.%s" % (
                ID, offer.id.value, random.randint(1, sys.maxint))
            log.debug(
                "Accepting offer to start a task",
                extra=dict(offer_host=offer.hostname, task_id=tid))
            task = _create_task(
                tid, offer, executor, task_resources)
            tasks.append(task)
        driver.launchTasks(offer.id, tasks)
    return n_fulfilled


def _create_task(tid, offer, executor, task_resources):
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
    task.executor.MergeFrom(executor)
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
    def __init__(self, executor, relay_channel, task_resources):
        self.executor = executor
        self.task_resources = task_resources
        self.relay_channel = relay_channel

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
        try:
            log.debug('mesos scheduler checking for relay requests')
            reqtasks, func = self.relay_channel.recv_pyobj()  # zmq.NOBLOCK)
        except zmq.ZMQError:
            log.debug('mesos scheduler has received no requests from relay')
            reqtasks, func = 0, None
        if func is None:
            for offer, _ in available_offers:
                driver.declineOffer(offer.id)
            return
        # create tasks depending on what relay says
        # TODO: _create_task should figure out whether this is a warmer or
        # cooler
        n_fulfilled = create_tasks(
            reqtasks=reqtasks, available_offers=available_offers,
            driver=driver, executor=self.executor,
            task_resources=self.task_resources)
        self.relay_channel.send_pyobj(n_fulfilled)

    def statusUpdate(self, driver, update):
        # TODO: continue from here
        print "Task %s is in state %d" % (update.task_id.value, update.state)

        # Ensure the binary data came through.
        # if update.data != "data with a \0 byte":
        #     print "The update data did not match!"
        #     print "  Expected: 'data with a \\x00 byte'"
        #     print "  Actual:  ", repr(str(update.data))
        #     sys.exit(1)

        # if update.state == mesos_pb2.TASK_FINISHED:
        #     self.tasksFinished += 1
        #     if self.tasksFinished == TOTAL_TASKS:
        #         print "All tasks done, waiting for final framework message"

        #     slave_id, executor_id = self.taskData[update.task_id.value]

        #     self.messagesSent += 1
        #     driver.sendFrameworkMessage(
        #         executor_id,
        #         slave_id,
        #         'data with a \0 byte')

    def frameworkMessage(self, driver, executorId, slaveId, message):
        # TODO: figure out when to kill the framework if too many task failures
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
