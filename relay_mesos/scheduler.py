from __future__ import division
import random
import time
import sys

import mesos.interface
from mesos.interface import mesos_pb2

from relay_mesos import log
from relay_mesos.util import catch


# Resource types supported by Mesos
SCALAR_KEYS = {'cpus': float, 'mem': int, 'disk': int}
RANGE_KEYS = {'ports': int}
SET_KEYS = {'disks': str}


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


def create_tasks(MV, available_offers, driver, command, ns):
    """
    Launch up to `MV` mesos tasks, depending on availability of mesos
    resources.

    `MV` max number of mesos tasks to spin up.  Relay chooses this number
    `available_offers` a dict of mesos offers and num tasks they can support
    `driver` a mesos driver instance
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
                "Accepting offer to start a task", extra=dict(
                    offer_host=offer.hostname, task_id=tid,
                    mesos_framework_name=ns.mesos_framework_name))
            task = _create_task(
                tid, offer, command, ns)
            tasks.append(task)
        driver.launchTasks(offer.id, tasks)
    return n_fulfilled


def _create_task_add_task_resources(task, ns):
    task_resources = dict(ns.mesos_task_resources)
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
        log.error(msg, extra=dict(
            unrecognized_keys=unrecognized_keys,
            mesos_framework_name=ns.mesos_framework_name))
        raise UserWarning(
            "%s unrecognized_keys: %s" % (msg, unrecognized_keys))


def _create_task(tid, offer, command, ns):
    """
    `tid` (str) task id
    `offer` a mesos Offer instance
    `ns.mesos_task_resources` the stuff a task would consume:
        {
            "cpus": 10,
            "mem": 1,
            "disk": 12,
            "ports": [(20, 34), (35, 35)],
            "disks": ["sda1"]
        }
    `ns.docker_image` (str|None)
        a docker image you wish to execute the command in
    `ns.volumes` a list of volumes that get mounted into the container:
        [
          ("host_path", "container_path", "mode"),
          ("/my/directory", "/path/on/container", "ro")
        ]
    """
    task = dict(
        task_id=mesos_pb2.TaskID(value=tid),
        slave_id=offer.slave_id,
        command=mesos_pb2.CommandInfo(
            value=command,
            uris=[mesos_pb2.CommandInfo.URI(value=uri) for uri in ns.uris],
            environment=mesos_pb2.Environment(variables=[
                mesos_pb2.Environment.Variable(name=k, value=v)
                for k, v in ns.mesos_environment])
        )
    )
    if ns.mesos_framework_name:
        task.update(
            name="relay.mesos task: %s: %s" % (ns.mesos_framework_name, tid))
    else:
        task.update(name="relay.mesos task: %s" % tid)
    # ability to inject os.environ values into the command
    if ns.docker_image:
        volumes = [
            mesos_pb2.Volume(
                host_path=host_path,
                container_path=container_path,
                mode=mesos_pb2.Volume.Mode.Value(mode.upper()))
            for host_path, container_path, mode in ns.volumes]
        task.update(
            container=mesos_pb2.ContainerInfo(
                type=mesos_pb2.ContainerInfo.DOCKER,
                volumes=volumes,
                docker=mesos_pb2.ContainerInfo.DockerInfo(
                    image=ns.docker_image,
                    force_pull_image=ns.force_pull_image,
                    network=mesos_pb2.ContainerInfo.DockerInfo.Network.Value(
                        ns.docker_network),
                    parameters=[
                        mesos_pb2.Parameter(key=k, value=v)
                        for k, v in ns.docker_parameters.items()],
                )
            ))
    task = mesos_pb2.TaskInfo(**task)
    _create_task_add_task_resources(task, ns)
    return task


class Scheduler(mesos.interface.Scheduler):
    def __init__(self, MV, exception_sender, mesos_ready, ns):
        self.ns = ns
        self.MV = MV
        self.mesos_ready = mesos_ready
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
        catch(self._registered, self.exception_sender)(
            driver, frameworkId, masterInfo)

    def _registered(self, driver, frameworkId, masterInfo):
        self.mesos_ready.acquire()
        self.mesos_ready.notify()
        self.mesos_ready.release()

        log.info(
            "Registered with master", extra=dict(
                framework_id=frameworkId.value, master_pid=masterInfo.pid,
                master_hostname=masterInfo.hostname, master_id=masterInfo.id,
                master_ip=masterInfo.ip, master_port=masterInfo.port,
                mesos_framework_name=self.ns.mesos_framework_name,
            ))

    def reregistered(self, driver, masterInfo):
        log.info(
            "Re-registered with master", extra=dict(
                master_pid=masterInfo.pid,
                master_hostname=masterInfo.hostname, master_id=masterInfo.id,
                master_ip=masterInfo.ip, master_port=masterInfo.port,
                mesos_framework_name=self.ns.mesos_framework_name,
            ))

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
        log.debug("Got resource offers", extra=dict(
            num_offers=len(offers),
            mesos_framework_name=self.ns.mesos_framework_name))
        available_offers, decline_offers = filter_offers(
            offers, dict(self.ns.mesos_task_resources))
        for offer in decline_offers:
            driver.declineOffer(offer.id)
        if not available_offers:
            log.debug(
                'None of the mesos offers had enough relevant resources',
                extra=dict(mesos_framework_name=self.ns.mesos_framework_name))
            return
        log.debug(
            'Mesos has offers available', extra=dict(
                available_offers=len(available_offers),
                max_runnable_tasks=sum(x[1] for x in available_offers),
                mesos_framework_name=self.ns.mesos_framework_name))
        MV, command = self._get_and_update_relay(available_offers)

        if command is None:
            for offer, _ in available_offers:
                driver.declineOffer(offer.id)
            return
        create_tasks(
            MV=abs(MV), available_offers=available_offers,
            driver=driver, command=command, ns=self.ns
        )
        driver.reviveOffers()

    def _get_and_update_relay(self, available_offers):
        """
        Get num tasks I should create and evaluate whether to use Relay's
        warmer or cooler command.  Update the MV with number of commands about
        to be created.

        Competes for the MV with these other threads, and will wait
        indefinitely for it:

          - other Mesos resourceOffers(...) calls to the Framework scheduler
          - Relay warmer and cooler functions attempting to ask the Framework
            to execute more tasks.
        """
        command = None
        with self.MV.get_lock():
            MV, t = self.MV
            # create tasks that fulfill relay's requests or return
            if MV == 0:
                log.debug(
                    'mesos scheduler has received no requests from relay',
                    extra=dict(
                        mesos_framework_name=self.ns.mesos_framework_name))
            else:
                if MV > 0 and self.ns.warmer:
                    command = self.ns.warmer
                elif MV < 0 and self.ns.cooler:
                    command = self.ns.cooler
                if abs(MV) < len(available_offers):
                    self.MV[:] = [0, time.time()]
                else:
                    new_MV = MV - (MV > 0 or -1) * max(abs(MV),
                                                       len(available_offers))
                    self.MV[:] = [new_MV, time.time()]
        return (MV, command)

    def statusUpdate(self, driver, update):
        catch(self._statusUpdate, self.exception_sender)(driver, update)

    def _statusUpdate(self, driver, update):
        log.debug('task status update: %s' % str(update.message), extra=dict(
            task_id=update.task_id.value, task_state=update.state,
            slave_id=update.slave_id.value, timestamp=update.timestamp,
            mesos_framework_name=self.ns.mesos_framework_name))
        if self.ns.max_failures == -1:
            return  # don't quit even if you are getting failures

        m = mesos_pb2
        if update.state in [m.TASK_FAILED, m.TASK_LOST]:
            self.failures += 1
        elif update.state in [m.TASK_FINISHED, m.TASK_STARTING]:
            self.failures = max(self.failures - 1, 0)
        if self.failures >= self.ns.max_failures:
            log.error(
                "Max allowable number of failures reached", extra=dict(
                    max_failures=self.failures,
                    mesos_framework_name=self.ns.mesos_framework_name))
            driver.stop()
            raise MaxFailuresReached(self.failures)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when a slave has been determined unreachable (e.g.,
        machine failure, network partition) Most frameworks will need to
        reschedule any tasks launched on this slave on a new slave.
        """
        pass  # Relay will recover

    def offerRescinded(self, driver, offerId):
        """
        Invoked when the status of a task has changed (e.g., a slave is
        lost and so the task is lost, a task finishes and an executor
        sends a status update saying so, etc). Note that returning from
        this callback _acknowledges_ receipt of this status update! If
        for whatever reason the scheduler aborts during this callback (or
        the process exits) another status update will be delivered (note,
        however, that this is currently not true if the slave sending the
        status update is lost/fails during that time).
        """
        log.debug('offer rescinded', extra=dict(
            offer_id=offerId.value,
            mesos_framework_name=self.ns.mesos_framework_name))
