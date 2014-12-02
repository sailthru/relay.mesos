"""
Bootstrap the execution of Relay but first do the things necessary to setup
Relay as a Mesos Framework
"""
import atexit
import os
import sys
import threading
import zmq

from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap
from relay_mesos import log
from relay_mesos.scheduler import Scheduler


def wc_wrapper_factory(f, mesos_channel):
    """
    Wrap a warmer or cooler function such that, just before executing it, we
    wait for mesos offers to ensure that the tasks can be created.
    """
    def warmer_cooler_wrapper(n):
        # inform mesos that it should spin up n tasks of type f, where f is
        # either the warmer or cooler.
        # TODO n_fulfilled is not necessary at the moment
        log.debug('waiting on mesos to spawn tasks')
        mesos_channel.send_pyobj((n, f))
        n_fulfilled = mesos_channel.recv_pyobj()
        log.debug('mesos spawned tasks')
        return n_fulfilled

    if f is None:
        return
    else:
        return warmer_cooler_wrapper


def make_req_rep():
    context = zmq.Context()
    req = context.socket(zmq.REQ)
    rep = context.socket(zmq.REP)
    rep.bind('inproc://relay.mesos')
    req.connect('inproc://relay.mesos')
    return req, rep


def main(ns):
    if ns.mesos_master is None:
        log.error("Oops!  You didn't define --mesos_master")
        build_arg_parser().print_usage()
        sys.exit(1)
    log.info(
        "Starting Relay Mesos!",
        extra={k: str(v) for k, v in ns.__dict__.items()})

    req, rep = make_req_rep()

    # override warmer and cooler
    ns.warmer = wc_wrapper_factory(ns.warmer, mesos_channel=req)
    ns.cooler = wc_wrapper_factory(ns.cooler, mesos_channel=req)

    mesos = threading.Thread(
        target=init_mesos_scheduler,
        kwargs=dict(ns=ns, relay_channel=rep),
        name="Relay.Mesos Scheduler")
    relay = threading.Thread(
        target=relay_main, args=(ns,), name="Relay.Runner Event Loop")
    mesos.start()  # start mesos framework
    relay.start()  # start relay's loop

    # the threads bounce control back and forth between mesos resourceOffers
    # and Relay's warmer/cooler functions using zmq sockets.  Relay
    # blocks until mesos resources are available.


def init_mesos_scheduler(ns, relay_channel):
    import mesos.interface
    from mesos.interface import mesos_pb2

    # build executor
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "Relay Executor"
    executor.command.value = "python -m relay_mesos.executor"
    executor.name = "Relay.Mesos executor"
    executor.source = "relay_test"

    # build framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Relay.Mesos Test Framework"
    framework.principal = "test-framework-python"

    # build driver
    driver = mesos.interface.SchedulerDriver(
        Scheduler(executor, relay_channel, dict(ns.task_resources)),
        framework,
        ns.mesos_master)
    atexit.register(driver.stop)

    # run things
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    driver.stop()  # Ensure that the driver process terminates.
    sys.exit(status)


build_arg_parser = at.build_arg_parser([
    at.group(
        "Relay.Mesos specific parameters",
        at.add_argument('--mesos_master', default=os.getenv('MESOS_MASTER')),
        at.add_argument(
            '--task_resources', type=lambda x: x.split('='), nargs='*',
            default={}),
    )
],
    description="Convert your Relay app into a Mesos Framework",
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
