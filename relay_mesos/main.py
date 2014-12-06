"""
Bootstrap the execution of Relay but first do the things necessary to setup
Relay as a Mesos Framework
"""
import atexit
import multiprocessing as mp
import os
import sys

from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap
from relay_mesos import log
from relay_mesos.util import catch
from relay_mesos.scheduler import Scheduler


def warmer_cooler_wrapper(MV):
    """
    Act as a warmer or cooler function such that, instead of executing code,
    we ask mesos to execute it.
    """
    def _warmer_cooler_wrapper(n):
        # inform mesos that it should spin up n tasks of type f, where f is
        # either the warmer or cooler.  Since Relay assumes that the choice of
        # `f` (either a warmer or cooler func) is determined by the sign of n,
        # we can too!
        log.debug('asking mesos to spawn tasks')
        with MV.get_lock():
            if abs(MV.value) < abs(n):
                MV.value = n
        log.debug('...finished asking mesos to spawn tasks')
    return _warmer_cooler_wrapper


def main(ns):
    if ns.mesos_master is None:
        log.error("Oops!  You didn't define --mesos_master")
        build_arg_parser().print_usage()
        sys.exit(1)
    log.info(
        "Starting Relay Mesos!",
        extra={k: str(v) for k, v in ns.__dict__.items()})

    # a distributed value storing the num and type of tasks mesos scheduler
    # should create at any given moment in time.
    # Sign of MV determines task type: warmer or cooler
    # ie. A positive value of n means n warmer tasks
    MV = mp.Value('l')  # max_val is a ctypes.c_int64

    # store exceptions that may be raised
    exception_receiver, exception_sender = mp.Pipe(False)

    # copy and then override warmer and cooler
    warmer, cooler = ns.warmer, ns.cooler
    ns.warmer = warmer_cooler_wrapper(MV)
    ns.cooler = warmer_cooler_wrapper(MV)

    mesos_name = "Relay.Mesos Scheduler"
    mesos = mp.Process(
        target=catch(init_mesos_scheduler, exception_sender),
        kwargs=dict(ns=ns, MV=MV, exception_sender=exception_sender,
                    warmer=warmer, cooler=cooler),
        name=mesos_name)
    relay_name = "Relay.Runner Event Loop"
    relay = mp.Process(
        target=catch(relay_main, exception_sender),
        args=(ns,),
        name=relay_name)
    mesos.start()  # start mesos framework
    relay.start()  # start relay's loop
    err = exception_receiver.recv()
    if err:
        log.error(
            'Terminating child processes because one of them raised'
            ' an exception')
        relay.terminate()
        mesos.terminate()
        sys.exit(1)
    # the threads bounce control back and forth between mesos resourceOffers
    # and Relay's warmer/cooler functions.
    # Relay requests resources, but only the resource with largest magnitude
    # is fulfilled the moment mesos resources are available.


def init_mesos_scheduler(ns, MV, exception_sender, warmer, cooler):
    import mesos.interface
    from mesos.interface import mesos_pb2
    try:
        import mesos.native
    except ImportError:
        log.error(
            "Oops! Mesos native bindings are not installed.  You can download"
            " these binaries from mesosphere.")
        raise

    log.info('starting mesos scheduler')

    # build framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Relay.Mesos Framework"
    framework.principal = "test-framework-python"  # TODO: what is this?

    # build driver
    driver = mesos.native.MesosSchedulerDriver(
        Scheduler(
            MV=MV, task_resources=dict(ns.task_resources),
            exception_sender=exception_sender, warmer=warmer, cooler=cooler),
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
        at.add_argument(
            '--mesos_master', default=os.getenv('RELAY_MESOS_MASTER')),
        at.add_argument(
            '--task_resources', type=lambda x: x.split('='), nargs='*',
            default=os.getenv('RELAY_TASK_RESOURCES', {})),
    ),
    at.warmer(type=str, help=(
        "A bash command to run on a mesos slave."
        " A warmer should eventually increase metric values.")),
    at.cooler(type=str, help=(
        "A bash command to run on a mesos slave."
        " A cooler should eventually decrease metric values.")),
],
    description="Convert your Relay app into a Mesos Framework",
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
