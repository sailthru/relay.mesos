import atexit
import multiprocessing as mp
import json
import signal
import sys
import time

from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap
from relay_mesos import log
from relay_mesos.util import catch
from relay_mesos.scheduler import Scheduler


def warmer_cooler_wrapper(MV, ns):
    """
    Act as a warmer or cooler function such that, instead of executing code,
    we ask mesos to execute it.
    """
    def _warmer_cooler_wrapper(n):
        # inform mesos that it should spin up n tasks of type f, where f is
        # either the warmer or cooler.  Since Relay assumes that the choice of
        # `f` (either a warmer or cooler func) is determined by the sign of n,
        # we can too!
        log.debug(
            'asking mesos to spawn tasks',
            extra=dict(
                mesos_framework_name=ns.mesos_framework_name,
                task_num=n, task_type="warmer" if n > 0 else "cooler"))
        t = time.time()
        with MV.get_lock():
            if MV[1] < t:
                MV[:] = (n, t)
        log.debug(
            '...finished asking mesos to spawn tasks',
            extra=dict(
                mesos_framework_name=ns.mesos_framework_name,
                task_num=n, task_type="warmer" if n > 0 else "cooler"))
    return _warmer_cooler_wrapper


def set_signals(mesos, relay, ns):
    """Kill child processes on sigint or sigterm"""
    def kill_children(signal, frame):
        log.error(
            'Received a signal that is trying to terminate this process.'
            ' Terminating mesos and relay child processes!', extra=dict(
                mesos_framework_name=ns.mesos_framework_name,
                signal=signal))
        try:
            mesos.terminate()
            log.info(
                'terminated mesos scheduler',
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
        except:
            log.exception(
                'could not terminate mesos scheduler',
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
        try:
            relay.terminate()
            log.info(
                'terminated relay',
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
        except:
            log.exception(
                'could not terminate relay',
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
        sys.exit(1)
    signal.signal(signal.SIGTERM, kill_children)
    signal.signal(signal.SIGINT, kill_children)


def main(ns):
    """
    Run Relay as a Mesos framework.
    Relay's event loop and the Mesos scheduler each run in separate processes
    and communicate through a multiprocessing.Pipe.

    These two processes bounce control back and forth between mesos
    resourceOffers and Relay's warmer/cooler functions.  Relay warmer/cooler
    functions request that mesos tasks get spun up, but those requests are only
    filled if the mesos scheduler receives enough relevant offers.  Relay's
    requests don't build up: only the largest request since the last fulfilled
    request is fulfilled at moment enough mesos resources are available.
    """
    if ns.mesos_master is None:
        log.error(
            "Oops!  You didn't define --mesos_master",
            extra=dict(mesos_framework_name=ns.mesos_framework_name))
        build_arg_parser().print_usage()
        sys.exit(1)
    if not ns.mesos_task_resources:
        log.warn(
            "You didn't define '--mesos_task_resources'."
            "  Tasks may not start on slaves",
            extra=dict(mesos_framework_name=ns.mesos_framework_name))
    log.info(
        "Starting Relay Mesos!",
        extra={k: str(v) for k, v in ns.__dict__.items()})

    # a distributed value storing the num and type of tasks mesos scheduler
    # should create at any given moment in time.
    # Sign of MV determines task type: warmer or cooler
    # ie. A positive value of n means n warmer tasks
    MV = mp.Array('d', [0, 0])  # max_val is a ctypes.c_int64

    # store exceptions that may be raised
    exception_receiver, exception_sender = mp.Pipe(False)
    # notify relay when mesos framework is ready
    mesos_ready = mp.Condition()

    # copy and then override warmer and cooler
    ns_relay = ns.__class__(**{k: v for k, v in ns.__dict__.items()})
    if ns.warmer:
        ns_relay.warmer = warmer_cooler_wrapper(MV, ns)
    if ns.cooler:
        ns_relay.cooler = warmer_cooler_wrapper(MV, ns)

    mesos_name = "Relay.Mesos Scheduler"
    mesos = mp.Process(
        target=catch(init_mesos_scheduler, exception_sender),
        kwargs=dict(ns=ns, MV=MV, exception_sender=exception_sender,
                    mesos_ready=mesos_ready),
        name=mesos_name)
    relay_name = "Relay.Runner Event Loop"
    relay = mp.Process(
        target=catch(init_relay, exception_sender),
        args=(ns_relay, mesos_ready, ns.mesos_framework_name),
        name=relay_name)
    mesos.start()  # start mesos framework
    relay.start()  # start relay's loop
    set_signals(mesos, relay, ns)

    while True:
        if exception_receiver.poll():
            exception_receiver.recv()
            log.error(
                'Terminating child processes because one of them raised'
                ' an exception', extra=dict(
                    is_relay_alive=relay.is_alive(),
                    is_mesos_alive=mesos.is_alive(),
                    mesos_framework_name=ns.mesos_framework_name))
            break
        if not relay.is_alive():
            log.error(
                "Relay died.  Check logs to see why.",
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
            break
        if not mesos.is_alive():
            log.error(
                "Mesos Scheduler died and didn't notify me of its exception."
                "  This may be a code bug.  Check logs.",
                extra=dict(mesos_framework_name=ns.mesos_framework_name))
            break
        # save cpu cycles by checking for subprocess failures less often
        if ns.delay > 5:
            time.sleep(5)
        else:
            time.sleep(ns.delay)

    relay.terminate()
    mesos.terminate()
    sys.exit(1)


def init_relay(ns_relay, mesos_ready, mesos_framework_name):
    log.debug(
        'Relay waiting to start until mesos framework is registered',
        extra=dict(mesos_framework_name=mesos_framework_name))
    mesos_ready.acquire()
    mesos_ready.wait()
    log.debug(
        'Relay notified that mesos framework is registered',
        extra=dict(mesos_framework_name=mesos_framework_name))
    relay_main(ns_relay)


def init_mesos_scheduler(ns, MV, exception_sender, mesos_ready):
    import mesos.interface
    from mesos.interface import mesos_pb2
    try:
        import mesos.native
    except ImportError:
        log.error(
            "Oops! Mesos native bindings are not installed.  You can download"
            " these binaries from mesosphere.",
            extra=dict(mesos_framework_name=ns.mesos_framework_name))
        raise

    log.info(
        'starting mesos scheduler',
        extra=dict(mesos_framework_name=ns.mesos_framework_name))

    # build framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Relay.Mesos: %s" % ns.mesos_framework_name
    if ns.mesos_framework_principal:
        framework.principal = ns.mesos_framework_principal
    if ns.mesos_framework_role:
        framework.role = ns.mesos_framework_role
    if ns.mesos_checkpoint:
        framework.checkpoint = True

    # build driver
    driver = mesos.native.MesosSchedulerDriver(
        Scheduler(
            MV=MV, exception_sender=exception_sender, mesos_ready=mesos_ready,
            ns=ns),
        framework,
        ns.mesos_master)
    atexit.register(driver.stop)

    # run things
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    driver.stop()  # Ensure that the driver process terminates.
    sys.exit(status)


# This add_argument func will prefix env vars with RELAY_MESOS.
# The normal at.add_argument func prefixes env vars with RELAY_
# Let's use the at.add_argument func for --mesos_XXX and the below for --XXX
add_argument = at.add_argument_default_from_env_factory(
    env_prefix='RELAY_MESOS_')


build_arg_parser = at.build_arg_parser([
    at.group(
        "How does Relay.mesos affect your metric?",
        at.warmer(type=str, help=(
            "A bash command to run on a mesos slave."
            " A warmer should eventually increase metric values.")),
        at.cooler(type=str, help=(
            "A bash command to run on a mesos slave."
            " A cooler should eventually decrease metric values.")),
    ),
    at.group(
        "Relay.Mesos parameters",
        at.add_argument(
            '--mesos_master',
            help="URI to mesos master. We support whatever mesos supports"
        ),
        at.add_argument(
            '--mesos_framework_principal',
            type=str, help=(
                "If you use Mesos Framework Rate Limiting, this framework's"
                " principal identifies which rate limiting policy to apply")),
        at.add_argument(
            '--mesos_framework_role',
            type=str, help=(
                "If you use Mesos Access Control Lists (ACLs) or apply"
                " weighting to frameworks, your framework needs to register"
                " with a role.")),
        at.add_argument(
            '--mesos_framework_name',
            default='framework',
            help="Name the framework so you can identify it in the Mesos UI"),
        at.add_argument(
            '--mesos_checkpoint', action='store_true', type=bool, default=False,
            help=(
                "This option enables Mesos Framework checkpointing.  This"
                " means that tasks spun up by Relay.Mesos will survive even if"
                " this Relay.Mesos instance dies.")),
        at.add_argument(
            '--mesos_task_resources',
            type=lambda x: dict(
                y.split('=') for y in x.replace(' ', ',').split(',')),
            default={}, help=(
                "Specify what resources your task needs to execute.  These"
                " can be any recognized mesos resource and must be specified"
                " as a string or comma separated list.  ie:"
                "  --mesos_task_resources cpus=10,mem=30000"
            )),
        at.add_argument(
            '--mesos_environment', type=lambda fp: [
                tuple(y.strip() for y in x.strip().split('=', 1))
                for x in open(fp).readlines()],
            default=[], help=(
                "A filepath containing environment variables to define on all"
                " warmer and cooler tasks."
                "File should contain one variable per line, in form VAR1=VAL1"
            )),
        add_argument(
            '--uris', type=lambda x: x.split(','), default=[], help=(
                "Comma-separated list of URIs to load before running command"
            )),
        add_argument(
            '--max_failures', type=int, default=-1, help=(
                "If tasks are failing too often, stop the driver and raise"
                " an error.  If given, this (always positive) number"
                " is a running count of (failures - successes - starting)"
                " tasks.  It is sensitive to many consecutive failures and"
                " will mostly ignore failures if a lot of tasks"
                " are starting or completing at once"
            )),
    ),
    at.group(
        "Relay.Mesos Docker parameters",
        add_argument(
            '--docker_parameters', default={}, type=json.loads, help=(
                "Supply arbitrary command-line options for the docker run"
                "command executed by the Mesos containerizer.  Note that any"
                "parameters passed in this manner are not guaranteed to be"
                "supported in the future.  Pass parameters as a JSON dict:\n"
                '  --docker_parameters \'{"volumes-from": "myimage", ...}\''
            )),
        add_argument(
            '--docker_image', help=(
                "The name of a docker image if you wish to execute the"
                " warmer and cooler in it")),
        add_argument(
            '--docker_network', choices=("HOST", "BRIDGE", "NONE"),
            default="BRIDGE", help=(
                "Docker: Set the Network mode for the container: --net ")),
        add_argument(
            '--force_pull_image', action='store_true', default=False,
            type=bool,
            help=(
                "Before Relay.Mesos starts a docker container, ensure that the"
                " container image is the most recently updated in the registry"
            )),
        add_argument(
            '--volumes',
            type=lambda x: tuple(tuple(y.split(':')) for y in x.split(',')),
            default=[], help=(
                "If using containers, you may wish to mount volumes into those"
                " containers.  Define the volumnes you wish to mount as"
                " a comma-separated list of volumes with the"
                " following format:"
                "  --mesos_volumes host_path:container_path:mode,"
                "host_path2:container_path2:mode,...")),
    ),
],
    description=(
        "Use Relay to auto-scale instances of a bash command"
        " or docker container on Mesos"),
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
