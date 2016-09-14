import atexit
import multiprocessing as mp
import threading
import json
import signal
import sys
import time

import argparse_tools as at
from argparse_tools import add_argument_default_from_env_factory
from relay_mesos import log
from relay_mesos.util import catch
from relay_mesos.scheduler import Scheduler
from mesos.interface import mesos_pb2
import stolos.api


class TimeoutError(Exception):
    pass


def set_signals(relay, ns):
    """Kill child processes on sigint or sigterm"""
    def kill_children(signal, frame):
        log.error(
            'Received a signal that is trying to terminate this process.'
            ' Terminating relay child process!', extra=dict(
                mesos_framework_name=ns.mesos_framework_name,
                signal=signal))
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

    # In order to support failover we need to keep track of the framework_id
    # of previous instances of this framework. The framework_id is stored using
    # the stolos queue backend. If it's set, assume we're recovering from a
    # failover
    framework_id_path = "relay_mesos.framework.%s" % ns.mesos_framework_name
    stolos.api.initialize([])
    qb_client = stolos.api.get_qbclient()
    existing_id = None
    if qb_client.exists(framework_id_path):
        existing_id = qb_client.get(framework_id_path)
        log.info("Found existing framework_id: %s" % existing_id)

    # a distributed value storing the num and type of tasks mesos scheduler
    # should create at any given moment in time.
    # Sign of MV determines task type: warmer or cooler
    # ie. A positive value of n means n warmer tasks
    MV = mp.Array('d', [0, 0])  # max_val is a ctypes.c_int64

    # store exceptions that may be raised
    exception_receiver, exception_sender = mp.Pipe(False)
    # notify relay when mesos framework is ready
    mesos_ready = threading.Event()
    relay_ready = mp.Event()

    # copy and then override warmer and cooler
    ns_relay = ns.__class__(**{k: v for k, v in ns.__dict__.items()})

    mesos_scheduler, mesos_driver = init_mesos_driver(ns=ns,
                                     MV=MV,
                                     exception_sender=exception_sender,
                                     mesos_ready=mesos_ready,
                                     framework_id=existing_id)
    mesos_name = "Relay.Mesos Scheduler"
    mesos = threading.Thread(
        target=catch(init_mesos_scheduler, exception_sender),
        kwargs=dict(driver=mesos_driver),
        name=mesos_name)
    relay_name = "Relay.Runner Event Loop"
    relay = mp.Process(
        target=catch(init_relay, exception_sender),
        args=(ns_relay, relay_ready, MV),
        name=relay_name)
    mesos.daemon = True
    mesos.start()  # start mesos framework

    mesos_ready.wait(ns_relay.init_timeout)
    if not mesos_ready.is_set():
        qb_client.delete(framework_id_path)
        raise TimeoutError("Mesos Scheduler took too long to come up!")
    relay.start()  # start relay's loop
    set_signals(relay, ns)

    relay_ready.wait(ns_relay.init_timeout)
    if not relay_ready.is_set():
        relay.terminate()
        raise TimeoutError("Relay took too long to come up!")

    log.info("Saving framework_id %s" % mesos_scheduler.framework_id)
    if existing_id:
        qb_client.set(framework_id_path, mesos_scheduler.framework_id)
    else:
        qb_client.create(framework_id_path, mesos_scheduler.framework_id)

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
        num_jobs = MV[0]
        if num_jobs != 0:
            log.debug("Number of jobs to run %d non-zero, reviving offers" % num_jobs)
            mesos_scheduler.revive_offers()

        # save cpu cycles by checking for subprocess failures less often
        time.sleep(min(5, ns.delay))

    # If we've gotten here, assume the framework has been cleanly shutdown and
    # won't failover
    qb_client.delete(framework_id_path)
    relay.terminate()
    sys.exit(1)


def init_relay(ns_relay, relay_ready, MV):
    import os
    import stolos.api
    app_name = os.environ['APP_NAME']
    stolos.api.initialize([])
    q = stolos.api.get_qbclient().LockingQueue(app_name)
    log.debug('Getting ready to start relay process')
    relay_ready.set()
    while True:
        qsize = q.size(queued=True, taken=True)
        # excluding the running tasks is expensive, so only do it if we know
        # that the number is relatively small. If the number is large, we'll
        # just return the total qsize and hope for the best
        if qsize < 5000:
            qsize = q.size(queued=True, taken=False)
        log.debug("got stolos queue size: %s" % qsize)  # temp testing
        t = time.time()
        with MV.get_lock():
            if MV[1] < t:
                MV[:] = (-qsize, t)
        time.sleep(ns_relay.delay)



def init_mesos_driver(ns, MV, exception_sender, mesos_ready, framework_id):
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
    framework = mesos_pb2.FrameworkInfo(
        checkpoint=ns.mesos_checkpoint,
        user="",  # Have Mesos fill in the current user.
        name="Relay.Mesos: %s" % ns.mesos_framework_name,
        failover_timeout=ns.failover_timeout
    )
    if framework_id:
        framework.id.value = framework_id
    if ns.mesos_framework_principal:
        framework.principal = ns.mesos_framework_principal
    if ns.mesos_framework_role:
        framework.role = ns.mesos_framework_role

    # build driver
    log.debug("Instantiating MesosSchedulerDriver")
    scheduler = Scheduler(MV=MV,
        exception_sender=exception_sender, mesos_ready=mesos_ready,
        ns=ns)
    driver = mesos.native.MesosSchedulerDriver(
        scheduler,
        framework,
        ns.mesos_master)
    atexit.register(driver.stop)
    return scheduler, driver


def init_mesos_scheduler(driver):
    log.debug("Running the instance of MesosSchedulerDriver")
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    driver.stop()  # Ensure that the driver process terminates.
    sys.exit(status)


# This add_argument func will prefix env vars with RELAY_MESOS.
# The normal at.add_argument func prefixes env vars with RELAY_
# Let's use the at.add_argument func for --mesos_XXX and the below for --XXX
add_argument = add_argument_default_from_env_factory(
    env_prefix='RELAY_MESOS_')

relay_add_argument = add_argument_default_from_env_factory(env_prefix='RELAY_')

def argparse_dict_type(inpt_str):
    return dict(
        y.split('=') for y in inpt_str.strip().replace(' ' , ',').split(','))


def argparse_tuple_kv_type(input_str):
    return tuple(tuple(y.split(':')) for y in input_str.split(','))


build_arg_parser = at.build_arg_parser([
   at.group(
        "Relay.Mesos -- General parameters",
        add_argument(
            '--init_timeout', default=20, help=(
                "By default, wait at most N seconds for the Mesos Scheduler"
                " to start up before the Relay scheduler starts.  If timeout"
                " is exceeded, raise an error and exit."
            )),
        relay_add_argument(
            '-d', '--delay', type=float, default=1,
                    help='num seconds to wait between metric polling. ie. 1/sample_rate'
            ),
        relay_add_argument(
            '-c', '--cooler', type=str,
                    help='bash command to run'
            ),
        relay_add_argument(
            '--failover_timeout', type=int, default=60*60*4,
                    help=("Number of seconds the master will wait before"
                          " killing the tasks of a failed scheduler."
                          " The default value is 4 hours")
            ),
    ),
    at.group(
        "Relay.Mesos -- Mesos-specific parameters",
        relay_add_argument(
            '--mesos_master',
            help="URI to mesos master. We support whatever mesos supports"
        ),
        relay_add_argument(
            '--mesos_framework_principal', type=str, help=(
                "If you use Mesos Framework Rate Limiting, this framework's"
                " principal identifies which rate limiting policy to apply")),
        relay_add_argument(
            '--mesos_framework_role', type=str, help=(
                "If you use Mesos Access Control Lists (ACLs) or apply"
                " weighting to frameworks, your framework needs to register"
                " with a role.")),
        relay_add_argument(
            '--mesos_framework_name',
            default='framework',
            help="Name the framework so you can identify it in the Mesos UI"),
        relay_add_argument(
            '--mesos_checkpoint', action='store_true', type=bool, default=False,
            help=(
                "This option enables Mesos Framework checkpointing.  This"
                " means that tasks spun up by Relay.Mesos will survive even if"
                " this Relay.Mesos instance dies.")),
        relay_add_argument(
            '--mesos_task_resources', type=argparse_dict_type,
            default={}, help=(
                "Specify what resources your task needs to execute.  These"
                " can be any recognized mesos resource and must be specified"
                " as a comma separated list.  ie:"
                "  --mesos_task_resources cpus=10,mem=30000"
            )),
        relay_add_argument(
            '--mesos_environment', type=lambda fp: [
                tuple(y.strip() for y in x.strip().split('=', 1))
                for x in open(fp).readlines()],
            default=[], help=(
                "A filepath containing environment variables to define on all"
                " warmer and cooler tasks."
                "File should contain one variable per line, in form VAR1=VAL1"
            )),
        relay_add_argument(
            '--mesos_attribute_matches_all', type=argparse_dict_type,
            help=(
                "Only spin up tasks on mesos slaves whose attributes match the"
                " given criteria.  Pass as a comma separated list.  ie:"
                " --mesos_attribute_matches_all host=my.host,flag=red"
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
        "Relay.Mesos -- Docker parameters",
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
            type=argparse_tuple_kv_type,
            default=[], help=(
                "If using containers, you may wish to mount volumes into those"
                " containers.  Define the volumnes you wish to mount as"
                " a comma-separated list of volumes with the"
                " following format:"
                "  --mesos_volumes host_path:container_path:mode,"
                "host_path2:container_path2:mode,...")),
    )
],
    description=(
        "Use Relay to auto-scale instances of a bash command"
        " or docker container on Mesos"))


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
