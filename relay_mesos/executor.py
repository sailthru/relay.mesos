#!/usr/bin/env python

# TODO: write my own
import sys
import subprocess
import threading

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


class RelayMesosExecutor(mesos.interface.Executor):
    def __init__(self, cmd='echo TODO'):  # TODO: remove ='echo TODO'
        self.cmd = cmd  # the bash command that is a warmer or cooler
        # TODO: consider supporting pickled functions

    def launchTask(self, driver, task):
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = 'some data? what is this TODO'  # TODO
            driver.sendStatusUpdate(update)

            # This is where one would perform the requested task.
            retcode = subprocess.call(self.cmd, shell=True)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.data = 'some data? what is this TODO'  # TODO
            if retcode == 0:
                update.state = mesos_pb2.TASK_FINISHED
            else:
                update.state = mesos_pb2.TASK_FAILED
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)


if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.native.MesosExecutorDriver(RelayMesosExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
