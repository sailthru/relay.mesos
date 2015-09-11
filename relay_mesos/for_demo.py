from __future__ import unicode_literals
import json
import os
import urllib2
from . import log


def num_active_mesos_tasks():
    """
    An example metric used by the relay.mesos demo to query mesos master
    for the number of currently running tasks.
    """
    while True:
        try:
            data = json.load(urllib2.urlopen(
                os.environ['RELAY_MESOS_MASTER_STATE_FOR_DEMO']))
            yield [len(x['tasks']) for x in data['frameworks']
                if x['name'] == 'Relay.Mesos: Demo Framework'][0]
        except:
            log.critical("Demo broken.  Could not access Mesos Master API")
            continue


def target_value():
    """
    An example target used by the relay.mesos demo to set the target number of
    currently running tasks at a given point in time
    """
    while True:
        yield 40  # you could have any arbitrary logic you wish here...
