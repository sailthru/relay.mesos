import logging
log = logging.getLogger('relay.mesos')

# expose configure_logging to those who wish to develop relay
from relay import configure_logging

from relay import log as _relay_runner_log
_parent_log = logging.getLogger('relay')
_relay_runner_log.propagate = True
log.propagate = True
configure_logging(True, log=_parent_log)  # parent_log will not propagate

import pkg_resources as _pkg_resources
__version__ = _pkg_resources.get_distribution('relay.mesos').version
