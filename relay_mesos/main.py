"""
Bootstrap the execution of Relay but first do the things necessary to setup
Relay as a Mesos Framework
"""

from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap


def main(ns):
    return relay_main(ns)


build_arg_parser = at.build_arg_parser([
    at.warmer(default='relay_mesos.warmer'),
    at.cooler(default='relay_mesos.cooler'),
    at.group(
        "Relay.Mesos specific parameters",
        at.add_argument('--abc')

    ), ],
    description="Convert your Relay app into a Mesos Framework",
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
