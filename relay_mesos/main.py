"""
Bootstrap the execution of Relay but first do the things necessary to setup
Relay as a Mesos Framework
"""

from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap


def mesos_offers_available():
    """
    Return num mesos offers available
    """
    raise NotImplementedError()
    return 0


def base_wrapper(metric, target, warmer, cooler):
    """
    This function lets us peek at the PV and SP values before Relay does.
    """
    mutable = {}

    def metric_wrapper():
        """
        set mutable['PV'] and before Relay gets to see it
        """
        gen = metric()
        while True:
            mutable['PV'] = next(gen)
            yield mutable['PV']

    def wc_wrapper_factory(f):
        """
        set mutable['MV'] to a (+) for warmer and (-) for cooler
        before executing the warmer/cooler
        """
        def warmer_cooler_wrapper(n):
            mutable['MV'] = n
            return f(n)
        return warmer_cooler_wrapper

    def target_wrapper():
        """If the mesos scheduler is at capacity, we trick Relay into
        believing that there is nothing to do by artificially setting SP == PV.
        When the scheduler receives offers again, Relay can resume.  We do
        this to guarantee that errors due to resource contraints don't accrue
        in Relay's history.  The assumption is that:
            1) Mesos cluster inavailability has nothing to do with the metric
            we're monitoring
            2) maintining a repeated history of Mesos-induced error over time
            will cause Relay to respond to noise and overreact
        """
        gen = target()
        while True:
            SP = next(gen)
            n = mesos_offers_available()
            MV = mutable['MV']
            # TODO: THIS ASSUMES the metric is instantly updated, but metrics
            # by definition are eventually consistent!  Maybe all this isn't
            # going to pan out as well as I hope
            if n < abs(MV):  # if less resources available than requested
                # make the next SP artificially close to PV (error free)
                unmet_requests = abs(MV) - n
                yield mutable['PV'] + unmet_requests * (abs(MV) == MV)
                # err = (SP - PV)
            else:  # otherwise be normal
                yield SP

    return (
        metric_wrapper, target_wrapper,
        wc_wrapper_factory(warmer), wc_wrapper_factory(cooler)
    )


def main(ns):
    ns.metric, ns.target, ns.warmer, ns.cooler = base_wrapper(
        ns.metric, ns.target, ns.warmer, ns.cooler)
    # TODO: mesos scheduler should *discard* any MV requests that it can't fill
    return relay_main(ns)


build_arg_parser = at.build_arg_parser([
    at.warmer(default='relay_mesos.warmer'),
    at.cooler(default='relay_mesos.cooler'),
    at.group(
        "Relay.Mesos specific parameters",
        at.add_argument('--abc')),
],
    description="Convert your Relay app into a Mesos Framework",
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
