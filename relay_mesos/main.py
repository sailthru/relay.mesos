"""
Bootstrap the execution of Relay but first do the things necessary to setup
Relay as a Mesos Framework
"""
from greenlet import greenlet, getcurrent
from relay import argparse_shared as at
from relay.runner import main as relay_main, build_arg_parser as relay_ap
from relay_mesos import log


def wc_wrapper_factory(f):
    """
    Wrap a warmer or cooler function such that, just before executing it, we
    wait for mesos offers to ensure that the tasks can be created.
    """
    def warmer_cooler_wrapper(n):
        # inform mesos that it should spin up n tasks of type f, where f is
        # either the warmer or cooler.
        # mutable['n_mesos_offers'] = mesosloop.switch(n, f)
        # TODO n_mesos_offers is not necessary at the moment

        # switch called from Relay
        getcurrent().parent.switch(n, f)
        n_fulfilled = getcurrent().parent.switch(n, f)
        return n_fulfilled

    if f is None:
        return
    else:
        return warmer_cooler_wrapper


def main(ns):
    # override warmer and cooler
    ns.warmer = wc_wrapper_factory(ns.warmer)
    ns.cooler = wc_wrapper_factory(ns.cooler)

    mesosloop = greenlet(init_mesos_scheduler)
    relayloop = greenlet(relay_main, parent=mesosloop)

    mesosloop.switch(relayloop)  # start mesos framework

    relayloop.switch(ns)  # start Relay. greenlets bounce control back and
    # forth between mesos resourceOffers and Relay's warmer/cooler functions.


def init_mesos_scheduler(relayloop):
    # TODO: this function should start a proper mesos scheduler.  get rid of
    # while loop and everything under it goes into the resourceOffers method.

    # TODO: why does this approach not cause relay to freak out?  I think
    # because relay blocks rather than build up an unnecessary error history
    # that it would otherwise create if MVs didn't actually happen for some
    # period of time.
    log.info('Initializing Mesos Scheduler')
    getcurrent().parent.switch()  # return context to main
    n = 0  # debug

    while True:  # assume this func is the resourceOffers that come in
        n+=1  # debug
        import random

        navailable = int(random.choice([0]*2+ [3] * 3 + [10]))
        # navailable = 15  # lets say I figured this out in resourceOffers

        # wait on anything to use these available offers
        # resume control in calling context (presumably relay)
        log.warn(
            'Mesos has offers available',
            extra=dict(available_offers=navailable))

        nrequests, func = relayloop.switch()
        if n % 1000 > 500:
            print 'skip'
            continue
        if func and navailable > 0:
            n = min(navailable, nrequests)
            func(n)
            relayloop.switch(n)
        else:
            relayloop.switch(None)


        log.warn(
            'Relay requested some offers', extra=dict(
                requested_offers=nrequests, func=func and func.func_name))
        # if nrequests == 0:
            # decline offers
        # else:
            # accept nrequests offers and start tasks


build_arg_parser = at.build_arg_parser([
    at.group(
        "Relay.Mesos specific parameters",
        at.add_argument('--abc')),
],
    description="Convert your Relay app into a Mesos Framework",
    parents=[relay_ap()], conflict_handler='resolve')


if __name__ == '__main__':
    NS = build_arg_parser().parse_args()
    main(NS)
