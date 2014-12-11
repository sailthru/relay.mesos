Relay.Mesos:  Run Relay and Mesos
==========

In short, Relay.Mesos runs Relay as a Mesos framework.  By combining
both of these tools, we can solve control loop problems that arise in
distributed systems.

What is Relay?
----------
Relay is "a thermostat for distributed systems."  It is a tool that
attempts to make a metric timeseries as similar to a target
as possible, and it works like thermostat does for temperature.

[Details on Relay's Github page.](
https://github.com/sailthru/relay/blob/master/README.md)

What is Mesos?
----------
Apache Mesos is "a distributed systems kernel."  It pools resources from
networked machines and then provides a platform that executes code
over those resources.  It's basically a bin-packing scheduler that identifies
which resources are available and then provides ways to use those resources.

[Details on Mesos's landing page.](http://mesos.apache.org/)

[White paper about Mesos (this is good
reading)](http://mesos.berkeley.edu/mesos_tech_report.pdf)


Quickstart
==========

1. Install Docker
    - https://docs.docker.com/installation
    - (if on a mac, you may need boot2docker and don't forget to add env vars to your .profile)
    - (if on ubuntu, you may need 3.16 kernel or later)

1. Identify docker in /etc/hosts to make web browsers work:

        # my boot2docker ip is given by: `boot2docker ip` or $DOCKER_HOST
        # I added this to my /etc/hosts file:
            192.168.59.103 localdocker

1. Run the demo script.
    - When you run this for the first time, docker may need to download a
      lot of the required images to get mesos running on your computer

            # ./bin/demo.sh     # run the demo
            # ./bin/demo.sh N   # run the demo with N mesos slaves  (N=1 is plenty)
            # ./bin/demo.sh -1  # remove all docker containers used in this demo

1. To see relay.mesos in action, navigate your browser to:

        http://localdocker:8080  # relay UI
        http://localdocker:5050  # mesos UI


Background
==========

Relay.Mesos is made up of two primary components: a Mesos framework and
a Relay event loop.  Relay continuously requests that the mesos
framework run a number of tasks.  The framework receives resource
offers from mesos and, if any Relay requests can be fulfilled, it will
attempt to fulfill them.  If Relay requests can't be fulfilled because
Mesos cluster is full, then the next time Relay.Mesos receives mesos resource
offers, it will attempt to fulfill the largest Relay request since
the last set of mesos resource offers was fulfilled.  In the current
implementation, if no mesos resource offers are available for a long
time, this can result in Relay.Mesos building up a history of error.  In
this case, Relay.Mesos may over-react the moment mesos offers become available
again, but it will eventually stabilize.

In Relay.Mesos, as with Relay generally, there are 4 main components:
metric, target, warmer and cooler.

The ```metric``` and ```target``` are both python generator functions (ie timeseries), that, when called,
each yield a number.  The ```metric``` is a signal that we're monitoring
and manipulating.  The ```target``` represents a desired value that
Relay attempts to make the ```metric``` mirror as closely as possible.

The ```warmer``` and ```cooler``` are expected to (eventually) modify
the metric.  Executing a ```warmer``` will increase the metric.
Executing a ```cooler``` will decrease the metric.  In Relay.Mesos, the
```warmer``` and ```cooler``` are bash commands.  These may be executed in
your custom docker container, if you wish.


Examples:
----------

Relay.Mesos can ensure that the number of jobs running at any given
time is enough to consume a queue.

    Metric = queue size
    Target = 0
    Warmer = "./start-worker.sh"
    (Cooler would not be defined)

Relay.Mesos can schedule the number of consumers or servers running at a
particular time of day

    Metric = number of consumers
    Target = max_consumers * is_night  # this could work too: sin(time_of_day) * max_consumers
    Warmer = "./start-consumer.sh"
    (Cooler would not be defined)

Relay.Mesos can attempt to maintain a desired amount of cpu usage

    Metric = cpu_used - expected_cpu_used
    Target = 0
    Cooler = "run a bash command that uses the cpu"
    (Warmer not defined)
