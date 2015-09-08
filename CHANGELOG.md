Differences between major versions of Relay.Mesos
=================================================


2.1 (unreleased)
----------------

- Basic support for Mesos Attributes (ie similar to Marathon Constraints)
- Docker-compose: use latest mesos from mesosphere, rather than breerly.


2.0 (2015-07-26)
----------------

####Backwards Incompatible
- Removes interpolation of environment variables into the bash command by python.

####New Features
- Docker: --docker_parameters for asking mesos to pass params to `docker run`
- --mesos_checkpoint option to enable framework-level checkpointing
- Adds ability to define environment variables into warmer and cooler tasks
- Docker: adds --net BRIDGE|HOST|NONE option
- Support for defining --uris
- Docker: --force_pull_image

####Bugs
- relay.mesos fails if --uris and --volumes are not defined


1.0 (2015-06-05)
----------------

####Backwards Incompatible
- RELAY_DOCKER_IMAGE and RELAY_TASKS_RESOURCES are now RELAY_MESOS_DOCKER_IMAGE and RELAY_MESOS_TASK...

####New Features / Bugs
- Docker: support for --volumes
- Demo: Uses docker-compose (latest version from github) to run demo
- Solves some timing issues between relay and mesos event loops
- Solves a terrible MV calculation bug that causes Relay.Mesos to diverge in some cases
