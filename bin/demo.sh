#!/usr/bin/env bash

# install boot2docker
# https://docs.docker.com/installation/mac/
# ** don't forget to add env vars to your .profile **

# also, you can add "<IP> docker" to /etc/hosts to make web browser work
# for the url: http://docker:5050


# (dangerously) remove previous containers in case they exist
docker rm -f relay.mesos
if [ "$2" = "go" ] ; then
  docker rm -f zookeeper
  docker rm -f mesos_master
  for n in `seq 1 ${1:-1}` ; do
    docker rm -f mesos_slave_${n}
  done

  # start zookeeper
  docker run -d --name zookeeper -d -p 2181:2181 -p 2888:2888 -p 3888:3888 jplock/zookeeper:3.4.6

  # start mesos master
  docker run -d --name mesos_master --link zookeeper:ZK -e MESOS_LOG_DIR=/var/log -e MESOS_WORK_DIR=/tmp/mesoswork -e MESOS_QUORUM=1 -p 5050:5050 -t -i redjack/mesos-master bash -c 'ZK_MESOS=$ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT/ mesos-master'

  # start mesos slave
  for n in `seq 1 ${1:-1}` ; do
    docker run -d --name mesos_slave_$n --link zookeeper:ZK --link mesos_master:MASTER -e MESOS_LOG_DIR=/var/log -e MESOS_WORK_DIR=/tmp/mesoswork -t -i redjack/mesos-slave bash -c 'ZK_MESOS=$ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT/ MESOS_MASTER=$MASTER_PORT_5050_TCP_ADDR:$MASTER_PORT_5050_TCP_PORT/ mesos-slave'
  done
fi

docker build -t relay.mesos .
docker run -d --name relay.mesos --link mesos_master:MASTER -t -i relay.mesos bash -c 'MESOS_MASTER=$MASTER_PORT_5050_TCP_ADDR:$MASTER_PORT_5050_TCP_PORT/ relay.mesos -w bash_echo_warmer -m bash_echo_metric -d .1 --sendstats tcp://192.168.59.3:5498 -t 60 --task_resources cpus=0.1 --lookback 300'

docker logs relay.mesos
echo docker is running?  `docker inspect -f {{.State.Running}} relay.mesos`
sleep 1
docker logs -f relay.mesos


# helpful
# docker ps
# docker kill $(docker ps -a -q)
# docker restart zookeeper
# docker restart mesos_master
# docker restart mesos_slave_1
# docker rm -f zookeeper
# docker rm -f mesos_master
# docker rm -f mesos_slave_1
