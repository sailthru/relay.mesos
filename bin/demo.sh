#!/usr/bin/env bash

# install boot2docker
# https://docs.docker.com/installation/mac/
# ** don't forget to add env vars to your .profile **

# also, you can add "<IP> docker" to /etc/hosts to make web browser work
# for the mesos webui: http://docker:5050
# and the relay webui: http://docker:8080  (this on in prog. TODO)


dir="$( cd "$( dirname "$( dirname "$0" )" )" && pwd )"

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
    # docker run --name mesos_slave_$n -v $dir/relay_mesos:/mnt/relay_mesos --link zookeeper:ZK --link mesos_master:MASTER -e MESOS_LOG_DIR=/var/log -e MESOS_WORK_DIR=/tmp/mesoswork -t -i redjack/mesos-slave bash -c 'ZK_MESOS=$ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT/ MESOS_MASTER=$MASTER_PORT_5050_TCP_ADDR:$MASTER_PORT_5050_TCP_PORT/ mesos-slave'
    # TODO: dont use a hacked docker image. use containers
    # for now, hack use a docker build that is the relay.mesos build + mesos slave build + slaves support docker containers
    docker run -d --name mesos_slave_$n -v $dir/relay_mesos:/mnt/relay_mesos --link zookeeper:ZK --link mesos_master:MASTER -e MESOS_LOG_DIR=/var/log -e MESOS_WORK_DIR=/tmp/mesoswork -t -i mesos_slave_11 bash -c 'ZK_MESOS=$ZK_PORT_2181_TCP_ADDR:$ZK_PORT_2181_TCP_PORT/ MESOS_MASTER=$MASTER_PORT_5050_TCP_ADDR:$MASTER_PORT_5050_TCP_PORT/ mesos-slave  -–containerizers=docker,mesos'
  done
fi

docker build -t relay.mesos .

docker run -d --name relay.mesos --link mesos_master:MASTER -t -i relay.mesos bash -c 'MESOS_MASTER=$MASTER_PORT_5050_TCP_ADDR:$MASTER_PORT_5050_TCP_PORT/ relay.mesos -w bash_echo_warmer -m bash_echo_metric -d .1 --sendstats tcp://192.168.59.3:5498 -t 60 --task_resources cpus=1 mem=10 --lookback 300'

( (sleep 10 ;echo relay.mesos demo quiting ; docker rm -f relay.mesos) & )

# kill this after a little while
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
