#!/usr/bin/env bash

# This script runs zookeeper and mesos inside docker containers.
# It then runs relay.mesos as a mesos framework inside a docker container.


# To see relay.mesos in action, navidate your browser to:
#     http://docker:8080  # relay.mesos
#     http://docker:5050  # mesos

# install boot2docker
# https://docs.docker.com/installation/mac/
# ** don't forget to add env vars to your .profile **

# also, you can add "<IP> docker" to /etc/hosts to make web browser work
# for the mesos webui: http://localdocker:5050
# and the relay webui: http://localdocker:8080  (this on in prog. TODO)


# ./bin/demo.sh  # start this demo
# ./bin/demo.sh -1  # remove the docker images used in this demo


dir="$( cd "$( dirname "$( dirname "$0" )" )" && pwd )"
num_dependent_images=3  # ie zookeeper, mesos-master and mesos-slave

if [ "${1:-0}" != "0" ] || \
  [ "`docker ps|grep relay.mesos |wc -l`" != "$num_dependent_images" ]
then
  echo remove previous containers in case they exist
  docker ps -a |grep relay.mesos|awk '{print $1}' \
    |xargs --no-run-if-empty docker rm -f
  if [ "${1:-0}" = "-1" ] ; then
    echo done removing prev containers
    echo exiting
    exit 0
  fi

  set -e
  set -u

  echo start zookeeper
  docker run -d --name relay.mesos__zookeeper -d \
    -p 2181:2181 \
    -p 2888:2888 \
    -p 3888:3888 \
    jplock/zookeeper:3.4.6


  echo start mesos master
  docker run -d --name relay.mesos__mesos_master \
    --link relay.mesos__zookeeper:zookeeper \
    -e MESOS_CLUSTER=relay.mesos__DEMO \
    -e MESOS_HOSTNAME=localdocker \
    -e MESOS_WORK_DIR=/var/lib/mesos \
    -e MESOS_LOG_DIR=/var/log \
    -e MESOS_QUORUM=1 \
    -e MESOS_ZK=zk://zookeeper:2181/mesos \
    -p 5050:5050 \
    breerly/mesos \
    mesos-master
    # -v /tmp/mesoswork:/tmp/mesoswork \
    # -v /var/run/docker.sock:/var/run/docker.sock \
    # -v /sys:/sys \
    # -v /proc:/proc \
    # -t -i \


  echo start mesos slave  # MESOS_CONTAINERIZERS=mesos,docker is broken because the /run/docker.sock is incorrect
  for n in `seq 1 ${1:-1}` ; do
    docker run -d --name relay.mesos__mesos_slave_$n \
      --link relay.mesos__zookeeper:zookeeper \
      --privileged=true \
      -e MESOS_CONTAINERIZERS=docker,mesos \
      -e MESOS_HOSTNAME=localdocker \
      -e MESOS_MASTER=zk://zookeeper:2181/mesos \
      -e MESOS_LOG_DIR=/var/log \
      -e MESOS_WORK_DIR=/var/lib/mesos \
      -v /tmp/mesoswork:/tmp/mesoswork \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v /sys:/sys \
      -v /proc:/proc \
      -p `expr 5050 + $n`:`expr 5050 + $n` \
      breerly/mesos \
      supervisord -n
      # --privileged \
      # --net=host \
      # -e MESOS_ISOLATOR=cgroups/cpu,cgroups/mem \
  done

  sleep .5

  echo -e "\n"
  echo Checking for $num_dependent_images images:
  docker ps -a|grep relay.mesos|awk '{print $NF}'
  echo -e "\n"
  if [ "`docker ps|grep relay.mesos |wc -l`" != "$num_dependent_images" ]
  then
    echo oops! docker didn\'t start at least one of the dependent images.
    echo you should check docker logs:
    echo '$ docker logs <image>'
    exit 1
  fi
fi


docker build -t relay.mesos .

docker run -d --name relay.mesos \
  --link relay.mesos__zookeeper:zookeeper \
  -e RELAY_COOLER="echo cooler" \
  -e RELAY_WARMER="echo warmer" \
  -t -i relay.mesos \
  bash -c \
  'relay.mesos '\
' -m bash_echo_metric '\
' -d .1 --sendstats tcp://192.168.59.3:5498'\
' -t 60 --task_resources cpus=1 mem=10 --lookback 300'\
' --mesos_master zk://zookeeper:2181/mesos'

# kill this after a little while
( (sleep 20 ;echo relay.mesos demo quiting ; docker rm -f relay.mesos) & )

docker logs -f relay.mesos


# helpful
# docker ps
# docker kill $(docker ps -a |grep relay.mesos|awk '{print $1}')
# docker restart relay.mesos__zookeeper
# docker restart relay.mesos__mesos_master
# docker restart relay.mesos__mesos_slave_1
# docker rm -f relay.mesos__zookeeper
# docker rm -f relay.mesos__mesos_master
# docker rm -f relay.mesos__mesos_slave_1
