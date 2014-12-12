#!/usr/bin/env bash

# This script runs zookeeper and mesos inside docker containers.
# It then runs relay.mesos inside a docker container.


# install docker
# https://docs.docker.com/installation
# (if on a mac, you may need boot2docker and
#  don't forget to add env vars to your .profile)
# (if on ubuntu, you may need 3.16 kernel or later)

# Then, add "<IP> docker" to /etc/hosts to make web browsers work
# ie my boot2docker ip is given by: `boot2docker ip`
# I added this to my /etc/hosts file:
#  192.168.59.103 localdocker

# Run this script.  When you run this for the first time, docker may need to
# download a lot of the required images to get mesos running on your computer

# ./bin/demo.sh     # run the demo
# ./bin/demo.sh N   # run the demo with N mesos slaves  (N=1 is plenty)
# ./bin/demo.sh -1  # remove all docker containers used in this demo

# To see relay.mesos in action, navigate your browser to:
#     http://localdocker:8080  # relay UI
#     http://localdocker:5050  # mesos UI



dir="$( cd "$( dirname "$( dirname "$0" )" )" && pwd )"
num_dependent_images=$(expr 3 + ${1:-1})

if [ "${1:--1}" = "-1" ] || \
  [ "`docker ps|grep relay.mesos |wc -l |tr -d ' '`" != "$num_dependent_images" ]
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

  echo start relay.web
  docker run -d --name relay.mesos__relay.web \
    -p 8080:8080 \
    -p 5673:5673 \
    adgaudio/relay.web

  sleep .5

  echo -e "\n"
  echo Checking for $num_dependent_images images:
  docker ps -a|grep relay.mesos|awk '{print $NF}'
  echo -e "\n"
  if [ "`docker ps|grep relay.mesos |wc -l |tr -d ' '`" != "$num_dependent_images" ]
  then
    echo oops! docker didn\'t start at least one of the dependent images.
    echo you should check docker logs:
    echo '$ docker logs <image>'
    exit 1
  fi
fi

(cd $dir ; docker build -t relay.mesos .)

docker run --rm --name relay.mesos \
  --link relay.mesos__zookeeper:zookeeper \
  --link relay.mesos__relay.web:web \
  -e RELAY_WARMER='dur=$(expr $(od -An -N2 -tu2 /dev/urandom) % 5 + 1)'\
' ; echo my task is running for $dur seconds ; sleep $dur' \
  -e RELAY_METRIC="relay_mesos.for_demo.num_active_mesos_tasks" \
  -e RELAY_TARGET='oscillating_setpoint' \
  -e RELAY_SENDSTATS='tcp://web:5673' \
  -e RELAY_TASK_RESOURCES='cpus=0.1 mem=1' \
  -e RELAY_MESOS_MASTER='zk://zookeeper:2181/mesos' \
  -e RELAY_MAX_FAILURES=10 \
  -e RELAY_DELAY=0.1 \
  -t -i relay.mesos


# helpful
# docker ps
# docker kill $(docker ps -a |grep relay.mesos|awk '{print $1}')
# docker restart relay.mesos__zookeeper
# docker restart relay.mesos__mesos_master
# docker restart relay.mesos__mesos_slave_1
# docker rm -f relay.mesos__zookeeper
# docker rm -f relay.mesos__mesos_master
# docker rm -f relay.mesos__mesos_slave_1
