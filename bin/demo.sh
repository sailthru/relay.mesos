#!/usr/bin/env bash

# This script runs zookeeper and mesos inside docker containers.
# It then runs relay.mesos inside a docker container.


# install docker and docker-compose
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

# To see relay.mesos in action, the demo will navigate your browser to:
#     http://localdocker:8080  # relay UI
#     http://localdocker:5050  # mesos UI

docker-compose build relay

(
echo sleeping 10 seconds and then opening two browser tabs
sleep 10
cat <<EOF | python
import webbrowser
webbrowser.open_new_tab("http://localdocker:8080")
webbrowser.open_new_tab("http://localdocker:5050")
EOF
) &
docker-compose up
