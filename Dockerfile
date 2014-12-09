FROM ubuntu
MAINTAINER Alex Gaudio <agaudio@sailthru.com>
ENV DEBIAN_FRONTEND noninteractive

WORKDIR /relay.mesos

ENV PATH /opt/anaconda/bin:$PATH

RUN apt-get update && apt-get install -f -y curl && apt-get clean
RUN curl -o miniconda.sh http://repo.continuum.io/miniconda/Miniconda-3.7.0-Linux-x86_64.sh \
  && /bin/bash miniconda.sh -b -p /opt/anaconda \
  && rm miniconda.sh \
  && echo 'export PATH=/opt/anaconda/bin:$PATH' > /etc/profile.d/conda.sh
RUN curl -o mesos.egg http://downloads.mesosphere.io/master/ubuntu/14.04/mesos-0.20.1-py2.7-linux-x86_64.egg \
  && conda install setuptools numpy pyzmq

ENV PYTHONPATH /relay.mesos:/relay.mesos/mesos.egg:$PYTHONPATH
COPY ./setup.py /relay.mesos/
RUN python setup.py install
COPY ./relay_mesos /relay.mesos/relay_mesos
RUN python setup.py develop

EXPOSE 8080
CMD relay.mesos
