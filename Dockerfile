FROM redjack/mesos

MAINTAINER Alex Gaudio <agaudio@sailthru.com>

RUN apt-get update && apt-get install -f -y \
    wget \
  && apt-get clean \
  && wget --quiet -O /Miniconda \
    http://repo.continuum.io/miniconda/Miniconda-3.7.0-Linux-x86_64.sh \
  && /bin/bash /Miniconda -b -p /opt/anaconda \
  && rm /Miniconda \
  && echo 'export PATH=/opt/anaconda/bin:$PATH' > /etc/profile.d/conda.sh
ENV PATH /opt/anaconda/bin:$PATH

RUN conda install setuptools numpy pyzmq
WORKDIR /relay.mesos
COPY ./setup.py /relay.mesos/
RUN python setup.py install
COPY ./relay_mesos /relay.mesos/relay_mesos
RUN python setup.py develop

# EXPOSE 8080
CMD relay.mesos
