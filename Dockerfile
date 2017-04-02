#
# FactoryTx Dockerfile
#
# To build (from FactoryTx directory):
# docker build -t factorytx .

FROM ubuntu:16.10
MAINTAINER Sight Machine <ops@sightmachine.com>

##############################
# Environment
##############################
RUN useradd -ms /bin/bash sm
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV USER sm
RUN adduser sm sudo
WORKDIR /home/sm
# WORKDIR /opt/sightmachine

##############################
# Dependencies
##############################
RUN rm -rvf /var/lib/apt/lists/* && \
     apt-get update && apt-get install -y --no-install-recommends \
     build-essential \
     pkg-config \
     python-software-properties \
     software-properties-common

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.6 \
    python3.6-dev \
    python3-pip \
    python3-setuptools \
    libev-dev \
    libpq-dev \
    libffi-dev
    # vim \
#    freetds-dev \
#    libmysqlclient-dev

RUN easy_install3 -U pip && python3.6 -m pip install --upgrade pip
# RUN python3.6 -m pip install -U distribute
# RUN mkdir -p /root/.pip && \
#     echo "[global]\nno-index = true\nfind-links = https://sm-mirror.s3-us-west-2.amazonaws.com/pip/index.html" > /root/.pip/pip.conf
COPY ./requirements.txt /opt/sightmachine/factorytx/requirements.txt
RUN cd /opt/sightmachine/factorytx && \
    python3.6 -m pip install -r requirements.txt


RUN mkdir -p /var/spool/sightmachine/factorytx/
RUN chown -R sm:sm /var/spool/sightmachine/
VOLUME /var/spool/sightmachine/factorytx

# Copy from factorytx from current repo and install
COPY ./ /opt/sightmachine/factorytx

RUN cd /opt/sightmachine/factorytx && \
    python3.6 -m pip install . && \
    python3.6 -m compileall .

RUN chown -R sm:sm /opt/sightmachine

# RUN python3.6 -m pytest tests
USER sm
# CMD factorytx
CMD ["/bin/bash"]
