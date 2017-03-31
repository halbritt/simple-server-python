#
# FactoryTx Dockerfile
#
# To build (from FactoryTx directory):
# sudo docker build -t factorytx .

FROM ubuntu:16.04
MAINTAINER Anthony Oliver <anthony@sightmachine.com>

##############################
# Environment
##############################
ENV USER sm
RUN mkdir /opt/sightmachine && useradd -ms /bin/bash sm && \
    chown sm:sm /opt/sightmachine && \
    mkdir /etc/rsyslog.d/ && \
    touch /etc/rsyslog.d/30-ma.conf && \
    echo '$ModLoad imudp\n$UDPServerRun 514\n$MaxMessageSize 64k\n$EscapeControlCharactersOnReceive off\n$RepeatedMsgContainsOriginalMsg on\n$template malog,"/var/log/ma/%app-name%.log"\n$template matenantlog,"/var/log/ma/%app-name%.%procid%.log"\nif ($app-name startswith "ma_") and ($procid == "base" or $procid == "") then ?malog\n& ~\nif ($app-name startswith "ma_") then ?matenantlog\n& ~' > /etc/rsyslog.d/30-ma.conf
WORKDIR /opt/sightmachine

##############################
# Dependencies
##############################
RUN sudo rm -rvf /var/lib/apt/lists/* && \
     apt-get update && apt-get install -y --no-install-recommends \
     build-essential \
     pkg-config \
     python-software-properties \
     software-properties-common

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.6 \
    python3.6-dev \
    python-dev \
    python-setuptools \
    libev-dev \
    libpq-dev \
    libffi-dev 
#    freetds-dev \
#    libmysqlclient-dev


# PIP setup and install requirements
RUN easy_install pip && pip install --upgrade pip
RUN pip install -U distribute
RUN echo "/usr/lib/atlas-base" | tee /etc/ld.so.conf.d/atlas-lib.conf && ldconfig && byobu-ctrl-a screen
RUN mkdir -p /root/.pip && \
    echo "[global]\nno-index = true\nfind-links = https://sm-mirror.s3-us-west-2.amazonaws.com/pip/index.html" > /root/.pip/pip.conf
COPY ./requirements.txt /opt/sightmachine/FactoryTx/requirements.txt
RUN cd /opt/sightmachine/FactoryTx && \
    pip install -r requirements.txt


RUN mkdir -p /var/spool/sightmachine/FactoryTx/
RUN chown -R sm:sm /var/spool/sightmachine/
VOLUME /var/spool/sightmachine/FactoryTx

# Copy from FactoryTx from current repo and install
COPY ./ /opt/sightmachine/FactoryTx

RUN cd /opt/sightmachine/FactoryTx && \
    python setup.py develop -u && \
    python setup.py develop && \
    python -m compileall .

RUN chown -R sm:sm /opt/sightmachine

RUN nosetests --with-xunit --with-coverage --cover-xml --cover-erase
#CMD factorytx
CMD ["/bin/bash"]
