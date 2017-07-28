<<<<<<< HEAD
# base-image for node on any machine using a template variable,
# see more about dockerfile templates here: http://docs.resin.io/deployment/docker-templates/
# and about resin base images here: http://docs.resin.io/runtime/resin-base-images/
# Note the node:slim image doesn't have node-gyp
FROM resin/amd64-node:6-slim

# use apt-get if you need to install dependencies,
# for instance if you need ALSA sound utils, just uncomment the lines below.
#RUN apt-get update && apt-get install -yq \
#    alsa-utils libasound2-dev && \
#    apt-get clean && rm -rf /var/lib/apt/lists/*

# Defines our working directory in container
WORKDIR /usr/src/app

# Copies the package.json first for better cache on later pushes
COPY package.json package.json

# This install npm dependencies on the resin.io build server,
# making sure to clean up the artifacts it creates in order to reduce the image size.
RUN JOBS=MAX npm install --production --unsafe-perm && npm cache clean && rm -rf /tmp/*
=======
# base-image for python on any machine using a template variable,
# see more about dockerfile templates here:http://docs.resin.io/pages/deployment/docker-templates
FROM resin/%%RESIN_MACHINE_NAME%%-python

# use apt-get if you need to install dependencies,
# for instance if you need ALSA sound utils, just uncomment the lines below.
# RUN apt-get update && apt-get install -yq \
#    alsa-utils libasound2-dev && \
#    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set our working directory
WORKDIR /usr/src/app

# Copy requirements.txt first for better cache on later pushes
COPY ./requirements.txt /requirements.txt

# pip install python deps from requirements.txt on the resin.io build server
RUN pip install -r /requirements.txt
>>>>>>> 319b8654758f4bc4498945c8634a8cbc03a79276

# This will copy all files in our root to the working  directory in the container
COPY . ./

<<<<<<< HEAD
# Enable systemd init system in container
ENV INITSYSTEM on

# server.js will run when container starts up on the device
CMD ["npm", "start"]
=======
# switch on systemd init system in container
ENV INITSYSTEM on

# main.py will run when container starts up on the device
CMD ["python","src/main.py"]
>>>>>>> 319b8654758f4bc4498945c8634a8cbc03a79276
