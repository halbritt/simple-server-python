import os
import multiprocessing
import time
import json
from datetime import datetime


from FactoryTx.Global import setup_log
from FactoryTx.components.dataplugins.DataPlugin import DataPlugin
from FactoryTx.components.dataplugins.opc import OpenOPC

log = None

class OPCService(DataPlugin):
    """
    This is the polling service for an OPC server.

    You need to have the following specified in the config:

    machines:
    - plugins:
      - type: opc
        name: OPC Service
        config:
          machine: 'Machine 55434'
          version: '1.0.0'
          host: '127.0.0.1'
          server: 'Kepware.KEPServerEX.V5'
          poll_rate: 1
          tags:
          - { name: 'Channel1.Device1.Tag1', exportname: Channel1Device1Tag1 }
          - { name: 'Channel1.Device1.Tag2', exportname: Channel1Device1Tag2 }
          outputdirectory: /path/to/data/dir/
    """

    __version__ = '1.0.0'

    reconnect_timeout = 5  # seconds
    reconnect_attempts = -1 # Forever
    connection_mode = "GOOD"
    logname = 'OPCService'

    def connect(self):

        self.client = OpenOPC.client()

        try:
            log.info('Connecting to OPC Server: {} on {}'.format(self.server, self.host))
            self.client.connect(self.server, self.host)
            self.connection_mode = "GOOD"
        except Exception as e:
            if str(e) == 'Connect: CONNECT_E_CANNOTCONNECT' and len(self.client.list()):
                log.warn("Server {} on {} is connected, but probably doesn't have full permissions.  Running in degraded mode".format(self.server, self.host))
                self.connection_mode = "DEGRADED"
            else:
                log.error('Cannot connect to OPC Server: {} {} Exception: {}'.format(self.server, self.host, e))
                return

        log.info('Successfully connected to "{}" on host: {}'.format(
            self.server, self.host))

        super(OPCService, self).connect()


    def disconnect(self):
        self.client.close()
        super(OPCService, self).disconnect()

    def reconnect(self):
        if self.client:
            self.client.close()

        super(OPCService, self).reconnect()


    def read(self):
        tag_names = [tag['name'] for tag in self.tags]
        timestamp = datetime.now()

        if self.connection_mode == 'GOOD':
            probe = self.client.read(tag_names) #we can use the true OPC v2 communication channel
        else:
            probe = self.client.properties(tag_names, id = [2]) #we are limited, and can't create groups and must read values thru sync calls to GetItemProperties.  Prop #2 is "Item Value"
            probe = [(p[0], p[-1], "Degraded", timestamp) for p in probe] #we get an extra field back in property mode, condition to look like a read
        return [(timestamp, probe)]

    def process(self, record):
        t, probe = record

        data = {}
        data['timestamp'] = t
        data['timestamp_epoch'] = (t - datetime(1970, 1, 1)).total_seconds()

        tagdata = {}
        taglookup = { t['name']: t for t in self.tags}

        for p in probe:
            #0 - tag name
            #1 - tag value
            #2 - tag state
            #3 - timestamp for read
            tag = taglookup[p[0]]
            tagdata[tag.get("exportname", tag['name'].split(".")[-1])] = {'value': p[1]}


        data['fieldvalues'] = tagdata
        data['poll_rate'] = self.poll_rate

        return data
