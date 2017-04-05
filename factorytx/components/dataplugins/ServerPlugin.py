import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import logging

from factorytx.components.dataplugins.DataPlugin import DataPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

component_manger = component_manager()
poll_manager = component_manger['pollingservices']

class ServerPlugin(DataPlugin):

    logname = "ServerPlugin"

    def __init__(self):
        super(ServerPlugin, self).__init__()

    def loadParameters(self, sdconfig, schema, conf):
        super(ServerPlugin, self).loadParameters(sdconfig, schema, conf)
        print(conf)
        server_conf = {'type':conf['protocol'], 'config':conf}
        self.server = super(ServerPlugin, self)._load_plugin(poll_manager, server_conf)

    def connect(self):
        self.server.start()

    def read(self):
        file_entries = []
        process_cnt = 0
        self.log.info("Looking for files in the FileTransport object.")
        for polling_obj in self.pollingservice_objs:
            new_entries = polling_obj.poll()
            found_entries = []
            for resource in new_entries:
                if resource[0][0] in self.resource_dict:
                    self.log.warn("The polling service says the new entry %s with id %s is already registered!", resource[1], resource[0])
                    continue
                log.debug("Processing the resource %s", resource)
                found_entries += [resource]
            #ecept Exception as e:
            #self.log.warn('Unable to list files: {}.'.format(e))
            #return
            self.log.info('Found %d entries from polling_service %s', len(new_entries), new_entries)
            self.log.info('Found %s registered entries.', found_entries)
            file_entries.extend(new_entries)

        if len(file_entries) == 0:
            self.log.info("Returning from read with no new entries to read. There are currently %s resources registered", len(self.resource_dict))
            return []

        file_entries = sorted(file_entries, key=lambda e: e[-1])
        return file_entries

    def process_resources(self, resources):
        return resources

    def start_server(self):
        print("The servers vars are", vars(self))
