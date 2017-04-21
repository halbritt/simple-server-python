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
        self.poll_rate = int(self.poll_rate)

    def connect(self):
        self.server.start()

    def read(self):
        print("Do something interesting here")
        file_entries = []
        process_cnt = 0
        self.log.info("Looking for files in the FileTransport object.")
        new_entries = self.server.poll()
        found_entries = []
        for resource in new_entries:
            if resource[0][0] in self.resource_dict:
                self.log.warn("The polling service says the new entry %s with id %s is already registered!", resource[1], resource[0])
                continue
            self.log.debug("Processing the resource %s", resource)
            found_entries += [resource]
        self.log.info('Found %d entries from polling_service %s', len(new_entries), new_entries)
        self.log.info('Found %s registered entries.', found_entries)
        file_entries.extend(new_entries)

        if len(file_entries) == 0:
            self.log.info("Returning from read with no new entries to read. There are currently %s resources registered", len(self.resource_dict))
            return []

        file_entries = sorted(file_entries, key=lambda e: e[-1])
        return file_entries

    def get_unprocessed_resources(self):
        unprocessed, untxed = [], []
        registered = self.server.get_registered_resources()
        for resource in registered:
            if not resource[0] in self.resource_dict:
                resource_id = resource[0]
                self.log.info("The resource %s is not registered here.", resource)
                arguments = resource[1].split(',')
                self.log.debug("The resource arguments are %s", arguments)
                resource = self.server.return_resource_class()(poll, *arguments)
                unprocessed += [(resource_id, resource)]
            else:
                self.log.info("This resource %s has been previously processed and is persisted", resource)
                corresponding = self.get_corresponding_chunks(resource)
                untxed = self.filter_corresponding(corresponding)
                transform = []
                for frame_id, resource in untxed:
                    self.log.info("Checking the status of %s", frame_id)
                    if frame_id in self.in_pipe:
                        self.log.info("Found some transformation on the frame %s", frame_id)
                    else:
                        self.log.warn("Found evidence of processing for %s, but no reference in a TX module, reprocessing.", frame_id)
                        transform += [(frame_id, resource)]
                untxed = transform
        self.log.info("Returning %s resources to be processed from the unprocessed function.", len(unprocessed))
        self.log.debug("The unprocessed entries are %s, %s", unprocessed, untxed)
        return unprocessed, untxed

    def process_resources(self, resources):
        print("In the processing function for %s", resources)
        processed = []
        for resource in resources:
            processed += [(resource[0], resource[1].load_resource())]
        return processed

    def start_server(self):
        print("The servers vars are", vars(self))
