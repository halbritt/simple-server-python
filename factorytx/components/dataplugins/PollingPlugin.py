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
parser_manager = component_manger['parsers']
pollingservice_manager = component_manger['pollingservices']

log = logging.getLogger("Polling Plugin")

class PollingPlugin(DataPlugin):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(PollingPlugin, self).__init__()
        self.last_file_entries = {}  # {path -> factorytx.transports.base.FileEntry}
        self.parser_objs = []
        self.pollingservice_objs = []

    def loadParameters(self, sdconfig, schema, conf):
        super(PollingPlugin, self).loadParameters(sdconfig, schema, conf)
        self.parser_objs = []
        self.pollingservice_objs = []
        self.log.debug("My polling config is %s", conf)
        for parser_cfg in self.parsers:
            self.log.debug("Loading the parser configuration %s", parser_cfg)
            parser_obj = self._load_plugin(parser_manager, parser_cfg)
            self.parser_objs.append(parser_obj)
        for polling_service_cfg in self.datasources:
            self.log.debug("Loading the polling service configuration %s", polling_service_cfg)
            polling_obj = self._load_plugin(pollingservice_manager, polling_service_cfg)
            polling_obj.completed_folder = self.completed_folder
            self.pollingservice_objs.append(polling_obj)

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

    def process_resource(self, resource, polling_service):
        for parser_obj in self.parser_objs:
            if not polling_service.name in parser_obj.datasources:
                log.info("The parser %s cant handle %s", parser_obj, polling_service.name)
                continue
            resource = polling_service.prepare_resource(resource)
            resource = parser_obj.parse(resource)
            self.log.info("The new records are %s lines long.", len(resource))
            self.log.info("The headers are %s.", resource.iloc(0))
        frame = resource
        log.debug("The processing entry yielded the sets %s of len", len(frame))
        return frame
