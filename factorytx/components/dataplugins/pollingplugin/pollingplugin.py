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

    def load_parameters(self, sdconfig, schema, conf):
        super(PollingPlugin, self).load_parameters(sdconfig, schema, conf)
        self.parser_objs = []
        self.pollingservice_objs = []
        self.log.debug("My polling config is %s", conf)
        if 'parsers' in self.options:
            for parser_cfg in self.options['parsers']:
                self.log.info("Loading the parser configuration %s", parser_cfg)
                parser_obj = self._load_plugin(parser_manager, parser_cfg)
                self.parser_objs.append(parser_obj)
        else:
            self.log.warning("There doesn't seem to be any parsers configured, starting a forwarding service")
        for polling_service_cfg in self.options['datasources']:
            self.log.info("Loading the polling service configuration %s", polling_service_cfg)
            polling_obj = self._load_plugin(pollingservice_manager, polling_service_cfg)
            self.pollingservice_objs.append(polling_obj)

    def process_resources(self, resources):
        processed, cnt, errors = [], 0, 0
        for resource in resources:
            print("Processing %s", resource)
            processed += [self.process(resource[0], resource[1])]
            cnt += 1
            #except:
            #    log.warn("Not able to process the resource %s, skipping", resource)
            #    errors += 1
        log.info("Processed %s resources while encountering %s errors.", cnt, errors)
        return processed

    def process(self, resource_id, resource):
        log.debug("Processing the resource %s", resource)
        records = self.process_resource(resource)
        log.debug("Found some records with %s columns", len(records))
        log.debug("Trying to save the resource with the right id %s", resource_id)
        return ([resource_id], records)

    def process_resource(self, resource):
        log.info("The resource to be processed is %s", resource)
        for parser_obj in self.parser_objs:
            self.log.info("The parser %s has datasources %s", parser_obj, parserobj.datasources)
            self.log.info("The resource we are processing has vars", resource.keys())
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
