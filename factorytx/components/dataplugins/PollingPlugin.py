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
transport_manager = component_manger['transports']

log = logging.getLogger("Polling Plugin")

class PollingPlugin(DataPlugin):
    __metaclass__ = abc.ABCMeta
    remove_remote_completed = True

    def __init__(self):
        super(PollingPlugin, self).__init__()
        self.last_file_entries = {}  # {path -> factorytx.transports.base.FileEntry}
        self.parser_objs = []
        self.pollingservice_objs = []

    def loadParameters(self, sdconfig, schema, conf):
        super(PollingPlugin, self).loadParameters(sdconfig, schema, conf)
        self.parser_objs = []
        self.pollingservice_objs = []
        for parser_cfg in self.parsers:
            self.log.debug("Loading the parser configuration %s", parser_cfg)
            parser_obj = self._load_plugin(parser_manager, parser_cfg)
            self.parser_objs.append(parser_obj)
        for polling_service_cfg in self.datasources:
            self.log.debug("Loading the polling service configuration %s", polling_service_cfg)
            polling_obj = self._load_plugin(transport_manager, polling_service_cfg)
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
        with tempfile.NamedTemporaryFile() as temp_file:
            log.debug("Copying the file")
            polling_service.copy_file(resource, temp_file.name)
            log.debug("force the temporary file %s to retain the time (dont have the access_time, so both get mtime %s", temp_file.name, resource.mtime)
            try:
                os.utime(temp_file.name, (int(float(resource.mtime)), int(float(resource.mtime))))
            except Exception as e:
                log.error("The error is %s", e)
            log.debug("Found the utime")
            file_size = os.path.getsize(temp_file.name)
            log.debug('Copied %s bytes from remote file "%s" to "%s".',
                           file_size, resource.path, temp_file.name)
            # XXX: os.path.basename may be the wrong function.
            completed_path = os.path.join(self.completed_folder,
                                          os.path.basename(resource.path))
            record_sets = []  # list of iterables containing sslogs.
            delete_file = False
            attachment = ""
            log.debug("The polling service datasource is %s", polling_service.name)
            for parser_obj in self.parser_objs:
                log.debug("The parser can handle the sources %s", parser_obj.datasources)
                if not polling_service.name in parser_obj.datasources:
                    log.info("This parser cant handle this resource")
                    continue
                # Special case for 0 byte files, we want to log, delete it and continue
                if file_size > 0:
                    if not parser_obj.can_parse(resource.path):
                        continue
                    parsed = True

                    new_records = parser_obj.parse(
                        remote_path=resource.path,    # relative path from root of polling_service
                        local_path=temp_file.name,      # temporary filename
                        completed_path=completed_path if os.path.exists(completed_path) else None,
                    )
                    self.log.info("THE NEW RECORDS ARE %s", len(new_records))
                    if new_records is not None:
                        return new_records
                else:
                    self.log.warning('Empty file found "%s" ignoring', resource.path)
                    delete_file = True
            self.log.info('here are the record sets %s', len(record_sets))
            #if parsed:
            #    self.log.debug('Moving "%s" to completed folder "%s".',
            #                   resource.path, self.completed_folder)
            #    shutil.move(temp_file.name, completed_path)
            #    if attachment:
            #        shutil.copy(completed_path, attachment)
            #    delete_file = True
            #if delete_file:
            #    temp_file.delete = False  # Mark the file as cleaned up.
            #    if self.remove_remote_completed:
            #        polling_service.delete_file(resource)
        log.debug("The processing entry yielded the sets %s of len", len(record_sets))
        return record_sets
