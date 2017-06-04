import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import logging

from factorytx.components.dataplugins.DataPlugin import DataPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx.components.dataplugins.resources.rawsslogs import RawSSLogs
from factorytx import utils

component_manger = component_manager()
poll_manager = component_manger['pollingservices']

class ServerPlugin(DataPlugin):

    logname = "ServerPlugin"

    def __init__(self):
        super(ServerPlugin, self).__init__()

    def load_parameters(self, sdconfig, schema, conf):
        super(ServerPlugin, self).load_parameters(sdconfig, schema, conf)
        print(conf)
        server_conf = {'type':conf['protocol'], 'config':conf}
        self.server = super(ServerPlugin, self)._load_plugin(poll_manager, server_conf)

    def remove_resource(self, resource_id):
        self.log.info("Removing the resource %s from my server", resource_id)
        return self.server.remove_resource(resource_id)

    def connect(self):
        self.server.start()

    def perform_teardown(self):
        self.server.stop()

    def read(self):
        file_entries = []
        process_cnt = 0
        self.log.info("Looking for uploads from my server.")
        new_entries = self.server.poll()
        found_entries = []
        self.log.info('Found %d entries from polling_server', len(new_entries))
        self.log.info("The records say we have entries %s", [x for x in self.resource_dict.items()])
        for resource in new_entries:
            if resource[0][0] in self.resource_dict:
                self.log.warn("The polling service says the new entry %s with id %s is already registered!", resource[1], resource[0])
                continue
            self.log.debug("Processing the resource %s", resource)
            found_entries += [resource]
        self.log.info('Found %s entries registered by my server.', found_entries)
        file_entries.extend(new_entries)

        if len(file_entries) == 0:
            self.log.info("Returning from read with no new entries to read. There are currently %s resources registered", len(self.resource_dict))
            return []

        file_entries = sorted(file_entries, key=lambda e: e[-1])
        return file_entries

    def get_unprocessed_resources(self):
        unprocessed, untxed = [], []
        registered = self.server.get_registered_resources()
        for resource_id, resource_enc in registered:
            if resource_id not in self.resource_dict:
                self.log.info("The resource %s is not registered here.", resource_id)
                arguments = resource_enc.split('--')
                self.log.debug("The resource arguments are %s", arguments)
                resource = self.server.return_resource_class()(*arguments)
                unprocessed += [(resource_id, resource)]
            else:
                self.log.info("This resource %s has been previously processed and is persisted", resource_id)
                corresponding = self.get_corresponding_chunks(resource_id)
                untxed = self.filter_corresponding(corresponding)
                transform = []
                for frame_id, resource in untxed:
                    self.log.info("Checking the status of %s", frame_id)
                    if frame_id in self.in_pipe:
                        self.log.info("Found some transformation on the frame %s", frame_id)
                    else:
                        self.log.warn("Found evidence of processing for %s, but no reference in a TX module, reprocessing.", frame_id)
                        transform += [(frame_id, (resource_id, resource_enc))]
                untxed = transform
        self.log.info("Returning %s resources to be processed from the unprocessed function.", len(unprocessed))
        self.log.debug("The unprocessed entries are %s, %s", unprocessed, untxed)
        return unprocessed, untxed

    def process_resources(self, resources):
        return self.aggregate_logs(resources)

    def aggregate_logs(self, resources):
        log_ids = []
        running_size = 0
        running_logs = 0
        max_size = int(self.options['max_size'])
        max_logs = int(self.options['max_logs'])
        log_ids = []
        log_data = []
        for resource in resources:
            rec_id, rec_data = resource[0], resource[1].load_resource()
            self.log.info("Aggregating the logs corresponding to id %s", rec_id)
            sslogs = rec_data[0]
            attachment_info = rec_data[1]
            num_logs = len(sslogs)
            self.log.info("The number of logs that we will process is %s", num_logs)
            if attachment_info['binary']:
                if attachment_info['original_size'] > max_size or attachment_info['original_size'] > max_size - running_size:
                    self.log.info("The resource %s goes over the running max or is itself bigger than the max size.", rec_id)
                    if log_ids:
                        yield RawSSLogs.create_raw_sslogs(log_ids, log_data)
                        log_ids = []
                        log_data = []
                        running_size = 0
                        running_logs = 0
                    for log_dict in sslogs.values():
                        log_dict['attachment_info'] = attachment_info
                        if attachment_info['original_size'] > max_size:
                            self.log.error("The attachment size %s is bigger than the max aggregate size %s, appending singleton!",
                                           attachment_info['original_size'], max_size)
                            yield RawSSLogs.create_raw_sslogs([rec_id], log_dict)
                        else:
                            self.log.info("The attachment size %s is bigger than the running size of %s", attachment_info['original_size'], running_size)
                            log_ids.append(rec_id)
                            log_data.append(log_dict)
                            running_size += attachment_info['original_size']
                            running_logs += 1
                else:
                    self.log.info("Appending the info in %s to the running totals", rec_id)
                    running_size += attachment_info['original_size']
                    for log_dict in sslogs.values():
                        log_dict['attachment_info'] = attachment_info
                        log_ids.append(rec_id)
                        log_data.append(log_dict)
                        running_logs += 1
            elif num_logs + running_logs > max_logs:
                self.log.warn("The number of sslogs will put the max size overlimit, recreating")
                yield RawSSLogs.create_raw_sslogs(log_ids, log_data)
                log_ids = [rec_id]
                log_data = []
                log_data.extend(sslogs.values())
                running_logs = 0
                running_size = 0
            else:
                self.log.info("The number of sslogs will be appended to the growing data")
                log_data.extend(sslogs.values())
                running_logs += num_logs
                log_ids.append(rec_id)
        if log_ids:
            self.log.info("Appending the ids %s", log_ids)
            yield RawSSLogs.create_raw_sslogs(log_ids, log_data)

    def start_server(self):
        print("The servers vars are", vars(self))
