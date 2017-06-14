import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import logging

from factorytx.components.dataplugins.pollingplugin.pollingplugin import PollingPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

component_manger = component_manager()
poll_manager = component_manger['pollingservices']

class ServerPlugin(PollingPlugin):

    logname = "ServerPlugin"

    def __init__(self):
        super(ServerPlugin, self).__init__()

    def load_parameters(self, sdconfig, schema, conf):
        super(ServerPlugin, self).load_parameters(sdconfig, schema, conf)
        self.server = self.pollingservice_objs[0]
        print("The final pollingservices are %s", self.pollingservice_objs)

    def remove_resource(self, resource_id):
        self.log.info("Removing the resource %s from my server", resource_id)
        return self.server.remove_resource(resource_id)

    def connect(self):
        self.log.info("My server options are %s", self.options)
        opt = self.options
        self.server.start(opt['host'], opt['port'], opt['apikeys'])

    def perform_teardown(self):
        self.server.stop()

    def process_resources(self, resources):
        log_ids = []
        running_size = 0
        running_logs = 0
        max_size = int(self.options['max_size'])
        max_logs = int(self.options['max_logs'])
        log_ids = []
        log_data = []
        resource_class = self.server.return_aggregate_class()
        for resource in resources:
            self.log.info("Processing the resource %s", resource)
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
                        yield resource_class(log_ids, {'sslog_list': log_data})
                        log_ids = []
                        log_data = []
                        running_size = 0
                        running_logs = 0
                    for log_dict in sslogs.values():
                        log_dict['attachment_info'] = attachment_info
                        if attachment_info['original_size'] > max_size:
                            self.log.error("The attachment size %s is bigger than the max aggregate size %s, appending singleton!",
                                           attachment_info['original_size'], max_size)
                            yield resource_class([rec_id], {'sslog_list': log_dict})
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
                yield resource_class(log_ids, {'sslog_list': log_data})
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
            yield resource_class(log_ids, {'sslog_list': log_data})
