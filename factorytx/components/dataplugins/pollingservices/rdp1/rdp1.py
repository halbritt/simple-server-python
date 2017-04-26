import os
import os.path
import shutil
import logging
import tempfile

from factorytx.components.dataplugins.pollingservices.pollingservicebase import PollingServiceBase
from factorytx.components.dataplugins.resources.rdp1payload import RDP1Payload
from factorytx.components.dataplugins.serverservices.rdp1server import RDP1Server


class RDP1(PollingServiceBase):

    logname = 'RDP1 Stream'

    def loadParameters(self, schema, conf):
        super(RDP1, self).loadParameters(schema, conf)
        print("The configuration for this Localfile transport is %s", conf)
        logname = ': '.join([self.logname, conf['name']])
        super(RDP1, self).setup_log(logname)
        self.root_path = os.path.abspath(self.root_path)

    def prepare_resource(self, resource):
        return resource

    def return_resource_class(self):
        return RDP1Payload

    def get_all_resources(self):
        new_resources = []
        print("The resource keys are", [x for x in self.resource_keys.keys()])
        for dirs, paths, filename in os.walk(self.data_store):
            if paths: continue
            print("Found the entry %s", dirs, paths, filename)
            for fle in filename:
                if fle.endswith('headers'): continue
                header_name = fle + 'headers'
                if header_name in filename:
                    resource = {'headers':header_name, 'data':fle, 'path': self.data_store, 'poll': self.logname}
                    resource = RDP1Payload(resource)
                    if resource.name in self.resource_keys:
                        continue
                    new_resources.append(resource)
        return new_resources

    def partition_resources(self, resources):
        return resources

    def chunk_resource(self, resource, max_size):
        return resource

    def resource_difference(self, resources, present_resources, last_resource):
        return resources

    def start(self):
        print("Do the right thing to start the server here")
        self.server = RDP1Server.start_server(self.host, self.port, self.data_store, self.apikeys)
