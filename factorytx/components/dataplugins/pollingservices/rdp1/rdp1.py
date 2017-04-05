import os
import os.path
import shutil
import logging
import tempfile

from factorytx.components.dataplugins.pollingservices.pollingservicebase import PollingServiceBase
from factorytx.components.dataplugins.resources.rdp1payload import RDP1Payload


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
        self.log.info("Need a resource function")
        return []

    def partition_resources(self, resources):
        return resources

    def chunk_resource(self, resource, max_size):
        return resource

    def resource_difference(self, resources, present_resources, last_resource):
        return resources
