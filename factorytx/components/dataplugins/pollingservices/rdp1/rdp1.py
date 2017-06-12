import os
import os.path
import shutil
import logging
import tempfile

from factorytx.components.dataplugins.pollingservices.filepolling import FilePolling
from factorytx.components.dataplugins.resources.rdp1payload import RDP1Payload
from factorytx.components.dataplugins.resources.rdp1logs import RDP1Logs
from factorytx.components.dataplugins.serverservices.rdp1server import RDP1Server


class RDP1(FilePolling):

    logname = 'RDP1 Stream'

    def load_parameters(self, schema, conf):
        super(RDP1, self).load_parameters(schema, conf)
        print("The configuration for this RDP1 transport is %s", conf)
        logname = ': '.join([self.logname, conf['name']])
        super(RDP1, self).setup_log(logname)

    def prepare_resource(self, resource):
        return resource

    def return_resource_class(self):
        return RDP1Payload

    def return_aggregate_class(self):
        return RDP1Logs

    def remove_resource(self, resource_id):
        if resource_id in self.resources:
            remove_path = os.path.join(self.root_path, resource_id)
            print("Removing the resource from persistence with id %s from %s", resource_id, remove_path)
            if os.path.exists(remove_path):
                print("Found the resource and removing it from %s", self.root_path)
                os.remove(remove_path)
                if os.path.exists(remove_path + 'headers'):
                    os.remove(remove_path + 'headers')
                if os.path.exists(remove_path + 'binaryattachment'):
                    os.remove(remove_path + 'binaryattachment')
                self.delete_resource_trace(resource_id)
                return True
            else:
                print("Couldn't find the path to remove for resource %s", resource_id)
        return False

    def get_all_resources(self):
        """ Gets all of the resources that I haven't registered previously. """
        self.log.info("The resource keys are %s", [x for x in self.resources.keys()])
        self.log.info("Returning the resources from the path %s", self.root_path)
        new_resources = []
        for dirs, paths, filename in os.walk(self.root_path):
            print("Found the entry %s", dirs, paths, filename)
            for fle in filename:
                if fle in self.resources:
                    print("The resource %s has already been returned.", fle)
                    continue
                if fle.endswith('headers'): continue
                header_name = fle + 'headers'
                binary_name = fle + 'binaryattachment'
                if header_name in filename:
                    print("Making the rdp1 payload with vars %s.", vars(self))
                    resource = {'headers':header_name, 'data':fle, 'path': self.root_path, 'poll': self.options['name']}
                    if binary_name in filename:
                        resource['binaryattachment'] = binary_name
                    resource = RDP1Payload(resource)
                    new_resources.append(resource)
        return sorted(new_resources)

    def start(self, host, port, apikeys):
        self.server = RDP1Server.start_server(host, port, self.root_path, apikeys)

    def stop(self):
        self.server.stop_server()
