import os
import os.path
import shutil
import logging
import tempfile

from factorytx.components.dataplugins.pollingservices.pollingservicebase import PollingServiceBase
from factorytx.components.dataplugins.resources.fileentry import FileEntry
from factorytx.components.dataplugins.pollingservices.filepolling.fileprotocols import FILE_PROTOCOLS



class FilePolling(PollingServiceBase):

    logname = 'FilePolling Service'

    def load_parameters(self, schema, conf):
        super(FilePolling, self).load_parameters(schema, conf)
        print("The configuration for this FilePolling service is %s", conf)
        logname = ': '.join([self.logname, conf['name']])
        super(FilePolling, self).setup_log(logname)
        resource_type = conf['type']
        if resource_type in FILE_PROTOCOLS:
            self.resource_type = FILE_PROTOCOLS[resource_type]['type']
            self.resource_metafiles = FILE_PROTOCOLS[resource_type]['metafiles']
        else:
            self.log.error("THERE WAS NO PROTOCOL FOUND FOR TYPE %s POLLING SERVICE", conf['type'])
        self.root_path = os.path.abspath(self.options['root_path'])

    def copy_file(self, file_entry, local_path):
        path = os.path.join(self.root_path, file_entry.path)
        shutil.copy2(path, local_path)

    def delete_file(self, file_entry):
        path = os.path.join(self.root_path, file_entry.path)
        os.remove(path)

    def prepare_resource(self, resource):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self.log.info("Copying the file")
            self.copy_file(resource, temp_file.name)
            self.log.info("Force temporary file to retain time")
            try:
                os.utime(temp_file.name, (int(float(resource.mtime)), int(float(resource.mtime))))
            except Exception as e:
                self.log.error("There was an error processing %s: %s", temp_file.name, e)
            file_size = os.path.getsize(temp_file.name)
            self.log.info("Copied %s bytes from remote file %s to %s", file_size, resource.path, temp_file.name)
            completed_path = os.path.join(self.data_store, os.path.basename(resource.path))
            self.log.info("Found the completed path %s", completed_path)
            resource.completed_path = completed_path
            resource.temp_file = temp_file.name
        self.log.info("Completed the resource preparation")
        return resource

    def remove_resource(self, resource_id):
        self.log.info("Removing the file entry %s as a resource from persistence")
        self.log.info("Do the right thing here")

    def return_resource_class(self):
        return FileEntry

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
                metanames = []
                original = True
                for metafile in self.resource_metafiles:
                    print("Searching for the metafile", metafile, fle)
                    if fle.endswith(metafile):
                        original = False
                        continue
                    if fle + metafile in filename:
                        metanames.append(fle + metafile)
                if original:
                    resource = {'data': fle, 'path': self.root_path, 'poll': self.options['name']}
                    for i, metafile in enumerate(metanames):
                        resource[self.resource_metafiles[i]] = metafile
                    print("Creating a new resource with dict %s", resource)
                    resource = self.resource_type(resource)
                    new_resources.append(resource)
        return sorted(new_resources)

    def partition_resources(self, resources):
        return resources

    def chunk_resource(self, resource, max_size):
        return resource

    def resource_difference(self, resources, present_resources, last_resource):
        return resources
