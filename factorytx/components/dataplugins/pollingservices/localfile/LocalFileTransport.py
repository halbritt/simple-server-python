import os
import os.path
import shutil
import logging
import tempfile

from factorytx.components.dataplugins.pollingservices.pollingservicebase import PollingServiceBase
from factorytx.components.dataplugins.resources.fileentry import FileEntry


class LocalFileTransport(PollingServiceBase):

    logname = 'LocalFileTransport'

    def load_parameters(self, schema, conf):
        super(LocalFileTransport, self).load_parameters(schema, conf)
        print("The configuration for this Localfile transport is %s", conf)
        logname = ': '.join([self.logname, conf['name']])
        super(LocalFileTransport, self).setup_log(logname)
        self.root_path = os.path.abspath(self.root_path)

    def copy_file(self, file_entry, local_path):
        path = os.path.join(self.root_path, file_entry.path)
        shutil.copy2(path, local_path)

    def delete_file(self, file_entry):
        path = os.path.join(self.root_path, file_entry.path)
        os.remove(path)

    def list_files(self):
        file_entries = []
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                file_entry = FileEntry(
                    transport=self,
                    path=os.path.relpath(path, start=self.root_path),
                    mtime=os.path.getmtime(path),
                    size=os.path.getsize(path),
                    root_path=self.root_path,
                )
                file_entries.append(file_entry)
        return file_entries

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
            completed_path = os.path.join(self.completed_folder, os.path.basename(resource.path))
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
        return self.list_files()

    def partition_resources(self, resources):
        return resources

    def chunk_resource(self, resource, max_size):
        return resource

    def resource_difference(self, resources, present_resources, last_resource):
        return resources
