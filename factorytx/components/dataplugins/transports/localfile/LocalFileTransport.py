import os
import os.path
import shutil
import logging

from factorytx.components.dataplugins.transports.base import FileEntry, FileTransport


class LocalFileTransport(FileTransport):

    logname = 'LocalFileTransport'

    def loadParameters(self, schema, conf):
        super(LocalFileTransport, self).loadParameters(schema, conf)
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
