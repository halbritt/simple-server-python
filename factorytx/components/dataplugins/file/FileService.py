import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import shelve

from factorytx.components.dataplugins.PollingPlugin import PollingPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

component_manger = component_manager()
parser_manager = component_manger['parsers']
polling_manager = component_manger['transports']

class FileService(PollingPlugin):
    __metaclass__ = abc.ABCMeta
    remove_remote_completed = True

    '''def filter_parseable_files(self, new_entries):
        """Filters a list of FileEntry objects to those that can be handled by
        at least one configured parser.

        """
        parseable_entries = []
        for new_entry in new_entries:
            for parser_obj in self.parser_objs:
                if parser_obj.can_parse(new_entry.path):
                    parseable_entries.append(new_entry)
                    break
        return parseable_entries

    def filter_ready_files(self, new_entries):
        """Filters a list of FileEntry objects to those that are ready to read
        (ie. those that are no longer being written to.)

        """
        ready_entries = []
        for new_entry in new_entries:
            last_entry = self.last_file_entries.get(new_entry.path)
            if last_entry is None:
                continue
            if new_entry.size == last_entry.size:
                ready_entries.append(new_entry)
            else:
                self.log.info('Skipping "%s", it is still being written; '
                              'was %d bytes, now it is %d.', new_entry.path,
                              last_entry.size, new_entry.size)
        self.last_file_entries = {e.path: e for e in new_entries}
        return ready_entries

    def filter_complete_files(self, new_entries):
        new_new_entries = [f for f in new_entries if not os.path.exists(os.path.join(self.completed_folder, f.basename)) or self.over_time(f)] 
        self.log.info("Identified {} files not already processed".format(len(new_new_entries)))
        return new_new_entries'''
