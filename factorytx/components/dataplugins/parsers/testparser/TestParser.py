"""Dummy parser for integration tests.

This plugin tracks processed files by adding a ParsedFile object to a
chronological list. This list can be accessed using `get_parsed_files(...)`,
or cleared using `clear_parsed_files()`.

sslogs are also emitted deterministically: every sslog processed has a
successively larger counter value, and includes the current and last contents
of the remote file.

"""

from collections import namedtuple
from datetime import datetime, timedelta
import fnmatch
import os.path

from factorytx.components.dataplugins.parsers.base import BaseParser

ParsedFile = namedtuple('ParsedFile', ['id', 'remote_path', 'completed_contents', 'new_contents'])

_HISTORY = []
_NEXT_COUNTER = 1  # Used to chronologically order sslogs.

def clear_parsed_files():
    global _HISTORY
    _HISTORY = []


def get_parsed_files(chronological=False):
    if chronological:
        return list(_HISTORY)
    else:
        return sorted(_HISTORY, key=lambda e: (e.remote_path, e.id))


class TestParser(BaseParser):
    def can_parse(self, remote_path):
        for filename_pattern in self.filename_patterns:
            if fnmatch.fnmatch(remote_path, filename_pattern):
                return True
        return False

    def parse(self, remote_path, local_path, completed_path):
        global _NEXT_COUNTER

        with open(local_path, 'rb') as fp:
            new_contents = fp.read()
        if completed_path is None:
            completed_contents = None
        else:
            with open(completed_path, 'rb') as fp:
                completed_contents = fp.read()
        parsed_file = ParsedFile(self.id, remote_path, completed_contents, new_contents)
        mtime = os.path.getmtime(local_path)
        _HISTORY.append(parsed_file)
        sslogs = []
        for _ in xrange(self.sslogs_per_file):
            sslogs.append({
                'counter': _NEXT_COUNTER,
                'sslog_type': self.sslog_type,
                'timestamp': datetime(1970, 1, 1) + timedelta(seconds=mtime),
                'fieldvalues': {
                    'remote_path': {'value': remote_path, 'units': None},
                    'completed_contents': {'value': completed_contents, 'units': None},
                    'contents': {'value': new_contents, 'units': None},
                }
            })
            _NEXT_COUNTER += 1
        return sslogs
