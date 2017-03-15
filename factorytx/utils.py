from base64 import urlsafe_b64encode
import logging
import collections
import copy
import hashlib
import itertools
import os
import sys
import uuid
import pandas
import numpy as np
from typing import TypeVar
from multiprocessing import Queue
from threading import Lock

status_var = TypeVar("status", None, bool)

def combineDicts(x={}, y={}):
    """
    Merge 2 dictonaries recursively
    :param dictionary1:
    :param dictionary2:
    :return:
    """

    dictionary1 = copy.deepcopy(x)
    dictionary2 = copy.deepcopy(y)

    output = {}

    if not dictionary1:
        output = copy.deepcopy(dictionary2)
    elif not dictionary2:
        output = copy.deepcopy(dictionary1)
    else:

        _dictionary1 = dictionary1

        if (not hasattr(dictionary1, 'items')
                and hasattr(dictionary1, '_data')):
            _dictionary1 = dictionary1._data

        for item, value in _dictionary1.items():
            if item in dictionary2:
                if isinstance(dictionary2[item], dict):
                    output[item] = combineDicts(value, dictionary2.pop(item))
            else:
                output[item] = value

        output.update(dictionary2)
    return output

def dehumanize(value):
    value = value.replace(' ', '')
    d = {
        'kb': 1024,
        'mb': 1024*1024,
        'gb': 1024*1024*1024,
    }
    if len(value) < 2:
        return int(value)
    z = value[-2:].lower()
    if z in d:
        return int(value[:-2]) * d[z]


def get_build_version():
    """Returns a string identifying the running factorytx artifact, or a fixed
    string if factorytx was not installed from an automated build.

    """

    # Scripts running in a virtualenv set `sys.real_prefix` and export the
    # path to the virtualenv root as `sys.prefix`.
    in_virtualenv = hasattr(sys, 'real_prefix')
    build_info_path = os.path.join(sys.prefix, "build")
    if not (in_virtualenv and os.path.isfile(build_info_path)):
        return "factorytx-unknown (manual install or missing build info)"
    with open(build_info_path, "r") as fp:
        version = fp.read().strip()
    return version


def hash4b(strdata):
    return hashlib.sha1(strdata.encode('utf-8')).hexdigest()[:8]


def make_guid():
    """Returns a short globally-unique ASCII string."""
    random_bytes = uuid.uuid4().bytes
    guid = urlsafe_b64encode(random_bytes)
    guid = str(guid.strip(b'='))
    return guid


def merge_schema_defaults(schema, conf):
    # TODO: Merge defaults recursively.
    for key, value in schema.get('properties', {}).items():
        if value.get('default') != None and conf.get(key) is None:
            conf[key] = value['default']

def df_chunks(dataframe, size):
    """pieces = []
    dflen = len(dataframe)
    dfmod = int(dflen / size)
    counter = 0
    # TODO: Right now something is going haywire with the indexing when we split the frame,
    # we need to fix this so that we can split things up into nice chunks automatically, but for now
    # i'm going to move on.
    return np.array_split(dataframe, dfmod)"""
    return [dataframe]

def chunks(iterable, size):
    """Returns an iterable of chunks of maximum length `size` from iterable.

    >>> [list(x) for x in chunks([1, 2, 3, 4, 5, 6, 7], 3)]
    [[1, 2, 3], [4, 5, 6], [7]]

    """
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def grouped(values, key):
    groups = collections.defaultdict(list)
    for value in values:
        groups[key(value)].append(value)
    return groups

class lock_dict():

    def __init__(self):
        self.queue = Queue()
        self.entries = set()
        self.lock = Lock()

    def see_entries(self):
        self.lock.acquire()
        return self.entries
        self.lock.release()

    def add_entry(self, entry):
        self.lock.acquire()
        self.entries.add(entry)
        self.lock.release()

    def return_queue(self):
        return self.queue

    def empty_queue(self):
        entry_list = []
        while not self.queue.empty():
            entry_list += [self.queue.get()]
        self.add_entry(entry_list)

    def delete_entry(entry):
        self.lock.acquire()
        if entry in self.entries:
            self.entries.remove(entry)
        self.lock.release()

def as_timezone(dt, tz):
    """Converts a timezone-aware datetime to the specified timezone."""
    return tz.normalize(dt.astimezone(tz))


def naive_to_local(naive_dt, local_tz):
    """Converts a naive local datetime to a timezone-aware datetime in
    the supplied timezone.
    """

    return local_tz.normalize(local_tz.localize(naive_dt))
