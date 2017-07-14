import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import shelve

from factorytx.components.dataplugins.pollingplugin import PollingPlugin
from factorytx import utils

class FileService(PollingPlugin):
    __metaclass__ = abc.ABCMeta
    remove_remote_completed = True
