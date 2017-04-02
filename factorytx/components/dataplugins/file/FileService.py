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

class FileService(PollingPlugin):
    __metaclass__ = abc.ABCMeta
    remove_remote_completed = True
