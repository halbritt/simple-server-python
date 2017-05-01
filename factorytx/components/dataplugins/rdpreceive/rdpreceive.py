import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import shelve

from factorytx.components.dataplugins.ServerPlugin import ServerPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

component_manger = component_manager()

class RDPReceive(ServerPlugin):

    logname = 'RDPReceive'

