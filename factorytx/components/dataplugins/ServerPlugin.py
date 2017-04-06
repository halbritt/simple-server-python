import abc
import itertools
import os, os.path
import shutil
import tempfile
import time
import logging

from factorytx.components.dataplugins.DataPlugin import DataPlugin
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

component_manger = component_manager()
poll_manager = component_manger['pollingservices']

class ServerPlugin(DataPlugin):

    logname = "ServerPlugin"

    def __init__(self):
        super(ServerPlugin, self).__init__()

    def loadParameters(self, sdconfig, schema, conf):
        super(ServerPlugin, self).loadParameters(sdconfig, schema, conf)
        print(conf)
        server_conf = {'type':conf['protocol'], 'config':conf}
        self.server = super(ServerPlugin, self)._load_plugin(poll_manager, server_conf)

    def connect(self):
        self.server.start()

    def read(self):
        print("Do something interesting here")

    def process_resources(self, resources):
        return resources

    def start_server(self):
        print("The servers vars are", vars(self))
