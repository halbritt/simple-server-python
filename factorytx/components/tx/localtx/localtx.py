import abc
import itertools
import os, os.path
import shutil
import tempfile
import time

from factorytx.components.tx.basetx import BaseTX
from factorytx.managers.PluginManager import component_manager
from factorytx import utils


class LocalTX(BaseTX):

    logname = 'LocalTX'

    def load_parameters(self, schema, conf):
        super(LocalTX, self).load_parameters(schema, conf)
        self.logname = ': '.join([self.logname, conf['source']])

    def TX(self, data):
        print("Local TX will now do its thing with vars %s.", vars(self))
        return False
