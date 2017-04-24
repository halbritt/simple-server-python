import time
import os
import re
import fnmatch
import json
import threading
import filecmp
import pandas as pd
import pytz
from logging import getLogger

from factorytx.Global import setup_log
from factorytx.utils import merge_schema_defaults, status_var
from dateutil import parser
from datetime import datetime


class BaseTX(object):

    def loadParameters(self, schema, plgn_cfg):
        print("The configuration for this tx is %s", plgn_cfg)
        print("The schema is %s", schema)
        self.datasources = plgn_cfg['datasources']
        del plgn_cfg['datasources']
        self.options = plgn_cfg
        print("MY new vars are %s", vars(self))
        self.setup_log()
        self.data_reference = {}
        for data in self.datasources:
            self.data_reference[data['name']] = data

    def setup_log(self):
        print(vars(self))
        self.log = setup_log(self.options['type'] + ": " + self.options['logname'], self.options['log_level'])

    def TX(self, data) -> status_var:
        """ txes the DATA, an encrypted dataframe, using my particular TX method. """
        return False
