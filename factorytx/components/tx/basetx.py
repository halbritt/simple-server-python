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
from factorytx.utils import merge_schema_defaults, status_var
from factorytx.DataService import DataService
from dateutil import parser
from datetime import datetime


class BaseTX(DataService):

    logname = "BaseTX"

    def load_parameters(self, schema, plgn_cfg):
        super(BaseTX, self).load_parameters({}, schema, plgn_cfg)

    def setup_log(self):
        print("Setting up the TX log")
        self.log = getLogger(self.options['type'] + ": " + self.options['logname'])

    def TX(self, data) -> status_var:
        """ txes the DATA, an encrypted dataframe, using my particular TX method. """
        return False
