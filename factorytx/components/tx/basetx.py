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
from dateutil import parser
from datetime import datetime


class BaseTX(object):

    def loadParameters(self, schema, plgn_cfg):
        self.__dict__.update(plgn_cfg)
        merge_schema_defaults(schema, self.__dict__)
        self.log = getLogger("Base TX")
        self.data_reference = {}
        for data in self.datasources:
            self.data_reference[data['name']] = data

    def TX(self, data) -> status_var:
        """ txes the DATA, an encrypted dataframe, using my particular TX method. """
        return False
