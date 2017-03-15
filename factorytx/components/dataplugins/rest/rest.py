from datetime import datetime
import requests
from FactoryTx.components.dataplugins.DataPlugin import DataPlugin
from FactoryTx.Global import setup_log
import logging
import os
import json
from bson import json_util
from requests.auth import HTTPBasicAuth


log = setup_log('rest')

class RESTService(DataPlugin):
    """
    This is the polling service for an REST endpoint.  

    You need to have the following specified in the config:

    machines:
    - plugins:
      - type: rest
        name: REST Service
        config:
          machine: 'Machine 55434'
          version: '1.0.0'
          host: 'http://127.0.0.1:5000/RESTTest'
          poll_rate: 5
          outputdirectory: /path/to/data/dir/          
          outputdirectory: '/tmp/'  #TODO make SD dir default
          cachedirectory: '/tmp/'
          cachefilename: 'XX_YY_ExampleMachine_1_events.json'
    """

    __version__ = '1.0.0'

    rest_params = None
    logname = "RESTService"
 
    def __init__(self):
        super(RESTService, self).__init__()
        self.cache = {'last_id': -1}

    def _init_cache_filename(self):
        # build cache filename:
        # cachedirectory + cachefilename from config file
        self.cache_file = os.path.join(self.cachedirectory, self.cachefilename)

    def loadParameters(self, sdconfig, schema, config):
        super(RESTService, self).loadParameters(sdconfig, schema, config)
        self._init_cache_filename()
        self.load_cache()


    def save_cache(self):
        if not os.path.exists(self.cachedirectory):
            os.makedirs(self.cachedirectory)
        json_cache = json.dumps(self.cache, default=json_util.default)
        try:
            with open(self.cache_file, 'w') as f:
                f.write(json_cache)
        except:
            log.error('Failed to save cache into {}'.format(self.cache_file))
        else:
            log.info('Saved cache: {}; Source: {}'.format(self.cache,
                                                          self._getSource()))

    def load_cache(self):
        cache_file = self.cache_file

        # if new pointer is not found - we try to find old one and load data from it
        if not os.path.exists(self.cache_file):
            log.info('No cache file exists.  Creating new cache: {}'.format(cache_file))
            self.save_cache()
        try:
            with open(cache_file, 'r') as f:
                cache = json.loads(f.read(), object_hook=json_util.object_hook)
        except:
            log.error('Failed to load cache from {}'.format(cache_file))
        else:
            self.cache = cache


    def read(self):
        log.info('Attepting to read from endpoint')
        result = []
        timestamp = datetime.now()
        try:
            if self.username:
                auth = HTTPBasicAuth(self.username, self.password)
            r = requests.get(self.host, auth=auth, verify=self.verifySSL)
            result = r.text
        except ValueError as e:
            log.warn('JSON could not be decoded from URL.  For non-JSON data please extend this method ')
        except Exception as e:
            log.warn('Read error from {}: {}'.format(self.host, e))
        return result


    def process(self, record):
        t, probe = record

        data = {}
        data['timestamp'] = t
        mapped = {}

        for tag in self.tags:
            if tag['name'] in probe:
                mapped[tag.get('exportname', tag['name'].split(".")[-1])] = {'value': probe[tag['name']]}

        data['fieldvalues'] = mapped
        data['poll_rate'] = self.poll_rate

        return data
