import multiprocessing
import time
import os
import sys
import logging
import pandas as pd
import ujson as json
from datetime import timedelta, datetime

from bson import ObjectId

from factorytx.DataService import DataService
from factorytx.managers.GlobalManager import global_manager
from factorytx.managers.PluginManager import component_manager
from factorytx.Global import setup_log
from factorytx.Config import get_config
from factorytx import utils
import threading

cfg = get_config()
components = component_manager()
transform_manager = components['transforms']
global_manager = global_manager()

class TransformAbstract(object):
    """
    Transform is a framework for building transformations that can handle
    pandas data frames and transform them

    """
    __version__ = "0.1"
    logname = "Transform"

    reconnect_timeout = 5  # seconds
    reconnect_attempts = float('inf')  # Keep retrying until connected
    lastvalue = None
    records_per_file = 5

    counter = 1

    def __init__(self):
        super(TransformAbstract, self).__init__()
        self._connected = False
        self._running = True
        self.client = None
        self.loaded_transforms = {}
        self.transforms = []

    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def __repr__(self):
        return "<Plugin {} {}>".format(self.__class__.__name__, self.name)

    def load_parameters(self, sdconfig, schema, conf):
        super(TransformAbstract, self).load_parameters(sdconfig, schema, conf)
        self.transform_objs = []
        self.transform_ref = {}
        for transform_cfg in self.transforms:
            self.log.info("Loading the transform configuration %s", transform_cfg)
            transform_obj = self._load_plugin(transform_manager, transform_cfg)
            self.log.info("The transform datasources are %s", transform_obj.datasources)
            if not 'log_level' in tx_cfg['config']:
                tx_cfg['config']['log_level'] = self.options['log_level']
            for source in transform_obj.datasources:
                if source['name'] in self.transform_ref:
                    self.transform_ref[source['name']][transform_cfg['name']] = transform_obj
                else:
                    self.transform_ref[source['name']] = {transform_cfg['name']:transform_obj}
            self.transform_objs.append(transform_obj)

    def is_empty(self) -> bool:
        """ Returns True exactly when I don't have any transforms to process. """
        return self.in_pipe.empty()

    def get_next_transform(self) -> dict:
        """ Returns the next transform that is coming down my pipe. """
        get = self.in_pipe.get()
        return get

    def transform(self, dataframe: pd.DataFrame, datasource_name: str) -> pd.DataFrame:
        """ Applies the correct transform to a DATAFRAME based on a given DATASOURCE_NAME,
            and subsequently returns the given dataframe with the transform applied.

        """
        self.log.info("In the transform function with frame of len %s", len(dataframe))
        transforms = self.get_transforms(datasource_name)
        for trans in transforms:
            self.log.info('Applying the transformation', trans)
            dataframe = self.apply_transform(dataframe, trans, datasource_name)
        return dataframe

    def get_transforms(self, datasource_name: str) -> list:
        """ Gets the transforms in order that I need to apply to a given DATASOURCE_NAME """
        self.log.info("getting %s with transform %s", datasource_name, self.transforms)
        transform_queue = []
        for transform in self.transforms:
            for datasource in transform['config']['datasources']:
                if datasource['name'] == datasource_name:
                    transform_queue += [transform]
        return transform_queue

    def apply_transform(self, dataframe: pd.DataFrame, trans: dict, datasource_name: str) -> pd.DataFrame:
        """ Given a DATAFRAME and and TRANSform dictionary, uses the DATASOURCE_NAME and the
            transform type to find and apply the right transform object to the dataframe.

        """
        self.log.info("Applying the transform %s to data %s of len %d", trans, datasource_name, len(dataframe))
        name = trans['type']
        print("The ref is %s", self.transform_ref)
        transform = self.transform_ref[datasource_name][trans['name']]
        dataframe = transform.apply_config_actions(dataframe)
        return dataframe

    def push_transform(self, dataframe_dict: dict) -> ():
        """ Pushes out the transformation dictionary. """
        cfg = {}
        self.out_pipe.put(dataframe_dict)
        self.log.info("Pushed transformation down the pipe")

    # TODO: Better transform connections
    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._connected = True
        return True

    def disconnect(self):
        self._connected = False
        return True

    def reconnect(self):
        # If we got here, we probably aren't connected
        self._connected = False
        try:
            self.connect()
        except Exception as e:
            self.log.error("The reconnection failed with error %s", e)

    def run(self) -> ():
        """ Finds dataframes and manipulates them according to my rules. """
        # reinitialize the log after forking, this is necessary on Windows
        # and probably not a terrible idea in UNIX
        log = setup_log(self.logname, self.options['log_level'])
        sys.modules[self.__class__.__module__].log = log
        self.log = log

        if os.name == 'nt':
            global_manager.dict = self.__dict__[
                '_Win32ServiceManager__global_dict']

        self.log.info("Running %s plugin...", self.name)

        try:
            self.connect()
        except Exception as e:
            self.log.error('Failed to connect to {}'.format(self.host))
            self.log.exception(e)
            self.reconnect()

        while self._running:
            try:
                if not self.is_empty():
                    next_transform = self.get_next_transform()
                    self.log.info("Getting Next Transform %s", next_transform)
                    if not next_transform.transformable:
                        self.push_transform(next_transform)
                    else:
                        self.log.debug("Transforming the data with id %s and resource %s", next_transform['frame_id'], next_transform['datasource'])
                        next_transform['frame'] = self.transform(next_transform['frame'], next_transform['datasource'])
                        self.log.debug("Pushing the Transform")
                        self.push_transform(next_transform)
                        self.log.debug("done")
            except Exception as e:
                self.log.exception('Failed to read data from: %r', e)
                self._connected = False
                self.reconnect()
                continue

            # sleep by 0.1
            if self.is_empty():
                self.log.debug("My poll rate is %s", self.options['poll_rate'])
                if not self.is_empty():
                    break
                for _ in range(int(float(self.options['poll_rate']) / 0.1)):
                    time.sleep(0.1)
                    if not self._running:
                        break


class Transform(TransformAbstract, multiprocessing.Process, DataService):
    pass


class TransformThread(TransformAbstract, threading.Thread, DataService):

    def __init__(self):
        super(TransformThread, self).__init__()
        self.daemon = True

        name = self.getName()
        new_name = name.replace('Thread', self.__class__.__name__)
        self.setName(new_name)

    def stop(self):
        self._running = False
