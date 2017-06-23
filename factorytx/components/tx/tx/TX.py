import multiprocessing
import pickle
import time
import os
import sys
import shelve
import logging
import pandas as pd
import ujson as json
from datetime import timedelta

from bson import ObjectId

from factorytx.DataService import DataService
from factorytx.managers.GlobalManager import global_manager
from factorytx.managers.PluginManager import component_manager
from factorytx.Global import setup_log
from factorytx import utils
import base64
import threading

components = component_manager()
tx_manager = components['tx']

global_manager = global_manager()

class TXAbstract(object):
    """
    TX is a framework for building transformations that can handle
    pandas data frames and transform them

    """
    __version__ = "1.0"

    logname = 'TX Module'
    reconnect_timeout = 5  # seconds
    reconnect_attempts = float('inf')  # Keep retrying until connected
    lastvalue = None
    records_per_file = 5

    counter = 1

    def __init__(self):
        super(TXAbstract, self).__init__()
        self._connected = False
        self._running = True
        self.client = None


    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def __repr__(self):
        return "<Plugin {} {}>".format(self.__class__.__name__, self.name)

    def load_parameters(self, sdconfig, schema, conf):
        super(TXAbstract, self).load_parameters(sdconfig, schema, conf)
        self.tx_objs = []
        self.log.info(self.options)
        self.tx_ref = {}
        for tx_cfg in self.options['tx']:
            self.log.info("Loading the TX configuration %s", tx_cfg)
            if not 'log_level' in tx_cfg['config']:
                tx_cfg['config']['log_level'] = self.options['log_level']
            tx_obj = self._load_plugin(tx_manager, tx_cfg)
            self.log.debug("The tx options are", tx_obj.options)
            self.tx_objs.append(tx_obj)

    def is_empty(self) -> ():
        """ Returns True exactly when my in_pipe is empty, and false otherwise. """
        return self.in_pipe.empty()

    def get_next_tx(self) -> dict:
        """ Returns the next tx object in my in_pipe queue. """
        get = self.in_pipe.get()
        return get

    def tx_frame(self, resource, size=0) -> bool:
        """ Given a DATASOURCE as a key with a FRAME_ID and a DATAFRAME to tx, proceeds to
            transmit the dataframe along the correct tx obj based on the datasource.

        """
        datasource = resource.datasource
        frame_id = resource.name
        resource_data = resource.resource_data
        passed_all = True
        frame_info = {'confirmation': False}
        self.tx_ref[frame_id] = frame_info
        self.log.info("The tx objects are %s", self.tx_objs)
        for tx in self.tx_objs:
            datasource_names = [x['name'] for x in tx.options['datasources']]
            self.log.debug("The options for my tx data are %s", tx.options['datasources'])
            self.log.debug("The datasource is %s", datasource)
            if datasource in datasource_names:
                self.log.debug("The datasource name is %s and this tx handles it.", datasource)
                confirmation = tx.TX(resource_data, size)
                if confirmation:
                    self.log.info("Sucessfuly TXed the frame %s with tx %s", frame_id, tx)
                else:
                    self.log.info("Failed to TX the frame %s", frame_id)
                    passed_all = False
        if passed_all:
            self.log.info("Sucessfuly TXed the frame %s with all txes", frame_id)
            confirm = self.remove_frame(resource)
            if confirm:
                frame_info['confirmation'] = True
                frame_info['tx_time'] = time.time()
                self.tx_ref[frame_id] = frame_info
                self.out_pipe[frame_id] = True
                self.log.info("Removed residual data from tx for %s", frame_id)
            elif confirm == False:
                self.log.error("Couldn't remove the path from the frame %s", frame_info)
            else:
                self.log.warn("The frame %s doesn't seem to be persisted in TX", frame_id)
            return True
        return False

    def cleanup_resources(self):
        del_frames = []
        for frame_id in self.tx_ref:
            if not frame_id in self.out_pipe:
                del_frames.append(frame_id)
        for frame in del_frames:
            del self.tx_ref[frame]

    def remove_frame(self, resource: str) -> utils.status_var:
        """ Given the FRAME_PATH to a saved dataframe, returns True exactly when the
            frame was removed from the fs, False exactly when there was trouble removing
            the frame, and None exactly when the path doesn't exist on the fs.

        """
        trace = resource.remove_trace()
        if trace:
            return True
        else:
            return False

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._connected = True
        return True

    def disconnect(self):
        self._connected = False
        return True

    def reconnect(self) -> ():
        """ Does whatever I need to do to connect. """
        # TODO we need to flesh out this method for TX specifically
        # If we got here, we probably aren't connected
        self._connected = False
        try: self.connect()
        except Exception as e:
            self.log.error("There was a problem with %s", e)

    def load_binary(self, attachment):
        formatted = {}
        with open(attachment['binary'], 'rb') as f:
            binary_attach = f.read()
        formatted['content'] = binary_attach
        formatted['content_type'] = attachment['original_content']
        formatted['content_encoding'] = 'raw'
        formatted['filename'] = attachment['original_file']
        return formatted

    def run(self) -> ():
        """ Proceeds in the loop of pulling off txes and transmitting them.

        """
        # reinitialize the log after forking, this is necessary on Windows
        # and probably not a terrible idea in UNIX
        log = setup_log(self.logname, self.options['log_level'])
        sys.modules[self.__class__.__module__].log = log
        self.log = log

        if os.name == 'nt':
            self.log.info(self.__dict__.keys())
            global_manager.dict = self.__dict__[
                '_Win32ServiceManager__global_dict']

        self.log.info("Running {} plugin...".format(self.name))

        try:
            self.connect()
        except Exception as e:
            self.log.error('Failed to connect to %s', self.options['host'])
            self.log.exception(e)
            self.reconnect()

        while self._running:
            try:
                if not self.is_empty():
                    self.log.debug("Getting Next TX")
                    res = self.get_next_tx()
                    if res.loaded:
                        logs = res.resource_data
                    else:
                        res.load_resource()
                        logs = res.resource_data
                    self.log.debug("The first tx arg is %s with logs of length %s", res.name, len(logs))
                    running_size = 0
                    for sslog_data in logs:
                        if 'attachment_info' in sslog_data:
                            self.log.info("Loading and attching a binary attachment for %s", sslog_data['attachment_info'])
                            sslog_data['attachment'] = self.load_binary(sslog_data['attachment_info'])
                            running_size += sslog_data['attachment_info']['original_size']
                            del sslog_data['attachment_info']
                    tx_done = False
                    while not tx_done:
                        self.log.debug("Transmitting tx with running size %s", running_size)
                        tx_status = self.tx_frame(res, running_size)
                        if not tx_status:
                            self.log.error("The TX %s was unable to send to all of its RDP receivers", res.name)
                        else:
                            self.log.debug("Sucessfully TXed the frame %s.", res.name)
                            tx_done = True
                    self.log.info("Moving on to a new TX")
            except Exception as e:
                self.log.exception('Failed to read data from: %r', e)
                self._connected = False
                self.reconnect()
                continue
            self.cleanup_resources()

            # sleep by 0.1
            if self.is_empty():
                for _ in range(int(float(self.options['poll_rate']) / 0.1)):
                    time.sleep(0.1)
                    if not self.is_empty():
                        break
                    if not self._running:
                        break


class TX(TXAbstract, multiprocessing.Process, DataService):
    pass


class TXThread(TXAbstract, threading.Thread, DataService):

    def __init__(self):
        super(TXThread, self).__init__()
        self.daemon = True

        name = self.getName()
        new_name = name.replace('Thread', self.__class__.__name__)
        self.setName(new_name)

    def stop(self):
        self._running = False
