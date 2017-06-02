import multiprocessing
import pickle
import time
import os
import sys
import shelve
import logging
import pandas as pd
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
try:
    import ujson as json
except:
    import json

log = logging.getLogger(__name__)

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
        if not os.path.exists(self.tx_persistence_location):
            os.mkdir(self.tx_persistence_location)
        self.tx_ref = shelve.open(os.path.join(self.tx_dict_location, self.tx_dict_name))
        for tx_cfg in self.tx:
            self.log.info("Loading the TX configuration %s", tx_cfg)
            if not 'log_level' in tx_cfg['config']:
                tx_cfg['config']['log_level'] = self.log_level
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

    def tx_frame(self, datasource: str, frame_id: str, dataframe: pd.DataFrame, size=0) -> bool:
        """ Given a DATASOURCE as a key with a FRAME_ID and a DATAFRAME to tx, proceeds to
            transmit the dataframe along the correct tx obj based on the datasource.

        """
        log.info("Pushing frame to final location")
        self.persist_frame(frame_id, dataframe)
        log.info("The dataframe has been saved")
        passed_all = True
        frame_info = self.tx_ref[frame_id]
        print("The tx objects are", self.tx_objs)
        for tx in self.tx_objs:
            print("The tx data reference is %s", tx.data_reference)
            print("The datasource is %s", datasource)
            if datasource in tx.data_reference:
                data = tx.data_reference[datasource]
                log.info("The datasource name is %s and this tx handles %s", datasource, data['name'])
                confirmation = tx.TX(dataframe, size)
                if confirmation:
                    log.info("Sucessfuly TXed the frame %s with tx %s", frame_id, tx)
                else:
                    log.info("Failed to TX the frame %s", frame_id)
                    passed_all = False
        if passed_all:
            log.info("Sucessfuly TXed the frame %s with all txes", frame_id)
            confirm = self.remove_frame(frame_info['frame_path'])
            if confirm:
                frame_info['confirmation'] = True
                frame_info['tx_time'] = time.time()
                self.tx_ref[frame_id] = frame_info
                self.out_pipe[frame_id] = True
                log.info("Removed residual data from tx for %s", frame_id)
            elif confirm == False:
                log.error("Couldn't remove the path from the frame %s", frame_info)
            else:
                log.warn("The frame %s doesn't seem to be persisted in TX", frame_id)
            return True
        return False

    def remove_frame(self, frame_path: str) -> utils.status_var:
        """ Given the FRAME_PATH to a saved dataframe, returns True exactly when the
            frame was removed from the fs, False exactly when there was trouble removing
            the frame, and None exactly when the path doesn't exist on the fs.

        """
        if os.path.exists(frame_path):
            try:
                os.remove(frame_path)
                log.info("Removed the temp frame %s", frame_path)
                return True
            except Exception as e:
                log.error("Trouble removing the path %s with error %s", frame_path, e)
                return False
        else:
            return None

    def persist_frame(self, frame_id: str, dataframe: pd.DataFrame) -> ():
        """ Save a DATAFRAME with a particular FRAME_ID so that if there is
            a connectivity issue with a TX module we can persist till later.

        """
        log.info("Persisting the frame %s", frame_id)
        path = os.path.join(self.tx_persistence_location, str(frame_id))
        try:
            with open(path, 'w') as f:
                log.info("Writing to the path %s", path)
                jsn = json.dumps(dataframe)
                f.write(jsn)
        except Exception as e:
            with open(path, 'wb') as f:
                log.info("Pickling to the path %s", path)
                pkl = pickle.dump(dataframe, f)
        self.tx_ref[frame_id] = {'confirmation':False, 'frame_path':path}
        log.info("Persisted the frame and registered it with my references")

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
        log = setup_log(self.logname, self.log_level)
        sys.modules[self.__class__.__module__].log = log
        self.log = log

        if os.name == 'nt':
            log.info(self.__dict__.keys())
            global_manager.dict = self.__dict__[
                '_Win32ServiceManager__global_dict']

        log.info("Running {} plugin...".format(self.name))

        try:
            self.connect()
        except Exception as e:
            log.error('Failed to connect to %s', self.host)
            log.exception(e)
            self.reconnect()

        while self._running:
            try:
                log.info("Looking for TX to find")
                if not self.is_empty():
                    log.info("Getting Next TX")
                    res = self.get_next_tx()
                    log.info("The first tx arg is %s", res['frame_id'])
                    logs = res['frame']
                    log.info("The sslogs are of length %s", len(logs))
                    running_size = 0
                    for sslog_data in logs:
                        if 'attachment_info' in sslog_data:
                            log.info("Loading and attching a binary attachment for %s", sslog_data['attachment_info'])
                            sslog_data['attachment'] = self.load_binary(sslog_data['attachment_info'])
                            running_size += sslog_data['attachment_info']['original_size']
                            del sslog_data['attachment_info']
                    tx_done = False
                    while not tx_done:
                        self.log.info("Transmitting tx with running size %s", running_size)
                        tx_status = self.tx_frame(res['datasource'], res['frame_id'], res['frame'], running_size)
                        if not tx_status:
                            log.error("The TX %s was unable to send to all of its RDP receivers", res['frame_id'])
                        else:
                            log.info("Sucessfully TXed the frame %s.", res['frame_id'])
                            tx_done = True
                    log.info("Moving on to a new TX")
            except Exception as e:
                log.exception('Failed to read data from: %r', e)
                self._connected = False
                self.reconnect()
                continue

            # sleep by 0.1
            if self.is_empty():
                for _ in range(int(float(self.polltime) / 0.1)):
                    time.sleep(0.1)
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
