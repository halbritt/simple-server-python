import multiprocessing
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

if global_manager.get_encryption():
    from cryptography.fernet import Fernet

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

    def loadParameters(self, sdconfig, schema, conf):
        super(TXAbstract, self).loadParameters(sdconfig, schema, conf)
        self.tx_objs = []
        if not os.path.exists(self.tx_persistence_location):
            os.mkdir(self.tx_persistence_location)
        self.tx_ref = shelve.open(os.path.join(self.tx_dict_location, self.tx_dict_name))
        for tx_cfg in self.tx:
            self.log.debug("Loading the TX configuration %s", tx_cfg)
            tx_obj = self._load_plugin(tx_manager, tx_cfg)
            self.tx_objs.append(tx_obj)

    def populate_out(self) -> ():
        """ Given my records of tx transmission, populates the out_pipe dictionary with the posted
            txes.

        """
        for frame_id, status in self.tx_ref.items():
            self.out_pipe[frame_id] = status

    def is_empty(self) -> ():
        """ Returns True exactly when my in_pipe is empty, and false otherwise. """
        return self.in_pipe.empty()

    def get_next_tx(self) -> dict:
        """ Returns the next tx object in my in_pipe queue. """
        get = self.in_pipe.get()
        return get

    def tx_frame(self, datasource: str, frame_id: str, dataframe: pd.DataFrame) -> ():
        """ Given a DATASOURCE as a key with a FRAME_ID and a DATAFRAME to tx, proceeds to
            transmit the dataframe along the correct tx obj based on the datasource.

        """
        log.info("Pushing frame to final location")
        self.persist_frame(frame_id, dataframe)
        log.info("The dataframe has been saved")
        passed_all = True
        frame_info = self.tx_ref[frame_id]
        for tx in self.tx_objs:
            if datasource in tx.data_reference:
                data = tx.data_reference[datasource]
                log.info("The datasource name is %s and this tx handles %s", datasource, data['name'])
                confirmation = tx.TX(dataframe)
                if confirmation:
                    log.info("Sucessfuly TXed the frame %s with tx %s", frame_id, tx.__name__)
                else:
                    log.info("Failed to TX the frame %s", frame_id)
                    passed_all = False
        if passed_all:
            log.info("Sucessfuly TXed the frame %s with tx %s", frame_id)
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
        with open(path, 'w') as f:
            log.info("Writing to the path %s", path)
            jsn = json.dumps(dataframe)
            f.write(jsn)
        self.tx_ref[frame_id] = {'confirmation':False, 'frame_path':path}
        self.out_pipe[frame_id] = False
        log.info("Persisted the frame and registered it with my references")
        log.info("The out is %s", vars(self.out_pipe))


    def encrypt(self, dataframe: pd.DataFrame):
        """ Given a DATAFRAME, encrypt it into the right format. """
        gm = global_manager
        if not gm.get_encryption():
            return records

        (encryption_public_key,
         encryption_padding,
         sha1_digest) = gm.get_encryption()

        symmetric_key = Fernet.generate_key()
        encrypter = Fernet(symmetric_key)
        encrypted_key = encryption_public_key.encrypt(
                        symmetric_key,
                        encryption_padding)

        exclude_keys = ["source", "timestamp", "counter", "poll_rate"]
        for ts, data in records.items():
            enc_data = {}
            enc_fields = []

            for k, v in data.items():
                if k in exclude_keys:
                    enc_data[k] = v
                else:
                    enc_data[k] = encrypter.encrypt(json.dumps(v))
                    enc_fields.append(k)

            enc_data['encryption'] = dict(
                key=base64.b64encode(encrypted_key),
                fields=enc_fields,
                pubkey_sha1=sha1_digest,
                encoding="field-encrypt-v1"
            )

            records[ts] = enc_data
        return records

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

        log.warning('Connection lost to {}. Trying to reconnect'
                    ''.format(self.host))

        count = 0
        keep_trying = True
        while keep_trying:
            time.sleep(self.reconnect_timeout)
            log.warning('Reconnection Attempt: {}'.format(count))

            try:
                self.connect()
            except Exception as e:
                log.warning("Connection Error: {}".format(e))

            if self.connected:
                return

            count += 1
            # Attempt to reconnect forever unless defined in config to override
            if (self.reconnect_attempts != -1
                    and self.reconnect_attempts < float('inf')):
                if count >= self.reconnect_attempts:
                    keep_trying = False

        raise Exception('Failed to reconnect after {} attempts'
                        ''.format(self.reconnect_attempts))

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
            self.populate_out()
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
                    log.info("TXing the data")
                    self.tx_frame(res['datasource'], res['frame_id'], res['frame'])
                    log.info("done")
            except Exception as e:
                log.exception('Failed to read data from: %r', e)
                self._connected = False
                self.reconnect()
                continue

            # sleep by 0.1
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
