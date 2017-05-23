import multiprocessing
import time
import os
import sys
import logging
import shelve
import pickle
from datetime import timedelta

from bson import ObjectId

from time import sleep
from factorytx.DataService import DataService
from factorytx.managers.GlobalManager import global_manager
from factorytx.Global import setup_log
from factorytx import utils
from pandas import DataFrame, read_json
import base64
from itertools import islice, chain
import threading

global_manager = global_manager()
from simpleeval import simple_eval


try:
    import ujson as json
except:
    import json
log = logging.getLogger("Data Plugin")

class DataPluginAbstract(object):
    """
    Data-plugin is a framework for building plugins that can handle
    asynch and offline processing of captured factorytx data

    We subclass the dataservice, but it can be used as a polling service,
    or as a post-processing service
    """
    __version__ = "0.1"

    reconnect_timeout = 5  # seconds
    reconnect_attempts = float('inf')  # Keep retrying until connected
    lastvalue = None
    records_per_file = 5000
    resource_dict = {}
    retransmission_time = 1000

    counter = 1

    def __init__(self):
        super(DataPluginAbstract, self).__init__()
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
        super(DataPluginAbstract, self).loadParameters(sdconfig, schema, conf)
        self.resource_dict = shelve.open(os.path.join(self.resource_dict_location,
                                                      self.resource_dict_name + 'plugin_resources'))
        self.tx_dict = shelve.open(os.path.join(self.resource_dict_location, self.resource_dict_name + 'tx-chunks'))
        self.last_frame_keys = set()

    def read(self):
        return

    def remove_resource(self, resource_id):
        return

    def process_resource(self, resource, resource_service):
        return

    def save_raw(self, records):
        return

    def load_raw(self, source):
        return

    def makeDictTree(self, inDict):
        tree = {}

        for key, finalvalue in inDict.iteritems():
            t = tree

            parts = key.split('.')
            for idx, part in enumerate(parts):
                val = finalvalue if idx == len(parts)-1 else {}
                t = t.setdefault(part, val)
        return tree

    def _getSource(self):
        return self.source if hasattr(self, 'source') else "Unknown"

    def encrypt(self, records):
        # TODO: Complete for at rest encryption
        return records

    def _load_plugin(self, manager, cfg):
        if 'config' in cfg:
            cfg['config'].update({'source': self.source, 'resource_dict_location':self.resource_dict_location})
        obj = super(DataPluginAbstract, self)._load_plugin(manager, cfg)
        return obj

    def process(self, resource_id, resource):
        log.debug("Processing the resource %s", resource)
        time = resource.mtime
        polling_service = resource.transport
        parsed = False
        log.debug("Trying to process the entry %s", resource)
        records = self.process_resource(resource, polling_service)
        log.debug("Found some records with %s columns", len(records))
        log.debug("Trying to save the resource with the right id %s", resource_id)
        return (resource_id, records)

    def save_json(self, record_ids, records):
        new_records = []
        print("The records we are saving have length %s", len(records))
        # record = self.encrypt(records) for at rest encryption
        raw_records = True
        try:
            json_data = records.to_json()
            raw_records = False
        except AttributeError as e:
            self.log.info("We are working with raw records")
        if raw_records:
            try:
                json_data = json.dumps(records)
            except Exception as e:
                self.log.info("The exception is %s, trying to pickle sslogs", e)
        log.info("The json data has been dumped")
        if not os.path.exists(self.outputdirectory):
            os.makedirs(self.outputdirectory)
        # TODO: NEED TO GET THE TIMESTAMP OUT OF DATA BEFOREHAND
        timestamp = 'None' # Get earliest timestamp in data
        guid = utils.make_guid()
        fname = '_'.join((timestamp, self._getSource(), guid))
        fname = os.path.join(self.outputdirectory, fname)
        dst_fname = fname + '.sm.json'
        tmp_fname = fname + '.jsontemp'
        if os.name == 'nt':
            fname = fname.replace(":", "_")
        try:
            with open(tmp_fname, 'w') as f:
                f.write(json_data)
            # rename .jsontemp to .sm.json
            os.rename(tmp_fname, dst_fname)
        except UnboundLocalError as e:
            self.log.info("Pickling the data rather than dumping json.")
            with open(tmp_fname, 'wb') as f:
                pickle.dump(records, f)
        except Exception as e:
            log.error('Failed to save data into {} {} {}'.format(
                self.outputdirectory, fname, e))
            raise
        else:
            self.log.info('Saved data into {}'.format(fname))
        self.log.info("Registering the data frame %s, %s, %s", record_ids, guid, dst_fname)
        self.register_data_frame(record_ids, guid, dst_fname)
        new_records.append((record_ids, guid, records))
        return new_records

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._connected = True
        return True

    def disconnect(self):
        self._connected = False
        return True

    def perform_teardown(self):
        return

    def reconnect(self):
        self._connected = False
        self.perform_teardown()
        log.warning('Connection lost to %s. Trying to reconnect to %s', self.host, self.logname)
        count = 0
        keep_trying = True
        while keep_trying:
            sleep(self.reconnect_timeout)
            log.warning('Reconnection Attempt: %s for %s', count, self.logname)
            try:
                self.connect()
            except Exception as e:
                log.warning("Connection Error: %s for %s", e, self.logname)
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

    def emit_records(self, records):
        log.info("Emitting %s records for the plugin %s", len(records[1]), self.logname)
        log.debug("The record info is %s", records[0])
        records_id, records = records
        self.log.info("Persisting %s records in JSON", len(records))
        records = self.save_json(records_id, records)
        self.log.info("Saved the JSON")
        if len(records) > 0:
            for record in records:
                self.log.info("Passing the records with id %s onto the next component", record[0])
                self.push_frame(record[0][0][1], record[1], record[2])
        else:
            self.log.info("There are no records to forward")

    def register_resources(self, resources):
        for resource, obj in resources:
            self.log.info("Registering %s", resource)
            self.resource_dict[resource[0]] = obj.encode("utf-8")
        return resources

    def cleanup_frame(self, frame_id):
        frame_info = self.tx_dict[frame_id]
        if 'frame_path' in frame_info:
            frame_path = frame_info['frame_path']
            if os.path.exists(frame_path):
                os.remove(frame_path)
                self.log.info("Removed the frame from persistence %s", frame_id)
                return True
            else:
                self.log.warn("The path %s doesn't seem to exist to be removed", frame_path)
                return False
        else:
            self.log.error("There doesn't seem to be an indication where this frame is persisted", frame_info)
            return None

    def convert_records(self, frame):
        print("Converting the frame %s", frame.keys())
        return DataFrame(frame)

    def push_frame(self, datasource, frame_id, frame):
        if self.validate_frame(frame):
            frame_data = self.tx_dict[frame_id]
            log.info("Transmitting the dataframe %s", frame_data)
            frame_data['transmission_time'] = time.time()
            self.tx_dict[frame_id] = frame_data
            log.info("Marked the time for %s", frame_id)
            self.out_pipe.put({'frame_id':frame_id, 'datasource':datasource, 'frame':frame})
            log.info("Pushed out a new dataframe with %s indexes.", len(frame))
        else:
            log.info("Failed to validate frame")

    def validate_frame(self, frame):
        # TODO: SOME ADEQUATE VALIDATION HERE
        if type(frame) == DataFrame:
            log.info("its a frame!")
            return True
        elif type(frame) == dict:
            log.info("its a dictionary representing an sslog!")
            return True
        else:
            log.info("Its not a frame.")
            return False

    def process_resources(self, resources):
        processed, cnt, errors = [], 0, 0
        for resource in resources:
            print("Processing %s", resource)
            processed += [self.process(resource[0], resource[1])]
            cnt += 1
            #except:
            #    log.warn("Not able to process the resource %s, skipping", resource)
            #    errors += 1
        log.info("Processed %s resources while encountering %s errors.", cnt, errors)
        return processed

    def register_data_frame(self, resource_id, data_frame_id, fname):
        log.info("Registering the dataframe with resource id %s", resource_id)
        self.tx_dict[data_frame_id] = {'registration_time':time.time(), 'resource_id':[x[0] for x in resource_id],
                                       'datasource':resource_id[0][1], 'frame_path': fname}
        self.log.info("Sucessfuly registered the resources %s to chunk %s", resource_id, data_frame_id)

    def over_time(self, name):
        if name in self.resource_dict:
            if not self.resource_dict[name][0] - time.time() > self.retransmission_time:
                return False
        return True

    def callback_frames(self):
        in_keys = set(self.in_pipe.keys())
        if not in_keys:
            return []
        completed_resources = []
        for key in in_keys:
            if key in self.tx_dict:
                todo = self.tx_dict[key]['resource_id']
                completed = []
                self.log.info("Found the callback info %s with key %s", self.tx_dict[key], key)
                for resource_id in todo:
                    trans = self.remove_resource(resource_id)
                    if trans:
                        completed += [resource_id]
                    else:
                        self.log.warn("The resource %s doesn't seem to exist along its path. ID: %s", self.tx_dict[key], resource_id)
                        del self.in_pipe[key]
                        del self.tx_dict[key]
                if completed == todo:
                    self.log.info("Sucessfully removed all callbacks associated with %s", key)
                    completed_resources += [key]
        for key in completed_resources:
            self.log.info("Deleting the key %s from persistence", key)
            del self.in_pipe[key]
            resource_data = self.tx_dict[key]['resource_id']
            for resource_id in resource_data:
                if resource_id in self.resource_dict:
                    del self.resource_dict[resource_id]
            del self.tx_dict[key]

    def run(self):
        # reinitialize the log after forking, this is necessary on Windows
        # and probably not a terrible idea in UNIX
        log = setup_log(self.logname, self.log_level)
        sys.modules[self.__class__.__module__].log = log
        self.log = log

        if os.name == 'nt':
            global_manager.dict = self.__dict__[
                '_Win32ServiceManager__global_dict']

        log.info("Running {} plugin...".format(self.name))
        # Create output directory if it is not created
        # todo, should have some (signal based?) way
        # to exit the service so we can join()
        if not os.path.exists(self.outputdirectory):
            os.makedirs(self.outputdirectory)

        try:
            self.connect()
        except Exception as e:
            log.error('Failed to connect to {}'.format(self.host))
            log.exception(e)
            self.reconnect()

        while self._running:
            log.info("%s: Looking for my data", self.logname)
            try:
                self.log.info("%s: Detecting New Records", self.host)
                resources = self.read()
                self.log.info("Found %s records, registering...", len(resources))
                resources = self.register_resources(resources)
                self.log.info("Registered the records, now processing")
                processed = self.process_resources(resources)
                self.log.info("Finished processing the new records")
                found_records = False
                for proc in processed:
                    found_records = True
                    self.emit_records(proc)
                if not found_records:
                    log.info("Found no records to process on this run")
            except Exception as e:
                self.log.exception('Failed to read data from "%s": %r', self.host, e)
                self._connected = False
                self.reconnect()
                continue
            log.info("Completed search for the data for the dataplugin %s", self.logname)

            # sleep by 0.1
            for _ in range(int(self.poll_rate / 0.1)):
                self.callback_frames()
                sleep(0.1)
                if not self._running:
                    break


class DataPlugin(DataPluginAbstract, multiprocessing.Process, DataService):
    pass


class DataPluginThread(DataPluginAbstract, threading.Thread, DataService):

    def __init__(self):
        super(DataPluginThread, self).__init__()
        self.daemon = True

        name = self.getName()
        new_name = name.replace('Thread', self.__class__.__name__)
        self.setName(new_name)

    def stop(self):
        self._running = False
