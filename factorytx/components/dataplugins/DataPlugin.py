""" The dataplugin code is responsible for managing the suite of data acquisition microservices.
    The module is responsible for the primary logic of accounting and transmitting incoming data.
"""
import multiprocessing
import os
import sys
import logging
import pickle
import threading
from abc import abstractmethod, ABCMeta
from time import sleep
from time import time as tme
from factorytx.DataService import DataService
from factorytx.managers.GlobalManager import global_manager
from factorytx.Global import setup_log
from factorytx import utils
from pandas import DataFrame
from itertools import chain
GLOBAL_MANAGER = global_manager()


LOG = logging.getLogger("Data Plugin")
try:
    import ujson as json
except Exception as import_error:
    LOG.warning("There was an error importing the ujson module: %s", import_error)
    import json

class DataPluginAbstract(object):
    """
    Data-plugin is a framework for building plugins that can handle
    asynch and offline processing of captured factorytx data

    We subclass the dataservice, but it can be used as a polling service,
    or as a post-processing service
    """
    __version__ = "0.1"
    __metaclass__ = ABCMeta

    logname = 'DataPlugin'
    reconnect_timeout = 5  # seconds
    reconnect_attempts = float('inf')  # Keep retrying until connected
    lastvalue = None
    records_per_file = 5000
    resource_dict = {}
    retransmission_time = 1000

    counter = 1

    def __init__(self):
        """ A DataPlugin is a microservice that runs by injesting data, this initializes the status
            of a particular plugin.
        """
        super(DataPluginAbstract, self).__init__()
        self._connected = False
        self._running = True
        self.client = None
        self.resource_dict = {}
        self.tx_dict = {}

    def __del__(self):
        """ Disconnects this plugin from the FTX instance. """
        try:
            self.disconnect()
        except Exception as disconnect_problem:
            self.log.warning("There was a problem disconnecting this plugin: %s", disconnect_problem)

    def __repr__(self):
        """ Prints the plugin class as well as the individual plugin name of this plugin. """
        return "<Plugin {} {}>".format(self.__class__.__name__, self.name)

    def load_parameters(self, sdconfig, schema, conf):
        """ Given a SDCONFIG, SCHEMA, and CONF files, goes ahead and loads the configuration and
            parameters needed to start this dataplugin.
        """
        super(DataPluginAbstract, self).load_parameters(sdconfig, schema, conf)

    @abstractmethod
    def read(self):
        """ This method must be defined in every plugin that subclasses the DataPlugin Class """
        pass

    @abstractmethod
    def remove_resource(self, resource_id):
        """ Given a RESOURCE_ID, adequately removes a resource from persistence.

        :param resource_id: An id that is returned by a subclass a resource.
        """
        pass

    @abstractmethod
    def process_resource(self, resource, resource_service):
        """ Given a RESOURCE object as well as a RESOURCE_SERVICE that is responsible for organizing
            and knowing about a given resource, processed the resource and returns the data ready
            to be forwarded to the transform/TX modules.

        :param resource: This is the resource object
        :param resource_service: This may be a polling service or a server service.
        """
        pass

    def _getSource(self):
        """ Goes and gets the source for this particular microservice. """
        return self.options['source'] if 'source' in self.options else "Unknown"

    def encrypt(self, records):
        """ Given a set of RECORDS, completes at rest encryption and returns the records.

        :param records: these records are the result of parsing data that is obtained.
        """
        # TODO: Complete for at rest encryption
        return records

    def _load_plugin(self, manager, cfg):
        """ Given a MANAGER and a CFG, loads the desired plugin and returns the object.

        :param manager: The manager for the type that we are trying to load.
        :param cfg: The configuration block needed to load a plugin.
        """
        if 'config' in cfg:
            cfg['config'].update({'source': self.options['source'],
                                  'resource_dict_location': self.options['resource_dict_location']})
        obj = super(DataPluginAbstract, self)._load_plugin(manager, cfg)
        return obj

    def save_json(self, record_ids, records):
        """ Given some RECORD_IDS and a set of RECORDS, proceeds to save the records in order to
            maintain not losing any data.

        :param record_ids: One or more records that we used to compile the records.
        :param records: The result of parsing or loading uploaded data and formatting it correctly
        """
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
        if not os.path.exists(self.options['outputdirectory']):
            os.makedirs(self.options['outputdirectory'])
        # TODO: NEED TO GET THE TIMESTAMP OUT OF DATA BEFOREHAND
        timestamp = 'None' # Get earliest timestamp in data
        guid = utils.make_guid()
        fname = '_'.join((timestamp, self._getSource(), guid))
        fname = os.path.join(self.options['outputdirectory'], fname)
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
                self.options['outputdirectory'], fname, e))
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
        self.log.warning('Connection lost to %s. Trying to reconnect to %s', self.options['host'], self.logname)
        count = 0
        keep_trying = True
        while keep_trying:
            sleep(self.reconnect_timeout)
            self.log.warning('Reconnection Attempt: %s for %s', count, self.logname)
            try:
                self.connect()
            except Exception as e:
                self.log.warning("Connection Error: %s for %s", e, self.logname)
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
        records_id, records = records
        self.log.debug("The record info is %s", records_id)
        self.log.info("Emitting %s records for the plugin %s", len(records), self.logname)
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
            self.log.error("There doesn't seem to be an indication where this frame is persisted",
                           frame_info)
            return None

    def convert_records(self, frame):
        print("Converting the frame %s", frame.keys())
        return DataFrame(frame)

    def push_frame(self, datasource, frame_id, frame):
        if self.validate_frame(frame):
            frame_data = self.tx_dict[frame_id]
            self.log.info("Transmitting the dataframe %s", frame_data)
            frame_data['transmission_time'] = tme()
            self.tx_dict[frame_id] = frame_data
            self.log.info("Marked the time for %s", frame_id)
            self.out_pipe.put({'frame_id':frame_id, 'datasource':datasource, 'frame':frame})
            self.log.info("Pushed out a new dataframe with %s indexes.", len(frame))
        else:
            self.log.info("Failed to validate frame")

    def validate_frame(self, frame):
        # TODO: SOME ADEQUATE VALIDATION HERE
        if isinstance(frame, DataFrame):
            self.log.info("its a frame!")
            return True
        elif isinstance(frame, list):  # HACK
            self.log.info("its a list of sslog dictionaries!")
            return True
        else:
            self.log.info("Its not a frame.")
            return False

    @abstractmethod
    def process_resources(self, resources):
        pass

    def register_data_frame(self, resource_id, data_frame_id, fname):
        self.log.info("Registering the dataframe with resource id %s", resource_id)
        self.tx_dict[data_frame_id] = {'registration_time':tme(),
                                       'resource_id': [x[0] for x in resource_id],
                                       'datasource':resource_id[0][1], 'frame_path': fname}
        self.log.info("Sucessfuly registered the resources %s to chunk %s", resource_id, data_frame_id)

    def over_time(self, name):
        if name in self.resource_dict:
            if not self.resource_dict[name][0] - tme() > self.retransmission_time:
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
                        self.log.warn("The resource %s doesn't seem to exist along its path. ID: %s",
                                      self.tx_dict[key], resource_id)
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
        log = setup_log(self.logname, self.options['log_level'])
        sys.modules[self.__class__.__module__].log = log
        self.log = log

        if os.name == 'nt':
            GLOBAL_MANAGER.dict = self.__dict__[
                '_Win32ServiceManager__global_dict']

        self.log.info("Running {} plugin...".format(self.name))
        # Create output directory if it is not created
        # todo, should have some (signal based?) way
        # to exit the service so we can join()
        if not os.path.exists(self.options['outputdirectory']):
            os.makedirs(self.options['outputdirectory'])

        try:
            self.connect()
        except Exception as e:
            self.log.error('Failed to connect to {}'.format(self.options['host']))
            self.log.exception(e)
            self.reconnect()

        while self._running:
            self.log.info("%s: Looking for my data", self.logname)
            try:
                self.log.info("%s: Detecting New Records", self.options['host'])
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
                    self.log.info("Found no records to process on this run")
            except Exception as e:
                self.log.exception('Failed to read data from "%s": %r', self.options['host'], e)
                self._connected = False
                self.reconnect()
                continue
            self.log.info("Completed search for the data for the dataplugin %s", self.logname)

            # sleep by 0.1
            print("The vars that I haave are", vars(self))
            print("The options I have are", self.options)
            for _ in range(int(self.options['poll_rate'] / 0.1)):
                self.callback_frames()
                sleep(0.1)
                if not self._running:
                    break


class DataPlugin(DataPluginAbstract, multiprocessing.Process, DataService):

    __metaclass__ = ABCMeta

    pass


class DataPluginThread(DataPluginAbstract, threading.Thread, DataService):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(DataPluginThread, self).__init__()
        self.daemon = True

        name = self.getName()
        new_name = name.replace('Thread', self.__class__.__name__)
        self.setName(new_name)

    def stop(self):
        self._running = False
