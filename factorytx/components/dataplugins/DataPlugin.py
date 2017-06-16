""" The dataplugin code is responsible for managing the suite of data acquisition microservices.
    The module is responsible for the primary logic of accounting and transmitting incoming data.
"""
import multiprocessing
import os
import sys
import logging
import pickle
import threading
import ujson as json
from abc import abstractmethod, ABCMeta
from time import sleep
from time import time as tme
from factorytx.DataService import DataService
from factorytx.managers.GlobalManager import global_manager
from factorytx.Global import setup_log
from factorytx import utils
from pandas import DataFrame
from itertools import chain
from factorytx.components.dataplugins.resources.processedresource import ProcessedResource
GLOBAL_MANAGER = global_manager()


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
        self.options = {}
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

    def read(self):
        resource_entries = []
        process_cnt = 0
        self.log.info("Looking for files in the FileTransport object.")
        for polling_obj in self.pollingservice_objs:
            new_entries = polling_obj.poll()
            for resource in new_entries:
                if resource[0][0] in self.resource_dict:
                    self.log.warn("The polling service says the new entry %s with id %s is already registered!", resource[1], resource[0])
                    continue
                self.log.debug("Processing the resource %s", resource)
                yield resource

    @abstractmethod
    def remove_resource(self, resource_id: str):
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
            cfg['config'].update({'source': self.options['source']})
        obj = super(DataPluginAbstract, self)._load_plugin(manager, cfg)
        return obj

    def save_json(self, records: ProcessedResource):
        """ Given some a set of RECORDS with type ProcessedResource, proceeds to save the records 
            in order to maintain not losing any data.

        :param records: The result of parsing or loading uploaded data and formatting it correctly
        """
        print("The records we are saving have length %s", len(records))
        # record = self.encrypt(records) for at rest encryption
        raw_records = True
        record_string = records.to_record_string()
        print("The options for us are:", self.options)
        self.log.info("Registering the records %s that we have persisted", records)
        self.register_data_frame(records)
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
        records = self.save_json(records)
        self.log.info("Saved the JSON")
        self.push_frame(records)

    def register_resources(self, resources):
        for res in resources:
            print("The res is %s", res)
            resource, obj = res
            self.log.info("Registering %s", resource)
            self.resource_dict[resource[0]] = obj.encode("utf-8")
            yield res

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

    def push_frame(self, frame):
        if self.validate_frame(frame):
            frame_data = self.tx_dict[frame.name]
            self.log.info("Transmitting the dataframe %s", frame_data)
            frame_data.transmission_time = tme()
            self.tx_dict[frame.name] = frame
            self.log.info("Marked the time for %s", frame)
            self.out_pipe.put(frame)
        else:
            self.log.info("Failed to validate frame")

    def validate_frame(self, frame):
        # TODO: SOME ADEQUATE VALIDATION HERE
        return True

    @abstractmethod
    def process_resources(self, resources):
        pass

    def register_data_frame(self, records):
        self.log.info("Registering the dataframe with resource id %s", records.resource_ids)
        self.tx_dict[records.name] = records
        self.log.info("Sucessfuly registered the resources %s", records)

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
                todo = self.tx_dict[key].resource_ids
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
            resource_data = self.tx_dict[key]
            for resource_id in resource_data.resource_ids:
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
                self.log.info("Found possible records, registering...")
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
