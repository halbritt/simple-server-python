import multiprocessing
import time
import os
import sys
import logging
import shelve
from datetime import timedelta

from bson import ObjectId

from time import sleep
from uuid import uuid4
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

if global_manager.get_encryption():
    from cryptography.fernet import Fernet


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
        self.resource_dict = shelve.open(os.path.join(self.resource_dict_location, self.resource_dict_name + 'plugin_resources'))
        self.tx_dict = shelve.open(os.path.join(self.resource_dict_location, self.resource_dict_name + 'tx-chunks'))

    def read(self):
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

    def _makeShareData(self, data):
        global_manager_dict = global_manager.get_dict()

        global_manager_dict[self.share] = data['fieldvalues']
        log.debug("global_manager_dict['{}']".format(self.share))

    def save_share(self, records):
        if records:
            self._makeShareData(self.process(records[-1]))

    def encrypt(self, records):
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

    def save_json(self, record_id, records):
        new_records = []
        print("The records we are saving have length %s", len(records))
        record = self.encrypt(records)
        try:
            chunks = utils.df_chunks(record, self.records_per_file)
        except Exception as e:
            log.error("The exception is %s for the chunking", e)
            print("There is a problem")
        for chunk in chunks:
            json_data = json.dumps(chunk)
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
            except Exception as e:
                log.error('Failed to save data into {} {} {}'.format(
                    self.outputdirectory, fname, e))
                raise
            else:
                self.log.info('Saved data into {}'.format(fname))
            self.register_data_frame(record_id, guid, dst_fname)
            new_records.append((record_id, guid, chunk))
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

    def reconnect(self):
        # If we got here, we probably aren't connected
        self._connected = False

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
        log.info("Emitting %s records for the plugin %s", len(records), self.logname)
        log.debug("The record info is %s", records[0])
        records_id, records = records
        use_share = getattr(self, 'share', False)
        use_raw_log = getattr(self, 'raw_log', False)
        share_and_save = getattr(self, 'share_and_save', False)

        if use_share:
            self.save_share(records)
            if share_and_save:
                records = self.save_json(records_id, records)
        elif use_raw_log:
            self.save_raw(records)
        else:
            self.log.info("Persisting %s records in JSON", len(records))
            records = self.save_json(records_id, records)
            self.log.info("Saved the JSON")
        if len(records) > 0:
            for record in records:
                self.log.info("Passing the records with id %s onto the next component", record[0])
                frame = self.convert_records(record[2])
                self.push_frame(record[0][1], record[1], frame)
        else:
            self.log.info("There are no records to forward")

    def check_and_emit_old_records(self):
        log.info("Checking if there are more records to be found for %s", self.logname)
        print("The in pipe contents are %s", self.in_pipe.items())
        log.debug("My I know of %s resources", len(self.resource_dict))
        unprocessed, untxed = self.get_unprocessed_resources()
        if len(unprocessed) > 0:
            self.log.info("Processing the resources %s", unprocessed)
            processed = self.process_resources(unprocessed)
            self.log.info("Persisting the records in JSON %s", len(processed))
            log.debug("The records are %s", processed)
            for record_id, record in processed:
                saved = self.save_json(record_id, record)
            self.log.info("Pushing the records")
            for record in saved:
                frame = self.convert_records(record[2])
                self.push_frame(record[0][1], record[1], frame)
        if len(untxed) > 0:
            self.log.info("Transmitting  untxed data chunks")
            log.debug("The untxed are %s.", untxed)
            for untx in untxed:
                frame_id = untx[0]
                datasource = untx[1]['datasource']
                frame_path = untx[1]['frame_path']
                if os.path.exists(frame_path):
                    log.debug("Loading the frame from %s")
                    with open(frame_path, 'r') as f:
                        frame = read_json(f)
                    self.log.info("Transmitting the frame %s again.")
                    self.push_frame(datasource, frame_id, frame)
                else:
                    self.log.error("The frame doesn't exist to be txed: %s.", frame_id)


    def get_corresponding_chunks(self, resource):
        log.debug("Trying to match the resource %s to its chunks", resource)
        log.debug("The TX dict is %s.", [x for x in self.tx_dict.items()])
        chunks = [(x, self.tx_dict[x]) for x in self.tx_dict if self.tx_dict[x]['resource_id'] == resource[0]]
        log.debug("The chunks are %s", chunks)
        return chunks

    def register_resources(self, resources):
        for resource, obj in resources:
            self.log.info("Registering %s", resource)
            self.resource_dict[resource[0]] = obj.encode("utf-8")
        return resources

    def get_unprocessed_resources(self):
        unprocessed, untxed = [], []
        for poll in self.pollingservice_objs:
            registered = poll.get_registered_resources()
            for resource in registered:
                if not resource[0] in self.resource_dict:
                    resource_id = resource[0]
                    self.log.info("The resource %s is not registered here.", resource)
                    arguments = resource[1].split(',')
                    log.debug("The resource arguments are %s", arguments)
                    resource = poll.return_resource_class()(poll, *arguments)
                    unprocessed += [(resource_id, resource)]
                else:
                    self.log.info("This resource %s has been previously processed and is persisted", resource)
                    corresponding = self.get_corresponding_chunks(resource)
                    untxed = self.filter_corresponding(corresponding)
                    transform = []
                    for frame_id, resource in untxed:
                        self.log.info("Checking the status of %s", frame_id)
                        if frame_id in self.in_pipe:
                            self.log.info("Found some transformation on the frame %s", frame_id)
                        else:
                            self.log.warn("Found evidence of processing for %s, but no reference in a TX module, reprocessing.", frame_id)
                            transform += [(frame_id, resource)]
                    untxed = transform
        self.log.info("Returning %s resources to be processed from the unprocessed function.", len(unprocessed))
        log.debug("The unprocessed entries are %s, %s", unprocessed, untxed)
        return unprocessed, untxed

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

    def filter_corresponding(self, pieces):
        self.log.info("The corresponding pieces are %s", pieces)
        survived = []
        for piece in pieces:
            frame_info = piece[1]
            frame_id = piece[0]
            self.log.info("Filtering out based on criteria")
            self.log.info(piece)
            if 'cleaned' in frame_info:
                self.log.debug("The piece %s has been scrubbed", frame_id)
            elif frame_id in self.in_pipe:
                self.log.info("The piece %s has been recieved from the transform, cleaning now", frame_id)
                confirm = self.cleanup_frame(frame_id)
                if confirm:
                    self.log.info("Scrubbed the persistence for the frame %s", frame_id)
                    frame_info['cleaned'] = time.time()
                    self.tx_dict[frame_id] = frame_info
                elif confirm == False:
                    self.log.warn("This frame seems to have been cleaned already %s", frame_id)
                else:
                    self.log.error("The frame %s wansn't persisted or possibly processed.")
            elif not 'transmission_time' in piece[1] or time.time() - piece[1]['transmission_time'] < 120:
                self.log.info("This piece has been recently transmitted, and will not be retransmitted now: %s", piece[0])
            else:
                survived.append(piece)
        return survived

    def convert_records(self, frame):
        print("Converting the frame %s", frame.keys())
        return DataFrame(frame)

    def push_frame(self, datasource, frame_id, frame):
        if self.validate_frame(frame):
            log.info("Transmitting the dataframe %s", self.tx_dict[frame_id])
            frame_data = self.tx_dict[frame_id]
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
        else:
            log.info("its not a frame.")
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
        self.tx_dict[data_frame_id] = {'registration_time':time.time(), 'resource_id':resource_id[0],
                                       'datasource':resource_id[1], 'm_time':resource_id[2],
                                       'frame_path': fname}
        self.log.info("Sucessfuly registered the resource %s to chunk %s", resource_id, data_frame_id)

    def over_time(self, name):
        if name in self.resource_dict:
            if not self.resource_dict[name][0] - time.time() > self.retransmission_time:
                return False
        return True

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
                if len(processed) > 0:
                    print("Emitting some new records of length %s", len(processed))
                    for proc in processed:
                        self.emit_records(proc)
                else:
                    self.log.warn("No records found on this run")
                self.log.info("%s: Finished emitting records, checking for old", self.host)
                self.check_and_emit_old_records()
            except Exception as e:
                self.log.exception('Failed to read data from "%s": %r', self.host, e)
                self._connected = False
                self.reconnect()
                continue
            log.info("Completed search for the data for the dataplugin %s", self.logname)

            # sleep by 0.1
            for _ in range(int(self.poll_rate / 0.1)):
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
