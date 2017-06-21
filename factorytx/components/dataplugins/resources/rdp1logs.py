import os
import pickle
from uuid import uuid4
from bson import objectid
from datetime import datetime
from dateutil import parser
import json
from pandas import DataFrame
from factorytx.components.dataplugins.resources.processedresource import ProcessedResource

class RDP1Logs(ProcessedResource):

    transformable = False

    def __init__(self, resource_ids, resource_data, resource_store, uuid=None, create_time=None):
        self.resource_store = resource_store
        self.resource_ids = [x[0] for x in resource_ids]
        self.datasource = resource_ids[0][1]
        self.plugin_type = resource_ids[0][2]
        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        if not create_time:
            self.create_time = datetime.utcnow().isoformat()
        else:
            self.create_time = create_time
        if 'sslog_list' in resource_data:
            self.sslog_list = resource_data['sslog_list']
            self.resource_data = resource_data['sslog_list']
            self.loaded = True
        else:
            self.sslog_list = {}
            self.resource_data = {}
            self.loaded = False
        self.name = self.encode('utf8')
        self.index = 0

    @property
    def basename(self):
        return self.name

    def encode(self, encoding):
        return self.create_time + '--' + self.uuid

    def persist_resource(self):
        try:
            print("The resource store is going to be %s", self.resource_store)
            if not os.path.exists(self.resource_store):
                print("Making the resource store", self.resource_store)
                os.makedirs(self.resource_store)
            persist_location = os.path.join(self.resource_store, self.name)
            with open(persist_location, 'wb') as f:
                pickle.dump(self, f)
            return RDP1Logs(self.resource_ids, {}, self.resource_store, self.uuid, self.create_time)
        except Exception as e:
            print("THERE WAS A PROBLEM PERSISTING")
            print("The error was", e)

    def remove_trace(self):
        persist_location = os.path.join(self.resource_store, self.name)
        if os.path.exists(persist_location):
            try:
                os.remove(persist_location)
                return True
            except Exception as e:
                print("There was an error removing the trace:", e)
                return False

    def load_resource(self):
        try:
            persist_location = os.path.join(self.resource_store, self.name)
            with open(persist_location, 'rb') as f:
                obj = pickle.load(f)
                self.resource_data = obj.resource_data
        except Exception as e:
            print("THERE WAS A PROBLEM LOADING.")

    def to_record_string(self):
        try:
            logs = pickle.dumps(self.resource_data)
            return logs
        except Exception as e:
            print("There was an error with pickling the list of sslogs")

    def __len__(self):
        return len(self.resource_data)

    def __eq__(self, other):
        return self.create_time == other.create_time and \
               self.resource_ids == other.resource_ids

    def __lt__(self, other):
        return self.create_time < other.create_time

    def __hash__(self):
        return hash((self.create_time, self.resource_ids))

    def __repr__(self):
        return "{RawSSLogs: created: %s, num_resource_ids: %s, uuid:%s }" % (self.create_time, len(self.resource_ids), self.uuid)
