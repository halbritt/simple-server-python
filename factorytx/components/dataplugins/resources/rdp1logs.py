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

    def __init__(self, resource_ids, sslog_list):
        self.resource_ids = [x[0] for x in resource_ids]
        self.datasource = resource_ids[0][1]
        self.plugin_type = resource_ids[0][2]
        self.uuid = str(uuid4())
        self.sslog_list = sslog_list
        self.resource_data = sslog_list
        self.create_time = datetime.utcnow().isoformat()
        self.name = self.encode('utf8')
        self.index = 0

    @property
    def basename(self):
        return self.name

    def factory_method(ids, log_list):
        return RDP1Logs(ids, log_list)

    def encode(self, encoding):
        print("Trying to encode the RAWLOGS", self.create_time, self.resource_ids)
        return self.create_time + '--' + self.uuid

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
        return "{RawSSLogs: created: %s, resource_ids: %s}" % (self.create_time, ','.join(self.resource_ids))
