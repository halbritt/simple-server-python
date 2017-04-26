import os
import json
from pandas import DataFrame
from factorytx.components.dataplugins.resource import Resource

class RDP1Payload(Resource):

    def __init__(self, payload):
        print("making an RDP payload from %s", payload)
        data = payload['data'].split(':')
        self.mtime = data[0]
        self.data_name = payload['data']
        self.poll_name = payload['poll']
        self.payload = payload
        self.path = payload['path']
        self.name = self.encode('utf8')

    def load_resource(self):
        with open(os.path.join(self.path, self.payload['data']), 'rb') as f:
            rawbody = json.loads(f.read())
        return DataFrame(rawbody)

    @property
    def basename(self):
        return self.name

    def encode(self, encoding):
        return self.data_name

    def __eq__(self, other):
        return self.data_name == other.data_name and \
               self.mtime == other.mtime and \
               self.data_name == other.data_name

    def __lt__(self, other):
        return float(self.mtime) + hash(self.data_name) < float(other.mtime) + hash(other.data_name)

    def __hash__(self):
        return hash((self.data_name, self.poll_name, self.mtime, self.path))

    def __repr__(self):
        return "{RDP1Payload: uploaded: %s, file_name: %s}" % (self.mtime, self.path)
