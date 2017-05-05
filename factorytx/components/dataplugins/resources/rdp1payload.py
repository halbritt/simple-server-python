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
        with open(os.path.join(payload['path'], payload['headers']), 'r') as f:
            json_data = json.loads(f.read())
            print("Found the RDP headers %s.", json_data)
            if 'Original_Filename' in json_data:
                self.original_filename = json_data['Original_Filename']
                self.original_content_type = json_data['Original_Content_Type']
        if 'binaryattachment' in payload:
            self.binaryattachment = os.path.join(payload['path'], payload['binaryattachment'])
        else:
            self.binaryattachment = None
        self.payload = payload
        self.path = payload['path']
        self.name = self.encode('utf8')

    def load_resource(self):
        with open(os.path.join(self.path, self.payload['data']), 'rb') as f:
            rawbody = json.loads(f.read())
        if self.binaryattachment:
            binary = self.binaryattachment
            original_file = self.original_filename
            original_content = self.original_content_type
        else:
            binary = False
            original_file = False
            original_content = False
        return rawbody, binary, original_file, original_content

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
