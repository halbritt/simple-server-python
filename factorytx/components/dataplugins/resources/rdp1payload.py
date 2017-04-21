import os
import json
from pandas import DataFrame
from factorytx.components.dataplugins.resource import Resource

class RDP1Payload(Resource):

    def __init__(self, payload):
        print("making an RDP payload from %s", payload)
        data = payload['data'].split(':')
        self.mtime = data[0]
        self.payload = payload
        self.path = payload['path']
        self.name = self.encode('utf8')

    def load_resource(self):
        with open(os.path.join(self.path, self.payload['data']), 'rb') as f:
            rawbody = json.loads(f.read())
        return DataFrame(rawbody)

    @property
    def basename(self):
        return "TODO nice basename string"

    def encode(self, encoding):
        return ','.join("something nice")

    def __eq__(self, other):
        # TODO: Nice equal function for rdp payloads
        return False

    def __hash__(self):
        # TODO: Nice hash function
        return 0

    def __repr__(self):
        return "TODO nice print string"
