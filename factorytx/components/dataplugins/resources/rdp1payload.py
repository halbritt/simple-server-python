import os
from bson import objectid
from datetime import datetime
from dateutil import parser
import json
from pandas import DataFrame
from factorytx.components.dataplugins.resource import Resource

class RDP1Payload(Resource):

    def __init__(self, payload):
        print("making an RDP payload from %s", payload)
        data = payload['data'].split('--')
        self.mtime = data[0]
        self.data_name = payload['data']
        self.poll_name = payload['poll']
        self.headers = payload['headers']
        with open(os.path.join(payload['path'], payload['headers']), 'r') as f:
            json_data = json.loads(f.read())
            print("Found the RDP headers %s.", json_data)
            if 'Original_Filename' in json_data:
                self.original_filename = json_data['Original_Filename']
                self.original_content_type = json_data['Original_Content_Type']
                self.original_size = json_data['Original_Size']
        if 'binaryattachment' in payload:
            self.binaryattachment = os.path.join(payload['path'], payload['binaryattachment'])
        else:
            self.binaryattachment = None
        self.payload = payload
        self.path = payload['path']
        self.name = self.encode('utf8')

    def load_resource(self):
        attachment_dic = {}
        with open(os.path.join(self.path, self.payload['data']), 'rb') as f:
            rawbody = json.loads(f.read())
        with open(os.path.join(self.path, self.payload['headers']), 'rb') as f:
            headers = json.loads(f.read())
        capture_time = headers['Capture_Time']
        if self.binaryattachment:
            attachment_dic['binary'] = self.binaryattachment
            attachment_dic['original_file'] = self.original_filename
            attachment_dic['original_content'] = self.original_content_type
            attachment_dic['original_size'] = self.original_size
            rawbody = {rawbody['timestamp']: rawbody}
        else:
            attachment_dic['binary'] = False
            attachment_dic['original_file'] = False
            attachment_dic['original_content'] = False
            attachment_dic['original_size'] = False
            capture_time = capture_time[0]
        sslogs = self.format_sslogs(rawbody, capture_time, attachment_dic['original_content'])
        return sslogs, attachment_dic

    def format_sslogs(self, rawlogs, capture_time, original_content=False):
        new_logs = {}
        print("The logs are %s", rawlogs)
        for key in rawlogs:
            log = rawlogs[key]
            try:
                log_id = log['_id']
                del log['_id']
                log['timestamp'] = parser.parse(log['timestamp'])
                if original_content:
                    content_type = original_content
                else:
                    content_type = None
                log_data = log
                try:
                        dt = datetime.strptime(capture_time, '%Y-%m-%dT%H:%M:%S.%f')
                except ValueError:
                        dt = datetime.strptime(capture_time, '%Y-%m-%dT%H:%M:%S')
                sslog = {'_id': objectid.ObjectId(log_id), 'content_type': content_type, 'data': log_data,
                         'capturerecords': [{'capturetime': dt, 'hostname': self.poll_name}]}
                new_logs[key] = sslog
            except Exception as e:
                print("The exception to sslog formatting is", e)
                raise
        return new_logs 

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
        UNIX_EPOCH = datetime(1970, 1, 1)
        try:
                selftime = datetime.strptime(self.mtime, '%Y-%m-%dT%H:%M:%S.%f')  # implicitly UTC
        except ValueError:
                selftime = datetime.strptime(self.mtime, '%Y-%m-%dT%H:%M:%S')
        try:
                othertime = datetime.strptime(other.mtime, '%Y-%m-%dT%H:%M:%S.%f')  # implicitly UTC
        except ValueError:
                othertime = datetime.strptime(other.mtime, '%Y-%m-%dT%H:%M:%S')

        self_ms = (selftime - UNIX_EPOCH).total_seconds()
        other_ms = (othertime - UNIX_EPOCH).total_seconds()
        return self_ms + hash(self.data_name) < other_ms + hash(other.data_name)

    def __hash__(self):
        return hash((self.data_name, self.poll_name, self.mtime, self.path))

    def __repr__(self):
        return "{RDP1Payload: uploaded: %s, file_name: %s}" % (self.mtime, self.path)
