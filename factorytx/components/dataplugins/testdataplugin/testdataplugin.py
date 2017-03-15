from datetime import datetime

from FactoryTx.Global import setup_log
from FactoryTx.components.dataplugins.DataPlugin import DataPlugin

log = setup_log('testdataplugin')


class TestDataPlugin(DataPlugin):
    """
    This is the polling service for an OPC UA server.

    You need to have the following specified in the config:

    machines:
    - plugins:
      - type: testdataplugin
        name: Test Data Plugin
        config:
          version: '1.0.0'
          host: google.com
          port: 80
          poll_rate: 300
          reconnect_attempts: 3
          outputdirectory: /path/to/data/dir/
    """

    __version__ = '1.0.0'

    reconnect_timeout = 5  # seconds
    reconnect_attempts = 5
    connection_calls = 0
    read_calls = 0
    load_raw_calls = 0
    save_raw_calls = 0
    process_calls = 0


    def connect(self):
        self.connection_calls += 1
        return super(TestDataPlugin, self).connect()

    def disconnect(self):
        return super(TestDataPlugin, self).disconnect()


    def read(self):
        self.read_calls += 1

        return [(timestamp, [c.value for c in response[1].ResultSets.ResultSet[0].Rows.Row[0].Columns[0]])]

    def save_raw(self, records):
        pass
        #todo

    def load_raw(self, source):
        pass
        #todo

    def process(self, record):
        t, probe = record

        data = {}
        data['timestamp'] = t
        data['fieldvalues'] = {}
        
        if probe.get("source"):
            data['source'] = probe.pop('source')

        for field in self.fields:
            data['fieldvalues'][field['name']] = { 'value': probe[field['column']]}

        data['poll_rate'] = self.poll_rate

        return data
