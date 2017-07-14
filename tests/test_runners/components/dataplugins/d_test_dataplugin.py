import os
import shutil
import json
import unittest
from datetime import datetime

from testfixtures import Comparison, compare
import yaml

from FactoryTx.components.dataplugins.testdataplugin.testdataplugin import TestDataPlugin
from FactoryTx.managers.PluginManager import component_manager

plugins = component_manager()['dataplugins']


class DataPluginTestCase(unittest.TestCase):

    config_yaml = """
machines:
- source: DataPluginTestCase_Source
  plugins:
  - type: testdataplugin
    name: Test Data Plugin
    config:
      version: '1.0.0'
      host: google.com
      port: 80
      poll_rate: 300
      reconnect_attempts: 3
      fields:
        - name: number
          column: number
      outputdirectory: /tmp/dataplugintest/
"""

    @classmethod
    def setUpClass(cls):
        cls.pm = plugins
        cls.pm.load_schemas()
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.config = cls.plugin_config['machines'][0]['plugins'][0]['config']

    def setUp(self):
        self.plugin = TestDataPlugin()
        self.plugin.loadParameters(
            {}, self.pm.get_plugin_schema('testdataplugin', '1.0.0'),
            self.config)
        if os.path.exists(self.plugin.outputdirectory):
            shutil.rmtree(self.plugin.outputdirectory)

    # test that we can instantiate the plugin
    def test_plugin_connect(self):
        self.assertFalse(self.plugin._connected)
        self.plugin.connect()
        self.assertTrue(self.plugin._connected)
        self.plugin.disconnect()
        self.assertFalse(self.plugin._connected)

    # test that we reconnect after 3 tries
    def test_reconnect(self, error_fixture):

        class DontConnectPlugin(TestDataPlugin):
            def connect(self):
                super(DontConnectPlugin, self).connect()
                self._connected = False
                return False
        self.plugin.reconnect_timeout = 0.01
        self.plugin.__class__ = DontConnectPlugin

        with self.assertRaises(Exception):
            self.plugin.reconnect()

        if not self.plugin.connection_calls == 3:
            error_fixture.submit_error("Wrong number of connections", [self.plugin.connection_calls, 3])
        error_fixture.report_errors()

    def unix_time(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return delta.total_seconds()

    def unix_time_millis(self, dt):
        return self.unix_time(dt) * 1000.0

    def test_save_json(self):
        # test if there are 2 records with the same timestamp
        ts = datetime(2015, 5, 4, 11, 47, 39)
        self.plugin.save_json([(ts, {"timestamp": ts, "number": 100}),
                               (ts, {"timestamp": ts, "number": 200})])

        self.assertTrue(os.path.exists(self.plugin.outputdirectory))
        files = os.listdir(self.plugin.outputdirectory)
        self.assertEquals(len(files), 1)

        with open(os.path.join(self.plugin.outputdirectory, files[0])) as f:
            data = json.load(f)

        d = {'2015-05-04T11:47:39.000000': {'_id': Comparison(str),
                                            'counter': 1,
                                            'fieldvalues': {
                                                'number': {'value': 100}
                                            },
                                            'poll_rate': 300,
                                            'source': 'Unknown',
                                            'timestamp':
                                                '2015-05-04T11:47:39.000000'},
             '2015-05-04T11:47:39.000001': {'_id': Comparison(str),
                                            'counter': 2,
                                            'fieldvalues': {
                                                'number': {'value': 200}
                                            },
                                            'poll_rate': 300,
                                            'source': 'Unknown',
                                            'timestamp':
                                                '2015-05-04T11:47:39.000000'}}
        compare(d, data)

        distinct_ids = set(sslog['_id'] for sslog in data.values())
        self.assertEqual(len(data), len(distinct_ids),
                         msg="sslogs ids should be globally unique")

    def test_save_json_duplicate_timestamps(self):
        ts = datetime(2015, 5, 4, 11, 47, 39)
        self.plugin.save_json([(ts, {"timestamp": ts, "number": 100})])
        self.plugin.save_json([(ts, {"timestamp": ts, "number": 101})])
        files = os.listdir(self.plugin.outputdirectory)
        self.assertEquals(len(files), 2)

    def test_plugin_source(self):
        # test if the plugin can set source 
        self.plugin.source = "Machine_1,Machine_2"
        ts = datetime(2015, 5, 4, 11, 47, 39)
        self.plugin.save_json([(ts, {"timestamp": ts, "number": 100, "source": "Machine_1"}),
                               (ts, {"timestamp": ts, "number": 200, "source": "Machine_2"})])

        self.assertTrue(os.path.exists(self.plugin.outputdirectory))
        files = os.listdir(self.plugin.outputdirectory)
        self.assertEquals(len(files), 1)

        with open(os.path.join(self.plugin.outputdirectory, files[0])) as f:
            data = json.load(f)

        d = {'2015-05-04T11:47:39.000000': {'_id': Comparison(str),
                                            'counter': 1,
                                            'fieldvalues': {
                                                'number': {'value': 100}
                                            },
                                            'poll_rate': 300,
                                            'source': 'Machine_1',
                                            'timestamp':
                                                '2015-05-04T11:47:39.000000'},
             '2015-05-04T11:47:39.000001': {'_id': Comparison(str),
                                            'counter': 2,
                                            'fieldvalues': {
                                                'number': {'value': 200}
                                            },
                                            'poll_rate': 300,
                                            'source': 'Machine_2',
                                            'timestamp':
                                                '2015-05-04T11:47:39.000000'}}
        compare(d, data)




if __name__ == '__main__':
    unittest.main()
