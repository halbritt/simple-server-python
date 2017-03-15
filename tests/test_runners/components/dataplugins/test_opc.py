import os
import unittest
from datetime import datetime

import yaml
from mock import Mock

from FactoryTx.components.dataplugins.opc import OPCService
from FactoryTx.managers.PluginManager import component_manager

plugins = component_manager()['dataplugins']


class OPCPluginTestCase(unittest.TestCase):

    use_mock = int(os.environ.get('SIMPLEDATA_TEST_OPC_USE_MOCK', '1')) == 1

    config_yaml = """
    machines:
    - plugins:
      - type: opc
        name: OPC Service
        config:
          machine: 'Machine 55434'
          version: '1.0.0'
          host: 'localhost'
          server: 'Kepware.KEPServerEX.V5'
          poll_rate: 1
          tags:
          - { name: 'Channel1.Device1.Tag1', exportname: Channel1Device1Tag1 }
          - { name: 'Channel1.Device1.Tag2', exportname: Channel1Device1Tag2 }
          outputdirectory: /tmp/opcuaplugintest
"""

    @classmethod
    def setUpClass(cls):
        cls.pm = plugins
        cls.pm.load_schemas()
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.server = None
        cls.config = cls.plugin_config['machines'][0]['plugins'][0]['config']

    def setUp(self):
        self.plugin = OPCService()
        self.plugin.loadParameters(
            {}, self.pm.get_plugin_schema('opc', '1.0.0'), self.config)

        if self.use_mock:
            self.plugin.connect = super(OPCService, self.plugin).connect
            self.plugin.client = Mock()
            self.plugin.read = Mock()

    # test that we can instantiate the plugin
    def test_plugin_connect(self):
        self.assertFalse(self.plugin._connected)
        self.plugin.connect()
        self.assertTrue(self.plugin._connected)
        self.plugin.disconnect()
        self.assertFalse(self.plugin._connected)

    def getRead(self, mode="GOOD"):

        if self.use_mock:
            fake_opc_record = [
                datetime(2015, 7, 23, 22, 25, 59, 487348),
                [('Channel1.Device1.Tag1', 2, mode,
                  datetime(2015, 7, 23, 22, 25, 59, 487348)),
                 ('Channel1.Device1.Tag2', 0, mode,
                  datetime(2015, 7, 23, 22, 25, 59, 487348))]]

            return fake_opc_record
        else:
            self.plugin.connection_mode = mode
            return self.plugin.read()

    def test_read(self):
        self.plugin.connect()
        ts, values = self.getRead()

        self.assertTrue(ts.__class__.__name__ == "datetime")
        self.assertTrue(values[0][0] == self.plugin.tags[0]['name'])
        self.assertTrue(values[0][1] > 0)
        self.assertTrue(values[1][1] == 0)

        ts, dvalues = self.getRead(mode="DEGRADED")
        self.assertTrue(ts.__class__.__name__ == "datetime")
        self.assertTrue(dvalues[0][0] == self.plugin.tags[0]['name'])
        self.assertTrue(dvalues[0][1] > 0)
        self.assertTrue(dvalues[1][1] == 0)
        self.assertTrue(dvalues[0][2].upper() == "DEGRADED")

        self.plugin.disconnect()

    def test_process(self):
        self.plugin.connect()
        t, values = self.getRead()

        sslog = self.plugin.process([t, values])
        self.assertEqual(sslog['timestamp'],
                         datetime(2015, 7, 23, 22, 25, 59, 487348))
        self.assertEqual(
            sslog['fieldvalues']['Channel1Device1Tag1']['value'],
            values[0][1])
        self.assertEqual(
            set(sslog['fieldvalues'].keys()),
            set([t['exportname'] for t in self.plugin.tags]))


if __name__ == '__main__':
    unittest.main()
