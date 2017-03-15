import os
import time
import unittest
from datetime import datetime

import yaml
from mock import patch


class OPCUAPluginTestCase(unittest.TestCase):

    use_mock = int(os.environ.get('SIMPLEDATA_TEST_OPC_USE_MOCK', '1')) == 1

    config_yaml = """
    machines:
    - plugins:
      - type: opcua
        name: OPC UA Service
        config:
          machine: 'Machine 55434'
          version: '1.0.0'
          host: '127.0.0.1'
          port: 4841
          poll_rate: 0.1
          tags:
          - { name: 'Channel1.Device1.Tag1', exportname: Channel1Device1Tag1 }
          - { name: 'Channel1.Device1.Tag2', exportname: Channel1Device1Tag2 }
          outputdirectory: /tmp/opcuaplugintest
"""

    @classmethod
    def setUpClass(cls):
        import FactoryTx.components.dataplugins.opcua as opcua

        from FactoryTx.managers.PluginManager import component_manager

        man = component_manager()
        cls.pm = man['dataplugins']
        cls.pm.load_schemas()
        cls.schema = cls.pm.get_plugin_schema('opcua', '1.0.0')
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.server = None

        if not cls.use_mock:
            server = opcua.Server()
            server.set_endpoint("opc.tcp://127.0.0.1:4841")
            server.start()

            uri = "http://testopc.sightmachine.com"
            idx = server.register_namespace(uri)

            objects = server.get_objects_node()

            myfolder = objects.add_folder(idx, "Channel1")
            myfolder2 = myfolder.add_folder(idx, "Device1")
            cls.server = server
            cls.myvariable1 = myfolder2.add_variable(idx, "Tag1", 0)
            cls.myvariable2 = myfolder2.add_variable(idx, "Tag2", 1)
        cls.config = cls.plugin_config['machines'][0]['plugins'][0]['config']

    @classmethod
    def tearDownClass(cls):
        if cls.server:
            cls.server.stop()
            time.sleep(0.1)

    def setUp(self):
        from FactoryTx.components.dataplugins.opcua import OPCUAService

        self.plugin = OPCUAService()
        self.plugin.loadParameters({}, self.schema, self.config)

    # test that we can instantiate the plugin
    def test_plugin_connect(self):
        if not self.use_mock:
            # test that we start disconnected
            self.assertFalse(self.plugin._connected)
            self.plugin.connect()
            self.assertTrue(self.plugin._connected)
            self.plugin.disconnect()
            self.assertFalse(self.plugin._connected)

    def getRead(self):

        if self.use_mock:
            fake_opc_record = {'timestamp':
                datetime(2015, 7, 23, 22, 25, 59, 487348), 'result':
                {'Channel1.Device1.Tag2': 1, 'Channel1.Device1.Tag1': 0}}

            opcs_read = 'FactoryTx.components.dataplugins.opcua.opc_ua.OPCUAService.read'
            with patch(opcs_read) as mock_opcua_read:
                mock_opcua_read.return_value = fake_opc_record
                opc_record = self.plugin.read()

            return opc_record
        else:
            self.plugin.connect()
            return self.plugin.read()

    def test_read(self):
        record = self.getRead()
        ts = record['timestamp']
        values = record['result']

        self.assertTrue(ts.__class__.__name__ == "datetime")
        self.assertTrue(values['Channel1.Device1.Tag1'] == 0)
        self.assertTrue(values['Channel1.Device1.Tag2'] == 1)
        # make sure we have timestamp, that tags are set on init

        if not self.use_mock:
            # set a node a string (server side)
            self.myvariable2.set_value("Humphery")

            ts, values = self.plugin.read()
            self.assertTrue(values['Channel1.Device1.Tag2'] == "Humphery")

            self.myvariable2.set_value(1)  # set the tag back

        self.plugin.disconnect()

    def test_process(self):
        record = self.getRead()
        t = record['timestamp']
        values = record['result']

        sslog = self.plugin.process({'timestamp': t, 'result': values})

        self.assertEqual(
            sslog['timestamp'],
            datetime(2015, 7, 23, 22, 25, 59, 487348))
        self.assertEqual(
            set(sslog['fieldvalues'].keys()),
            set([t['exportname'] for t in self.plugin.tags]))


if __name__ == '__main__':
    unittest.main()
