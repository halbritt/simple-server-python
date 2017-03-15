import unittest

import mock

from FactoryTx.managers.ServiceManager import ServiceManager
from FactoryTx.DataService import DataService
from FactoryTx.Config import Config


class ServiceManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.example_schema = {
           '$schema-version': '1.0.0',
           '$schema': 'http://json-schema.org/schema',
           '$plugin-type': 'acquisition',
           'properties': {
               'runtime': {
                   'minimum': 1,
                   'type': 'integer'
               }
           },
           'required': ['runtime']
        }

        self.time_patcher = mock.patch('FactoryTx.managers.ServiceManager.time')
        self.time_mock = self.time_patcher.start()

        self.config_patcher = mock.patch(
            'FactoryTx.managers.ServiceManager.CONFIG',
            spec=Config)
        self.config_mock = self.config_patcher.start()
        self.config_mock.plugin_conf_list = [
            ('example', {'source': 'EXAMPLE',
                         'version': '1.0.0',
                         'runtime': 100}),
        ]

        self.example_plugin_mock = mock.Mock(DataService)
        self.example_plugin_mock.__version__ = '1.0.0'
        self.example_plugin_mock.return_value = mock.Mock(DataService)
        self.example_plugin_mock.return_value.__version__ = '1.0.0'
        self.example_plugin_mock.return_value.stop = mock.Mock()
        self.example_plugin_mock.return_value.join = mock.Mock()
        self.example_plugin_mock.return_value.pid = 10000

        self.plgn_mng_patcher = mock.patch(
            'FactoryTx.managers.ServiceManager.PLUGIN_MANAGER',
            autospec=True)
        self.plgn_mng_mock = self.plgn_mng_patcher.start()
        self.plgn_mng_mock.plugins.keys.return_value = ['example']
        self.schemas = {
            'example': {'1.0.0': self.example_schema},
        }
        gps = self.plgn_mng_mock.get_plugin_schema
        gps.side_effect = lambda t, v: self.schemas.get(t, {}).get(v)

        self.plugins = {
            'example': self.example_plugin_mock
        }
        gp = self.plgn_mng_mock.get_plugin
        gp.side_effect = lambda t: self.plugins.get(t)

    def tearDown(self):
        self.time_patcher.stop()
        self.config_patcher.stop()
        self.plgn_mng_patcher.stop()

    def test_init(self):
        sm = ServiceManager()
        self.assertTrue(sm.poll_loop)
        self.assertEqual(sm.services, [])

    def test_start_services(self):
        epm_instance = self.example_plugin_mock.return_value
        sm = ServiceManager()
        sm.start_services()

        self.assertEqual(sm.services, [(0, 'example', epm_instance)])
        lp = epm_instance.loadParameters
        lp.assert_called_once_with(self.config_mock,
                                   self.example_schema,
                                   {'source': 'EXAMPLE',
                                    'version': '1.0.0',
                                    'runtime': 100})
        epm_instance.start.assert_called_once_with()

    def test_start_services_no_plugins(self):
        self.config_mock.plugin_conf_list = []
        sm = ServiceManager()
        sm.start_services()
        self.assertEqual(sm.services, [])

    def test_load_service(self):
        epm_instance = self.example_plugin_mock.return_value

        self.config_mock.plugin_conf_list = [
            ('example', {'source': 'EXAMPLE',
                         'version': '1.0.0',
                         'runtime': 100}),
        ]

        sm = ServiceManager()
        plgn = sm.load_service(0)
        self.assertIs(plgn, epm_instance)
        lp = epm_instance.loadParameters
        lp.assert_called_once_with(self.config_mock,
                                   self.example_schema,
                                   {'source': 'EXAMPLE',
                                    'version': '1.0.0',
                                    'runtime': 100})

    def test_poll(self):
        sm = ServiceManager()
        sm.start_services()

        s = self.time_mock.sleep
        s.side_effect = lambda n: setattr(sm, 'poll_loop', False)

        sm.poll()

    def test_poll_restart_service(self):
        epm_instance = self.example_plugin_mock.return_value
        epm_instance.is_alive.return_value = False

        sm = ServiceManager()
        sm.start_services()
        self.assertEqual(sm.services, [(0, 'example', epm_instance)])

        s = self.time_mock.sleep
        s.side_effect = lambda n: setattr(sm, 'poll_loop', False)

        sm.poll()

        epm_instance.stop.assert_called_once_with()
        epm_instance.join.assert_called_once_with(1)


if __name__ == '__main__':
    unittest.main()
