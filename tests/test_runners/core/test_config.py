import os
import unittest
import multiprocessing

import mock

from FactoryTx.Config import Config, ConfigError


class ConfigTestCase(unittest.TestCase):

    def setUp(self):
        self.config = {
            'plugins': {
                'conf.d': '/opt/sightmachine/factorytx/plugins/conf.d/',
                'data': '/var/spool/sightmachine/factorytx/',
            }
        }

        self.example_cfg = {
            'machines': [
                {
                    'plugins': [
                        {
                            'type': 'example',
                            'name': 'example Plugin',
                            'config': {
                                'version': '1.0.0',
                                'runtime': 100
                            }
                        }
                    ],
                    'source': 'EXAMPLE'
                }
            ],
            'watchers': [
                {
                    'version': '1.0.0',
                    'max_retry': 15
                }
            ]
        }

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
        self.rdp_schema = {
           '$schema-version': '1.0.0',
           '$schema': 'http://json-schema.org/schema',
           '$plugin-type': 'acquisition',
           'properties': {
               'max_retry': {
                   'minimum': 1,
                   'type': 'integer'
               }
           },
           'required': ['max_retry']
        }

        self.glob_patcher = mock.patch('FactoryTx.Config.glob', autospec=True)
        self.glob_mock = self.glob_patcher.start()
        self.glob_mock.glob.return_value = [
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'
        ]

        self.os_patcher = mock.patch('FactoryTx.Config.os', autospec=True)
        self.os_mock = self.os_patcher.start()
        self.os_mock.path.join = os.path.join  # use real
        self.os_mock.path.abspath = os.path.abspath
        self.os_mock.path.basename = os.path.basename
        self.os_mock.path.dirname = os.path.dirname
        self.exist = [
            '/etc/sightmachine/factorytx',
            '/etc/sightmachine/factorytx/factorytx.conf',
            '/opt/sightmachine/factorytx/plugins/conf.d',
        ]
        self.os_mock.path.exists.side_effect = lambda a: a in self.exist
        self.os_mock.listdir.return_value = []

        self.open_mock = mock.mock_open()
        self.open_patcher = mock.patch('FactoryTx.Config.open',
                                       self.open_mock,
                                       create=True)
        self.open_patcher.start()

        self.yaml_patcher = mock.patch('FactoryTx.Config.yaml', autospec=True)
        self.yaml_mock = self.yaml_patcher.start()
        self.yaml_mock.load.side_effect = [
            self.config,
            self.example_cfg,
        ]

        self.plgn_mng_patcher = mock.patch(
            'FactoryTx.Config.plugin_manager',
            autospec=True)
        self.plgn_mng_mock = self.plgn_mng_patcher.start()
        self.plgn_mng_mock.plugins.keys.return_value = [
            'example', 'remotedatapost'
        ]
        self.schemas = {
            'example': {'1.0.0': self.example_schema},
            'remotedatapost': {'1.0.0': self.rdp_schema},
        }
        gps = self.plgn_mng_mock.get_plugin_schema
        gps.side_effect = lambda t, v: self.schemas.get(t, {}).get(v)

        self.s3con_patcher = mock.patch('FactoryTx.Config.S3Connection',
                                        autospec=True)
        self.s3con_mock = self.s3con_patcher.start()

    def tearDown(self):
        self.glob_patcher.stop()
        self.os_patcher.stop()
        self.open_patcher.stop()
        self.yaml_patcher.stop()
        self.plgn_mng_patcher.stop()
        self.s3con_patcher.stop()

    def test_load_config_default(self):
        config = Config()
        result = config.load()
        self.assertTrue(result)
        self.assertEqual(config, self.config)

        self.open_mock.assert_any_call(
            '/etc/sightmachine/factorytx/factorytx.conf', 'r')
        self.open_mock.assert_any_call(
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg', 'r')
        self.assertEqual(self.open_mock.call_count, 2)

        self.assertEqual(
            config.plugin_conf_files,
            ['/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'])
        self.assertEqual(config.plugin_confs,
                         {'/opt/sightmachine/factorytx/plugins/conf.d/'
                          'example.cfg': self.example_cfg})
        self.assertEqual(config.plugin_conf_list, [])

    def test_load_config_from_file(self):
        config = Config()
        result = config.load(
            conf_file='/etc/sightmachine/factorytx/factorytx.conf')
        self.assertTrue(result)
        self.assertEqual(config, self.config)

        self.open_mock.assert_any_call(
            '/etc/sightmachine/factorytx/factorytx.conf', 'r')
        self.open_mock.assert_any_call(
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg', 'r')
        self.assertEqual(self.open_mock.call_count, 2)

        self.assertEqual(
            config.plugin_conf_files,
            ['/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'])
        self.assertEqual(config.plugin_confs,
                         {'/opt/sightmachine/factorytx/plugins/conf.d/'
                          'example.cfg': self.example_cfg})
        self.assertEqual(config.plugin_conf_list, [])

    def test_load_config_default_nt(self):
        self.os_mock.name = 'nt'
        self.os_mock.getcwd.return_value = '/mydir'

        self.exist = [
            '/mydir/factorytx.conf',
            '/opt/sightmachine/factorytx/plugins/conf.d',
        ]

        config = Config()
        result = config.load()
        self.assertTrue(result)
        self.assertEqual(config, self.config)

        self.open_mock.assert_any_call(
            '/mydir/factorytx.conf', 'r')
        self.open_mock.assert_any_call(
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg', 'r')
        self.assertEqual(self.open_mock.call_count, 2)

        self.assertEqual(
            config.plugin_conf_files,
            ['/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'])
        self.assertEqual(config.plugin_confs,
                         {'/opt/sightmachine/factorytx/plugins/conf.d/'
                          'example.cfg': self.example_cfg})
        self.assertEqual(config.plugin_conf_list, [])

    def test_load_config_factorytx(self):
        config = Config()
        self.exist = [
            os.path.abspath('FactoryTx/factorytx.conf'),
            '/opt/sightmachine/factorytx/plugins/conf.d',
        ]
        result = config.load()
        self.assertTrue(result)
        self.assertEqual(config, self.config)

        self.open_mock.assert_any_call(
            os.path.abspath('FactoryTx/factorytx.conf'), 'r')
        self.open_mock.assert_any_call(
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg', 'r')
        self.assertEqual(self.open_mock.call_count, 2)

        self.assertEqual(
            config.plugin_conf_files,
            ['/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'])
        self.assertEqual(config.plugin_confs,
                         {'/opt/sightmachine/factorytx/plugins/conf.d/'
                          'example.cfg': self.example_cfg})
        self.assertEqual(config.plugin_conf_list, [])

    def test_load_config_directory(self):
        self.exist = [
            '/mydir/factorytx',
            '/mydir/factorytx/factorytx.conf',
            '/opt/sightmachine/factorytx/plugins/conf.d',
        ]
        config = Config()
        result = config.load(directory='/mydir/factorytx')
        self.assertTrue(result)
        self.assertEqual(config, self.config)

        self.open_mock.assert_any_call(
            '/mydir/factorytx/factorytx.conf', 'r')
        self.open_mock.assert_any_call(
            '/opt/sightmachine/factorytx/plugins/conf.d/example.cfg', 'r')
        self.assertEqual(self.open_mock.call_count, 2)

        self.assertEqual(
            config.plugin_conf_files,
            ['/opt/sightmachine/factorytx/plugins/conf.d/example.cfg'])
        self.assertEqual(config.plugin_confs,
                         {'/opt/sightmachine/factorytx/plugins/conf.d/'
                          'example.cfg': self.example_cfg})
        self.assertEqual(config.plugin_conf_list, [])

    def test_load_config_no_directory(self):
        self.exist = []
        config = Config()
        result = config.load(directory='/mydir/factorytx')
        self.assertFalse(result)

    def test_load_config_no_file(self):
        self.exist = ['/mydir/factorytx']
        config = Config()
        result = config.load(directory='/mydir/factorytx')
        self.assertFalse(result)

    def test_load_config_no_default_file(self):
        self.exist = ['/etc/sightmachine/factorytx']
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_load_config_bad_yaml(self):
        self.yaml_mock.load.side_effect = Exception
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_load_config_bad_yaml2(self):
        self.yaml_mock.load.side_effect = [self.config, Exception]
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_load_config_no_confd(self):
        self.config['plugins']['conf.d'] = None
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_load_config_no_data(self):
        self.config['plugins']['data'] = None
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_load_config_empty_confd_dir(self):
        self.glob_mock.glob.return_value = []
        config = Config()
        result = config.load()
        self.assertFalse(result)

    def test_validate_config_default(self):
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertTrue(result)
        self.assertEqual(config.plugin_conf_list,
                         [('example', {'source': 'EXAMPLE',
                                       'version': '1.0.0',
                                       'runtime': 100
                                       }),
                          ('remotedatapost', {'max_retry': 15,
                                              'version': '1.0.0'
                                              })])

    def test_validate_config_bad_type(self):
        self.example_cfg['machines'][0]['plugins'][0]['type'] = 'unknown'
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_validate_config_no_type(self):
        del self.example_cfg['machines'][0]['plugins'][0]['type']
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_validate_config_no_schema(self):
        plgns_conf = self.example_cfg['machines'][0]['plugins']
        plgns_conf[0]['config']['version'] = '0.1'
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_validate_config_invalid(self):
        plgns_conf = self.example_cfg['machines'][0]['plugins']
        plgns_conf[0]['config']['runtime'] = 'bad'
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_validate_config_invalid_version(self):
        self.example_cfg['watchers'][0]['version'] = '0.1'
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_validate_config_invalid_watcher(self):
        self.example_cfg['watchers'][0]['max_retry'] = 'bad'
        config = Config()
        self.assertTrue(config.load())
        result = config.validate_configs()
        self.assertFalse(result)

    def test_save_config(self):
        config = Config()
        config._save_config('./mydir/', '{"mydata": 10}', 'conf.json')
        self.open_mock.assert_called_once_with('./mydir/conf.json', 'w')
        f = self.open_mock()
        f.write.assert_called_once_with('{"mydata": 10}')

    def test_save_config_exception(self):
        config = Config()
        self.open_mock.side_effect = Exception
        with self.assertRaises(Exception):
            config._save_config('./mydir/', '{"mydata": 10}', 'conf.json')

    def test_update_config_no_conf(self):
        config = Config()
        config.load()
        with self.assertRaises(ConfigError):
            config.update_config(['/tmp/new_conf.cfg'])

    def test_update_config(self):
        self.config['s3'] = {}
        self.config['s3']['aws_access_key'] = '<key>'
        self.config['s3']['aws_secret_access_key'] = '<sa_key>'
        self.config['s3']['aws_bucket'] = '<bucket>'
        self.config['s3']['aws_path_prefix'] = '/prefix'

        config = Config()
        config.load()
        config.update_config(['new_conf.cfg'])

        self.s3con_mock.assert_called_once_with('<key>', '<sa_key>')
        s3con = self.s3con_mock.return_value
        s3con.get_bucket.assert_called_once_with('<bucket>', validate=False)
        bucket = s3con.get_bucket.return_value
        bucket.get_key.assert_called_once_with('/prefix/new_conf.cfg')
        key = bucket.get_key.return_value
        key.get_contents_as_string.assert_called_once_with()
        content = key.get_contents_as_string.return_value
        self.open_mock.assert_called_with(
            '/opt/sightmachine/factorytx/plugins/conf.d/new_conf.cfg', 'w')
        f = self.open_mock()
        f.write.assert_called_once_with(content)

    def test_update_config_multiple(self):
        self.config['plugins']['conf.d'] = '/short/path/'
        self.config['s3'] = {}
        self.config['s3']['aws_access_key'] = '<key>'
        self.config['s3']['aws_secret_access_key'] = '<sa_key>'
        self.config['s3']['aws_bucket'] = '<bucket>'
        self.config['s3']['aws_path_prefix'] = '/prefix'

        s3con = self.s3con_mock.return_value
        bucket = s3con.get_bucket.return_value
        key = bucket.get_key.return_value
        contents = ['content'+str(i) for i in range(3)]
        key.get_contents_as_string.side_effect = contents

        config = Config()
        config.load()

        config.update_config([
            'new_conf1.cfg',
            'new_conf2.cfg',
            'new_conf3.cfg'
        ])

        self.s3con_mock.assert_called_with('<key>', '<sa_key>')
        s3con.get_bucket.assert_called_with('<bucket>', validate=False)
        self.assertEqual(bucket.get_key.call_count, 3)
        bucket.get_key.assert_has_calls(mock.call('/prefix/new_conf1.cfg'))
        bucket.get_key.assert_has_calls(mock.call('/prefix/new_conf2.cfg'))
        bucket.get_key.assert_has_calls(mock.call('/prefix/new_conf3.cfg'))
        key.get_contents_as_string.assert_called_with()
        self.open_mock.assert_has_calls(
            mock.call('/short/path/new_conf1.cfg', 'w'))
        self.open_mock.assert_has_calls(
            mock.call('/short/path/new_conf2.cfg', 'w'))
        self.open_mock.assert_has_calls(
            mock.call('/short/path/new_conf3.cfg', 'w'))
        f = self.open_mock()
        self.assertEqual(bucket.get_key.call_count, 3)
        f.write.assert_has_calls(mock.call('content0'))
        f.write.assert_has_calls(mock.call('content1'))
        f.write.assert_has_calls(mock.call('content2'))

    def test_update_config_exception(self):
        self.config['s3'] = {}
        self.config['s3']['aws_access_key'] = '<key>'
        self.config['s3']['aws_secret_access_key'] = '<sa_key>'
        self.config['s3']['aws_bucket'] = '<bucket>'
        self.config['s3']['aws_path_prefix'] = '/prefix'

        self.s3con_mock.side_effect = Exception('error')

        config = Config()
        config.load()

        with self.assertRaises(Exception):
            config.update_config(['new_conf.cfg'])

    def test_get_nprocs_1(self):
        self.plgn_mng_mock.get_plugin.return_value = mock.Mock

        config = Config()
        self.assertEqual(config.get_nprocs(), 1)
        config.load()
        self.assertEqual(config.get_nprocs(), 1)
        config.validate_configs()
        self.assertEqual(config.get_nprocs(), 1)

    def test_get_nprocs_3(self):
        self.plgn_mng_mock.get_plugin.return_value = multiprocessing.Process

        config = Config()
        self.assertEqual(config.get_nprocs(), 1)
        config.load()
        self.assertEqual(config.get_nprocs(), 1)
        config.validate_configs()
        self.assertEqual(config.get_nprocs(), 3)


if __name__ == '__main__':
    unittest.main()
