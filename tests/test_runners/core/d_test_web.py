import json
import unittest

import mock

from FactoryTx.web import Web


class WebTestCase(unittest.TestCase):

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
        self.example2_schema = {
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
        self.example3_schema = {
           '$schema-version': '1.0.0',
           '$schema': 'http://json-schema.org/schema',
           '$plugin-type': 'acquisition',
           'properties': {
               'min_dist': {
                   'minimum': 1,
                   'type': 'integer'
               }
           },
           'required': ['min_dist']
        }

        self.plgn_mng_patcher = mock.patch('FactoryTx.web.Web.plugin_manager')
        self.plgn_mng_mock = self.plgn_mng_patcher.start()
        self.plgn_mng_mock.plugin_schemas = {
           'example': [self.example_schema],
           'example2': [self.example2_schema],
           'example3': [self.example3_schema]
        }

        Web.app.config['TESTING'] = True
        self.app = Web.app.test_client()

    def tearDown(self):
        self.plgn_mng_patcher.stop()

        Web.app.config['TESTING'] = False

    def test_main(self):
        r = self.app.get('/')
        self.assertEqual(json.loads(r.data),
                         {
                           "sitemap": [
                             "/api/plugins/",
                             "/"
                           ]
                         })

    def test_plugins(self):
        r = self.app.get('/api/plugins/')
        self.assertEqual(json.loads(r.data),
                         {
                             'plugins': {
                                 'example': [self.example_schema],
                                 'example2': [self.example2_schema],
                                 'example3': [self.example3_schema]
                             }
                         })


if __name__ == '__main__':
    unittest.main()
