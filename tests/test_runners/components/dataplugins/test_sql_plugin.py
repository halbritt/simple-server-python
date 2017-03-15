import os
import unittest
from datetime import datetime

import yaml, json
import mock
import sqlalchemy as sqla
from testfixtures import Comparison, compare

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.sql.sql import SQL, ConnectionManager

from tests.utils import UnitTestUtils

manager = component_manager()['dataplugins']


class SQLPlugin_TestCase(unittest.TestCase):

    cwd = os.getcwd()
    outputdirectory = '%s/tests/test_output/test_sql_plugin' % cwd

    config_yaml = """
machines:
- plugins:
  - type: sql
    name: SQP Plugin
    config:
      version: '1.1.0'
      host: '10.42.1.7'
      port: 5432
      poll_rate: 5
      outputdirectory: '%s'
      cachedirectory: '/tmp/'
      cachefilename: 'cache_UnitTest_Machine_1.json'
      codes:
        - column: "EVENT_TYPE"
          code: TESTCODE
          conditional: "{0} == 101"
      limit: 10
      db_type: 'postgresql+psycopg2'
      db_user: 'postgres'
      db_pass: 'postgres'
      db_name: 'stoll_skr3_0806'
      query:
       SELECT e.*, r1."TXT", r2."TXT"
        FROM skr.e_1429615385 e
        LEFT JOIN skrpps_02.event_res r1
          ON e."EVENT_TYPE" = r1."EVENT_TYPE"
         AND r1."EVENT_ID" IS NULL
         AND r1."LANG" = 'EN'
        LEFT JOIN skrpps_02.event_res r2
          ON e."EVENT_TYPE" = r2."EVENT_TYPE"
         AND e."EVENT_ID" = r2."EVENT_ID"
         AND r2."LANG" = 'EN'
      time_field_name: "TIME_S"
      json_key_field: "ID"
      id_field: e."ID"
      records_per_file: 5
  source: Machine1
""" % outputdirectory

    @classmethod
    def setUpClass(cls):
        super(SQLPlugin_TestCase, cls).setUpClass()

        cls.pm = manager
        cls.pm.load_schemas()
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.schema = cls.pm.get_plugin_schema('sql', '1.1.0')

        cls.config_sql = cls.plugin_config[
            'machines'][0]['plugins'][0]['config']
        cls.config_sql['source'] = 'UnitTest_Machine_1'

    @classmethod
    def tearDownClass(cls):
        super(SQLPlugin_TestCase, cls).tearDownClass()

    def setUp(self):
        super(SQLPlugin_TestCase, self).setUp()

        self.fake_records = [
            (121, datetime(2015, 5, 4, 11, 47, 39), 1, 2, '2', long(8743000000050000879), 120, 43203860, 'Metadata', 'Productivity changed'),
            (122, datetime(2015, 5, 4, 11, 47, 39), 1, 3, '55', long(6735000000060000883), 120, 43203861, 'Metadata', 'Piece number increased'),
            (123, datetime(2015, 5, 4, 11, 47, 39), 2, 5, '1007;1025;1024;1255|CWAZ;618;4259', long(8419000000070000888), 120, 43203862, 'Machine states', 'Stop: Stop resistance'),
            (124, datetime(2015, 5, 4, 13, 35, 30), 2, 1, '', long(8390000000080000828), 120, 43203863, 'Machine states', 'Machine is running'),
            (125, datetime(2015, 5, 5, 10, 43, 45), 101, 3, '#connect id=29 ip=172.25.100.22|5', long(4781000000000000911), 120, 43203864, 'SKR event server', 'Connection to machine established'),
            (126, datetime(2015, 5, 5, 10, 43, 49), 2, 32001, '1', long(8336000000010000900), 120, 43203865, 'Machine states', 'Booting CMS'),
            (127, datetime(2015, 5, 5, 10, 43, 50), 1, 5, '0', long(7851000000020000962), 120, 43203866, 'Metadata', 'Shift modified'),
            (128, datetime(2015, 5, 5, 10, 43, 50), 2, 2, '1007;1025;1024;1255|CWAZ;618;4259', long(6117000000030000986), 120, 43203867, 'Machine states', 'Stop: Engaging rod'),
            (129, datetime(2015, 5, 5, 11, 11, 54), 2, 1, '', long(6364000000040000973), 120, 43203868, 'Machine states', 'Machine is running'),
            (130, datetime(2015, 5, 5, 11, 11, 54), 1, 2, '2', long(6281000000050000993), 120, 43203869, 'Metadata', 'Productivity changed')
        ]
        self.fake_keys = ['ID', 'TIME_S', 'EVENT_TYPE', 'EVENT_ID', 'EVENT_TXT',
                          'DEBUG_INF', 'TIME_GMT_DIFF', 'IDM', 'TXT', 'TXT']


        self.cm_patcher = mock.patch('FactoryTx.core.sql.sql.connection_manager',
                                     spec=ConnectionManager)
        self.cm_mock = self.cm_patcher.start()
        self.engine_mock = self.cm_mock.create_engine.return_value
        self.engine_mock.pool.status.return_value = 'Status'
        self.resp_mock = self.engine_mock.execute.return_value
        self.resp_mock.mock_add_spec(sqla.engine.ResultProxy, spec_set=True)
        self.resp_mock.fetchall.return_value = self.fake_records
        self.resp_mock.keys.return_value = self.fake_keys

        self.plugin = SQL()
        self.plugin.name = 'SQL-1'
        self.plugin.loadParameters({}, self.schema, self.config_sql)

        UnitTestUtils.delete_and_mkdir(self.outputdirectory)

    def tearDown(self):
        super(SQLPlugin_TestCase, self).tearDown()
        self.cm_patcher.stop()
        UnitTestUtils.delete_dir(self.outputdirectory)

    def test_sql_plugin_connect_read_save_disconnect(self):
        self.plugin.connect()
        records = self.plugin.read()
        self.assertEquals(records, self.fake_records, "Data is not what was expected.")
        self.assertEquals(self.plugin.cache['last_id'], 130)
        self.assertEquals(self.plugin.exec_args['last_id'], 130)
        self.assertEqual(
            self.plugin.cache_file,
            '/tmp/cache_UnitTest_Machine_1.json')
        self.plugin.save_json(records)
        self.plugin.load_cache()
        self.plugin.disconnect()

    def test_sql_plugin_no_records(self):
        self.resp_mock.fetchall.return_value = []

        self.plugin.connect()

        data = self.plugin.read()
        self.assertEquals(data, [])

        self.plugin.save_json(data)
        self.plugin.load_cache()
        self.plugin.disconnect()

    def test_process(self):
        self.plugin.keys = self.fake_keys
        record = self.fake_records[0]
        self.plugin.codes = [] #remove codes for this test
        data = self.plugin.process(record)
        sdata = self.plugin._create_shared_data_record(data)
        compare(
            sdata,
            {datetime(2015, 5, 4, 11, 47, 39): {'_id': Comparison(str),
                                                'counter': 1,
                                                'fieldvalues': {'DEBUG_INF': {'value': long(8743000000050000879)},
                                                                'EVENT_ID': {'value': 2},
                                                                'EVENT_TXT': {'value': '2'},
                                                                'EVENT_TYPE': {'value': 1},
                                                                'ID': {'value': 121},
                                                                'IDM': {'value': 43203860},
                                                                'TIME_GMT_DIFF': {'value': 120},
                                                                'TXT': {'value': 'Productivity changed'}},
                                                'source': 'UnitTest_Machine_1',
                                                'timestamp': datetime(2015, 5, 4, 11, 47, 39)}})

    def test_grep_table_name(self):
        r = self.plugin._grep_table_name()
        self.assertEqual(r, 'skr.e_1429615385')

        self.plugin.query = 'SELECT * FROM FOO'
        r = self.plugin._grep_table_name()
        self.assertEqual(r, 'foo')

        self.plugin.query = 'SELECT *\nFROM FOO'
        r = self.plugin._grep_table_name()
        self.assertEqual(r, 'foo')

        self.plugin.query = 'SELECT * FROM FOO\n'
        r = self.plugin._grep_table_name()
        self.assertEqual(r, 'foo')

        self.plugin.query = 'SELECT *\nFROM FOO\n'
        r = self.plugin._grep_table_name()
        self.assertEqual(r, 'foo')

        self.plugin.query = ''
        r = self.plugin._grep_table_name()
        self.assertEqual(r, '')


    def test_sql_codes(self):
        self.plugin.connect()

        data = self.plugin.read()
        self.plugin.save_json(data)
        for output_file in os.listdir(self.outputdirectory):
          with open(os.path.join(self.outputdirectory, output_file)) as output_filehandle:
              output_data = json.load(output_filehandle)
              for ts, sslog in output_data.items():
                  assert sslog.has_key('codes')
                  assert sslog['codes'].has_key("TESTCODE")
                  assert sslog['codes']['TESTCODE'] == (sslog['fieldvalues']['EVENT_TYPE']['value'] == 101)
        self.plugin.load_cache()
        self.plugin.disconnect()


if __name__ == '__main__':
    unittest.main()
