import os
import os.path
import unittest

from testfixtures import TempDirectory
import yaml

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.parsers.testspreadsheetparser import TestSpreadSheetParser


components = component_manager()
parsers = components['parsers']
transports = components['transports']
plugins = components['dataplugins']


class SpreadSheetParserTests(unittest.TestCase):
    config_template = """
version: 1.0.0
source: {self.source}
completed_folder: {self.completed_dir.path}
outputdirectory: {self.output_dir.path}
transports:
- type: localfile
  config:
    version: 1.0.0
    root_path: {self.src_dir_1.path}
parsers:
- type: testspreadsheetparser
  config:
    version: 1.0.0
    source: 'cfg_test_codes'
    filename_patterns: ['*.txt']
    id: text
    codes:
      -
        column: "Column1"
        code: "Column1Code"
      -
        column: "Column2"
        code: "Column2Code"
        conditional: "{0} > 1"
    parse_options:
      -
        load:
          actions:
            -
              rename:
                columns:
                  BUILD_STATION_TS: timestamp
                  Leak_Tst_value: "LEAK_TST_VALUE"
          counter: true
          parse:
            header: 0
            sep: "\t"
            index_col: false
          regex:
            -
              source: STAMPED_ID_VALUE
              target: serial
              pattern: '^.+(\d\d\d\dH\d+)\s+$'
        report_pattern: file*.txt
"""

    cfg_valid_regex = """
    version: 1.0.0
    source: 'cfg_test_codes'
    parse_options:
      -
        load:
          actions:
            -
              set_index: ['Start Time']
          parse: {
             header: 0,
             sep: "\t",
             index_col: false,
             parse_dates: ['Start Time', 'End Time']
          }
          regex:
            -
              source: Code
              target: serial
              pattern: '^.+(\d\d\d\dH\d+)\s+$'
        report_pattern: test*.csv
"""

    cfg_valid_codes = """
    version: 1.0.0
    source: 'cfg_test_codes'
    codes:
      -
         column: col1
         code: col1
         conditional: test
    parse_options:
      -
        load:
          counter: true
        report_pattern: test*.csv
"""

    cfg_invalid_codes = """
    version: 1.0.0
    source: 'cfg_test_codes'
    codes:
      -
         code: col1
         conditional: test
    parse_options:
      -
        load:
          counter: true
    report_pattern: test*.csv
"""

    cfg_test_codes = """
    version: 1.0.0
    source: 'cfg_test_codes'
    codes:
      -
         column: Code
         code: cond0
      -
         column: Code
         code: cond1
         conditional: "{0} == 1"
      -
         column: Code
         code: cond2
         conditional: "{0} != 1"
      -
         column: Code
         code: cond3
         conditional: "{0} <= 1"
      -
         column: Code
         code: cond4
         conditional: "{0} >= 1"
      -
         column: Code
         code: cond5
         conditional: "{0} > 1"
      -
         column: Code
         code: cond6
         conditional: "{0} < 1"
    parse_options:
      -
        load:
          counter: true
          actions: [{'set_index': ['Start Time']}]
          parse: {
             header: 0,
             sep: "\t",
             index_col: false,
             parse_dates: ['Start Time', 'End Time']
          }
        report_pattern: test*.csv
"""

    cfg_test_invalid_cond_codes = """
    version: 1.0.0
    source: 'cfg_test_codes'
    codes:
      -
         column: Code
         code: cond1
         conditional: "{0} !! 1"
    parse_options:
      -
        load:
          counter: true
          actions: [{'set_index': ['Start Time']}]
          parse: {
             header: 0,
             sep: "\t",
             index_col: false,
             parse_dates: ['Start Time', 'End Time']
          }
        report_pattern: test*.csv
"""

    cfg_test_invalid_regex = """
    version: 1.0.0
    source: 'cfg_test_codes'
    parse_options:
      -
        load:
          counter: true
          actions: [{'set_index': ['Start Time']}]
          parse: {
             header: 0,
             sep: "\t",
             index_col: false,
             parse_dates: ['Start Time', 'End Time']
          }
          regex:
            -
              source: STAMPED_ID_VALUE
              target: serial
        report_pattern: test*.csv
"""

    cfg_test_missingcolumn_regex = """
    version: 1.0.0
    source: 'cfg_test_codes'
    parse_options:
      -
        load:
          counter: true
          actions: [{'set_index': ['Start Time']}]
          parse: {
             header: 0,
             sep: "\t",
             index_col: false,
             parse_dates: ['Start Time', 'End Time']
          }
          regex:
            -
              source: wrong_col
              target: serial
              pattern: 'test'
        report_pattern: test*.csv
"""

    @classmethod
    def setUpClass(cls):
        super(SpreadSheetParserTests, cls).setUpClass()
        # cls.pm = plugins
        # cls.pm.load_schemas()
        # cls.schema = cls.pm.get_plugin_schema('file', '1.0.0')

        # transports.load_schemas()

        cls.pm = parsers
        cls.pm.load_schemas()
        cls.ssp_schema = cls.pm.get_plugin_schema('spreadsheetparser', '1.0.0')

        cls.testfile = "%s/tests/sample_data/spreadsheetparser/test.csv" % os.getcwd()

    def setUp(self):
        self.source = 'AA_BB_ExampleMachine_1'

        self.src_dir_1 = TempDirectory()
        self.output_dir = TempDirectory()
        self.completed_dir = TempDirectory()

        # some of the tests use this
        # config_yaml = self.config_template.format(self=self)
        # self.plugin = TestSpreadSheetParser()
        # self.plugin.loadParameters({}, self.schema, yaml.load(config_yaml))

    def tearDown(self):
        self.src_dir_1.cleanup()
        self.output_dir.cleanup()
        self.completed_dir.cleanup()

    def test_config_valid_codes(self):
        _cfg = self.cfg_valid_codes.format(self=self)
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(_cfg )
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        self.plugin.read_csv(self.testfile, skiprows=None, **report_params)
        self.assertNotEqual(self.plugin.codescond, 1)

    def test_config_invalid_codes(self):
        _cfg = self.cfg_invalid_codes.format(self=self)
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(_cfg)
        with self.assertRaises(Exception):
            self.plugin.loadParameters(self.ssp_schema, _yaml)

    def test_conditional_codes(self):
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(self.cfg_test_codes)
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        sheet = self.plugin.read_csv(self.testfile, skiprows=None, **report_params)
        self.assertNotEqual(self.plugin.codescond, 1)
        sslogs = list(self.plugin.sheet_to_sslogs(sheet))
        self.assertNotEqual(sslogs, None)
        self.assertNotEqual(sslogs[0]['codes'], None)
        self.assertEqual(len(sslogs[0]['codes']), 7)

    def test_invalid_conditional_codes(self):
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(self.cfg_test_invalid_cond_codes)
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        sheet = self.plugin.read_csv(self.testfile, skiprows=None, **report_params)
        self.assertNotEqual(self.plugin.codescond, 1)
        with self.assertRaises(Exception):
            _ = list(self.plugin.sheet_to_sslogs(sheet))

    def test_config_valid_regex(self):
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(self.cfg_valid_regex)
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        _ = self.plugin.read_csv(self.testfile, skiprows=None, **report_params)

    def test_invalid_cfg_regex(self):
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(self.cfg_test_invalid_regex)
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        with self.assertRaises(Exception):
            _ = self.plugin.read_csv(self.testfile, skiprows=None, **report_params)

    def test_invalid_missing_column_regex(self):
        self.plugin = TestSpreadSheetParser()
        _yaml = yaml.load(self.cfg_test_missingcolumn_regex)
        self.plugin.loadParameters(self.ssp_schema, _yaml)
        report_params = self.plugin.parse_options[0]
        with self.assertRaises(Exception):
            _ = self.plugin.read_csv(self.testfile, skiprows=None, **report_params)

if __name__ == '__main__':
    unittest.main()
