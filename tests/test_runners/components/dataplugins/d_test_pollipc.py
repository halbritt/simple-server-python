import unittest
import os
import glob
import json
import shutil
import yaml
import time

from mock import patch
from testfixtures import TempDirectory

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.pollipc.PollIPC import PollIPC
from tests.utils.FakeSMBConnection import FakeSMBConnection

plugins = component_manager()['dataplugins']


class PollIPCTests(unittest.TestCase):

    source = 'UnitTestMachine1'
    folderPath = ''
    config_template = """
machines:
- source: {source}
  plugins:
  - type: pollipc
    source: {source}
    version: 1.0.0
    name: PollIPC Service
    machine: 'Customer MES'
    version: '1.0.0'
    username: test
    password: test
    hostName: 'test_ipcmachine'
    host: '127.0.0.1'
    port: 80
    timeout: 10
    polltime: 1
    sharedFolder: {shared_folder}/
    folderPath: {path}/
    outputdirectory: ''
    removeFiles: 1
    counterfield:
        field: 'Counter'
    runningfield: 'Status'
    skipFiles:
    - '*.abc'
    - '*.mno'
    filenamemetadata:
    - name: Counter
      address: 0
    - name: Camera
      address: 1
    - name: Date
      address: 2
    - name: Time
      address: 3
    xml:
        fields:
            Cycle: 'int'
            Voltage: 'float'
            Status: 'string'
            Counter: 'int'
            Input: 'int'
            Output: 'int'
            ConveyorSpeed: 'float'
    """

    def getSampleFilesArg(self, relfilename, content_type, headers):
        cwd = os.getcwd()
        filename = "%s/SimpleData/tests/unit/%s" % (cwd, relfilename)
        files = {'imagefile': (relfilename, open(filename, 'rb'), content_type, headers)}
        return files

    @classmethod
    def setUpClass(cls):
        super(PollIPCTests, cls).setUpClass()

        cls.pm = plugins
        cls.pm.load_schemas()
        cls.schema = cls.pm.get_plugin_schema('pollipc', '1.0.0')

    @classmethod
    def tearDownClass(cls):
        super(PollIPCTests, cls).tearDownClass()

    def setUp(self):
        super(PollIPCTests, self).setUp()

        self.databuffer = TempDirectory()
        self.input_directory = TempDirectory()

        config_yaml = self.config_template.format(
            source=self.source,
            shared_folder=self.input_directory.path,
            path=self.folderPath,
            databuffer=self.databuffer.path,
        )
        plugin_config = yaml.load(config_yaml)
        self.config = plugin_config['machines'][0]['plugins'][0]

        self.plugin = PollIPC()
        sdconfig = {'plugins': {'data': self.databuffer.path}}
        self.plugin.loadParameters(sdconfig, self.schema, self.config)

    def tearDown(self):
        super(PollIPCTests, self).tearDown()
        self.databuffer.cleanup()
        self.input_directory.cleanup()

    @patch('FactoryTx.core.pollipc.PollIPC.SMBConnection')
    def helper_PostDataHelper(self, mock_connection):
        fake_obj = {
            'file_attributes': {},
            'filesize': 0
        }
        u = self.config['username']
        p = self.config['password']
        h = self.config['hostName']
        name = 'unittest-dcn'
        mock_connection.return_value = FakeSMBConnection(fake_obj, u, p, name, h)

        self.plugin.OpenSMBConnection()
        self.plugin.PollIPC()

        sec_sleep = 2  # Run for 2 seconds (polling at 1 second) - change value if running in debugger
        # sec_sleep = 1000 # DEBUGGING
        time.sleep(sec_sleep)

        self.plugin.StopPolling()
        self.checkForSMJsonFiles()

    def checkForSMJsonFiles(self):
        path = "%s/%s*.sm.json" % (self.databuffer.path, self.source)
        sm_json_files = glob.glob(path)
        self.assertTrue(len(sm_json_files) == 2)
        for f in sm_json_files:
            print("FILENAME=%s" % f)

    def test_PostData_XML(self):

        relfn = "524_AutoNoSewCamera1_2014-10-07_14-30-00.bmp"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/img/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.input_directory.path, relfn)
        shutil.copyfile(srcfn, destfn)

        relfn = "sample_dcn.xml"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.input_directory.path, relfn)
        shutil.copyfile(srcfn, destfn)

        self.helper_PostDataHelper()

    def test_getCounterFromXML(self):
        jsondata_withcounter = {'Counter': {'value':100}}
        jsondata_withoutcounter = {'ABC': {'value':50}}
        xml_filename = "101_1_23_12.xml"
        self.assertEqual(self.plugin._getCounterFromXML(jsondata_withcounter, "" ), 100)
        self.assertEqual(self.plugin._getCounterFromXML(jsondata_withoutcounter, xml_filename), 101)

    def test_skipFile(self):
        filename = "random.abc"
        self.assertTrue(self.plugin.skipFile(filename))
        filename = "random.mno"
        self.assertTrue(self.plugin.skipFile(filename))
        filename = "random.ply"
        self.assertFalse(self.plugin.skipFile(filename))

    def test_parse_alarm_csv(self):
        filename = "Alarm.txt"
        self.input_directory.write(
            filename,
            "2016_2_15_15_16_19, Warning, 101, Pattern Match Align Fail",
        )
        self.plugin.parseCSV({
                "match": "*Alarm.txt",
                "format": "alarm",
                "sslog_type": "alarm",
                "column_names": [
                    "Timestamp",
                    "Alarm Level",
                    "Alarm Code",
                    "Alarm Description",
                ]
            },
            originalFileName="1_2016_05_31_06_31_11_Alarm.txt",
            newFileName=self.input_directory.getpath(filename),
        )
        sslog_filenames = os.listdir(self.databuffer.path)
        self.assertEqual(len(sslog_filenames), 1)
        with open(self.databuffer.getpath(sslog_filenames[0]), 'rb') as fp:
            sslog = json.load(fp)
        self.assertEqual(sslog.values()[0]['fieldvalues'], {
            'Timestamp': {'value': '2016_2_15_15_16_19', 'units': None},
            'Alarm Level': {'value': 'Warning', 'units': None},
            'Alarm Code': {'value': 101, 'units': None},
            'Alarm Description': {'value': 'Pattern Match Align Fail', 'units': None},
        })

    def test_parse_key_value_csv(self):
        filename = "kv.csv"
        self.input_directory.write(filename, "FooVersion,2016.12.10\nBarVersion,1\n")
        self.plugin.parseCSV({
                "match": "*.csv",
                "format": "key_value",
                "sslog_type": "version",
            },
            originalFileName="1_Version_info.csv",
            newFileName=self.input_directory.getpath(filename),
        )
        sslog_filenames = os.listdir(self.databuffer.path)
        self.assertEqual(len(sslog_filenames), 1)
        with open(self.databuffer.getpath(sslog_filenames[0]), 'rb') as fp:
            sslog = json.load(fp)
        self.assertEqual(sslog.values()[0]['fieldvalues'], {
            'FooVersion': {'value': '2016.12.10', 'units': None},
            'BarVersion': {'value': '1', 'units': None},
        })


if __name__ == '__main__':
    unittest.main()
