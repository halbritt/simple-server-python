import unittest
import os
import shutil
import yaml
import queue

import mock
from mock import patch

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.tx.DataFilesPostManager import WatchdogManager
from FactoryTx.components.tx.remotedatapost.RDP import RDP

from tests.utils.FakeResponseRequests import FakeResponseRequests
from tests.utils import UnitTestUtils

manager = component_manager()['tx']


class RemoteDataPostTests(unittest.TestCase):

    source = 'UnitTest_Machine_1'
    config_yaml = """
machines:
- source: UnitTest_Machine_1
  plugins:
  - type: remotedatapost
    name: Remote Data Post Service
    machine: 'Customer MES'
    version: '1.0.0'
    apikey: "JJJfake_api_keyJJJ"
    host: '192.168.33.13'
    port: '80'
    plcposttimeout: 10
    ipcposttimeout: 30
    maxIPCRetries: 3
    debug: true
    source: {0}
    outputdirectory: /home/sm/Code/simpledata_outputdirectory/
    """.format(source)

    def getSampleFilesArg(self, relfilename, content_type, headers):
        cwd = os.getcwd()
        filename = "%s/SimpleData/tests/unit/%s" % (cwd, relfilename)
        files = {'imagefile': (relfilename, open(filename, 'rb'), content_type, headers)}
        return files

    @classmethod
    def setUpClass(cls):
        super(RemoteDataPostTests, cls).setUpClass()

        cls.pm = manager
        cls.pm.load_schemas()
        cls.schema = cls.pm.get_plugin_schema('remotedatapost', '1.0.0')
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.config = cls.plugin_config['machines'][0]['plugins'][0]

    @classmethod
    def tearDownClass(cls):
        super(RemoteDataPostTests, cls).tearDownClass()

    def setUp(self):
        super(RemoteDataPostTests, self).setUp()

        self.wd_mng_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.watchdog_manager',
            spec=WatchdogManager)
        self.wd_mng_mock = self.wd_mng_patcher.start()
        self.queue_mock = mock.Mock(spec=queue.Queue)
        self.wd_mng_mock.get_new_files_queue.return_value = self.queue_mock
        self.queue_mock.get_nowait.side_effect = [
            '/mydir/4.json',
            '/mydir/3.json',
            queue.Empty,
        ]

        self.plugin = RDP()
        sdconfig = {'plugins': {'data': '%s/tests' % os.getcwd()}}
        self.plugin.loadParameters(sdconfig, self.schema, self.config)
        self.plugin.remoteDataPoster.postprocess_file_handling  = {'remove_on_failure': True, 'remove_on_success': True}

        self.dirDataBuffer = "%s/tests/databuffer/default" % os.getcwd()
        UnitTestUtils.delete_and_mkdir(self.dirDataBuffer)

        self._orig_mkdir = os.mkdir
        os.mkdir = UnitTestUtils.MockMkdir()

    def tearDown(self):
        super(RemoteDataPostTests, self).tearDown()
        self.wd_mng_patcher.stop()

        os.mkdir = self._orig_mkdir
        try:
            self.plugin.terminate()
        except:
            pass

        UnitTestUtils.delete_dir(self.dirDataBuffer)

    @patch('requests.post')
    def helper_PostDataHelper(self, is_ipc, mock_request_post):

        if is_ipc:
            instr = '{"id":"abc", "imagefilesize": 1030921}'
            fake_resp = FakeResponseRequests(201, instr)
            mock_request_post.return_value = fake_resp
        else:
            fake_resp = FakeResponseRequests(201, '12345678901234567890123456')
            mock_request_post.return_value = fake_resp

        self.plugin.remoteDataPoster.PostDataHelper()
        return mock_request_post

    def test_PostData_Image(self):

        relfn = "524_AutoNoSewCamera1_2014-10-07_14-30-00.bmp"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/img/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.dirDataBuffer, relfn)
        shutil.copyfile(srcfn, destfn)

        relfn = "Binary.sm.json"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.dirDataBuffer, relfn)
        shutil.copyfile(srcfn, destfn)

        self.helper_PostDataHelper(True)

    def test_PostData_JSON(self):

        relfn = "AutoNoSew-1_pretty.sm.json"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.dirDataBuffer, relfn)
        shutil.copyfile(srcfn, destfn)

        self.helper_PostDataHelper(False)

    @patch('requests.post')
    def test_PostData_Image_Retries(self, mock_request_post):

        relfn = "524_AutoNoSewCamera1_2014-10-07_14-30-00.bmp"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/img/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.dirDataBuffer, relfn)
        shutil.copyfile(srcfn, destfn)

        with open(destfn, 'rb') as f:
            readstr = f.read()

        relfn = "Binary.sm.json"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        destfn = "%s/%s" % (self.dirDataBuffer, relfn)
        shutil.copyfile(srcfn, destfn)

        #simulate failure
        mock_request_post.return_value = FakeResponseRequests(404, '')
        self.plugin.remoteDataPoster.PostDataHelper()

        #simulate success
        mock_request_post.return_value = FakeResponseRequests(201, '{"id":"abc", "imagefilesize": 1030921}')
        self.plugin.remoteDataPoster.reload_files=True
        self.plugin.remoteDataPoster.PostDataHelper()
        name, args, kwargs = mock_request_post.mock_calls[0]
        self.assertTrue(len(kwargs['files']['ipcfile'][1]) == len(readstr))


    def test_isBinary(self):
        #xml is binary
        json_data = {"2014-09-15T14:00:55.535541": {"isBinary": True, "filename": "524_AutoNoSewCamera1_2014-10-07_14-30-00.xml"}}
        self.assertTrue(self.plugin.remoteDataPoster.isBinaryPost(json_data)[0])

        #imgfile is binary
        relfn = "Binary.sm.json"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        json_data = self.plugin.remoteDataPoster.read_json_file(srcfn)
        self.assertTrue(self.plugin.remoteDataPoster.isBinaryPost(json_data)[0])

        #PLC json file is not binary
        relfn = "AutoNoSew-1_pretty.sm.json"
        srcfn = "%s/tests/sample_data/one_autonosew_etl/%s" % (os.getcwd(), relfn)
        json_data = self.plugin.remoteDataPoster.read_json_file(srcfn)
        self.assertFalse(self.plugin.remoteDataPoster.isBinaryPost(json_data)[0])




if __name__ == '__main__':
    unittest.main()
