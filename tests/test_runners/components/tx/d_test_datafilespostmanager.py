import os
import unittest
import queue
import logging


import mock
import requests
from watchdog import events

from FactoryTx.components.tx.DataFilesPostManager import DataFilesPostManager
from FactoryTx.components.tx.DataFilesPostManager import WatchdogManager


class WatchdogManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.os_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.os',
            autospec=True)
        self.os_mock = self.os_patcher.start()
        self.os_mock.path.join = os.path.join  # use real
        self.os_mock.path.abspath = os.path.abspath
        self.os_mock.path.basename = os.path.basename
        self.os_mock.path.dirname = os.path.dirname
        self.exist = []
        self.os_mock.path.exists.side_effect = lambda a: a in self.exist
        self.os_mock.listdir.return_value = []

        self.observer_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.Observer',
            autospec=True)
        self.observer_mock = self.observer_patcher.start()
        self.observer_inst = self.observer_mock.return_value

        # create test instance
        self.wd_mng = WatchdogManager()

    def tearDown(self):
        self.os_patcher.stop()
        self.observer_patcher.stop()

    def test_init(self):
        self.assertEqual(self.wd_mng.queues, {})
        self.assertEqual(self.wd_mng.observer, self.observer_inst)
        self.assertTrue(self.wd_mng.observer.daemon)
        self.assertFalse(self.wd_mng.observer_started)

        self.observer_mock.assert_called_once_with()

    def test_get_queue(self):
        queue1 = self.wd_mng.get_new_files_queue('/mydir/*.json')
        queue2 = self.wd_mng.get_new_files_queue('/mydir/prefix_*.xml')

        r = self.wd_mng._get_queue('/mydir/123.json')
        self.assertIs(r, queue1)
        r = self.wd_mng._get_queue('/mydir/qwe.json')
        self.assertEqual(r, queue1)
        r = self.wd_mng._get_queue('/mydir/prefix_456.xml')
        self.assertEqual(r, queue2)
        r = self.wd_mng._get_queue('/mydir/abc.png')
        self.assertIsNone(r)

    def test_process_event(self):
        queue = self.wd_mng.get_new_files_queue('/mydir/*.json')
        self.observer_inst.schedule.assert_called_once_with(
            self.wd_mng, '/mydir')
        self.assertEqual(self.wd_mng.queues,
                         {'/mydir/*.json': queue})

        event = mock.Mock(spec=[
            'src_path',
            'dest_path',
            'is_directory',
            'event_type'

        ])
        event.src_path = '/mydir/1.json'
        event.event_type = events.EVENT_TYPE_MODIFIED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.observer_inst.start.assert_called_once_with()
        self.observer_inst.start.reset_mock()

        event.src_path = '/mydir/2.json'
        event.event_type = events.EVENT_TYPE_MODIFIED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir/1.png'
        event.event_type = events.EVENT_TYPE_MODIFIED
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir/3.json'
        event.event_type = events.EVENT_TYPE_CREATED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir/4.jsontemp'
        event.event_type = events.EVENT_TYPE_CREATED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir/4.jsontemp'
        event.event_type = events.EVENT_TYPE_MODIFIED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir/4.jsontemp'
        event.dest_path = '/mydir/4.json'
        event.event_type = events.EVENT_TYPE_MOVED
        event.is_directory = False
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        event.src_path = '/mydir'
        event.event_type = events.EVENT_TYPE_MODIFIED
        event.is_directory = True
        self.wd_mng.dispatch(event)
        self.assertFalse(self.observer_inst.start.called)

        self.assertIsInstance(queue, queue.Queue)
        self.assertEqual(queue.get_nowait(), '/mydir/1.json')
        self.assertEqual(queue.get_nowait(), '/mydir/2.json')
        self.assertEqual(queue.get_nowait(), '/mydir/4.json')
        self.assertRaises(queue.Empty, queue.get_nowait)


class DataFilePostManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.os_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.os',
            autospec=True)
        self.os_mock = self.os_patcher.start()
        self.os_mock.path.join = os.path.join  # use real
        self.os_mock.path.abspath = os.path.abspath
        self.os_mock.path.basename = os.path.basename
        self.os_mock.path.dirname = os.path.dirname
        self.exist = []
        self.os_mock.path.exists.side_effect = lambda a: a in self.exist
        self.os_mock.listdir.return_value = []

        self.glob_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.glob',
            autospec=True)
        self.glob_mock = self.glob_patcher.start()
        self.glob_mock.glob.return_value = [
            '/mydir/2.json',
            '/mydir/1.json',
        ]

        self.open_mock = mock.mock_open(read_data='{"status": 1001}')
        self.open_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.open',
            self.open_mock, create=True)
        self.open_patcher.start()

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

        self.shutil_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.shutil',
            autospec=True)
        self.shutil_mock = self.shutil_patcher.start()

        self.time_patcher = mock.patch(
            'FactoryTx.core.DataFilesPostManager.time',
            autospec=True)
        self.time_mock = self.time_patcher.start()

        self.log_mock = mock.Mock(spec=logging.Logger)

        # create test instance
        self.dfp_mng = DataFilesPostManager()
        self.dfp_mng.log = self.log_mock

    def tearDown(self):
        self.os_patcher.stop()
        self.glob_patcher.stop()
        self.open_patcher.stop()
        self.wd_mng_patcher.stop()
        self.shutil_patcher.stop()
        self.time_patcher.stop()

    def test_init(self):
        self.assertEqual(self.dfp_mng.queues, {})
        self.assertIsNone(self.dfp_mng.transformInputData(None))
        self.assertIsNone(self.dfp_mng.postfunc())
        self.assertIsNone(self.dfp_mng.validateFilePostResponse(None))

    def test_load_files(self):
        files1 = self.dfp_mng.load_files(
            folder='/mydir',
            extensions=['/*.json'])

        self.assertEqual(files1, ['/mydir/1.json', '/mydir/2.json'])
        self.glob_mock.glob.assert_called_once_with('/mydir/*.json')
        self.glob_mock.glob.reset_mock()

        self.wd_mng_mock.get_new_files_queue.assert_called_once_with(
            '/mydir/*.json')
        self.assertEqual(
            self.dfp_mng.queues,
            {'/mydir/*.json': self.queue_mock})
        self.wd_mng_mock.get_new_files_queue.reset_mock()

        files2 = self.dfp_mng.load_files(
            folder='/mydir',
            extensions=['/*.json'])

        self.assertEqual(files2, ['/mydir/3.json', '/mydir/4.json'])
        self.assertFalse(self.glob_mock.glob.called)
        self.assertFalse(self.wd_mng_mock.get_new_files_queue.called)
        self.queue_mock.get_nowait.assert_called_with()
        self.assertEqual(self.queue_mock.get_nowait.call_count, 3)

        self.queue_mock.get_nowait.side_effect = [
            queue.Empty,
        ]

        files3 = self.dfp_mng.load_files(
            folder='/mydir',
            extensions=['/*.json'])

        self.assertEqual(files3, [])

    def test_read_json_file(self):
        result = self.dfp_mng.read_json_file('/mydir/1.json')
        self.assertEqual(result, {'status': 1001})

    def test_read_json_file_exception(self):
        self.open_mock.side_effect = Exception('error')
        result = self.dfp_mng.read_json_file('/mydir/1.json')
        self.assertIsNone(result)

    def test_handle_file_onsuccess(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_success': True}
        self.dfp_mng.handle_file_onsuccess('/mydir/1.json')
        self.os_mock.remove.assert_called_once_with('/mydir/1.json')

    def test_handle_file_onsuccess_exception(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_success': True}
        self.os_mock.remove.side_effect = Exception('error')
        self.dfp_mng.handle_file_onsuccess('/mydir/1.json')
        self.os_mock.remove.assert_called_once_with('/mydir/1.json')

    def test_handle_file_onsuccess_move_to_folder(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_success': False, 'postprocess_success_folder': '/tmp/databuffer/success'}
        self.dfp_mng.handle_file_onsuccess('/mydir/1.json')
        self.shutil_mock.move.assert_called_once_with(
            '/mydir/1.json', '/tmp/databuffer/success/1.json')
        self.assertFalse(self.os_mock.remove.called)

    def test_handle_file_onerror_remove(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_failure': True}
        self.dfp_mng.handle_file_onerror('/mydir/1.json')
        self.assertFalse(self.shutil_mock.move.called)
        self.os_mock.remove.assert_called_once_with('/mydir/1.json')

    def test_handle_file_onerror_remove_exception(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_failure': True}
        self.os_mock.remove.side_effect = Exception('error')
        self.dfp_mng.handle_file_onerror('/mydir/1.json')
        self.assertFalse(self.shutil_mock.move.called)
        self.os_mock.remove.assert_called_once_with('/mydir/1.json')

    def test_handle_file_onerror_move_to_folder(self):
        self.dfp_mng.postprocess_file_handling = {'remove_on_failure': False, 'postprocess_failure_folder': '/tmp/databuffer/failure'}
        self.dfp_mng.handle_file_onerror('/mydir/1.json')
        self.shutil_mock.move.assert_called_once_with(
            '/mydir/1.json', '/tmp/databuffer/failure/1.json')
        self.assertFalse(self.os_mock.remove.called)

    def test_submit_data(self):
        self.dfp_mng.postfunc = mock.Mock()
        resp_mock = self.dfp_mng.postfunc.return_value

        result = self.dfp_mng.submitData(files={'status': (1, 2, 3)})
        self.assertIs(result, resp_mock)
        self.dfp_mng.postfunc.assert_called_once_with(
            files={'status': (1, 2, 3)})

    def test_submit_data_retry_not_valid(self):
        self.dfp_mng.postfunc = mock.Mock()
        resp_mock = self.dfp_mng.postfunc.return_value

        result = self.dfp_mng.submitData(files={'status': (1, 2, 3)})
        self.assertIs(result, resp_mock)

    def test_submit_data_retry_not_valid_exception(self):
        self.dfp_mng.postfunc = mock.Mock(
            side_effect=Exception('error'))
        with self.assertRaises(Exception):
            self.dfp_mng.submitData(files={'status': (1, 2, 3)})

    def test_submit_data_retry_not_valid_req_exception(self):
        self.dfp_mng.postfunc = mock.Mock(
            side_effect=requests.RequestException('error'))

        with self.assertRaises(requests.RequestException):
            self.dfp_mng.submitData(files={'status': (1, 2, 3)})


if __name__ == '__main__':
    unittest.main()
