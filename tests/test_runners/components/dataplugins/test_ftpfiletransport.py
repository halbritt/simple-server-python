import os
import unittest
import yaml
import mock
import io
#from SimpleData.transports.ftp import FTPFileTransport


@unittest.skip("Using outdated API")
class TestFTPFileTransport(unittest.TestCase):

    def setUp(self):
        cfg = """
        version: 1.0.0
        remote_ip: 127.0.0.1
        remote_name: localhost
        remote_path: test
        username: sm
        password: '123'
        remote_port: 21
        timeout: 60
        pattern: '.*'
        shared_folder: 'test'
        poll_rate: 10
        output_folder: '/tmp'
        completed_folder: '/tmp/completed'
        remote_path: ''
        fileprocessors:
        - processor: example
          version: 1.0.0
          config: none
        source: 'FTP_Test_Cases'
        """
        s = io.StringIO(cfg)
        self.config = yaml.load(s)
        self.ftp_patcher = mock.patch(
            'SimpleData.transports.ftp.ftpfiletransport.FTP')
        self.ftp_mock = self.ftp_patcher.start()
        self.instance = self.ftp_mock.return_value

        self.ftp = FTPFileTransport()
        self.ftp.__dict__.update(self.config)
        self.ftp.log = mock.MagicMock()

    def tearDown(self):
        self.ftp_patcher.stop()

    def test_connect_exception(self):
        self.ftp_mock.side_effect = AttributeError
        ret = self.ftp.connect()
        self.assertEqual(self.ftp.log.error.call_count, 1)
        self.assertEqual(self.ftp.connected, False)
        self.assertEqual(ret, False)

    def test_connect_success(self):
        ret = self.ftp.connect()
        self.assertEqual(ret, True)
        self.assertEqual(self.ftp.connected, True)

    def test_list_exeption(self):
        self.instance.dir.side_effect = AttributeError
        res = self.ftp.list_path(dir_path='est')
        self.assertEqual(self.ftp.log.error.call_count, 1)
        self.assertEqual(self.ftp.connected, False)
        self.assertEqual(res, [])

    def test_list_one_file(self):
        self.instance.dir = lambda x: x('test.txt')
        res = self.ftp.list_path(dir_path='test')
        self.assertEqual(res, ['test/test.txt'])

    def test_list_one_file_filtered(self):
        self.instance.dir = lambda x: x('rwxrwxr-x Dec 23 17:15 test.txt')
        res = self.ftp.list_path(dir_path='test', pattern='.*.csv')
        self.assertEqual(res, [])

    def test_list_two_files_filtered(self):
        lst = ['rwxrwxr-x Dec 23 17:15 test.txt', 'rwxrwxr-x Dec 23 17:15 test.csv']

        def dir_callback(callback):
            for line in lst:
                callback(line)

        self.instance.dir = dir_callback
        res = self.ftp.list_path(dir_path='test', pattern='.*.csv')
        self.assertEqual(res, ['test/test.csv'])

    def test_list_recursive(self):
        lst = ['rwxrwxr-x Dec 23 17:15 test.txt',
               'rwxrwxr-x Dec 23 17:15 test.csv',
               'drwxrwxr-x Dec 23 17:15 folder']

        def dir_callback(callback):
            for line in lst:
                callback(line)
            lst.pop()

        self.instance.dir = dir_callback
        res = self.ftp.list_path(dir_path='test', recursive=True)
        self.assertEqual(res, ['test/test.txt',
                               'test/test.csv',
                               'test/folder/test.txt',
                               'test/folder/test.csv'])

    def test_copy_file_exeption(self):
        file_name = os.path.join('test', 'test.txt')

        self.instance.retrbinary.side_effect = AttributeError

        res = self.ftp.copy_file(file_path=file_name)
        self.assertEqual(self.ftp.log.error.call_count, 1)
        self.assertEqual(self.ftp.connected, False)
        self.assertEqual(res, None)

    def test_copy_file(self):
        file_name = os.path.join('test', 'test.txt')

        open_mock = mock.mock_open()

        with mock.patch('__builtin__.open', open_mock):
            res = self.ftp.copy_file(file_path=file_name)
            self.assertEqual(res, '/tmp/test.txt')
            self.assertEqual(open_mock.call_count, 1)

    def test_delete_exeption(self):
        file_name = os.path.join('test', 'test.txt')

        self.instance.delete.side_effect = AttributeError
        res = self.ftp.delete_file(file_path=file_name)
        self.assertEqual(self.ftp.log.error.call_count, 1)
        self.assertEqual(self.ftp.connected, False)
        self.assertEqual(res, False)

    def test_delete(self):
        file_name = os.path.join('test', 'test.txt')

        self.instance.delete.return_value = True
        res = self.ftp.delete_file(file_path=file_name)
        self.assertEqual(res, True)

    def test_create_exeption(self):
        file_name = os.path.join('test', 'test.txt')

        self.instance.storbinary.side_effect = AttributeError
        res = self.ftp.create_file(file_path=file_name, fileobj='')
        self.assertEqual(self.ftp.log.error.call_count, 1)
        self.assertEqual(self.ftp.connected, False)
        self.assertEqual(res, False)

    def test_create(self):
        file_name = os.path.join('test', 'test.txt')

        self.instance.storbinary.return_value = True
        res = self.ftp.create_file(file_path=file_name,  fileobj='')
        self.assertEqual(res, True)


if __name__ == '__main__':
    unittest.main()
