import os
import unittest
import yaml
import io
# import mock

from testfixtures import compare, tempdir, TempDirectory
from FactoryTx.components.dataplugins.transports.base import FileEntry, BaseTransport
from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.transports.smb.smbfiletransport import SMBFileTransport

components = component_manager()
transports = components['transports']
parsers = components['transports']

"""
    Dependency:
    1. Creating a test 'share' directory with the below Samba/smb.conf changes

    Setup Notes: to run this test please follow the setup procedures below:
    1. Install samba.
        On Ubuntu: sudo apt-get install samba
    2. Create a 'sm' user account with password '123'
        On Ubuntu: sudo adduser sm
    3. Add the user 'sm' to Samba's smbpasswd
        On Ubuntu: smbpasswd -a sm
    4. Locate the 'test' shared folder and add the below config to the bottom of the /etc/samba/smb.conf file
         [test]
            comment = test
            browseable = yes
            path = <path to your test directory>
            guest ok = no
            read only = no
            create mask = 0766
            directory mask = 0766
            valid users = sm
    5.  Make sure your <path to your test directory> allows 'other' read/write access to the files and parent directory
"""

@unittest.skip("Manually Required Test Setup.  See Notes above.")
class TestSMBFileTransport(unittest.TestCase):
    config_template = """
  version: 1.0.0
  remote_ip: '127.0.0.1'
  remote_name: 'localhost'
  username: 'sm'
  password: '123'
  shared_folder: 'test'
  dir_path: ''
  source: 'testsmb'
"""
    SAMBA_MOUNT_PATH = '/mnt/samba/osk'
    SRC_FILE = 'src-test.txt'
    DEST_FILE = 'dest-test.txt'
    MTIME = 1458162978  # 2016-03-16T21:16:18Z
    CONTENTS = 'this is test contents\n'

    def setUp(self):

        self.srcdir = self.SAMBA_MOUNT_PATH

        s = io.StringIO(self.config_template)
        self.config = yaml.load(s)

        self.tm = transports
        self.tm.load_schemas()
        self.schema = self.tm.get_plugin_schema('smb', '1.0.0')
        transports.load_schemas()

        config_yaml = self.config_template.format(self=self)

        self.smbft = SMBFileTransport()
        self.smbft.loadParameters(self.schema, yaml.load(config_yaml))

    def tearDown(self):
        self.smbft.disconnect()

    def write_src_file_path(self, target_path, filename, mtime, contents):
        fullpath = os.path.join(target_path, filename)
        with open(fullpath, 'wb') as f:
            f.write(contents)
        os.utime(fullpath, (mtime, mtime))

    def write_src_file(self, filename, mtime, contents):
        self.write_src_file_path(self.srcdir, filename, mtime, contents)

    def test_connect_good_server(self):
        """
        test a proper connection
        :return: none
        """
        self.smbft.connect()
        self.assertEqual(self.smbft.connected, True)

    def test_list_one_file(self):
        """
        test if a single file on the share exists and its path is defined properly
        :return: none
        """
        # fobj = mock.MagicMock()
        # fobj.path = self.SRC_FILE
        # self.instance.list_files.return_value = [fobj]

        self.write_src_file(self.SRC_FILE, self.MTIME, self.CONTENTS)

        self.smbft.dir_path = '/'
        try:
            res = self.smbft.list_files()
        except Exception:
            pass
        self.assertGreaterEqual(len(res), 1)
        if len(res) > 0:
            self.assertEqual(res[0].path, u'/test.txt')

    def test_list_one_file_in_subdir(self):
        """
        test if a single file in a sub-directory on the share exists and its path is defined properly
        :return:
        """
        target_path = self.srcdir + '/test'
        # fobj.path = os.path.join(target_path, self.SRC_FILE)
        self.write_src_file_path(target_path, self.SRC_FILE, self.MTIME, self.CONTENTS)
        self.smbft.dir_path = '/test/'
        try:
            res = self.smbft.list_files()
        except Exception:
            pass
        self.assertGreaterEqual(len(res), 1)
        if len(res) > 0:
            self.assertEqual(res[0].path, u'/test/test.txt')

    def test_list_bad_folder(self):
        """
        negative test case, look for sub-directory on share that does not exists
        :return:
        """
        self.smbft.dir_path = 'est'
        try:
            res = self.smbft.list_files()
            self.assertEqual(res, [])
        except Exception:
            pass

    @tempdir()
    def test_copy_empty_file(self, destdir):
        """
        test case, copy a empty file
        :return: none
        """
        self.write_src_file('test-empty.txt', self.MTIME, "")
        # self.instance.copy_file.return_value = ({}, 0)
        file_entry = FileEntry(
            transport=self.smbft,
            path="test-empty.txt",
            mtime=self.MTIME,
            size=0
        )
        res = self.smbft.copy_file(remote_file_entry=file_entry, local_path=destdir.getpath(self.DEST_FILE))
        self.assertEqual(res, None)

    @tempdir()
    def test_copy_file_exception(self, destdir):
        """
        negative test case, copy invalid file expect exception
        :return: none
        """
        file_entry = FileEntry(
            transport=self.smbft,
            path=os.path.join('test', 'test000.txt'),
            mtime=self.MTIME,
            size=0
        )

        # self.instance.copy_file.side_effect = AttributeError
        try:
            self.smbft.copy_file(remote_file_entry=file_entry, local_path=destdir.getpath(self.DEST_FILE))
        except Exception:
            pass

        # res = self.smbft.copy_file(remote_file_entry=file_entry, local_path=os.path.join(TempDirectory(), "test.txt"))
        # self.assertEqual(self.smbft.log.error.call_count, 1)
        self.assertEqual(self.smbft.connected, False)

    @tempdir()
    def test_copy_file(self, destdir):
        """
        test case, copy file
        :return:
        """
        file_entry = FileEntry(
            transport=self.smbft,
            path=os.path.join('test', self.SRC_FILE),
            mtime=self.MTIME,
            size = len(self.CONTENTS)
        )

        # open_mock = mock.mock_open()
        # self.instance.copy_file.return_value = None

        # with mock.patch('__builtin__.open', open_mock):
        res = self.smbft.copy_file(remote_file_entry=file_entry, local_path=destdir.getpath(self.DEST_FILE))
        self.assertEqual(res, None)
            # res = self.smbft.copy_file(remote_file_entry=file_entry, local_path=os.path.join(TempDirectory(), "test.txt"))

    def test_delete_exception(self):
        """
        negative test case, delete a non-existent file
        :return:
        """
        file_entry = FileEntry(
            transport=self.smbft,
            path=os.path.join('test', 'test000.txt'),
            mtime=self.MTIME,
            size=0
        )

        try:
            res = self.smbft.delete_file(remote_file_entry=file_entry)
            self.assertEqual(res, None)
        except Exception:
            pass

        self.assertEqual(self.smbft.connected, False)

    def test_delete(self):
        """
        test case, delete a file
        :return:
        """
        file_entry = FileEntry(
            transport=self.smbft,
            path=os.path.join('test', 'test.txt'),
            mtime=self.MTIME,
            size = len(self.CONTENTS)
        )

        try:
            res = self.smbft.delete_file(remote_file_entry=file_entry)
            self.assertEqual(res, None)
        except Exception:
            pass

    def test_connect_bad_server(self):
        """
        negative test case, test a bad connection where the server does not exists
        :return: none
        """
        # self.instance.connect.side_effect = AttributeError
        self.smbft.disconnect()
        tmp = self.smbft.remote_ip
        self.smbft.remote_ip = "128.0.0.1"
        try:
            ret = self.smbft.connect()
        except Exception:
            pass

        self.smbft.remote_ip = tmp
        self.assertEqual(self.smbft.connected, False)
        self.assertEqual(ret, False)


if __name__ == '__main__':
    unittest.main()
