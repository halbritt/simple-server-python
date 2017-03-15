import os
import unittest

from testfixtures import compare, tempdir, TempDirectory

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.transports.base import FileEntry
from FactoryTx.components.dataplugins.transports.localfile import LocalFileTransport

transports = component_manager()['transports']

class LocalFileTransportTest(unittest.TestCase):
    PATH = 'test.txt'
    DEST_PATH = 'dest-test2.txt'
    MTIME = 1458162978  # 2016-03-16T21:16:18Z
    CONTENTS = 'this is a test\n'

    @classmethod
    def setUpClass(cls):
        super(LocalFileTransportTest, cls).setUpClass()
        cls.manager = transports
        cls.manager.load_schemas()

    def setUp(self):
        super(LocalFileTransportTest, self).setUp()
        self.srcdir = TempDirectory()  # Location that files are copied from.

        schema = self.manager.get_plugin_schema('localfile', '1.0.0')
        self.transport = LocalFileTransport()
        self.transport.loadParameters(schema, {'root_path': self.srcdir.path})

    def tearDown(self):
        super(LocalFileTransportTest, self).tearDown()
        self.srcdir.cleanup()

    def write_src_file(self, filename, mtime, contents):
        self.srcdir.write(filename, contents)
        os.utime(self.srcdir.getpath(filename), (mtime, mtime))

    def test_list_files(self):
        compare(self.transport.list_files(), [])

        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)

        compare(self.transport.list_files(), [
            FileEntry(transport=self.transport,
                      path=self.PATH,
                      mtime=self.MTIME,
                      size=len(self.CONTENTS)),
        ])

    def test_delete_file(self):
        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)
        file_entry, = self.transport.list_files()
        self.transport.delete_file(file_entry)

        compare(self.transport.list_files(), [])

    def test_delete_file_twice(self):
        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)
        file_entry, = self.transport.list_files()
        self.transport.delete_file(file_entry)
        with self.assertRaises(Exception):
            self.transport.delete_file(file_entry)

    @tempdir()
    def test_copy_file(self, destdir):
        destpath = destdir.getpath(self.DEST_PATH)

        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)
        file_entry, = self.transport.list_files()
        self.transport.copy_file(file_entry, destpath)

        compare(self.CONTENTS, destdir.read(self.DEST_PATH))
        compare(self.MTIME, os.path.getmtime(destpath))

        # The original file should still be present in the source folder.
        compare(self.transport.list_files(), [
            FileEntry(transport=self.transport,
                      path=self.PATH,
                      mtime=self.MTIME,
                      size=len(self.CONTENTS)),
        ])

    @tempdir()
    def test_copy_file_exists(self, destdir):
        destpath = destdir.getpath(self.DEST_PATH)
        destdir.write(self.DEST_PATH, 'some test data\n')

        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)
        file_entry, = self.transport.list_files()
        self.transport.copy_file(file_entry, destpath)

        compare(self.CONTENTS, destdir.read(self.DEST_PATH))
        compare(self.MTIME, os.path.getmtime(destpath))

    @tempdir()
    def test_copy_file_missing(self, destdir):
        destpath = destdir.getpath(self.DEST_PATH)
        self.write_src_file(self.PATH, self.MTIME, self.CONTENTS)
        file_entry, = self.transport.list_files()
        self.transport.delete_file(file_entry)

        with self.assertRaises(Exception):
            self.transport.copy_file(file_entry, destpath)

        destdir.check()  # The destination directory should remain empty.
