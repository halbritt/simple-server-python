from datetime import datetime, timedelta
import json
import os
import os.path
import stat
import unittest

import mock
from testfixtures import compare, Replacer, TempDirectory
from testfixtures import Comparison as C
from testfixtures import StringComparison as S
import yaml

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.parsers.testparser.TestParser import TestParser
from FactoryTx.components.dataplugins.file import FileService
import FactoryTx.components.dataplugins.transports.localfile  # Needed to patch LocalFileTransport
from tests.utils.UnitTestUtils import load_sslogs
from nose.tools import nottest

components = component_manager()
testparser = TestParser()
parser_manager = components['parsers']
transport_manager = components['transports']


class FileServiceTests(unittest.TestCase):
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
- type: localfile
  config:
    version: 1.0.0
    root_path: {self.src_dir_2.path}
parsers:
- type: testparser
  config:
    version: 1.0.0
    filename_patterns: ['*.txt']
    id: text
    sslog_type: text
- type: testparser
  config:
    version: 1.0.0
    filename_patterns: ['*.ply', '*.xyz', '*.shape.txt']
    id: shapes
    sslog_type: shapes
    sslogs_per_file: 0
- type: testparser
  config:
    version: 1.0.0
    filename_patterns: ['*.ini']
    id: recipes
    sslog_type: recipes
    sslogs_per_file: 2
"""

    @classmethod
    def setUpClass(cls):
        super(FileServiceTests, cls).setUpClass()
        cls.pm = components['dataplugins']
        cls.pm.load_schemas()
        cls.schema = cls.pm.get_plugin_schema('file', '1.0.0')

        parser_manager.load_schemas()
        transport_manager.load_schemas()

    def setUp(self):
        self.source = 'AA_BB_ExampleMachine_1'

        self.src_dir_1 = TempDirectory()
        self.src_dir_2 = TempDirectory()
        self.output_dir = TempDirectory()
        self.completed_dir = TempDirectory()

        config_yaml = self.config_template.format(self=self)
        self.plugin = FileService()
        self.plugin.loadParameters({}, self.schema, yaml.load(config_yaml))

        testparser.clear_parsed_files()  # Clear the history of the dummy parsers.

    def tearDown(self):
        self.src_dir_1.cleanup()
        self.src_dir_2.cleanup()
        self.output_dir.cleanup()
        self.completed_dir.cleanup()

    def test_ready_files_empty(self):
        self.plugin.run_once()
        compare(testparser.get_parsed_files(), [])

    def test_ready_files_written_once(self):
        # The first time we see a file we should ignore it, since it might
        # not be completely written yet.
        self.src_dir_2.write('test1.txt', 'example-content')
        self.src_dir_1.write('test2.txt', 'example-content2\n')
        self.plugin.run_once()
        first_files = testparser.get_parsed_files()
        self.plugin.run_once()
        final_files = testparser.get_parsed_files()

        compare(first_files, [])
        compare(final_files, [
            testparser.ParsedFile('text', 'test1.txt', None, 'example-content'),
            testparser.ParsedFile('text', 'test2.txt', None, 'example-content2\n'),
        ])

    def test_ready_files_multiple_writes(self):
        # Files should not be returned from check_new_files until the file size
        # stops changing during the polling period.
        self.src_dir_1.write('test1.txt', 'example-content')
        self.src_dir_1.write('test2.txt', 'example-content2\n')
        self.src_dir_1.write('test3.txt', 'example-content3\n')

        self.plugin.run_once()
        first_files = testparser.get_parsed_files()
        first_remaining = os.listdir(self.src_dir_1.path)
        self.src_dir_1.write('test2.txt', 'example-content2+1\n')
        self.src_dir_1.write('test3.txt', 'example-content3+1\n')

        self.plugin.run_once()
        second_files = testparser.get_parsed_files()
        second_remaining = os.listdir(self.src_dir_1.path)

        testparser.clear_parsed_files()
        self.plugin.run_once()
        final_files = testparser.get_parsed_files()
        final_remaining = os.listdir(self.src_dir_1.path)

        compare(first_files, [])
        compare(sorted(first_remaining), ['test1.txt', 'test2.txt', 'test3.txt'])
        compare(second_files, [
            testparser.ParsedFile('text', 'test1.txt', None, 'example-content'),
        ])
        compare(sorted(second_remaining), ['test2.txt', 'test3.txt'])
        compare(final_files, [
            testparser.ParsedFile('text', 'test2.txt', None, 'example-content2+1\n'),
            testparser.ParsedFile('text', 'test3.txt', None, 'example-content3+1\n'),
        ])
        compare(final_remaining, [])

    def test_parser_routing_not_matched(self):
        # Unmatched files should be left in the source directory, and should
        # not be passed to any parsers. No completed files should be created.
        contents = 'not matched\n'
        self.src_dir_1.write('test.bar', contents)

        copy_mock = mock.Mock()
        with Replacer() as replacer:
            replacer.replace('FactoryTx.transports.localfile.LocalFileTransport.copy_file',
                             copy_mock)
            self.plugin.run_once()
            self.plugin.run_once()

        # Check that ignored files are not copied through the transport.
        compare(copy_mock.mock_calls, [])

        compare(testparser.get_parsed_files(), [])
        compare(self.src_dir_1.read('test.bar'), contents)

        self.completed_dir.check()

    def test_parser_routing_single_match(self):
        # Matched files should be passed to the appropriate parser and deleted
        # from the source directory. Matching completed files should always be
        # created.
        text_contents = 'text file\n'
        recipe_contents = '1. Just add water!\n2. ???\n3. Profit!'
        self.src_dir_1.write('test.txt', text_contents)
        self.src_dir_1.write('test.ini', recipe_contents)
        self.plugin.run_once()
        self.plugin.run_once()

        compare(testparser.get_parsed_files(), [
            testparser.ParsedFile('recipes', 'test.ini', None, recipe_contents),
            testparser.ParsedFile('text', 'test.txt', None, text_contents),
        ])

        self.src_dir_1.check()

        self.completed_dir.check('test.ini', 'test.txt')
        compare(self.completed_dir.read('test.txt'), text_contents)
        compare(self.completed_dir.read('test.ini'), recipe_contents)

    def test_parser_routing_many_matches(self):
        # Matched files should be passed to the appropriate parser and deleted
        # from the source directory. Matching completed files should always be
        # created.

        contents = {
            '0.shape.txt': 'shape 0',
            '1.shape.txt': 'shape 1',
            '2.shape.txt': 'shape 2',
            'test.txt': 'text file\n',
            'test.txt.bak': 'text file\n',
            '1.xyz': '1 1 1\n2 2 2\n1 2 3\n',
        }

        for filename in ['0.shape.txt', '1.shape.txt', 'test.txt', 'test.txt.bak']:
            self.src_dir_1.write(filename, contents[filename])
        for filename in ['2.shape.txt', '1.xyz']:
            self.src_dir_2.write(filename, contents[filename])
        self.plugin.run_once()
        self.plugin.run_once()

        parsed_files = testparser.get_parsed_files()

        compare(parsed_files, [
            testparser.ParsedFile('shapes', '0.shape.txt', None, contents['0.shape.txt']),
            testparser.ParsedFile('text', '0.shape.txt', None, contents['0.shape.txt']),
            testparser.ParsedFile('shapes', '1.shape.txt', None, contents['1.shape.txt']),
            testparser.ParsedFile('text', '1.shape.txt', None, contents['1.shape.txt']),
            testparser.ParsedFile('shapes', '1.xyz', None, contents['1.xyz']),
            testparser.ParsedFile('shapes', '2.shape.txt', None, contents['2.shape.txt']),
            testparser.ParsedFile('text', '2.shape.txt', None, contents['2.shape.txt']),
            testparser.ParsedFile('text', 'test.txt', None, contents['test.txt']),
        ])

        parsed_filenames = sorted(set([f.remote_path for f in parsed_files]))

        self.src_dir_1.check('test.txt.bak')
        self.completed_dir.check(*parsed_filenames)

        for filename in parsed_filenames:
            compare(self.completed_dir.read(filename), contents[filename])

    def test_reading_completed_file(self):
        # When a file that has been parsed previously is read, the parser should
        # be supplied with a path to a file containing the contents as of the
        # prior parser invocation.
        #
        # Since .ply files are configured to produce no sslogs, this test also
        # doubles as a check that files are still moved to completed if no
        # sslog is emitted.
        original_contents = '11 12 3\n'
        new_contents = '11 12 3\n42 42 42\n'
        other_contents_1 = '12 12 13\n'
        other_contents_2 = '11 22 33\n'

        self.src_dir_1.write('old-1.ply', original_contents)
        self.src_dir_1.write('other-1.ply', other_contents_1)

        self.plugin.run_once()
        self.plugin.run_once()
        testparser.clear_parsed_files()

        self.src_dir_1.write('old-1.ply', new_contents)
        self.src_dir_1.write('other-2.ply', other_contents_2)

        self.plugin.run_once()
        self.plugin.run_once()

        parsed_files = testparser.get_parsed_files()

        compare(parsed_files, [
            testparser.ParsedFile('shapes', 'old-1.ply', original_contents, new_contents),
            testparser.ParsedFile('shapes', 'other-2.ply', None, other_contents_2),
        ])

        self.src_dir_1.check()
        self.src_dir_2.check()
        self.completed_dir.check('old-1.ply', 'other-1.ply', 'other-2.ply')
        compare(self.completed_dir.read('old-1.ply'), new_contents)
        compare(self.completed_dir.read('other-1.ply'), other_contents_1)
        compare(self.completed_dir.read('other-2.ply'), other_contents_2)
        self.output_dir.check()

    MTIME = 1458256052
    MTIMES = {
        '2.txt': MTIME + 15,  # 2016-03-17T23:07:47Z
        '1.ply': MTIME + 17,  # 2016-03-17T23:07:49Z
        '0.ini': MTIME + 20,  # 2016-03-17T23:07:52Z
        '1.txt': MTIME + 21,  # 2016-03-17T23:07:53Z
    }

    def _write_mtime_ordered_files(self):
        self.src_dir_2.write('0.ini', '0')
        self.src_dir_1.write('2.txt', '2')
        self.src_dir_1.write('1.txt', '1txt')
        self.src_dir_1.write('1.ply', '1')

        os.utime(self.src_dir_1.getpath('2.txt'), (self.MTIME + 20, self.MTIMES['2.txt']))
        os.utime(self.src_dir_1.getpath('1.ply'), (self.MTIME + 19, self.MTIMES['1.ply']))
        os.utime(self.src_dir_2.getpath('0.ini'), (self.MTIME + 19, self.MTIMES['0.ini']))
        os.utime(self.src_dir_1.getpath('1.txt'), (self.MTIME + 20, self.MTIMES['1.txt']))

    def _sslog_comparison(self, sslog_type, remote_path, mtime,
                          completed_contents, contents):
        mtime_as_dt = datetime(1970, 1, 1) + timedelta(seconds=mtime)
        timestamp = mtime_as_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
        return {
            '_id': S('.*'),
            'counter': C(int),
            'sslog_type': sslog_type,
            'source': self.source,
            'timestamp': timestamp,
            'fieldvalues': {
                'remote_path': {'value': remote_path, 'units': None},
                'completed_contents': {'value': completed_contents, 'units': None},
                'contents': {'value': contents, 'units': None},
            },
        }

    def _check_mtime_ordered_sslogs(self):
        sslogs = load_sslogs(self.output_dir.path)
        compare(sslogs, [
            self._sslog_comparison('text', '2.txt', self.MTIMES['2.txt'], None, '2'),
            # .ply files do not output sslogs in this test case.
            self._sslog_comparison('recipes', '0.ini', self.MTIMES['0.ini'], None, '0'),
            self._sslog_comparison('recipes', '0.ini', self.MTIMES['0.ini'], None, '0'),
            self._sslog_comparison('text', '1.txt', self.MTIMES['1.txt'], None, '1txt'),
        ])

    def test_ordered_by_mtime(self):
        # Files handled by a single plugin should be parsed with the lowest
        # mtime first, even across transport boundaries.
        self._write_mtime_ordered_files()

        self.plugin.run_once()
        self.plugin.run_once()

        parsed_files = testparser.get_parsed_files(chronological=True)
        compare(parsed_files, [
            testparser.ParsedFile('text', '2.txt', None, '2'),
            testparser.ParsedFile('shapes', '1.ply', None, '1'),
            testparser.ParsedFile('recipes', '0.ini', None, '0'),
            testparser.ParsedFile('text', '1.txt', None, '1txt'),
        ])

        self._check_mtime_ordered_sslogs()

    def test_ordered_by_mtime_despite_list_errors(self):
        # Files handled by a single plugin should be parsed with the lowest
        # mtime first, even if one or more transports are disconnected.
        self._write_mtime_ordered_files()

        old_list_files = FactoryTx.transports.localfile.LocalFileTransport.list_files

        with Replacer() as replacer:
            # Replace list_files with a function that succeeds for one
            # transport, but fails for the other.
            def failing_list_files(transport):
                if transport.root_path == self.src_dir_2.path:
                    raise Exception('Unexpected exception in copy_files!')
                return old_list_files()

            replacer.replace('FactoryTx.transports.localfile.LocalFileTransport.list_files',
                             failing_list_files)

            with self.assertRaises(Exception):
                self.plugin.run_once()
            with self.assertRaises(Exception):
                self.plugin.run_once()

        self.plugin.run_once()
        self.plugin.run_once()

        self._check_mtime_ordered_sslogs()

    @nottest
    def test_ordered_by_mtime_despite_copy_errors(self):
        # Files handled by a single plugin should be parsed with the lowest
        # mtime first, even if exceptions occur in copy_file.
        self._write_mtime_ordered_files()

        old_copy_file = FactoryTx.transports.localfile.LocalFileTransport.copy_file
        call_count = [0]

        with Replacer() as replacer:
            # Replace copy_file with a function that runs once successfully,
            # then fails indefinitely.
            def failing_copy_file(self, file_entry, local_path):
                if call_count[0] > 0:
                    raise Exception('Unexpected exception in copy_file!')
                call_count[0] += 1
                return old_copy_file(file_entry, local_path)

            replacer.replace('FactoryTx.transports.localfile.LocalFileTransport.copy_file',
                             failing_copy_file)

            self.plugin.run_once()
            with self.assertRaises(Exception):
                self.plugin.run_once()

        self.plugin.run_once()
        self._check_mtime_ordered_sslogs()
    
    @nottest
    def test_ordered_by_mtime_despite_delete_errors(self):
        # Files handled by a single plugin should be parsed with the lowest
        # mtime first, even if exceptions occur in delete_file.
        #
        # Files that are not deleted successfully will be reparsed, however
        # the completed path will be set on the second call.
        self._write_mtime_ordered_files()

        old_delete_file = FactoryTx.transports.localfile.LocalFileTransport.delete_file
        call_count = [0]

        with Replacer() as replacer:
            # Replace delete_file with a function that runs once successfully,
            # then fails indefinitely.
            def failing_delete_file(self, file_entry):
                if call_count[0] > 0:
                    raise Exception('Unexpected exception in delete_file!')
                call_count[0] += 1
                return old_delete_file(file_entry)

            replacer.replace('FactoryTx.transports.localfile.LocalFileTransport.delete_file',
                             failing_delete_file)

            self.plugin.run_once()
            with self.assertRaises(Exception):
                self.plugin.run_once()

        self.plugin.run_once()

        # If a delete fails, then an sslog will still have been emitted and the
        # file will have been written to the completed directory -- it's up to
        # the parser to detect this and avoid emitting duplicate sslogs.
        sslogs = load_sslogs(self.output_dir.path)
        compare(sslogs, [
            self._sslog_comparison('text', '2.txt', self.MTIMES['2.txt'], None, '2'),
            self._sslog_comparison('text', '2.txt', self.MTIMES['2.txt'], '2', '2'),
            # .ply files do not output sslogs in this test case.
            self._sslog_comparison('recipes', '0.ini', self.MTIMES['0.ini'], None, '0'),
            self._sslog_comparison('recipes', '0.ini', self.MTIMES['0.ini'], None, '0'),
            self._sslog_comparison('text', '1.txt', self.MTIMES['1.txt'], None, '1txt'),
        ])

    @nottest
    def test_ordered_by_mtime_despite_write_errors(self):
        # Files handled by a single plugin should be emitted with the lowest
        # mtime first, even if exceptions occur while writing sslogs.

        self._write_mtime_ordered_files()

        try:
            # XXX: Does Windows support read-only directories?
            os.chmod(self.output_dir.path, stat.S_IXUSR)
            self.plugin.run_once()  # No writes attempted on first traversal.

            with self.assertRaises(Exception):
                self.plugin.run_once()
        finally:
            os.chmod(self.output_dir.path, stat.S_IRWXU)

        self.plugin.run_once()
        self.plugin.run_once()

        self._check_mtime_ordered_sslogs()

def test_in_service(pipeline_environment):
    print("pipeline:", pipeline_environment)
    assert 0
