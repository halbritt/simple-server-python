import os
import unittest
import tempfile
import shutil
import uuid

import mock
from testfixtures import TempDirectory

from FactoryTx.components.transforms.sslogtransform.sslogtransform import SSLogTransform
from FactoryTx.managers.PluginManager import component_manager

manager = component_manager()['transforms']

class SpreadSheetTestCase(unittest.TestCase):
    def setUp(self):
        self.completed_dir = TempDirectory()
        self.output_dir = TempDirectory()
        self.target_dir = TempDirectory()

        self.config = {
            'version': '1.0.0',
            'polltime': 10,
            'target_folders': [self.target_dir.path],
            'output_folder': self.output_dir.path,
            'completed_folder': self.completed_dir.path,
            'parse_options': [
                {'report_pattern': '*Activ*.csv',
                'load': {}
                },
                {
                'report_pattern': '*Ord*.csv',
                'load': {}
                }]
            }

        self.sdconfig = {
            'plugins': {
                'data': '/mydir/spool/'
            }
        }

        self.pm = manager
        self.pm.load_schemas()
        self.schema = self.pm.get_plugin_schema('spreadsheet', '1.0.0')
        self.plugin = SSLogtransform()
        self.plugin.loadParameters(self.sdconfig, self.schema, self.config)

    def tearDown(self):
        self.completed_dir.cleanup()
        self.output_dir.cleanup()
        self.target_dir.cleanup()

    def test_load_config(self):
        self.assertEquals(self.plugin.polltime, 10)
        self.assertEquals(self.plugin.target_folders, [self.target_dir.path])
        self.assertEquals(self.plugin.output_folder, self.output_dir.path)
        self.assertEquals(self.plugin.completed_folder, self.completed_dir.path)
        self.assertEquals(len(self.plugin.parse_options), 2)
        self.assertEquals(self.plugin.parse_options[0]['report_pattern'],
                          '*Activ*.csv')
        self.assertEquals(self.plugin.parse_options[1]['report_pattern'],
                          '*Ord*.csv')

        self.plugin.connect()
        self.assertTrue(self.plugin.connected)

    def test_check_files_empty(self):
        files, sizes = self.plugin.check_new_files(self.target_dir.path, {})
        self.assertEqual(files, [])
        self.assertEqual(sizes, {})

    def test_check_files_written_once(self):
        # The first time we see a file we should ignore it, since it might
        # not be completely written yet.
        self.target_dir.write('test1.txt', 'foo')
        self.target_dir.write('test2.txt', 'bar')
        first_files, sizes = self.plugin.check_new_files(self.target_dir.path, {})
        second_files, sizes = self.plugin.check_new_files(self.target_dir.path, sizes)
        self.assertEqual(first_files, [])
        self.assertEqual(sorted(second_files), [
            self.target_dir.getpath('test1.txt'),
            self.target_dir.getpath('test2.txt'),
        ])

    def test_check_files_sizes_shrinks(self):
        # The sizes dict returned by check_new_files should not retain entries.
        self.target_dir.write('test.txt', 'foo')
        _, sizes = self.plugin.check_new_files(self.target_dir.path, {})
        os.remove(self.target_dir.getpath('test.txt'))
        _, sizes = self.plugin.check_new_files(self.target_dir.path, sizes)
        self.assertEqual(sizes, {})

    def test_check_files_multiple_writes(self):
        # Files should not be returned from check_new_files until the file size
        # stops changing during the polling period.
        self.target_dir.write('test.txt', 'first lin')
        first_files, sizes = self.plugin.check_new_files(self.target_dir.path, {})
        self.target_dir.write('test.txt', 'first line\nsecond line\n')
        second_files, sizes = self.plugin.check_new_files(self.target_dir.path, sizes)
        self.target_dir.write('test.txt', 'first line\nsecond line\nthird')
        third_files, sizes = self.plugin.check_new_files(self.target_dir.path, sizes)
        last_files, sizes = self.plugin.check_new_files(self.target_dir.path, sizes)
        self.assertEqual(first_files, [])
        self.assertEqual(second_files, [])
        self.assertEqual(third_files, [])
        self.assertEqual(last_files, [self.target_dir.getpath('test.txt')])

    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.read_csv')
    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.save_csv')
    def test_process_files(self, mock_save, mock_read):
        data = 'Hi, Test data'
        self.target_dir.write('Activity.csv', data)
        self.target_dir.write('Order.csv', data)
        self.plugin.process([self.target_dir.getpath('Activity.csv'),
                             self.target_dir.getpath('Order.csv')])
        self.assertEquals(mock_read.call_count, 2)

    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.read_csv')
    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.save_csv')
    def test_process_equals_files(self, mock_save, mock_read):
        data = 'Hi, Test data'
        self.target_dir.write('Activity.csv', data)
        self.target_dir.write('Order.csv', data)
        self.completed_dir.write('Activity.csv', data)
        self.completed_dir.write('Order.csv', data)
        self.plugin.process([self.target_dir.getpath('Activity.csv'),
                             self.target_dir.getpath('Order.csv')])
        self.assertEquals(mock_read.call_count, 0)

    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.read_csv')
    @mock.patch('FactoryTx.components.transforms.sslogtransform.SSLogtransform.save_csv')
    def test_process_diff_files(self, mock_save, mock_read):
        original_data = '\n'.join(['Hi, Test {}'.format(i) for i in xrange(10)]) + '\n'
        new_data = original_data + '\nHi, Test End'
        self.target_dir.write('Activity.csv', new_data)
        self.target_dir.write('Order.csv', new_data)
        self.completed_dir.write('Activity.csv', original_data)
        self.completed_dir.write('Order.csv', original_data)
        self.plugin.process([self.target_dir.getpath('Activity.csv'),
                             self.target_dir.getpath('Order.csv')])
        self.assertEqual(mock_read.call_args[1]['skiprows'], [i for i in range(1,10)])
        self.assertEquals(mock_read.call_count, 2)

    def test_check_old_version(self):
        file_1 = tempfile.NamedTemporaryFile(delete=False)
        tmp_dir = tempfile.mkdtemp()
        self.plugin.completed_folder = tmp_dir

        # No old file version in completed
        self.assertEqual(self.plugin.check_old_version(file_1.name), 0)

        shutil.copy2(file_1.name, tmp_dir)
        file_2 = os.path.join(tmp_dir, file_1.name.split(os.sep)[-1])

        # File in completed are equal
        self.assertEqual(self.plugin.check_old_version(file_1.name), -1)

        data = str(uuid.uuid1()) + '\n'

        with open(file_2, 'w') as fd:
            fd.write(data)

        # File in completed bigger
        self.assertEqual(self.plugin.check_old_version(file_1.name), -1)

        for _ in range(5):
            file_1.write(data)
        file_1.write(data)
        file_1.close()

        # Change starts from line 1
        self.assertEqual(self.plugin.check_old_version(file_1.name), 1)

        with open(file_2, 'a') as fd:
            fd.write(data)

        # Change starts from line 2
        self.assertEqual(self.plugin.check_old_version(file_1.name), 2)

        with open(file_2, 'a') as fd:
            fd.write('Some other data')

        # Change starts from line 2, but old file is longer.
        self.assertEqual(self.plugin.check_old_version(file_1.name), 2)

        os.unlink(file_1.name)
        os.unlink(os.path.join(tmp_dir, file_1.name.split(os.sep)[-1]))
        os.rmdir(tmp_dir)

if __name__ == '__main__':
    unittest.main()
