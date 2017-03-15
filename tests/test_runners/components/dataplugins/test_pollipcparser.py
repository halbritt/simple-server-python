import os
import mock
import unittest
import tempfile
import xml.etree.ElementTree as ET

#from FactoryTx.fileparsers.pollipcparser import PollIPCParser


def prt(data):
    print(data)


@unittest.skip("Using outdated API")
class TestPollIPCParser(unittest.TestCase):

    def setUp(self):
        conf = {
            'filenamemetadata': [{'name': 'FactoryPartner', 'address': 0},
                               {'name': 'FactoryLocation', 'address': 1},
                               {'name': 'MachineType', 'address': 2},
                               {'name': 'MachineCounter', 'address': 3},
                               {'name': 'ImageCounterIndex', 'address': 4},
                               {'name': 'Camera', 'address': 5},
                               {'name': 'Date', 'address': 6},
                               {'name': 'Time', 'address': 7}],
            'binaryDelimiter': ['_'],
            'runningfield': {},
            'xml': {'fields': {}},
            'completed_folder': '/tmp/completed',
            'outputdirectory': '/tmp',
            'source': 'SMB_Transport_Test_1',
            'version': '1.0.0',
            'counterfield': {'field': 'Counter'},
            'output_folder': '/tmp'
        }

        self.parser = PollIPCParser()
        self.parser.__dict__.update(conf)
        self.parser.log = mock.MagicMock()
        self.parser.log.warning = prt

        # Patch os module
        self.os_patcher = mock.patch(
            'FactoryTx.parsers.pollipcparser.pollipcparser.os',
            spec=os)
        self.os_mock = self.os_patcher.start()
        self.os_mock.path.splitext = os.path.splitext
        self.os_mock.path.join = os.path.join

        # Patch time module
        self.time_patcher = mock.patch(
            'FactoryTx.parsers.pollipcparser.pollipcparser.time')
        self.time_mock = self.time_patcher.start()
        self.time_mock.time.return_value = 0

    def tearDown(self):
        self.os_patcher.stop()
        self.time_patcher.stop()

    def test_process_filtered_ext(self):
        files = ['test/test.tmp', 'test/test.TMP',
                 'test/test.db', 'test/test.DB',
                 'test/test.DBs', 'test/test.XML',
                 'test/test.xml']

        self.parser.parse_xml = mock.MagicMock()
        self.parser.write_data_json = mock.MagicMock()

        self.parser.parse_xml.return_value = False
        self.parser.write_data_json.return_value = False

        for fn in files:
            ret = self.parser.process(fn)
            self.assertEqual(ret, False)
        self.assertEqual(self.parser.parse_xml.call_count, 2)
        self.assertEqual(self.parser.write_data_json.call_count, 1)

    @unittest.skip  # Does not work outside GMT+3. Related to SD-171.
    def test_file_creation_time_stamp(self):
        ret = self.parser.file_creation_time_stamp('/tmp/test.xml')
        self.os_mock.path.getmtime = 0
        self.assertEqual(ret, '1970-01-01T03:00:01.000000')

    @unittest.skip  # Does not work outside GMT+3. Related to SD-171.
    def test_time_stamp(self):
        ret = self.parser.time_stamp()
        self.assertEqual(ret, '1970-01-01T03:00:00.000000')

    def test_tsplit(self):
        fn_1 = 'one_two_tree_four'
        fn_2 = 'one&two_tree&four'
        delims_1 = ['_']
        delims_2 = ['_', '&']
        ret_1 = self.parser.tsplit(fn_1, delimiters=delims_1)
        ret_2 = self.parser.tsplit(fn_2, delimiters=delims_2)

        self.assertEqual(ret_1, ['one', 'two', 'tree', 'four'])
        self.assertEqual(ret_2, ['one', 'two', 'tree', 'four'])

    def test_parse_xml(self):
        xml = """<data>
<Counter unit="Count">0</Counter>
<Status unit="string">Running</Status>
<Cycle unit="Count">100</Cycle>
<Voltage unit="Volts">10.5</Voltage>
<Input unit="Count">15</Input>
<Output unit="Count">10</Output>
<ConveyorSpeed unit="Hertz">15.3</ConveyorSpeed>
</data>
"""

        self.parser.file_creation_time_stamp = mock.MagicMock()
        self.parser.file_creation_time_stamp.return_value = '1970-01-01T03:00:01.000000'
        self.parser._get_counter_from_xml = mock.MagicMock()
        self.parser._get_counter_from_xml.return_value = 1

        tmp_file = tempfile.NamedTemporaryFile(suffix='.xml', delete=False)
        tmp_file.write(xml)
        tmp_file.seek(0)

        ret = self.parser.parse_xml(tmp_file.name)

        self.assertEqual(ret, True)
        os.remove(tmp_file.name)
        os.remove('/tmp/SMB_Transport_Test_1_1970-01-01T03:00:01.000000.tmp')

    def test_parse_xml_exeption(self):
        ret = self.parser.parse_xml('unknown_file.txt')

        self.assertEqual(ret, None)
        self.assertEqual(self.parser.log.exception.call_count, 1)

    def test_write_data_json(self):
        self.parser.file_creation_time_stamp = mock.MagicMock()
        self.parser.file_creation_time_stamp.return_value = '1970-01-01T03:00:01.000000'

        self.parser._get_counter_from_xml = mock.MagicMock()
        self.parser._get_counter_from_xml.return_value = 1

        self.parser._populate_filename_metadata_parts = mock.MagicMock()

        open_mock = mock.mock_open()

        with mock.patch('__builtin__.open', open_mock):
            ret = self.parser.write_data_json('/tmp/test.xml')

        self.assertEqual(ret, True)
