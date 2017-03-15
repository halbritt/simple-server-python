from binascii import unhexlify
import copy
import json
import os
import socket
import threading
import time
import unittest

from mock import patch
from testfixtures import TempDirectory, compare
import yaml

import FactoryTx
from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.components.dataplugins.mitsubishiplc.PollMitsubishiPLC import PollMitsubishiPLC
from FactoryTx.components.dataplugins.mitsubishiplc import melsec
from tests.utils import FakePLCSocket

plugins = component_manager()['dataplugins']


class MelsecProtocolTestCase(unittest.TestCase):
    BLOCKS = yaml.load("""
    - baseaddress: '000000'
      datafields:
        - {address: '0000', datatype: word, name: Input-Count, units: BINARY}
        - {address: '0010', bit: 0, datatype: bit, name: RUNNING, units: Status}
        - {address: '0010', bit: 5, datatype: bit, name: DEBUG, units: Status}
        - {address: '0010', bit: 14, datatype: bit, name: IPC-CONNECTED, units: Status}
        - {address: '0010', bit: 15, datatype: bit, name: ROBOT-CONNECTED, units: Status}
        - {address: '0011', charlength: 4, datatype: text, name: Serial-text, units: BCD}
        - {address: '0011', datatype: dword, name: Serial-dword, units: BCD}
        - {address: '0013', datatype: word, name: Year, units: BCD}
        - {address: '0014', charpos: high, datatype: char, name: Day, units: BCD}
        - {address: '0014', charpos: low, datatype: char, name: Month, units: BCD}
        - {address: '0015', charpos: high, datatype: char, name: Side, units: ASCII}
        - {address: '0015', charpos: low, datatype: char, name: Size, units: ASCII}
        - {address: '0016', datatype: word, name: Good-Or-No-Good, units: ASCII}
        - {address: '0017', charlength: 16, datatype: text, name: Model, units: ASCII}
        - {address: '001F', charpos: high, datatype: char, name: Pressure, units: kPa}
        - {address: '001F', charpos: low, datatype: char, name: Temperature, units: Celsius}
        - {address: '0020', datatype: word, name: Length, units: 'cm / 100'}
        - {address: '0021', charlength: 4, datatype: text, name: Persistent-Count-text, units: Count}
        - {address: '0021', datatype: dword, name: Persistent-Count-dword, units: Count}
        - {address: '0023', charlength: 6, datatype: text, name: Padded-String, units: ASCII}
      register: 'W*'
      wordlength: 40
    """)
    ANNUNCIATOR_BLOCKS = yaml.load("""
    - baseaddress: '000064'
      datafields:
        - {address: '0064', bit: 0, datatype: bit, name: Everything-Is-Broken, units: AlarmCode}
      register: 'F*'
      wordlength: 1
    """)

    def setUp(self):
        socketpair = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket, self.client_socket = socketpair
        self.client_socket.settimeout(0.1)
        self.server_socket.settimeout(0.1)

    def tearDown(self):
        self.client_socket.close()
        self.server_socket.close()

    def run_fake_server(self, expected_request, response_chunks):
        actual_request = [None]
        def server_task():
            request = ""
            while len(request) < len(expected_request):
                try:
                    chunk = self.server_socket.recv(2048)
                except socket.timeout:
                    break
                if chunk == "":
                    break
                request += chunk
            actual_request[0] = request
            for i, chunk in enumerate(response_chunks):
                if i > 0:
                    time.sleep(0.010)
                self.server_socket.sendall(chunk)
        thread = threading.Thread(target=server_task)
        thread.daemon = True
        thread.start()
        return actual_request, thread

    def test_valid_ascii_request(self):
        expected_request = '500000FF03FF000018001004010000W*0000000028'
        response_chunks = [
            'D00000FF03FF000',
            '0A4000060BA00220000000000000000000000000000000000000000000000000',
            '0000000D94D11122527201512254D4C0047654C7242206F31272035202020202',
            '02013675A0ABB32057A00000000000000000000',
        ]
        expected_fieldvalues = {
            'Input-Count': {'units': 'BINARY', 'value': 24762},
            'RUNNING': {'units': 'Status', 'value': '1'},
            'DEBUG': {'units': 'Status', 'value': '0'},
            'IPC-CONNECTED': {'units': 'Status', 'value': '1'},
            'ROBOT-CONNECTED': {'units': 'Status', 'value': '1'},
            'Serial-text': {'units': 'BCD', 'value': 25271112},
            'Serial-dword': {'units': 'BCD', 'value': 25271112},
            'Year': {'units': 'BCD', 'value': 2015},
            'Month': {'units': 'BCD', 'value': 12},
            'Day': {'units': 'BCD', 'value': 25},
            'Size': {'units': 'ASCII', 'value': 'M'},
            'Side': {'units': 'ASCII', 'value': 'L'},
            'Good-Or-No-Good': {'units': 'ASCII', 'value': 'G'},
            'Model': {'units': 'ASCII', 'value': "LeBro '15"},
            'Pressure': {'units': 'kPa', 'value': 103},
            'Temperature': {'units': 'Celsius', 'value': 19},
            'Length': {'units': 'cm / 100', 'value': 23050},
            'Persistent-Count-text': {'units': 'Count', 'value': 91929394},
            'Persistent-Count-dword': {'units': 'Count', 'value': 91929394},
            'Padded-String': {'units': 'ASCII', 'value': ''},
        }
        actual_request, thread = self.run_fake_server(expected_request, response_chunks)
        fieldvalues = melsec.read_blocks(self.client_socket, self.BLOCKS, 'ascii')
        thread.join(0.1)
        compare(actual_request[0].lower(), expected_request.lower())
        compare(fieldvalues, expected_fieldvalues)

    def test_ascii_annunciator_request(self):
        # The annunciator (F*) uses decimal addressing instead of hex.
        expected_request = '500000FF03FF000018001004010000F*0001000001'
        response_chunks = ['D00000ff03ff0000080000B762']
        expected_fieldvalues = {
            'Everything-Is-Broken': {'units': 'AlarmCode', 'value': '0'},
        }
        actual_request, thread = self.run_fake_server(expected_request, response_chunks)
        fieldvalues = melsec.read_blocks(self.client_socket, self.ANNUNCIATOR_BLOCKS, 'ascii')
        thread.join(0.1)
        compare(actual_request[0].lower(), expected_request.lower())
        compare(fieldvalues, expected_fieldvalues)

    def test_failed_ascii_request(self):
        expected_request = '500000FF03FF000018001044010000W*0000000028'
        response_chunks = ['D00000FF03FF000004C059']
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.BLOCKS, 'ascii')
        expected_message = 'PLC returned error code 0xC059 (Wrong Command/Sub-Command)'
        self.assertEqual(str(cm.exception), expected_message)

    def test_short_ascii_response(self):
        expected_request = '500000FF03FF000018001044010000W*0000000028'
        response_chunks = ['D00000FF03FF00000']
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.BLOCKS, 'ascii')
        expected_regex = 'Timed out after reading 17 of [0-9]+ bytes'
        self.assertRegexpMatches(str(cm.exception), expected_regex)

    def test_valid_binary_request(self):
        expected_request = unhexlify('500000ffff03000c00100001040000000000b42800')
        response_chunks = map(unhexlify, [
            'd00000ffff030052000000ba602200000000000000000000000000'
            '000000000000000000000000000000004dd912112725152025124c',
            '4d47004c6542726f202731352020202020202067130a5a32bb7a05'
            '41424300000000000000',
        ])
        expected_fieldvalues = {
            'Input-Count': {'units': 'BINARY', 'value': 24762},
            'RUNNING': {'units': 'Status', 'value': '1'},
            'DEBUG': {'units': 'Status', 'value': '0'},
            'IPC-CONNECTED': {'units': 'Status', 'value': '1'},
            'ROBOT-CONNECTED': {'units': 'Status', 'value': '1'},
            'Serial-text': {'units': 'BCD', 'value': 25271112},
            'Serial-dword': {'units': 'BCD', 'value': 25271112},
            'Year': {'units': 'BCD', 'value': 2015},
            'Month': {'units': 'BCD', 'value': 12},
            'Day': {'units': 'BCD', 'value': 25},
            'Size': {'units': 'ASCII', 'value': 'M'},
            'Side': {'units': 'ASCII', 'value': 'L'},
            'Good-Or-No-Good': {'units': 'ASCII', 'value': 'G'},
            'Model': {'units': 'ASCII', 'value': "LeBro '15"},
            'Pressure': {'units': 'kPa', 'value': 103},
            'Temperature': {'units': 'Celsius', 'value': 19},
            'Length': {'units': 'cm / 100', 'value': 23050},
            'Persistent-Count-text': {'units': 'Count', 'value': 91929394},
            'Persistent-Count-dword': {'units': 'Count', 'value': 91929394},
            'Padded-String': {'units': 'ASCII', 'value': 'ABC'},
        }
        actual_request, thread = self.run_fake_server(expected_request, response_chunks)
        fieldvalues = melsec.read_blocks(self.client_socket, self.BLOCKS, 'binary')
        thread.join(0.1)
        compare(actual_request[0], expected_request)
        compare(fieldvalues, expected_fieldvalues)

    def test_failed_binary_request(self):
        expected_request = unhexlify('500000ffff03000c00100001040000000000b42800')
        response_chunks = [unhexlify('d00000ffff030002000880')]
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.BLOCKS, 'binary')
        self.assertEqual(str(cm.exception), 'PLC returned error code 0x8008 (Unknown PLC error)')

    def test_short_binary_response(self):
        expected_request = unhexlify('500000ffff03000c00100001040000000000b42800')
        response_chunks = map(unhexlify, [
            'd00000ffff030052000000ba602200000000000000000000000000',
            '000000000000000000000000000000004dd912112725152025124c',
        ])
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.BLOCKS, 'binary')
        expected_regex = 'Timed out after reading 45 of [0-9]+ bytes.'
        self.assertRegexpMatches(str(cm.exception), expected_regex)

    def test_long_response(self):
        expected_request = unhexlify('500000ffff03000c00100001040000640000930100')
        response_chunks = [unhexlify('d00000ffff030006000000ba602200')]
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.ANNUNCIATOR_BLOCKS, 'binary')
        expected_message = 'Requested 2 bytes (1 words), received 4.'
        self.assertEqual(str(cm.exception), expected_message)

    def test_trailing_garbage_in_response(self):
        expected_request = unhexlify('500000ffff03000c00100001040000640000930100')
        response_chunks = [unhexlify('d00000ffff030004000000ba602200')]
        self.run_fake_server(expected_request, response_chunks)
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.ANNUNCIATOR_BLOCKS, 'binary')
        expected_message = 'Extra data received: expected 4 bytes, received 6.'
        self.assertEqual(str(cm.exception), expected_message)

    def test_interrupted_request(self):
        # Cloe the socket prematurely instead of waiting for the timeout.
        expected_request = unhexlify('500000ffff03000c00100001040000000000b42800')
        response_chunks = map(unhexlify, [
            'd00000ffff030052000000ba602200000000000000000000000000',
            '000000000000000000000000000000004dd912112725152025124c',
        ])
        _, server_thread = self.run_fake_server(expected_request, response_chunks)
        def close_socket_task():
            server_thread.join(1.0)
            self.server_socket.close()
        threading.Thread(target=close_socket_task).start()
        with self.assertRaises(melsec.RequestError) as cm:
            melsec.read_blocks(self.client_socket, self.BLOCKS, 'binary')
        expected_regex = 'Premature EOF after reading 45 of [0-9]+ bytes.'
        self.assertRegexpMatches(str(cm.exception), expected_regex)


class ConfigValidationTestCase(unittest.TestCase):
    def check_blocks_are_valid(self, blocks):
        melsec.validate_blocks(yaml.load(blocks))

    def check_blocks_are_invalid(self, blocks, invalid_fields):
        with self.assertRaises(ValueError) as cm:
            melsec.validate_blocks(yaml.load(blocks))
        message = str(cm.exception)
        self.assertRegexpMatches(message, ".*would overflow.*")
        for field in invalid_fields:
            self.assertIn(field, message)

    def test_valid_blocks(self):
        self.check_blocks_are_valid("""
          - baseaddress: '000000'
            datafields:
            - {address: '0000', datatype: word, name: Count, units: Binary}
            register: 'W*'
            wordlength: 1
          - baseaddress: '000002'
            datafields:
            - {address: '0002', charpos: high, datatype: char, name: Day, units: BCD}
            - {address: '0002', charpos: low, datatype: char, name: Grade, units: ASCII}
            - {address: '0003', charlength: 5, datatype: text, name: Factories, units: ASCII}
            register: 'W*'
            wordlength: 6
          - baseaddress: '000000'
            datafields:
            - {address: '0002', bit: 10, datatype: bit, name: Running, units: Status}
            register: 'W*'
            wordlength: 10
        """)

    def test_invalid_blocks(self):
        self.check_blocks_are_invalid("""
          - baseaddress: '000001'
            datafields:
            - {address: '0000', datatype: word, name: Count, units: Binary}
            - {address: '0001', datatype: word, name: Weight, units: kg}
            register: 'W*'
            wordlength: 1
        """, ['Count'])
        self.check_blocks_are_invalid("""
          - baseaddress: '000002'
            datafields:
            - {address: '0001', charpos: high, datatype: char, name: Day, units: BCD}
            - {address: '0002', charpos: low, datatype: char, name: Grade, units: ASCII}
            - {address: '0004', charlength: 5, datatype: text, name: Factory, units: ASCII}
            register: 'W*'
            wordlength: 4
          - baseaddress: '000010'
            datafields:
            - {address: '0010', charlength: 5, datatype: text, name: Serial, units: BCD}
            - {address: '0014', bit: 15, datatype: bit, name: Running, units: Status}
            register: 'D*'
            wordlength: 4
        """, ['Day', 'Factory', 'Running'])


class PollMitsubishiPLCTests(unittest.TestCase):

    source = 'UnitTestMachine1'
    sharedFolder = "%s/tests/databuffer/" % os.getcwd()
    folderPath = 'default/'
    config_yaml = """
machines:
- source: {0}
  plugins:
  - type: pollmitsubishi
    source: {0}
    version: 1.0.0
    name: PollMitsubishi Service
    machine: 'Customer MES'
    version: '1.0.0'
    username: test
    password: test
    hostName: 'test_ipcmachine'
    host: '127.0.0.1'
    port: 80
    timeout: 10
    polltime: 1
    sharedFolder: {1}
    folderPath: {2}
    removeFiles: 1
    counterfield:
        field: 'Left_CurrentTotalCount'
    counterfield_left:
        field: 'Left_CurrentTotalCount'
    counterfield_right:
        field: 'Right_CurrentTotalCount'
    runningfield: 'Status'
    data_code: 'ascii'
    polltime: 0.25
    aggregationtime: 10
    databufferlength: 4096
    debug: 0
    outputdirectory: 'databuffer/PLC/'
    storage: 'JSON'
    split_data: false
    split_prefixs:
        - 'Left'
        - 'Right'
    split_label_suffixes:
        - 'LL'
        - 'RR'
    runningfield:
      field: 'Auto'
    field:
      isMultiStatus: True
      multiStatus:
        Auto: 'Auto'
        Warning: 'Warning'
        Manual: 'Manual'
        Idle: 'Idle'
        Alarm: 'Alarm'
    monitlog: '../logs/monitlog.log'
    datablocks:
    - register: 'W*'
      baseaddress: '000000'
      wordlength: 12
      datafields:
        - address: '0000'
          datatype: 'bit'
          name: 'Idle'
          units: 'Status'
          bit: 0
        - address: '0001'
          datatype: 'bit'
          name: 'Manual'
          units: 'Status'
          bit: 1
        - address: '0002'
          datatype: 'bit'
          name: 'Auto'
          units: 'Status'
          bit: 2
        - address: '0003'
          datatype: 'bit'
          name: 'Alarm'
          units: 'Status'
          bit: 3
        - address: '0004'
          datatype: 'bit'
          name: 'Warning'
          units: 'Status'
          bit: 4
        - address: '0005'
          datatype: 'word'
          name: 'Year'
          units: 'BCD'
        - address: '0006'
          datatype: 'word'
          name: 'Mon'
          units: 'BCD'
        - address: '0007'
          datatype: 'word'
          name: 'Day'
          units: 'BCD'
        - address: '0008'
          datatype: 'word'
          name: 'Hour'
          units: 'BCD'
        - address: '0009'
          datatype: 'word'
          name: 'Min'
          units: 'BCD'
        - address: '000A'
          datatype: 'word'
          name: 'Sec'
          units: 'BCD'
        - address: '000B'
          datatype: 'word'
          name: 'CycleCount'
          units: 'BINARY'
    """.format(source, sharedFolder, folderPath)

    def getSampleFilesArg(self, relfilename, content_type, headers):
        cwd = os.getcwd()
        filename = "%s/SimpleData/tests/unit/%s" % (cwd, relfilename)
        files = {'imagefile': (relfilename, open(filename, 'rb'), content_type, headers)}
        return files

    @classmethod
    def setUpClass(cls):
        super(PollMitsubishiPLCTests, cls).setUpClass()

        cls.pm = plugins
        cls.pm.load_schemas()
        cls.schema = cls.pm.get_plugin_schema('mitsubishiplc', '1.0.0')
        cls.plugin_config = yaml.load(cls.config_yaml)
        cls.config = cls.plugin_config['machines'][0]['plugins'][0]

    @classmethod
    def tearDownClass(cls):
        super(PollMitsubishiPLCTests, cls).tearDownClass()

    def setUp(self):
        super(PollMitsubishiPLCTests, self).setUp()

        self.data_root = TempDirectory()
        self.plugin = PollMitsubishiPLC()
        self.sdconfig = {'plugins': {'data': self.data_root.path}}
        self.plugin.loadPLCParameters(self.sdconfig, self.schema, self.config)

    def tearDown(self):
        super(PollMitsubishiPLCTests, self).tearDown()

        try:
            self.plugin.terminate()
        except:
            pass

        self.data_root.cleanup()

    # TODO: Replace the following no-op test with new end-to-end tests:
    # 1. Can receive valid data.
    #   a. Data captured every ${polltime} seconds despite delays.
    #   b. Data written every ${aggregationtime} seconds.
    # 2. Can receive and split data.
    # 3. Retries opening a socket until it succeeds.
    # 4. Handles protocol errors correctly.
    # 5. Handles network errors correctly.

    def verifyState(self):
        pass

    @patch('select.select')
    @patch('socket.socket')
    @patch('socket.getaddrinfo')
    @patch.object(PollMitsubishiPLC, 'create_sslogs')
    def test_success_ascii(self, mock_create_sslogs, mock_socket_getaddrinfo, mock_socket_socket, mock_select_select):
        #                          len codedata
        #                      1         2         3         4         5         6         7
        #            01234567890123456789012345678901234567890123456789012345678901234567890
        fake_recv = "0123456789012L00340000111111111111111100012015001200310017005500335432"
        fake_socval = FakePLCSocket.fake_socket_socket(fake_recv, False)
        mock_socket_getaddrinfo.return_value = [('af','st','pr','cn','sa')]
        mock_socket_socket.return_value = fake_socval
        mock_select_select.return_value = ["Ready"]
        mock_create_sslogs.return_value = [{'timestamp': time.time()}]

        self.plugin.OpenSocket()
        self.plugin.PollPLC()

        sec_sleep = 0.75
        # sec_sleep = 1000 # DEBUGGING
        time.sleep(sec_sleep)

        self.plugin.StopPolling()
        self.verifyState()

    # TODO: Add integration tests for field-splitting.
    # TODO: Add integration tests for collect_events.


if __name__ == '__main__':
    unittest.main()
