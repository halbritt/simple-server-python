"""
MELSEC 3E protocol implementation that can perform batch reads from a
Mitsubishi MELSEC-Q/L PLC and turn the results into python values.

Clients perform reads by calling `read_blocks`. If an protocol error
occurs, a `RequestError` will be raised; if a socket error then the
native socket error may be raised instead. See the docstring for
`read_blocks` for more information.

See the MELSEC Communication Protocol Reference Manual for details
(https://drive.google.com/a/sightmachine.com/file/d/0Bzrm576Cy1YVY0ZMemJNNndMM0k/view
 -- download the PDF instead of using Google Docs so that the hyperlinks work.)

Sections of interest:
p.  39        header fields and framing for the 3E protocol
p.  66        table of addressable devices
pp. 84 - 86   information about the batch read command

Error code tables can be found on pp. 300 - 328 of the
Q Corresponding Ethernet Interface Module User's Manual
(https://drive.google.com/a/sightmachine.com/file/d/0Bzrm576Cy1YVYkZvS2hkaDhXQ1E/view)

"""

from binascii import hexlify, unhexlify
from collections import namedtuple
import logging
import re
import socket

__all__ = ['RequestError', 'read_blocks', 'validate_blocks']

log = logging.getLogger(__name__)


ASCII_REQUEST_SUBHEADER = "5000"       # Subheader (request type) for a 3E ASCII request.
BINARY_REQUEST_SUBHEADER = "\x50\x00"  # Subheader (request type) for a 3E binary request.

STATION_NO = 0x00                      # Station No. for a connected host station.
PC_NO = 0xFF                           # PC No. for a connected host station.
MODULE_IO_NO = 0x03FF                  # Module I/O No. for the primary CPU.
MODULE_STATION_NO = 0x00               # Module Station No. for the primary CPU.
MONITORING_TIMER = 0x0010              # Wait up to 16 * 250 ms for a response.

BATCH_READ_COMMAND = 0x0401
BATCH_READ_SUBCOMMAND = 0x0000

# Device codes represent the targets of read operations. For example, D*
# is the device register (PLC memory), while W* is the link register.
#
# To read data from a device, we need to know two things:
# 1. What octet represents this device for requests in binary mode? (ASCII mode
#    uses the device key, ex. D* instead.)
# 2. How is the device number (base address) encoded in ASCII mode?
#
# For additional device types, see the device code list on p. 66 of the
# MELSEC Communication Protocol Reference Manual.
DeviceCode = namedtuple('DeviceCode', ['octet', 'encoding'])
DEVICE_CODES = {
    'D*': DeviceCode(0xA8, 'decimal'),
    'F*': DeviceCode(0x93, 'decimal'),
    'W*': DeviceCode(0xB4, 'hexadecimal'),
}

# End codes which represent error responses from the PLC.
ERROR_CODES = {
    0x0055: "Online Changes Disabled",
    0x4031: "Requested address out of range",
    0xC050: "Can not convert received ASCII data",
    0xC056: "Allowable address range exceeded",
    0xC058: "ASCII Data length doesn't match specified value",
    0xC059: "Wrong Command/Sub-Command",
    0xC05B: "QCPU Embedded Ethernet can't reach device",
    0xC05C: "Incorrect requested data",
    0xC05D: "Monitor registration not defined",
    0xC05F: "Request not executable on QCPU Embedded Ethernet",
    0xC060: "Incorrect requested data",
    0xC061: "Data length doesn't match specified value",
    0xC06F: "Wrong message format (ASCII/BIN) received",
    0xC070: "Wrong memory Extension for target station",
    0xC0B5: "Mismatch data for CPU module",
    0xC200: "Incorrect Remote Password",
    0xC201: "Remote port locked",
    0xC204: "Requesting station differs from the one which unlocked port",
}


class RequestError(Exception):
    """Raised for protocol errors (ex. premature EOF) or PLC error responses."""
    @classmethod
    def from_error_code(cls, end_code):
        assert end_code != 0x0000
        if 0x4000 <= end_code <= 0x4FFF:
            msg = "CPU Detection error"
        if 0xC051 <= end_code <= 0xC055:
            msg = "Attempted to read too much data in one block"
        elif end_code in ERROR_CODES:
            msg = ERROR_CODES[end_code]
        else:
            msg = "Unknown PLC error".format(end_code)
        return cls("PLC returned error code 0x{:04X} ({})".format(end_code, msg))


def to_littleendian(number, byte_count):
    """Converts an int to a little-endian bytestring of the specified length.

    >>> to_littleendian(307, byte_count=3)
    '\x33\x01\x00'

    """
    assert isinstance(number, int)
    buf = ''
    for i in xrange(byte_count):
        buf += chr((number >> (i * 8)) & 0xFF)
    return buf


def from_littleendian(bytestring):
    assert isinstance(bytestring, basestring)
    total = 0
    for byte in reversed(bytestring):
        total = total << 8 | ord(byte)
    return total


def to_hex(integer, digit_count):
    """Convert an int to a hexadecimal number with the given number of digits.

    >>> to_hex(317, digit_count=6)
    '00013D'

    """
    format_string = '{:0' + str(digit_count) + 'X}'
    return format_string.format(integer)


def from_hex(string):
    assert isinstance(string, basestring)
    return int(string, 16)


def from_bcd(bytestring):
    """Converts a little-endian binary-coded decimal number to an int or long.

    >>> from_bcd('\x25\x12\x15\x20')
    20151225

    """
    assert isinstance(bytestring, basestring)
    total = 0
    for byte in reversed(bytestring):
        high = (ord(byte) & 0xF0) >> 4
        low = ord(byte) & 0x0F
        total = total * 100 + high * 10 + low
    return total


def get_field_length(field):
    """Returns the length in bytes of a data field.

    :param field: dict representing the datafield to convert. For example,
        {'address': '0600', 'datatype': 'word', 'name': 'Year', 'units': 'BCD'}.
        See the schema for datafields for more information.

    """
    if field['datatype'] in ('bit', 'word'):
        length = 2
    elif field['datatype'] == 'dword':
        length = 4
    elif field['datatype'] == 'char':
        length = 1
    elif field['datatype'] == 'text':
        length = field['charlength']
    else:
        raise ValueError('Unknown datatype "{}"'.format(field['datatype']))
    return length


# Fields are fixed-size, so ASCII values are padded with spaces or NULL chars.
ASCII_PADDING_RE = re.compile('[\x00 ]+$')

def field_to_python(baseaddress, field, data):
    """Converts a single data field to a python value.

    :param baseaddress: hex-encoded base address of the datablock the field
        was read from.
    :param field: dict representing the datafield to convert. For example,
        {'address': '0600', 'datatype': 'word', 'name': 'Year', 'units': 'BCD'}.
        See the schema for datafields for more information.
    :param data: bytestring representing binary data to convert to python.
    :returns: a str or int representing the converted value.

    """
    word_offset = from_hex(field['address']) - from_hex(baseaddress)
    byte_length = get_field_length(field)
    byte_offset = word_offset * 2

    if field['datatype'] == 'char':
        # Use second byte if charpos == low, though the real low byte is first.
        # Required for backwards compatibility with the original PLC poller.
        byte_offset += field.get('charpos') == 'low'

    chunk = data[byte_offset:byte_offset + byte_length]
    assert len(chunk) == byte_length

    if field['units'] == 'BCD':
        return from_bcd(chunk)
    if field['datatype'] == 'text' and field['units'] == 'ASCII':
        chunk = ASCII_PADDING_RE.sub('', chunk)
        return chunk

    value = from_littleendian(chunk)

    if field['datatype'] == 'dword' and field['units'] == 'BINARY':
        return value
    if field['datatype'] == 'bit':
        # XXX: Should this be a boolean instead?
        return '1' if value & (1 << field['bit']) else '0'
    if field['units'] == 'ASCII':
        return unichr(value)
    else:
        return value


def build_ascii_request(block):
    baseaddress = from_hex(block['baseaddress'])
    device_code = DEVICE_CODES[block['register']]
    if device_code.encoding == 'decimal':
        head_device_number = '{:06d}'.format(baseaddress)
    elif device_code.encoding == 'hexadecimal':
        head_device_number = '{:06X}'.format(baseaddress)
    command = ''.join([
        to_hex(MONITORING_TIMER, digit_count=4),
        to_hex(BATCH_READ_COMMAND, digit_count=4),
        to_hex(BATCH_READ_SUBCOMMAND, digit_count=4),
        block['register'],   # 2 bytes, ex. D*
        head_device_number,  # 6 digits, ex. 000100
        to_hex(block['wordlength'], digit_count=4),
    ])
    request = ''.join([
        ASCII_REQUEST_SUBHEADER,  # 4 bytes
        to_hex(STATION_NO, digit_count=2),
        to_hex(PC_NO, digit_count=2),
        to_hex(MODULE_IO_NO, digit_count=4),
        to_hex(MODULE_STATION_NO, digit_count=2),
        to_hex(len(command), digit_count=4),
        command,
    ])
    return request


def build_binary_request(block):
    baseaddress = from_hex(block['baseaddress'])
    command = ''.join([
        to_littleendian(MONITORING_TIMER, byte_count=2),
        to_littleendian(BATCH_READ_COMMAND, byte_count=2),
        to_littleendian(BATCH_READ_SUBCOMMAND, byte_count=2),
        to_littleendian(baseaddress, byte_count=3),
        to_littleendian(DEVICE_CODES[block['register']].octet, byte_count=1),
        to_littleendian(block['wordlength'], byte_count=2),
    ])
    request = ''.join([
        BINARY_REQUEST_SUBHEADER,  # 2 bytes
        to_littleendian(STATION_NO, byte_count=1),
        to_littleendian(PC_NO, byte_count=1),
        to_littleendian(MODULE_IO_NO, byte_count=2),
        to_littleendian(MODULE_STATION_NO, byte_count=1),
        to_littleendian(len(command), byte_count=2),
        command,
    ])
    return request


PRINTABLE_ASCII_RE = re.compile("^[\x20-\x7E]+$")

def is_printable(s):
    return PRINTABLE_ASCII_RE.match(s)


def send_request(sock, request):
    if is_printable(request):
        log.debug("TX (raw): %r", request)
    else:
        log.debug("TX (hexlified): %r", hexlify(request))
    sock.sendall(request)


def read_response(sock, length, prefix):
    """Builds and returns a bytestring of at least `length` bytes by reading
    from `sock`. The response and length include the specified prefix.

    """
    response = prefix
    while len(response) < length:
        try:
            chunk = sock.recv(2048)
            if is_printable(chunk):
                log.debug("RX (raw): %r", chunk)
            else:
                log.debug("RX (hexlified): %r", hexlify(chunk))
        except socket.timeout:
            msg = "Timed out after reading {} of {} bytes.".format(len(response), length)
            raise RequestError(msg)
        if chunk == "":
            msg = "Premature EOF after reading {} of {} bytes.".format(len(response), length)
            raise RequestError(msg)
        response += chunk
    return response


ASCII_HEADER_LEN = 18   # Need to read 18 bytes to get the ASCII frame length.
BINARY_HEADER_LEN = 9   # Need 9 bytes to get length of a binary frame.

def read_blocks(sock, blocks, data_code):
    """Reads data from one or more blocks of address space on the PLC and
    returns a dict of field values.

    This method assumes that the socket, blocks, and data_code have previously
    been validated by the plugin schema.

    :param sock: TCP socket connected to the Ethernet module on the PLC.
    :param blocks: list of data blocks, as per the subschema for 'datablocks'.
    :param data_code: either 'ascii' or 'binary', specifying the encoding configured
        on the PLC.
    :returns: dict mapping from field names to results, ex.
        {'Year': {'value': 2015, units: 'BCD'}, 'Month': ...}
    :raises RequestError: if a protocol error occurs or the PLC responds
        with an error code.
    :raises socket.error: if an error occurs at or below the transport layer.

    """

    fieldvalues = {}
    for block in blocks:
        if data_code == 'ascii':
            request = build_ascii_request(block)
            send_request(sock, request)
            header = read_response(sock, ASCII_HEADER_LEN, prefix="")
            length = from_hex(header[14:18])
            body = read_response(sock, length, prefix=header[ASCII_HEADER_LEN:])
            end_code = from_hex(body[:4])
        elif data_code == 'binary':
            request = build_binary_request(block)
            send_request(sock, request)
            header = read_response(sock, BINARY_HEADER_LEN, prefix="")
            length = from_littleendian(header[7:9])
            body = read_response(sock, length, prefix=header[BINARY_HEADER_LEN:])
            end_code = from_littleendian(body[:2])
        if len(body) != length:
            msg = "Extra data received: expected {} bytes, received {}.".format(length, len(body))
            raise RequestError(msg)
        if end_code != 0x0000:  # The request failed.
            raise RequestError.from_error_code(end_code)
        if data_code == 'ascii':
            # The ASCII protocol returns a series of hex-encoded 16-bit words.
            # We must convert them to binary and swap each pair of bytes to
            # match the little-endian memory layout on the PLC.
            body = unhexlify(body)
            body = ''.join(body[i + 1] + body[i] for i in xrange(0, len(body), 2))
        data = body[2:]
        if len(data) != 2 * block['wordlength']:
            msg = ("Requested {} bytes ({} words), received {}."
                   "".format(2 * block['wordlength'], block['wordlength'], len(data)))
            raise RequestError(msg)
        for field in block['datafields']:
            fieldvalues[field['name']] = {
                'value': field_to_python(block['baseaddress'], field, data),
                'units': field['units'],
            }
    return fieldvalues


def validate_blocks(blocks):
    out_of_bounds_fields = []
    for block in blocks:
        block_start_byte = from_hex(block['baseaddress']) * 2
        block_end_byte = block_start_byte + block['wordlength'] * 2
        for field in block['datafields']:
            field_start_byte = from_hex(field['address']) * 2
            field_end_byte = field_start_byte + get_field_length(field)
            if field_start_byte < block_start_byte:
                out_of_bounds_fields.append(field['name'])
            elif field_end_byte > block_end_byte:
                out_of_bounds_fields.append(field['name'])
    if out_of_bounds_fields:
        out_of_bounds_fields = sorted(out_of_bounds_fields)
        raise ValueError("The following fields would overflow their blocks: {}"
                         .format(', '.join(out_of_bounds_fields)))
