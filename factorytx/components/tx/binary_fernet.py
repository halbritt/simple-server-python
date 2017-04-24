#
from __future__ import absolute_import, division, print_function

import base64
import os
import six
import struct
import time

from cryptography.fernet import InvalidToken
from cryptography.exceptions import InvalidSignature, AlreadyFinalized
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.hmac import HMAC
import hashlib

_MAX_CLOCK_SKEW = 60


CLOSED, READ, WRITE = 0, 1, 3


def chunk_iter(s, chunk_len):
    # type: (byte, int) -> typing.Generator[byte, None, None]
    for i in xrange(0, len(s), chunk_len):
        yield s[i:i + chunk_len]


class BinaryFernetFile(object):
    """Class which allows reading and writing Binary Fernet-encrypted files
    using file streams.

    Binary Fernet is a variant of Fernet [1], an encoding for encrypted
    messages. Binary Fernet is based on the same cryptographic primitives as
    Fernet (PKCS #7, AES128-CBC, and HMAC-SHA256.) The sole difference is that
    Binary Fernet does not base64-encode the encrypted message. This makes it
    suitable for use in bandwidth-intensive applications like transmitting
    sensor data.

    [1] https://github.com/fernet/spec/blob/master/Spec.md

    """

    HEADER_SIZE = 25
    SIGNATURE_SIZE = 32
    FERNET_KEY_SIZE = 32

    buffer_size = 64 * 1024

    def __init__(self, key, backend=None):
        if backend is None:
            backend = default_backend()

        key = base64.urlsafe_b64decode(key)
        if len(key) != self.FERNET_KEY_SIZE:
            raise ValueError(
                "Fernet key must be 32 url-safe base64-encoded bytes."
            )

        self._signing_key = key[:16]
        self._encryption_key = key[16:]
        self._backend = backend

        # Encryption/Decryption state
        self._hmac = self._cryptor = None
        self._enc_buffer = b''

        # File state
        self._fileobj = None
        self._filemode = CLOSED

    @classmethod
    def generate_key(cls):
        return base64.urlsafe_b64encode(os.urandom(cls.FERNET_KEY_SIZE))

    @staticmethod
    def truncate_key(key):
        # type: (str) -> str
        """Truncates a string of random bits to an appropriate length to be used
        as a Fernet encryption key. This requires that the input string is at
        least 32 bytes long.

        NOTE: This is not a key-derivation function! It should only be applied
        to long random strings, such as Sight Machine's 512-bit API keys.

        :param key: input string to generate encryption key
        :return: string for use as an encryption key

        """
        if len(key) < BinaryFernetFile.FERNET_KEY_SIZE:
            msg = "String to truncate must be at least {} bytes long. {!r} is too short."
            raise ValueError(msg.format(BinaryFernetFile.FERNET_KEY_SIZE, key))
        binarykey = hashlib.sha256(key).digest()
        return base64.urlsafe_b64encode(binarykey)

    def _check_fileobj(self, mode):
        """Raises a ValueError if the underlying file object has been closed.

        """
        if self._fileobj is None:
            raise ValueError('I/O operation on closed file.')
        if self._filemode != mode:
            raise IOError('I/O operation not allowed on file.')

    def open(self, filename=None, fileobj=None, mode=None, perm=None, ttl=None):
        # type: (str, file, str, int, int) -> BinaryFernetFile
        if filename is None and fileobj is None:
            raise TypeError('Need to specify filename or fileobj')

        # Make sure we don't inadvertently enable universal newlines on the
        # underlying file object - in read mode, this causes data corruption.
        if mode:
            mode = mode.replace('U', '')
            if any((opt not in 'rwab') for opt in mode):
                raise IOError("Mode {} not supported".format(mode))
        # guarantee the file is opened in binary mode on platforms
        # that care about that sort of thing
        if mode and 'b' not in mode:
            mode += 'b'

        if fileobj is None:
            fileobj = __builtin__.open(filename, mode or 'rb', perm)
        self._fileobj = fileobj

        if filename is None:
            # Issue #13781: os.fdopen() creates a fileobj with a bogus name
            # attribute. Avoid saving this in the gzip header's filename field.
            if hasattr(fileobj, 'name') and fileobj.name != '<fdopen>':
                filename = fileobj.name
            else:
                filename = ''

        if mode is None:
            if hasattr(fileobj, 'mode'):
                mode = fileobj.mode
            else:
                mode = 'rb'

        if mode[0:1] == 'r':
            self._filemode = READ
            self.name = filename
            self._read_header(fileobj, ttl)

        elif mode[0:1] == 'w' or mode[0:1] == 'a':
            self._filemode = WRITE
            self._write_header()

        else:
            raise IOError("Mode {} not supported".format(mode))

        return self

    def close(self):
        if self._fileobj is not None:
            if self._filemode == WRITE:
                self._write_footer()

            self._filemode = CLOSED
            self._fileobj = None

    def __enter__(self):
        if self._fileobj is None:
            raise IOError('File not open for read or write')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _write_header(self, iv=None, current_time=None):
        # type: (byte, int) -> None
        """
        Setup encryptors and other necessary state

        Write preamble/header for encryption file to file.

        :param iv:
        :param current_time:

        :return:
        """

        if iv is None:
            iv = os.urandom(16)
        if current_time is None:
            current_time = int(time.time())

        # Setup padder & encryption for body
        self._padding = padding.PKCS7(algorithms.AES.block_size).padder()

        self._cryptor = Cipher(
            algorithms.AES(self._encryption_key), modes.CBC(iv), self._backend
        ).encryptor()

        # Setup signature for file
        self._hmac = HMAC(self._signing_key, hashes.SHA256(), backend=self._backend)

        preamble = b'\x80' + struct.pack(">Q", current_time) + iv
        self._hmac.update(preamble)
        self._fileobj.write(preamble)

    def _write_footer(self):
        # type: () -> None
        """
        Flush remaining data and write HMAC signature
        """
        # Flush padder
        enc_chunk = self._cryptor.update(
            self._padding.finalize()
        )
        self._hmac.update(enc_chunk)
        self._fileobj.write(enc_chunk)
        self._padding = None

        # Flush encrypter
        enc_chunk = self._cryptor.finalize()
        self._hmac.update(enc_chunk)
        self._fileobj.write(enc_chunk)
        self._cryptor = None

        # Flush hmac
        enc_chunk = self._hmac.finalize()
        self._fileobj.write(enc_chunk)
        self._hmac = None

    def write(self, data):
        # type: (byte) -> int
        """

            1. Pad data
            2. encrypt data
            3. update signature
            4. base64 encode data

        """
        self._check_fileobj(WRITE)

        enc_chunk = self._cryptor.update(
            self._padding.update(data)
        )
        self._hmac.update(enc_chunk)
        self._fileobj.write(enc_chunk)

        return len(data)

    def _check_signature(self, fileobj, current_time, ttl):
        # type: (file, int, int) -> byte

        buffer_size = 1024
        data = fileobj.read(buffer_size)

        if not data or six.indexbytes(data, 0) != 0x80 or len(data) < self.HEADER_SIZE:
            raise InvalidToken

        try:
            timestamp, = struct.unpack(">Q", data[1:9])

        except struct.error:
            raise InvalidToken

        if ttl is not None:
            if timestamp + ttl < current_time:
                raise InvalidToken
        if current_time + _MAX_CLOCK_SKEW < timestamp:
            raise InvalidToken

        iv = data[9:25]

        h = HMAC(self._signing_key, hashes.SHA256(), backend=self._backend)
        try:
            # Guard the last 32 bytes as the HMAC signature
            # If only 32 bytes left must have finished reading file
            while True:
                h.update(data[:-self.SIGNATURE_SIZE])
                data = data[-self.SIGNATURE_SIZE:]

                buf = fileobj.read(buffer_size)
                if not buf:
                    break

                data += buf

        except TypeError:
            raise InvalidToken

        if len(data) != self.SIGNATURE_SIZE:
            raise InvalidToken

        try:
            h.verify(data)
        except InvalidSignature:
            raise InvalidToken

        return iv

    def _read_header(self, fileobj, ttl=None):
        # type: (file, int) -> None

        current_time = int(time.time())

        iv = self._check_signature(fileobj, current_time, ttl)

        # Reset file pointer past header
        fileobj.seek(0)
        self._fileobj.read(self.HEADER_SIZE)

        self._padding = padding.PKCS7(algorithms.AES.block_size).unpadder()
        self._cryptor = Cipher(
            algorithms.AES(self._encryption_key), modes.CBC(iv), self._backend
        ).decryptor()

        self._enc_buffer = b''

    def _read_all(self):
        # type: () -> byte

        # Need to guard against HMAC signature
        self._enc_buffer += self._fileobj.read()
        enc_chunk = self._enc_buffer[:-self.SIGNATURE_SIZE]
        self._enc_buffer = self._enc_buffer[-self.SIGNATURE_SIZE:]

        return b''.join((
            self._padding.update(
                self._cryptor.update(enc_chunk)
            ),
            self._padding.update(
                self._cryptor.finalize()
            ),
            self._padding.finalize()
        ))

    def _read(self, size):
        # type: (int) -> byte

        if size < algorithms.AES.block_size:
            raise ValueError('Size must be at least {} bytes'.format(algorithms.AES.block_size))

        # Need to guard against HMAC signature
        buf = self._fileobj.read(size + self.SIGNATURE_SIZE)

        if buf:
            self._enc_buffer += buf
            enc_chunk = self._enc_buffer[:-self.SIGNATURE_SIZE]
            self._enc_buffer = self._enc_buffer[-self.SIGNATURE_SIZE:]

            return self._padding.update(
                self._cryptor.update(enc_chunk)
            )

        # Else, finalize decryption
        try:
            return b''.join((
                self._padding.update(
                    self._cryptor.finalize()
                ),
                self._padding.finalize()
            ))

        except AlreadyFinalized:
            # Finished decryption, return empty string
            return b''

    def read(self, size=None, strict_size=False):
        # type: (int, bool) -> byte

        if size is None:
            return self._read_all()

        if strict_size:
            raise NotImplementedError

        # Do not care if result does not met size given
        return self._read(size)
