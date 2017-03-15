import abc
import logging
import os.path

from factorytx.utils import merge_schema_defaults
from factorytx.components.dataplugins.pollingservices import PollingServiceBase, Resource

log = logging.getLogger("File Polling Base Classes")


class FileEntry(Resource):
    """Represents a file (or resource) on a remote system.

    :ivar transport: reference to the transport associated with the file.
    :ivar path: relative path of the file on the remote system. If the FileEntry
        represents a virtual file, the path should remain the same across
        subsequent calls to list_files (ie. if the remote system updates a.txt,
        the path should still remain the same instead of becoming a.v2.txt.)
    :ivar mtime: time the file was last modified represented as seconds since
        the Unix epoch.
    :ivar size: size of the file (or resource) in bytes.

    """

    def __init__(self, transport, mtime, path, size, root_path):
        self.transport = transport
        self.path = path
        self.mtime = mtime
        self.size = size
        self.root_path = root_path
        self.name = self.encode('utf8')

    @property
    def basename(self):
        return os.path.basename(self.path)

    def encode(self, encoding):
        return ','.join([str(x) for x in [self.mtime, self.path, self.size, self.root_path]])

    def __eq__(self, other):
        return (self.transport == other.transport and
                self.path == other.path and
                self.mtime == other.mtime and
                self.size == other.size)

    def __hash__(self):
        return hash((self.transport, self.path, self.mtime, self.size, self.root_path))

    def __repr__(self):
        return ("FileEntry(transport={self.transport!r}, path={self.path!r}, "
                "mtime={self.mtime!r}, size={self.size!r}, root_path={self.root_path!r})").format(self=self)


class FileTransport(PollingServiceBase):
    """Base class for file or resource transports, ex. to access files over
    SMB, FTP, HTTP, etc.

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(FileTransport, self).__init__()

    def loadParameters(self, schema, conf):
        super(FileTransport, self).loadParameters(schema, conf)
        if conf is None:
            conf = {}
        self.__dict__.update(conf)
        merge_schema_defaults(schema, self.__dict__)

    @abc.abstractmethod
    def copy_file(self, file_entry, local_path):
        """Copies a remote file to the specified local path, preserving the
        last modification time.

        :param file_entry: FileEntry object representing the file to fetch.
        :param local_path: path to save the file to. Leading directory
            components must already exist. If a file already exists at the
            path it will be overwritten.
        :returns: None
        :raises Exception: if copying the file did not succeed.

        """
        pass

    @abc.abstractmethod
    def delete_file(self, file_entry):
        """Removes a remote file.

        :param file_entry: FileEntry object representing the file to remove.
        :returns: None
        :raises Exception: if deleting the file did not succeed.

        """
        pass

    @abc.abstractmethod
    def list_files(self):
        """Returns a list of FileEntry objects representing all files currently
        available via the transport.

        :returns: a list of FileEntry objects.
        :raises Exception: if the transport was not able to list all files on
            the remote system.

        """
        pass
