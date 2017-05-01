from abc import ABCMeta, abstractmethod

class Resource(metaclass=ABCMeta):
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
    @property
    @abstractmethod
    def basename(self):
        pass

    @abstractmethod
    def __eq__(self, other):
        pass

    @abstractmethod
    def __hash__(self):
        """ DO IT """
        pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def encode(self, encoding):
        pass
