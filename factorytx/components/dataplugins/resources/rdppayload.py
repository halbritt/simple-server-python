from factorytx.components.dataplugins.resource import Resource

class RDPPayload(Resource):
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

    def __init__(self, payload):
        self.payload = payload
        self.name = self.encode('utf8')

    @property
    def basename(self):
        return "TODO nice basename string"

    def encode(self, encoding):
        return ','.join("something nice")

    def __eq__(self, other):
        # TODO: Nice equal function for rdp payloads
        return False

    def __hash__(self):
        # TODO: Nice hash function
        return 0

    def __repr__(self):
        return "TODO nice print string"
