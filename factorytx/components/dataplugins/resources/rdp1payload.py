from factorytx.components.dataplugins.resource import Resource

class RDP1Payload(Resource):

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
