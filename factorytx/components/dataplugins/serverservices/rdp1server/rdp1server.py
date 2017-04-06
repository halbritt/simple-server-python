from factorytx.components.dataplugins.serverservices.baserdpserver import BaseRDPServer

class RDP1Server(BaseRDPServer):

    def upload(payload):
        print("Uploading the payload %s", payload)
