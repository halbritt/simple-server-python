from FactoryTx.DataService import DataService
import threading
import time
from FactoryTx.components.tx.remotedatapost.RemoteDataPost import RemoteDataPost

# log = setup_log('RemoteDataPost')
#FIX ME: This is for using PDB until we have plugin re-written
#class RDP(DataService):
class RDP(threading.Thread, DataService):
    __version__ = '1.0.0'
    connected = False
    remoteDataPoster = None

    def __init__(self):
        super(RDP, self).__init__()
        self.daemon = True
        self.remoteDataPoster = RemoteDataPost()

    def __repr__(self):
        return '{}'.format(self.name)

    #FIX ME: This is for using PDB until we have plugin re-written
    # def start(self):
    #     self.run()

    # def join(self):
    #     pass

    # def is_alive(self):
    #     pass

    # def name(self):
    #     pass
    #END FIX ME

    def connect(self):
        # Here you would setup connection to server
        self.connected = True

    def reconnect(self):
        # Here is where you would handle reconnection
        pass

    def loadParameters(self, sdconfig, schema, config):
        self.remoteDataPoster.loadParameters(sdconfig, schema, config)

    def run(self):
        run = True
        self.remoteDataPoster.PostData()
        while run:
            time.sleep(1)
        self.remoteDataPoster.StopPosting()
        self.log.info('Shutting down service: {}'.format(self.name))
