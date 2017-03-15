from FactoryTx.Global import setup_log
from FactoryTx.DataService import DataService
import threading
import time
from datetime import datetime
from FactoryTx.components.dataplugins.mitsubishiplc.PollMitsubishiPLC import PollMitsubishiPLC


#FIX ME: This is for using PDB until we have plugin re-written
#class MitsubishiPLCService(DataService):
class MitsubishiPLCService(threading.Thread, DataService):
    __version__ = '1.0.0'
    connected = False
    plcpoller = None

    def __init__(self):
        super(MitsubishiPLCService, self).__init__()
        self.plcpoller = PollMitsubishiPLC()
        self.daemon = True

    def __repr__(self):
        return '{}'.format(self.name)

    def connect(self):
        # Here you would setup connection to server
        self.plcpoller.OpenSocket()
        self.connected = True

    def reconnect(self):
        # Here is where you would handle reconnection
        pass

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

    def loadParameters(self, sdconfig, schema, config):
        self.plcpoller.loadPLCParameters(sdconfig, schema, config)
    
    def run(self):
        run = True     
        self.plcpoller.PollPLC()        
        while run:
            time.sleep(1)
        self.plcpoller.stopPLCPolling()
        self.log.info('Shutting down service: {}'.format(self.name))
