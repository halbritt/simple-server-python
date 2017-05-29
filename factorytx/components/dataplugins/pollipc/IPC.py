from FactoryTx.DataService import DataService
from FactoryTx.Global import setup_log
import threading
import time
from datetime import datetime
from FactoryTx.components.dataplugins.pollipc.PollIPC import PollIPC

#FIX ME: This is for using PDB until we have plugin re-written
#class IPC(DataService):
class IPC(threading.Thread, DataService):
    __version__ = '1.0.0'
    connected = False
    ipcpoller = None

    def __init__(self):
        super(IPC, self).__init__()
        self.ipcpoller = PollIPC()
        self.daemon = True

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
        self.ipcpoller.OpenSMBConnection()
        self.connected = True

    def reconnect(self):
        # Here is where you would handle reconnection
        pass

    def load_parameters(self, sdconfig, schema, config):
        self.ipcpoller.load_parameters(sdconfig, schema, config)

    def run(self):
        run = True
        self.ipcpoller.PollIPC()
        while run:
            time.sleep(1)
        self.ipcpoller.StopPolling()
        self.log.info("Stopping Service: {}".format(self.name))
