import datetime
import glob
import json
import logging
import os
import socket
import threading
import time

from bson import ObjectId

from FactoryTx.Global import setup_log
from FactoryTx.postprocessors import collect_events, compute_fields, split_sslog
from FactoryTx.utils import grouped
from . import melsec


def setupOutputDirectory(outputdirectory):
    if not os.path.exists(outputdirectory):
        os.makedirs(outputdirectory)


# ****************************************
class PollMitsubishiPLC():
    def __init__(self):
        self.soc = None
        self.PLCConnected = False #Are we currently connected to the PLC
        self.disconnectCount = 0
        self.jsonpackage = []  # list of sslog dicts flushed out in batches of duration aggregation_time
        self.split_sslog_cache = {}

    def loadPLCParameters(self, sdconfig, schema, config):
        self.__dict__.update(config)
        logname = 'MitsubishiPLC'
        if hasattr(self, 'source'):
            logname = '{}-{}'.format(logname, self.source)
        self.log = setup_log(logname)
        for key, value in schema.get('properties', {}).iteritems():
            if value.get('default', None) != None:
                if (not hasattr(self, key)) or (hasattr(self, key) and getattr(self, key) == None):
                    setattr(self, key, value.get('default'))
        self.root_dir = sdconfig.get('plugins', {}).get('data', None)
        if self.outputdirectory[0:1] == "/":
            self.outputdirectory = self.outputdirectory[1:]
        self.databuffer = glob.os.path.abspath(os.path.join(glob.os.path.abspath(self.root_dir), self.outputdirectory))
        self.do_shutdown = False
        self.aggregationstart = datetime.datetime.utcnow()
        setupOutputDirectory(self.databuffer)

        melsec.validate_blocks(self.datablocks)

        if self.debug:
            melsec.log.setLevel(logging.DEBUG)
        else:
            melsec.log.setLevel(logging.INFO)

        if self.split_data:
            self.convert_left_right_config()

    def convert_left_right_config(self):
        """Converts MSP-style left- / right-prefix configs to generalized
        sslog-splitting configurations.

        """

        self.split_sslogs = [
            {
                'source': self.source + '_' + self.split_label_suffixes[0],
                'counterfield': self.counterfield_left['field'],
                'fieldprefixes': [
                    {
                        'prefix': self.split_prefixs[0] + '_',
                        'replace_with': '',
                    },
                ],
            },
            {
                'source': self.source + '_' + self.split_label_suffixes[1],
                'counterfield': self.counterfield_right['field'],
                'fieldprefixes': [
                    {
                        'prefix': self.split_prefixs[1] + '_',
                        'replace_with': '',
                    },
                ],
            },
        ]

    # ****************************************
    # * scheduler : thread timer object      *
    # *   called about every 250ms           *
    # *   Poll PLC via TCP/IP pre-open       *
    # *   socket and store data in pre-open  *
    # *   text file in CSV format.           *
    # * -------------------------------------*
    # *   in:  None                          *
    # *   out: None                          *
    # ****************************************
    def scheduler(self):
        try:
            if self.PLCConnected and self.polling < 2:
                if self.lastTime == 0:
                    self.lastTime = time.time()
                if self.polling == 0:
                    self.polling = 1
                    fieldvalues = melsec.read_blocks(self.soc, self.datablocks, self.data_code)
                    new_sslogs = self.create_sslogs(fieldvalues)
                    self.jsonpackage.extend(new_sslogs)

                    # if enough time lapsed, write to disk
                    if (datetime.datetime.utcnow() - self.aggregationstart).total_seconds() >= self.aggregationtime and self.jsonpackage:
                        self.writeDataJSON(self.jsonpackage)
                        self.jsonpackage = []
                        self.aggregationstart = datetime.datetime.utcnow()

                    # successful retrieval.  Start the next thread and return:
                    self.polling = 0
                    self.lastTime += self.polltime
                    self.thr = threading.Timer(self.lastTime - time.time(), self.scheduler)
                    self.thr.daemon = True
                    self.thr.start()
                    return
                else:
                    self.polling = 0  # can probably cut this?
            else:
                self.log.info('PLC not connected')
                self.log.info('Disconnected for: ', self.disconnectCount)
                isConnected = self.OpenSocket()
                if not isConnected:
                    time.sleep(3)
                self.thr = threading.Timer(self.lastTime - time.time(),self.scheduler)
                self.thr.daemon = True
                self.thr.start()
        except Exception as e:
            self.polling = 2
            self.PLCConnected = False
            if isinstance(e, (melsec.RequestError, socket.error)):
                self.log.error("PLC polling error: %s", e)
            else:
                self.log.exception("Unexpected exception while polling PLC : %s", e)
            self.log.info("Attempting to reconnect to the PLC ...")
            isConnected = False
            timeBeforeDisconnect = self.timeStamp()
            while not self.do_shutdown:
                isConnected = self.OpenSocket()
                if isConnected:
                    break
                time.sleep(3)
            if self.do_shutdown:
                self.log.warn('PLC scheduling thread perfoming a shutdown instead of reconnect')
                return
            self.log.info('Connection reestablished')
            self.log.info('Last time before disconnect: {}'.format(timeBeforeDisconnect))
            self.log.info('Current time: {}'.format(self.timeStamp()))
            self.log.info('Resuming PLC polling')
            self.lastTime = 0
            self.PLCConnected = True
            self.polling = 0
            self.thr = threading.Timer(0,self.scheduler)
            self.thr.daemon = True
            self.thr.start()

    def PollPLC(self):
        self.polling = 0
        self.lastTime = 0
        self.counter = 0
        self.running = 0
        self.lastTime = 0
        self.OpenSocket()
        self.thr = threading.Timer(self.polltime, self.scheduler)
        self.thr.daemon = True
        self.thr.start()

    # ****************************************
    # * timeStamp : get timestamp with date  *
    # *   and time in format :               *
    # *   YYYY-MM-DDTHH:MM:SS.us             *
    # * -------------------------------------*
    # *   in:  None                          *
    # *   out: String, 26 chars              *
    # ****************************************
    def timeStamp(self):
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return str(st)

    def create_sslogs(self, fieldvalues):
        base_sslog = {
            '_id': str(ObjectId()),
            'version': self.version,
            'plugin': "mitsubishiplc",
            'timestamp': self.timeStamp(),
            'fieldvalues': fieldvalues,
            'running': self.runningfield['field']
        }

        compute_fields(base_sslog, self.compute_fields)

        if self.split_sslogs:
            base_sslog['source'] = None
            base_sslog['counter'] = None
            new_sslogs = split_sslog(self.split_sslogs, base_sslog, self.split_sslog_cache)
        else:
            base_sslog['source'] = self.source
            base_sslog['counter'] = fieldvalues[self.counterfield['field']]['value']
            new_sslogs = [base_sslog]

        new_sslogs = [collect_events(self.collect_events, s) for s in new_sslogs]
        self.normalize_counters(new_sslogs)

        return new_sslogs

    def normalize_counters(self, new_sslogs):
        # ETL expects that all sslogs will have integer counters, so
        # we need to convert other values (ex. booleans) to integers.
        #
        # TODO: Apply this logic to DataPlugin as well.
        for sslog in new_sslogs:
            counter = sslog["counter"]
            try:
                sslog["counter"] = int(counter)
            except (TypeError, ValueError):
                self.log.warning("Received non-integer counter: %r", counter)

    def writeDataJSON(self, input_data):
        rnd_part = os.urandom(16).encode('hex')
        sslogs_by_source = grouped(self.jsonpackage, key=lambda s: s['source'])

        for source, sslogs in sslogs_by_source.items():
            basename = source + '_' + self.timeStamp() + '_' + rnd_part
            path = os.path.join(self.databuffer, basename)
            with open(path + '.tmp', 'w') as fd:
                encoded = json.dumps({s['timestamp']: s for s in sslogs})
                fd.write(encoded + '\n')

            os.rename(path + '.tmp', path + '.sm.json')
            self.log.info('Wrote a {} second batch of PLC data to: {}'.format(self.aggregationtime, path + '.sm.json'))
        return 0

    def OpenSocket(self):
        self.log.info("Attempting to open PLC socket")

        if self.soc is not None:
            try:
                self.soc.close()
            finally:
                self.soc = None

        try:
            self.soc = socket.create_connection((self.host, self.port), timeout=5.0)
            # Disable Nagle's algorithm to speed up polling multiple blocks.
            self.soc.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.soc.settimeout(1.0)
            self.log.info('PLC Socket Established')
            self.PLCConnected = True
            self.disconnectCount = 0
            self.polling = 0
            return True
        except socket.error as e:
            self.log.error("Failed to connect to PLC: %s", e)
            self.soc = None
            self.PLCConnected = False
            self.disconnectCount += 1
            return False

    def StopPolling(self):
        # ----- User stopped : cleaning up
        try:
            self.do_shutdown = True
            self.thr.cancel()
            if self.polling != 0:
                if self.polling != 2:
                    self.polling = 2
                    self.log.info("Waiting end of polling...")
                    while self.polling != 0:
                        pass
            self.soc.close()
        except:
            self.log.warn('PLC thread cleanup failed')

        self.log.info('PLC polling stopped')
