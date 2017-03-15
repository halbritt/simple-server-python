import socket
import sys
import os
import glob
import time
import datetime
import threading
import select
import inspect
import tempfile, shutil
import json
from smb.SMBConnection import SMBConnection
import logging
import xml.etree.ElementTree as ET
import traceback
import pandas as pd
import numpy as np
import math
import fnmatch
import re
import pytz

from bson import ObjectId

from FactoryTx import postprocessors
from FactoryTx import utils

# log = logging.getLogger(__name__)
from FactoryTx.Global import setup_log
# log = setup_log('PollIPC')


def setupOutputDirectory(outputdirectory):
    if not os.path.exists(outputdirectory):
        # self.log.info('Creating IPC buffer directory')
        os.makedirs(outputdirectory)
    # else:
    #     self.log.info('IPC buffer directory exists')


# ****************************************
class PollIPC():

    def loadParameters(self, sdconfig, schema, config):
        self.__dict__.update(config)
        logname = 'PollIPC'
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
        self.remotefiles = []
        self.split_sslog_cache = {}
        setupOutputDirectory(self.databuffer)
        self.do_shutdown = False

    def scheduler(self):
        try:
            self.log.info('Scheduler running')
            if self.lastTime == 0:
                self.lastTime = time.time()
            if self.polling == 0:
                self.polling = 1
                self.poller()
                if self.remotefiles:
                    self.log.info('Found {} IPC files. Attempting retrieval'.format(len(self.remotefiles)))
                    self.copyRemoteFiles()
                else:
                    self.log.info('No remote files found on IPC share')
                if self.polling != 2:
                    self.polling = 0
                    self.lastTime += self.polltime
                    self.thr = threading.Timer(self.lastTime - time.time(),self.scheduler)
                    self.thr.daemon = True
                    self.thr.start()
                else:
                    self.polling = 0
        except:
            self.log.exception('Caught exception while polling. Attempting to reconnect to IPC')
            isConnected = False
            timeBeforeDisconnect = self.timeStamp()
            while not self.do_shutdown:
                isConnected = self.OpenSMBConnection()
                if isConnected:
                    break
                time.sleep(self.polltime)
            if self.do_shutdown:
                self.log.warn('IPC scheduling thread perfoming a shutdown instead of reconnect')
                self.polling = 0
                return
            self.log.info('IPC Connection reestablished')
            self.log.info('Last time before disconnect: {}'.format(timeBeforeDisconnect))
            self.log.info('Current time: {}'.format(self.timeStamp()))
            self.log.info('Resuming IPC polling')
            self.lastTime = 0
            self.polling = 0
            self.thr = threading.Timer(0,self.scheduler)
            self.thr.daemon = True
            self.thr.start()
            return
        return

    def displayFileCount(self):
        self.log.info('Found {} files'.format(len(self.remotefiles)))

    def skipFile(self, filePath):
        return any([fnmatch.fnmatch(filePath, pattern) for pattern in self.skipFiles])

    def copyRemoteFiles(self):
        self.log.info('Initiating transfer of remote files')
        for cur_file in self.remotefiles:
            #import pdb; pdb.set_trace()
            self.log.info('Filename: %s' % (cur_file.filename))
            cur_fn = cur_file.filename
            base, extension = os.path.splitext(cur_fn)

            if not cur_file.isDirectory:

                filePath = cur_fn
                if self.folderPath != "/":
                    filePath = self.folderPath + cur_fn

                if self.skipFile(filePath):
                    self.conn.deleteFiles(self.sharedFolder, filePath)
                    self.log.info('Skipped copying remote file and deleted: ' + filePath)
                    continue

                if extension != '.tmp' and extension != '.db':

                    self.log.info('Attempting transfer of: ' + filePath)
                    file_obj = tempfile.NamedTemporaryFile(delete=False)
                    file_attributes, filesize = self.conn.retrieveFile(self.sharedFolder, filePath, file_obj)
                    file_obj.close()
                    self.log.info('Received: ' + filePath)

                    newFileName = self.source + "_" + utils.make_guid() + "_"  + cur_fn
                    newFilePath = os.path.join(self.databuffer, newFileName)
                    shutil.copy(file_obj.name, newFilePath)
                    if self.removeFiles == 1 or self.removeFiles == True: # WARNING: If this is not set to 1, this will create duplicate data - MTA (2015-02-25)
                        self.conn.deleteFiles(self.sharedFolder, filePath)
                        self.log.info('Removed remote file: ' + filePath)

                    # Create JSON file for corresponding image
                    if (extension.lower() == '.xml'):
                        self.parseXML(cur_file, cur_fn, newFileName)
                    else:
                        # Try to match each configured CSV type before we conclude
                        # it's a binary file. Some have odd suffixes, ex. .TXT.
                        csv_type = None
                        lower_filename = cur_fn.lower()
                        for each_type in self.csv:
                            if fnmatch.fnmatchcase(lower_filename, each_type["match"].lower()):
                                csv_type = each_type
                                break
                        if csv_type is not None:
                            self.parseCSV(each_type, cur_fn, newFileName)
                        else:
                            self.writeDataJSON(cur_file, cur_fn, newFileName)

            else:
                self.log.info('Current file is a directory.  Skipping.')

    def _getCounterFromXML(self, jsondata, originalFileName):
        if self.counterfield['field'] in jsondata:
            return int(jsondata[self.counterfield['field']]['value'])
        else:
            filemetadata = {}
            self._populateFileNameMetaDataParts(filemetadata, originalFileName)
            return int(filemetadata.get(self.counterfield['field'], -1))

    def _copy_sslog(self, base_sslog, route_spec):
        """Returns a copy of the sslog dict `base_sslog`, with the source and
        field names replaced according to `route_spec`. See the documentation
        for the "route_files" directive in the schema for more details.

        """

        new_sslog = base_sslog.copy()
        new_sslog['source'] = route_spec['source']

        if 'fieldvalues' in base_sslog:
            if route_spec.get('strip_regexp') is None:
                new_sslog['fieldvalues'] = base_sslog['fieldvalues'].copy()
            else:
                strip_regexp = re.compile(route_spec['strip_regexp'])
                new_sslog['fieldvalues'] = {}
                for field_name, value in base_sslog['fieldvalues'].iteritems():
                    new_field_name = strip_regexp.sub('', field_name)
                    new_sslog['fieldvalues'][new_field_name] = value

        return new_sslog

    def write_sslogs(self, base_sslog):
        """Writes the sslog dict `base_sslog` to the data buffer, optionally
        duplicating it into multiple sslogs for different data sources. See the
        "route_files" directive in the schema for details.

        """

        original_path = os.path.join(base_sslog['outputdirectory'], base_sslog['filename'])

        if len(self.route_files) == 0:
            sslogs = [base_sslog]
        else:
            sslogs = []
            original_filename = base_sslog['original_filename']
            lower_filename = original_filename.lower()

            for route_spec in self.route_files:
                for pattern in route_spec['patterns']:
                    if fnmatch.fnmatchcase(lower_filename, pattern.lower()):
                        new_sslog = self._copy_sslog(base_sslog, route_spec)
                        sslogs.append(new_sslog)

            if len(sslogs) == 0:
                # Any file which does not match at least one route specification
                # should be associated with all sources configured.
                for route_spec in self.route_files:
                    new_sslog = self._copy_sslog(base_sslog, route_spec)
                    sslogs.append(new_sslog)

            # Copy binary files before writing any sslogs so that remotedatapost
            # doesn't delete the original file before we finish making copies.
            if base_sslog['isBinary']:
                keep_original = False
                for i, sslog in enumerate(sslogs):
                    # The base source and new source may match, ex. to preserve
                    # backwards compatibility with pre-line sslogs.
                    if base_sslog['source'] == sslog['source']:
                        keep_original = True
                        continue
                    new_path = original_path.replace(base_sslog['source'], sslog['source'])
                    if new_path == original_path:
                        new_path = original_path + '_' + str(i)
                    shutil.copyfile(original_path, new_path)
                    sslog['filename'] = os.path.basename(new_path)

                if not keep_original:
                    os.remove(original_path)

        for sslog in sslogs:
            sslog['_id'] = str(ObjectId())
            rnd_part = os.urandom(16).encode('hex')
            path = os.path.join(self.databuffer, sslog['source'] + '_' + sslog['timestamp'] + rnd_part)
            self.log.info('Creating JSON log for "%s"', path)

            with open(path + '.tmp', 'w') as fp:
                encoded = json.dumps({sslog['timestamp']: sslog})
                fp.write(encoded + '\n')
            os.rename(path + '.tmp', path + '.sm.json')

    def parseXML(self, cur_file, originalFileName, newFileName):
        newFilePath = os.path.join(self.databuffer, newFileName)
        try:
            tree = ET.parse(newFilePath)
            root = tree.getroot()
            jsondata = {}
            for child in root:
                jsondata[child.tag] = {}
                if self.xml:
                    if child.tag in self.xml['fields'].keys(): #custom int/float cleansing required
                        val = child.text
                        if self.xml['fields'][child.tag] == 'int':
                            val = int(val)
                        elif self.xml['fields'][child.tag] == 'float':
                            val = float(val)
                        jsondata[child.tag]['value'] =  val
                        jsondata[child.tag]['units'] =  child.attrib.get('unit',"")
                    else:
                        jsondata[child.tag]['value'] =  child.text
                        jsondata[child.tag]['units'] =  child.attrib.get('unit',"")
                    if child.get('unit', None) == "File":
                        jsondata[child.tag]['value'] = self.source + "_" + jsondata[child.tag]['value']
            ret = {}
            cur_time = self.fileCreationTimeStamp(newFilePath)
            ret['source'] = self.source
            if hasattr(self, 'sslog_type'):
                ret['sslog_type'] = self.sslog_type
            if not ret.get('sslog_type') and originalFileName[-3:] == 'xml':
                ret['sslog_type'] = 'xml'
            ret['version'] = self.version
            ret['plugin'] = "pollipc"
            ret['fieldvalues'] = jsondata
            ret['timestamp'] = cur_time
            ret['counter'] = self._getCounterFromXML(jsondata, originalFileName)
            ret['running'] = self.runningfield # Is this being used? -MTA 2015/03/02
            ret['isXMLGenerated'] = True
            ret['isBinary'] = True
            ret['original_folder'] = self.folderPath
            ret['original_filename'] = originalFileName
            ret['outputdirectory'] = self.databuffer
            ret['filename'] = newFileName

            self.write_sslogs(ret)

            # if json parse succeeded, remove XML file
            # os.remove(cur_file)
        except:
            self.log.warning('XML File failed to parse to json. Please check setup configuration or incoming data format change')


    #####
    # In the long run, this will be replaced with new CSV poller
    # But for now, borrow some of that code, insert it into the XML "template" and let the IPC poller do it too
    #####
    def parseColumnarCSV(self, newFilePath, column_names=None, sep=',', skip=None):
        if column_names is not None:
            sheet = pd.read_csv(newFilePath, header=None, names=column_names,
                                skipinitialspace=True, sep=sep, skiprows=skip)
        else:
            sheet = pd.read_csv(newFilePath, header=0, skipinitialspace=True, sep=sep, skiprows=skip)
        fieldvalues = {}
        if len(sheet) != 1:
            self.log.warning("Received columnar CSV with %d rows, expected 1.",
                             len(sheet))
        if len(sheet) > 0:
            for colname in sheet.columns.values.tolist():
                value = sheet.ix[0, colname]
                if isinstance(value, np.generic):
                    value = value.tolist()
                fieldvalues[colname] = {'value': value, 'units': None}
        return fieldvalues

    def parseKeyValueCSV(self, newFilePath, key_column, value_column, sep=',', skip=None):
        sheet = pd.read_csv(newFilePath, header=None, skipinitialspace=True,
                            error_bad_lines=False, warn_bad_lines=True, sep=sep, skiprows=skip)
        fieldvalues = {}
        for idx in xrange(0, len(sheet)):
            value = sheet.ix[idx, value_column]
            if isinstance(value, np.generic):
                value = value.tolist()
            fieldvalues[sheet.ix[idx, key_column]] = {"value": value, 'units': None}
        return fieldvalues

    def parseCSV(self, csv_type, originalFileName, newFileName):
        newFilePath = os.path.join(self.databuffer, newFileName)
        try:
            sep = csv_type.get('sep', ',')
            skip = csv_type.get('skip', None)
            if csv_type["format"] == "alarm":
                fieldvalues = self.parseColumnarCSV(
                    newFilePath, column_names=csv_type["column_names"], sep=sep, skip=skip)
            elif csv_type["format"] == "columnar":
                fieldvalues = self.parseColumnarCSV(newFilePath, sep=sep, skip=skip)
            elif csv_type["format"] == "key_value":
                fieldvalues = self.parseKeyValueCSV(newFilePath, key_column=0, value_column=1, sep=sep, skip=skip)
            elif csv_type["format"] == "value_key":
                fieldvalues = self.parseKeyValueCSV(newFilePath, key_column=1, value_column=0, sep=sep, skip=skip)
            elif csv_type["format"] == "ipc_version":
                fieldvalues = self.parseKeyValueCSV(newFilePath, key_column=1, value_column=2, sep=sep, skip=skip)
            elif csv_type["format"] == "ucb_version":
                fieldvalues = self.parseKeyValueCSV(newFilePath, key_column=2, value_column=3, sep=sep, skip=skip)
            else:
                raise ValueError("Unknown CSV format {}!".format(csv_type["format"]))

            prefix = csv_type.get("field_prefix", "")
            fieldvalues = {(prefix + str(k)).strip().replace('.', '_'): v
                           for k, v in fieldvalues.iteritems()}

            ret = {}
            cur_time = self.fileCreationTimeStamp(newFilePath)
            ret['source'] = self.source
            ret['sslog_type'] = csv_type.get('sslog_type', 'csv')
            ret['version'] = self.version
            ret['plugin'] = "pollipc"
            ret['fieldvalues'] = fieldvalues
            ret['timestamp'] = cur_time
            ret['counter'] = self._getCounterFromXML(fieldvalues, originalFileName)
            ret['running'] = self.runningfield # Is this being used? -MTA 2015/03/02
            ret['isXMLGenerated'] = False
            ret['isBinary'] = True
            ret['original_folder'] = self.folderPath
            ret['original_filename'] = originalFileName
            ret['outputdirectory'] = self.databuffer
            ret['filename'] = newFileName

            self.write_sslogs(ret)

            # if json parse succeeded, remove XML file
            # os.remove(cur_file)
        except:
            self.log.exception('Failed to parse CSV "%s"', originalFileName)


    def fileCreationTimeStamp(self, cur_file):
        if hasattr(self, 'ts_from_filename') and self.ts_from_filename:
            try:
                # Failsafe so this doesn't crash on old version of this functionality
                if type(self.ts_from_filename) != str:
                    timepart = '_'.join(cur_file.split('_')[1:7])
                    ts = datetime.datetime.strptime(timepart, '%Y_%m_%d_%H_%M_%S')
                else:
                    filepattern = re.compile(self.ts_from_filename)
                    match = filepattern.match(cur_file)
                    if match:
                        td = match.groupdict()
                        ts = datetime.datetime(int(td['year']), int(td['month']), int(td['day']),
                                               int(td['hour']), int(td['min']), int(td['sec']))
                    else:
                        raise Exception('Bad timestamp on file {}, rule {}'.format(cur_file, self.ts_from_filename))

                if hasattr(self, 'timezone'):
                    try:
                        local = pytz.timezone(self.timezone)
                        ts = local.localize(ts).astimezone(pytz.utc)
                    except Exception as e:
                        self.log.warn('Unable to convert timestamp to UTC: {}'.format(e))

                ts = ts.strftime('%Y-%m-%dT%H:%M:%S.%f')

            except Exception as e:
                self.log.info('Could not parse filename timestamp: {}'.format(e))
                ts = datetime.datetime.fromtimestamp(os.path.getmtime(cur_file)).strftime('%Y-%m-%dT%H:%M:%S.%f')
        else:
            ts = datetime.datetime.fromtimestamp(os.path.getmtime(cur_file)).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return ts

    def timeStamp(self):
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return str(st)

    def tsplit(self, string, delimiters):
        """Behaves str.split but supports multiple delimiters."""
        delimiters = tuple(delimiters)
        stack = [string,]
        
        for delimiter in delimiters:
            for i, substring in enumerate(stack):
                substack = substring.split(delimiter)
                stack.pop(i)
                for j, _substring in enumerate(substack):
                    stack.insert(i+j, _substring)
        return stack

    def splitFilenameMetaData(self, basefilename):
        return self.tsplit(basefilename, self.binaryDelimiter)

    def _populateFileNameMetaDataParts(self, jsondata, filename):
        extensions = filename.split(os.extsep)
        basefile = os.path.basename(extensions[0])
        fileparts = self.splitFilenameMetaData(basefile) # basefile.split('_')

        for cur_part in self.filenamemetadata:
            try:
                jsondata[cur_part['name']] = fileparts[cur_part['address']]
            except:
                self.log.warn('Image metadata update mismatch. Check IPC filename structure against configuration')
                pass

        if self.counterfield:
            try:
                jsondata['counter'] = int(jsondata[self.counterfield['field']])
            except:
                self.log.warn('Image counter update mismatch. Check IPC filename structure against configuration')
                pass


    def writeDataJSON(self, cur_file, originalFileName, newFileName):
        newFilePath = os.path.join(self.databuffer, newFileName)
        timestamp = self.fileCreationTimeStamp(newFilePath)
        jsondata = {
            'source': self.source,
            'version': self.version,
            'plugin': 'pollipc',
            'timestamp': timestamp,
            'original_folder': self.folderPath,
            'original_filename': originalFileName,
            'outputdirectory': self.databuffer,
            'filename': newFileName,
            'isBinary': True,
        }

        self._populateFileNameMetaDataParts(jsondata, originalFileName)
        self.write_sslogs(jsondata)

    # ****************************************
    # * poller : Poll the PLC via pre-open   *
    # *   TCP/IP socket.                     *
    # *   Poll PLC via TCP/IP pre-open       *
    # *   socket and store data in pre-open  *
    # *   text file in CSV format.           *
    # * -------------------------------------*
    # *   in:  None                          *
    # *   out: Variant:                      *
    # *          String: message is success  *
    #            int: error code if fail     *
    # ****************************************
    def poller(self,timeout_s=1):
        self.log.info('Polling IPC files')
        files = self.conn.listPath(self.sharedFolder, self.folderPath)
        self.remotefiles = files[2:]
        return

    # ****************************************
    # * errLog: Send Error to stderr with    *
    # *   timestamp.                         *
    # * -------------------------------------*
    # *   in:  String, text message          *
    # *   out: None                          *
    # ****************************************
    def errLog(self,txt):
        module = inspect.stack()[1][3]
        if module == "<module>":
            module = "main"
        module += "()"
        sys.stderr.write("[%s]:%s:%s:%s\n" % (self.timeStamp(), __main__.__file__, module, txt))
        return

    def OpenSMBConnection(self):
        self.conn = SMBConnection(self.username, self.password, 'nodePC', self.hostName, use_ntlm_v2 = True, is_direct_tcp=True)
        try:
            self.log.info('Attempting to Connect to SMB Server')
            self.conn.connect(self.host, self.port, timeout=self.timeout)
            self.log.info('SMB Connection Opened')
            return True
        except:
            self.log.error('Error connecting to smb connection') #throw additional error handing here
            return False


    def PollIPC(self):
        # ----- Start polling every 0.25 seconds -----
        self.log.info('Initiating IPC polling')
        self.polling = 0
        self.lastTime = 0
        self.thr = threading.Timer(self.polltime, self.scheduler)
        self.thr.daemon = True
        self.thr.start()

    def StopPolling(self):
        # ----- User stopped : cleaning up
        self.log.info('Stopping IPC polling')
        try:
            self.do_shutdown = True
            self.thr.cancel()
            if self.polling != 0:
                if self.polling != 2:
                    self.polling = 2
                    self.log.info("IPC services waiting for shutdown...")
                    while self.polling != 0:
                        pass
        except:
            self.log.warn('IPC thread cleanup failed')
        self.log.info('IPC polling stopped')
