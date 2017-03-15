import json
import glob
from os.path import isfile, join, getmtime
import os
import logging
import time, sys, traceback
import codecs
import mimetypes
import sys
import uuid
import io
import xml.etree.ElementTree as ET
import datetime
import requests

import logging
from FactoryTx.Global import setup_log
from FactoryTx.components.tx.DataFilesPostManager import DataFilesPostManager
# log = setup_log('RemoteDataPost')

class RemoteDataPost(DataFilesPostManager):

    def loadParameters(self, sdconfig, schema, config):
        self.__dict__.update(config)
        logname = 'RemoteDataPost'
        if hasattr(self, 'source'):
            logname = '{}-{}'.format(logname, self.source)
        self.log = setup_log(logname)

        for key, value in schema.get('properties', {}).iteritems():
            if value.get('default', None) != None:
                if (not hasattr(self, key)) or (hasattr(self, key) and getattr(self, key) == None):
                    setattr(self, key, value.get('default'))

        self.root_dir = sdconfig.get('plugins', {}).get('data', None)
        if self.path[0:1] == "/":
            self.path = self.path[1:]

        if config.has_key('path'):
            self.databuffer = glob.os.path.abspath(config.get('path'))
        else:
            self.databuffer = glob.os.path.abspath(os.path.join(glob.os.path.abspath(self.root_dir), self.path))

        self.urlpath = self.url
        self.do_shutdown = False
        self.is_shutdown = False
        self.url = str(self.protocol) + "://" + str(self.host) + ":" + str(self.port) + str(self.urlpath)
        self.reload_files = False

    def PostDataHelper(self):
        self.post_json_files()

    def validateFilePostResponse(self, resp):
        if not resp:
            return False
        response = resp.text
        #####################################################
        ## According to Ryan 2/5/2015
        ## Put back old logic as well
        #####################################################
        skipJSONCheck = False

        # OLD LOGIC BELOW
        try:
            if len(response) == 26: #if valid mongo ID response (24 length plus quotations", delete original JSON
                skipJSONCheck=True
        except:
            pass
        # END OLD LOGIC

        # NEW LOGIC
        if not skipJSONCheck:
            try:
                jsonresp = json.loads(response)
                sslogid = jsonresp.get('last_valid_id', None) or jsonresp.get('last_reject_id', None) \
                          or jsonresp.get('id', None) # This last OR is for backwards compatibility
                if len(sslogid) > 0: #if valid mongo ID response, delete original JSON
                    skipJSONCheck = True
            except:
                pass
        # END NEW LOGIC
        #####################################################
        return skipJSONCheck

    def postfunc(self, data=None, files=None, headers=None, timeout=10):
        if files:
            resp = requests.post(self.url, files=files, headers=headers, timeout=timeout, verify=self.sslverify)
        else:
            self.log.info("posting {} with verify = {}".format(self.url, self.sslverify)) 
            resp = requests.post(self.url, json=data, headers=headers, timeout=timeout, verify=self.sslverify)
        return resp


    def isBinaryPost(self, json_dict):
        isBinary= False
        json_data = None
        if json_dict.get('isBinary', None) == True:
            isBinary = True
            json_data = json_dict
        try: # XML files and PLC created files have the data in a dictionary with the timestamp as the key
            if json_dict[json_dict.keys()[0]].get('isBinary', None) == True:
                isBinary = True
                json_data = json_dict[json_dict.keys()[0]]
        except Exception as e:
                pass

        if not isBinary: json_data = json_dict
        return isBinary, json_data

    def prepare_binary_file_post_args(self, filename, filepath):
        binary_mime_type = mimetypes.guess_type(filepath)[0] or 'application/octet-stream'
        with open(filepath, 'rb') as f:
            file_data =  f.read()
        return (filename, file_data, binary_mime_type)

    def _is_good_response(self, resp):
        if resp.status_code == 415:
            return False
        resp.raise_for_status()
        return self.validateFilePostResponse(resp)

    def post_json_files(self):
        # create the databuffer if it doesn't exists, make setup easier
        try:
            if not os.path.exists(self.databuffer):
                os.makedirs(self.databuffer)
        except Exception:
            self.log.exception('Failed to create databuffer folder!')
            return

        files = self.load_files(folder=self.databuffer, extensions=[self.fname_filter], reload_files=self.reload_files)
        if not files:
            return

        self.log.info("There are {} files to process".format(len(files)))

        for cur_file in files: # Iterate over each metadata json file

            json_dict = self.read_json_file(cur_file)
            if not json_dict:
                self.handle_file_onerror(cur_file)
                continue

            isBinary, json_data = self.isBinaryPost(json_dict)

            if json_data != None and isBinary == True: # We are processing a binary file
                filename = json_data.get('filename', None)
                filepath = os.path.join(self.databuffer, filename)
                json_data['extension'] = os.path.splitext(filepath)[1]
                json_data['SDposttimestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')

                try:
                    binary_file_tuple = self.prepare_binary_file_post_args(filename, filepath)
                except:
                    time.sleep(0.1)
                    self.log.error("ERROR! Unable to find matching binary file {}".format(filepath)) # The edge case someone is saving binary files with the same filename could overwrite binary data
                    self.handle_file_onerror(cur_file) # Remove the pointer to the duplicate binary that is left in the folder
                    continue

                json_tuple = ('metadata', json.dumps(json_data), 'application/json')
                self.log.info("POSTING {}".format(filepath))

                headers = {}
                if self.apikey: headers = {'X-SM-API-Key': self.apikey}
                resp = self.submitData(**{'files':{ 'ipcfile': binary_file_tuple, 'metadata': json_tuple }, 'headers':headers})

                self.log.info("Binary file upload response: HTTP %s, %s",
                              resp.status_code, resp.text)

                if self._is_good_response(resp):
                    try:
                        jsonresp = json.loads(resp.text)
                    except:
                        self.log.error(resp.text)
                        self.handle_file_onerror(cur_file)
                        continue

                    # Comment out imagefilesize checking for now
                    #remove = True if 'imagefilesize' not in jsonresp else origfilesize == jsonresp['imagefilesize']
                    remove = True
                    if remove:
                        self.handle_file_onsuccess(cur_file)
                        self.handle_file_onsuccess(filepath)
                        self.log.info("REMOVED {}".format(cur_file))
                        self.log.info("REMOVED {}".format(filepath))
                else:
                    self.handle_file_onerror(cur_file)
                    self.handle_file_onerror(filepath)

            else: # We are processing a strictly json file (ex. PLC)
                json_data = json_dict # Just to conform to the previous name
                headers = {'Content-Type': 'application/json'}
                if self.apikey: headers['X-SM-API-Key'] = self.apikey

                resp = self.submitData(**{'data':json_data, 'headers':headers, 'timeout':self.timeout})
                self.log.info('JSON file upload response: HTTP %s, %s',
                              resp.status_code, resp.text)

                if self._is_good_response(resp):
                    self.handle_file_onsuccess(cur_file)
                    self.log.info("REMOVED {}".format(cur_file))
                else:
                    self.handle_file_onerror(cur_file)

        self.reload_files = False


    def PostData(self):
        self.log.info('Starting remote data post to control node')
        self.is_shutdown = False
        self.poll = True
        while self.poll:
            try:
                self.PostDataHelper()
                time.sleep(1)
            except Exception as e:
                if self.do_shutdown:
                    self.log.error('RemoteDataPost encountered an error while uploading, but has been instructed to shutdown')
                    self.is_shutdown = True
                    return
                self.reload_files = True
                self.log.error(e)
                self.log.error('RemoteDataPost Error while uploading files to remote server.  Waiting 5 seconds before retry')
                time.sleep(5)
                self.log.info('RemoteDataPost: Attempting to resume posting')

        # if we got here, we are shutdown
        self.is_shutdown = True

    def StopPosting(self):
        self.log.info('RemoteDataPost beginning shutdown...')
        self.poll = False
        self.do_shutdown = True
        while not self.is_shutdown:
            self.log.info('RemoteDataPost waiting for posting processes to complete')
            time.sleep(2)
        self.log.info('RemoteDataPost shutdown complete')
