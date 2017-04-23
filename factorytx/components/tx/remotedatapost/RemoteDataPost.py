import json
import requests
import base64
import bson
import zlib
import sys
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
import hashlib
from io import BytesIO
from factorytx.components.tx.basetx import BaseTX
from factorytx.managers.PluginManager import component_manager
from factorytx.Global import setup_log
from factorytx import utils


class RemoteDataPost(BaseTX):

    logname = 'RDP'
    gzip_level = -1
    gzip_wbits = 31

    def loadParameters(self, schema, conf):
        self.logname = ': '.join([self.logname, conf['source']])
        conf['logname'] = self.logname
        super(RemoteDataPost, self).loadParameters(schema, conf)
        self.request_setup = self.setup_request()

    def TX(self, data):
        self.log.info("RDP TX will now do its thing with vars %s.", vars(self))
        for x in data:
            loaded = json.loads(x)
            self.log.info("There is now some %s records to process", len(loaded))
            loaded = self.format_sslogs(loaded)
            self.log.info("Now we have formatted the sslogs for rdp transmission.")
            payload = self.make_payload(loaded)
            self.log.debug("Made the payload")
            ship = self.send_http_request(payload)
            self.log.info("Finished the TX: %s", ship)
        return True

    def setup_request(self):
        tenantname = self.options['tenantname']
        req_session = requests.Session()
        site_domain = self.options['sitedomain']
        protocol = self.options['protocol']
        hosturl = '{}://{}.{}'.format(protocol, tenantname, site_domain)
        if self.options['use_encryption']:
            rel_url = '/v1/rdp2/sslogs_test_encrypt'
        else:
            rel_url = '/v1/rdp2/sslogs_test_no_encrypt'
        full_url = '{}{}'.format(hosturl, rel_url)
        headers = {}
        if self.options['apikeyalias']:
            headers.update({'X-SM-API-Key-Alias': self.options['apikeyalias']})
        else:
            self.log.error("ERROR: apikeyalias is not specified")
        encryption_key = self.options['apikey']
        return {'session':req_session, 'url':full_url, 'headers':headers, 'key':encryption_key,
                'host':hosturl, 'route':rel_url}

    def send_http_request(self, payload):
        self.log.info("Going to send the payload now")
        cfg = self.options
        setup = self.request_setup
        if cfg['use_encryption']:
            filetuple = ('p.tmp', payload)
        else:
            filetuple = ('p.tmp', payload, 'application/octet-stream', {'Transfer-Encoding': 'gzip'})
        multipart_form_data = {'sslog': filetuple}
        self.log.info("Going to put now with the setup", setup)
        resp = setup['session'].put(setup['url'], files=multipart_form_data, headers=setup['headers'])
        self.log.info("Got the response %s", resp)
        status_code = resp.status_code
        if status_code < 200 or status_code >= 300:
            self.log.info("Iteration) ERROR: Failed to retrieve response: code=%s, text=%s" % (status_code, resp.text))
            result = {'code': status_code, 'text': resp.text}
            sys.stdout.write("E")
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

    def encode_data(self, data):
        # set password
        binarykey = hashlib.sha256(bytes(self.options['apikey'], 'utf-8')).digest()
        key = base64.urlsafe_b64encode(binarykey)
        output = io.BytesIO()
        with BinaryFernetFile(key).open(fileobj=output, mode='wb') as cipher:
            cipher.write(data)

        return output.getvalue()

    @staticmethod
    def gzip_data(data):
        compressor = zlib.compressobj(self.gzip_level, zlib.DEFLATED, self.gzip_wbits)
        out = compressor.compress(data) + compressor.flush()
        return out
