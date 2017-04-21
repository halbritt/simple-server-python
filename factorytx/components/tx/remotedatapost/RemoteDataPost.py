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
            try:
                resp = json.loads(resp.text)
                result = {'code': status_code, 'summary': resp, 'size': len(payload)}
                sys.stdout.write(".")
                # self.log.info("Iteration %d) SUCCESS: %s" % (iterCnt, resp))
            except Exception as e:
                # self.log.info("Iteration %d) ERROR: Failed to parse initial batch response" % iterCnt)
                result = {'code': status_code, 'parse_failed': True}
                sys.stdout.write("E")
        return result

    def format_sslogs(self, bson_content):
        sslogs = [x[1] for x in sorted(bson_content.items(), key=lambda y: y[0])]
        bson_arr = []
        for sslog in sslogs:
            bson_sslog = bson.BSON.encode(sslog)
            bson_arr.append(bson_sslog)
        bson_out = b''.join(bson_arr)
        return bson_out

    def make_payload(self, sslogs):
        gzip_out = self.gzip_data(sslogs)

        if self.options['use_encryption']:
            encode_out = self.encode_data(gzip_out)
        else:
            encode_out = bytes(gzip_out.getvalue())

        return encode_out

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
