import json
import io
import time
import requests
import base64
import bson
import zlib
import sys
from datetime import datetime
from logging import getLogger
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
import hashlib
from io import BytesIO
from factorytx.components.tx.basetx import BaseTX
from factorytx.managers.PluginManager import component_manager
from factorytx import utils
from factorytx.components.tx.binary_fernet import BinaryFernetFile


class RemoteDataPost(BaseTX):

    logname = 'RDP'
    gzip_level = -1
    gzip_wbits = 31

    def load_parameters(self, schema, conf):
        self.logname = ': '.join([self.logname, conf['source']])
        self.log = getLogger(self.logname)
        conf['logname'] = self.logname
        super(RemoteDataPost, self).load_parameters(schema, conf)
        self.request_setup = self.setup_request()

    def TX(self, data, size):
        self.log.debug("RDP TX will now do its thing with vars %s.", vars(self))
        self.log.debug("Processing data of length %s", len(data))
        loaded = self.format_sslogs(data)
        self.log.debug("Now we have formatted the sslogs for rdp transmission.")
        payload = self.make_payload(loaded)
        self.log.debug("Made the payload")
        txed = False
        while not txed:
            self.log.debug("Submitting a payload")
            tx_init = datetime.utcnow()
            ship = self.send_http_request(payload)
            tx_finish = datetime.utcnow()
            duration_seconds = (tx_finish - tx_init).total_seconds()
            size_kb = len(payload) / 1024.0
            self.log.info("Transmission took %.3f seconds for %d sslogs / %.1f KB (starting at %s.) "
                          "Throughput: %.1f KB / s", duration_seconds, len(data), size_kb,
                          tx_init.isoformat(), size_kb / duration_seconds)
            if ship['code'] < 200 or ship['code'] >= 300:
                self.log.info("Failed to tx the data of size %s because of a status code %s from the server.",
                              size, ship['code'])
                self.log.info('Will retry shipping the payload again.')
                time.sleep(5)
            else:
                self.log.debug("Finished the TX: %s", ship)
                txed = True
        return True

    def setup_request(self):
        tenantname = self.options['tenantname']
        req_session = requests.Session()
        site_domain = self.options['sitedomain']
        protocol = self.options['protocol']
        hosturl = '{}://{}.{}'.format(protocol, tenantname, site_domain)
        if self.options['use_encryption']:
            rel_url = '/v1/rdp2/sslogs'
        else:
            rel_url = '/v1/rdp2/sslogs'
        full_url = '{}{}'.format(hosturl, rel_url)
        headers = {}
        if self.options['apikeyid']:
            headers.update({'X-SM-API-Key-ID': self.options['apikeyid']})
        else:
            self.log.error("ERROR: apikeyid is not specified")
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
        try:
            resp = setup['session'].put(setup['url'], files=multipart_form_data, headers=setup['headers'], allow_redirects=False)
            status_code = resp.status_code
        except Exception as e:
            self.log.error("Failed to perform the put with the setup %s", setup)
            self.log.error("The exception is %s", e)
            status_code = -1
            resp = False
        self.log.info("Got the response %s", resp)
        if status_code < 200 or status_code >= 300:
            self.log.info("Iteration) ERROR: Failed to retrieve response: code=%s" % status_code)
            if status_code == -1:
                self.log.info("The attempt to reach the remote server failed, it may not be running")
            elif status_code == 403:
                self.log.info("Error accessing the running ma server, possibly a missing API key in MA")
            elif status_code == 400:
                self.log.info("This was a bad request, possibly because of a misconfigured API key.")
            else:
                self.log.info("The response error is %s", resp.reason)
            try:
                result = {'code': status_code, 'text': resp.text}
            except AttributeError as e:
                result = {'code': status_code, 'text': None}
                self.log.error("There was a failure because there was no body to the response.")
                self.log.error("The missing is %s", e)
            sys.stdout.write("E")
        else:
            try:
                resp = json.loads(resp.text)
                if 'success' in resp and resp['success'] is True:
                    result = {'code': status_code, 'summary': resp, 'size': len(payload)}
                    sys.stdout.write(".")
                    self.log.info("Iteration) SUCCESS: %s" % (resp))
                else:
                    result = {'code': 400, 'summary': resp}
                    self.log.info("There was an error which resulted in a redirect, check firewall settings to the upload path")
            except Exception as e:
                self.log.info("Iteration) ERROR: Failed to parse initial batch response")
                result = {'code': status_code, 'parse_failed': True}
                sys.stdout.write("E")
        return result

    @staticmethod
    def sslog_sort_key(sslog):
        return (sslog['data']['source'], sslog['data']['timestamp'])

    def format_sslogs(self, bson_content):
        sslogs = sorted(bson_content, key=self.sslog_sort_key)
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

    @classmethod
    def gzip_data(cls, data):
        compressor = zlib.compressobj(cls.gzip_level, zlib.DEFLATED, cls.gzip_wbits)
        out = compressor.compress(data) + compressor.flush()
        return out
