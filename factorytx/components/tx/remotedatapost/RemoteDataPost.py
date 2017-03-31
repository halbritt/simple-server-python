import json
import requests
import base64
import bson
import gzip
import sys
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from io import BytesIO
from factorytx.components.tx.basetx import BaseTX
from factorytx.managers.PluginManager import component_manager
from factorytx import utils


class RemoteDataPost(BaseTX):

    logname = 'RDP'

    def loadParameters(self, schema, conf):
        super(RemoteDataPost, self).loadParameters(schema, conf)
        self.logname = ': '.join([self.logname, conf['source']])
        self.request_setup = self.setup_request()

    def TX(self, data):
        print("RDP TX will now do its thing with vars %s.", vars(self))
        for x in data:
            loaded = json.loads(x)
            print("There is now some %s records to process with first %s", len(loaded))
            loaded = self.format_sslogs(loaded)
            print("Now we have formatted the sslogs for rdp transmission.")
            payload = self.make_payload(loaded)
            self.log.debug("Made the payload")
            ship = self.send_http_request(payload)
            self.log.info("Finished the TX", ship)
        return False

    def setup_request(self):
        tenantname = self.tenantname
        req_session = requests.Session()
        site_domain = self.sitedomain
        protocol = self.protocol
        hosturl = '{}://{}.{}'.format(protocol, tenantname, site_domain)
        if self.use_encryption:
            rel_url = '/v1/rdp2/sslogs_test_encrypt'
        else:
            rel_url = '/v1/rdp2/sslogs_test_no_encrypt'
        full_url = '{}{}'.format(hosturl, rel_url)
        headers = {}
        if self.apikeyalias:
            headers.update({'X-SM-API-Key-Alias': self.apikeyalias})
        else:
            print("ERROR: apikeyalias is not specified")
        encryption_key = self.apikey
        return {'session':req_session, 'url':full_url, 'headers':headers, 'key':encryption_key,
                'host':hosturl, 'route':rel_url}

    def send_http_request(self, payload):
        setup = self.request_setup
        if self.use_encryption:
            filetuple = ('p.tmp', payload)
        else:
            filetuple = ('p.tmp', payload, 'application/octet-stream', {'Transfer-Encoding': 'gzip'})
        multipart_form_data = {'sslog': filetuple}
        print("Going to put now with the setup", setup)
        resp = setup['session'].put(setup['url'], files=multipart_form_data, headers=setup['headers'])
        print("Got the response %s", resp)
        status_code = resp.status_code
        if status_code < 200 or status_code >= 300:
            print("Iteration) ERROR: Failed to retrieve response: code=%s, text=%s" % (status_code, resp.text))
            result = {'code': status_code, 'text': resp.text}
            sys.stdout.write("E")
        else:
            try:
                resp = json.loads(resp.text)
                result = {'code': status_code, 'summary': resp, 'size': len(payload)}
                sys.stdout.write(".")
                # print("Iteration %d) SUCCESS: %s" % (iterCnt, resp))
            except Exception as e:
                # print("Iteration %d) ERROR: Failed to parse initial batch response" % iterCnt)
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

        if self.use_encryption:
            encode_out = self.encode_data(gzip_out)
        else:
            encode_out = bytes(gzip_out.getvalue())

        return encode_out

    def encode_data(self, data):
        # set password
        salt = b"staticsalt"
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000,
                         backend=default_backend())
        key = base64.urlsafe_b64encode(kdf.derive(bytes(self.apikey, 'utf-8')))
        cipher_suite = Fernet(key)
        encoded_text = cipher_suite.encrypt(bytes(data.getvalue()))

        return encoded_text

    @staticmethod
    def gzip_data(data):
        out = BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(data)
        return out
