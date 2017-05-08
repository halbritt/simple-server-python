import cherrypy
from datetime import datetime
import os
import time
import json


class RDP1Server:

    logname = "RDP1"

    def __init__(self, data_store):
        if not os.path.exists(data_store):
            print("The datastore is", data_store)
            os.makedirs(data_store)
        self.data_store = data_store

    @cherrypy.expose
    def index():
        return "RDP1Server"

    @cherrypy.expose
    def upload(self, metadata=None, ipcfile=None):
        print("Uploading an RDP1 payload.")
        key_name = cherrypy.request.headers["X-Sm-Api-Key"]
        if key_name not in self.apikeys:
            cherrypy.response.status = 400
            print("The request doesn't have a proper API key configured")
            return
        file_name = self.generate_name(cherrypy.request.headers)
        orig_filename = None
        orig_content = None
        if ipcfile:
            print("Uploading binary attachment")
            try:
                ipchead = ipcfile.headers
                files = ipcfile
                sslog = metadata.file.read()
                lsslog = json.loads(sslog)
                last_id = lsslog['_id']
                orig_filename = lsslog['original_filename']
                orig_content = metadata.headers['Content-Type']
                valid_count = 1
                size = 0
                with open(os.path.join(self.data_store, file_name + 'binaryattachment'), 'wb') as out:
                    while True:
                        data = ipcfile.file.read(8192)
                        if not data:
                            break
                        out.write(data)
                        size += len(data)
                    print("Finished uploading the binary attachment of size %s", size)
                if not sslog:
                    body = [{}]
                else:
                    body = [sslog]
                rawbody = sslog
            except Exception as e:
                print("There doesn't look to be any file attachments here %s.", e)
                files = []
        else:
            print("Loading a json upload")
            cl = cherrypy.request.headers['Content-Length']
            rawbody = cherrypy.request.body.read(int(cl))
            body = json.loads(rawbody)
            print("The len/body of the payload is", len(body), body[[x for x in body][-1]])
            last_id = body[[x for x in body][-1]]
            valid_count = len(body)
        try:
            if len(body) == 0:
                raise Exception("There is no payload length to this body")
            with open(os.path.join(self.data_store, file_name), 'wb') as f:
                print("Persisting the file %s", file_name)
                f.write(rawbody)
            with open(os.path.join(self.data_store, file_name + 'headers'), 'w') as f:
                print("Persisting the headers")
                headers = cherrypy.request.headers
                if orig_filename:
                    headers['original_filename'] = orig_filename
                    headers['original_content_type'] = orig_content
                headers = json.dumps(cherrypy.request.headers)
                f.write(headers)
            response_dic = {"valid_count": valid_count, "last_reject_id": None, "reject_count": 0,
                            "last_valid_id": last_id, "timestamp": datetime.utcnow().isoformat(),
                            "valid": True, "reject_errors": [], "id": last_id}
            cherrypy.response.status = 201
            cherrypy.response.body = response_dic
        except Exception as e:
            print("The error is %s", e)
            cherrypy.response.status = 500

    def stop_server():
        cherrypy.engine.exit()

    def start_server(host, port, data_store, apikeys):
        server = RDP1Server(data_store)
        server.apikeys = apikeys
        dispatch = cherrypy.dispatch.RoutesDispatcher()
        dispatch.connect('sslog_upload', '/rlog/sslogger', controller=server, action='upload')
        cherrypy.config.update({'server.socket_port': port, 'server.socket_host': host})
        cherrypy.tree.mount(server, "/", {'/': {'request.dispatch': dispatch}})
        cherrypy.engine.start()
        return server

    def generate_name(self, headers):
        time_now = time.time()
        length = headers['Content-Length']
        key_name = headers['X-Sm-Api-Key']
        final_name = ':'.join([str(time_now), length, key_name[:min(10, len(key_name))], '.' + self.logname])
        return final_name
