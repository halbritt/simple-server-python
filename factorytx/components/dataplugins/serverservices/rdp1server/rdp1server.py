import cherrypy
from datetime import datetime
import os
import time
import json


class RDP1Server:

    logname = "RDP1"

    def __init__(self, data_store: str) -> ():
        if not os.path.exists(data_store):
            print("The datastore is", data_store)
            os.makedirs(data_store)
        self.data_store = data_store

    @cherrypy.expose
    def index() -> str:
        return "RDP1Server"

    @cherrypy.expose
    def upload(self, metadata=None, ipcfile=None):
        """ This method uploads an RDP1 packet of sslogs possibly including an attachment,
            persists the data on the filesystem, and then returns a response dictionary with
            metadata regarding the upload process

        """
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
                attachment_path = os.path.join(self.data_store, file_name + 'binaryattachment')
                with open(attachment_path, 'wb') as out:
                    while True:
                        data = ipcfile.file.read(8192)
                        if not data:
                            break
                        out.write(data)
                        size += len(data)
                    print("Finished uploading the binary attachment of size %s", size)
                orig_size = os.path.getsize(attachment_path)
                if not sslog:
                    body = [{}]
                else:
                    capture_time = datetime.utcnow().isoformat()
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
            capture_time = datetime.utcnow().isoformat(),
            print("The len/body of the payload is", len(body))
            last_id = body[[x for x in body][-1]]
            valid_count = len(body)
        try:
            if len(body) == 0:
                raise Exception("There is no payload length to this body")
            with open(os.path.join(self.data_store, file_name + 'headers'), 'w') as f:
                print("Persisting the headers")
                headers = cherrypy.request.headers
                headers['capture_time'] = capture_time
                if orig_filename:
                    headers['original_filename'] = orig_filename
                    headers['original_content_type'] = orig_content
                    headers['original_size'] = orig_size
                headers = json.dumps(cherrypy.request.headers)
                f.write(headers)
            with open(os.path.join(self.data_store, file_name), 'wb') as f:
                print("Persisting the file %s", file_name)
                f.write(rawbody)
            response_dic = {"valid_count": valid_count, "last_reject_id": None, "reject_count": 0,
                            "last_valid_id": last_id, "timestamp": datetime.utcnow().isoformat(),
                            "valid": True, "reject_errors": [], "id": last_id}
            cherrypy.response.status = 201
            cherrypy.response.body = response_dic
        except Exception as e:
            print("The error is %s", e)
            cherrypy.response.status = 500

    def stop_server(self):
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
        time_now = datetime.utcnow().isoformat()
        length = headers['Content-Length']
        key_name = headers['X-Sm-Api-Key']
        final_name = '--'.join([str(time_now), length, key_name[:min(10, len(key_name))], '.' + self.logname])
        return final_name
