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
    def upload(self):
        print("Uploading the payload %s", cherrypy.request.headers)
        key_name = cherrypy.request.headers["X-Sm-Api-Key"]
        print("The api keys I know are %s", self.apikeys)
        if key_name not in self.apikeys:
            cherrypy.response.status = 400
            print("The request doesn't have a proper API key configured")
            return
        cl = cherrypy.request.headers['Content-Length']
        file_name = self.generate_name(cherrypy.request.headers)
        rawbody = cherrypy.request.body.read(int(cl))
        body = json.loads(rawbody)
        print("The len/body of the payload is", len(body), body[[x for x in body][-1]])
        try:
            if len(body) == 0:
                raise Exception("There is no payload length to this body")
            with open(os.path.join(self.data_store, file_name), 'wb') as f:
                print("Persisting the file %s", file_name)
                f.write(rawbody)
            with open(os.path.join(self.data_store, file_name + 'headers'), 'w') as f:
                print("Persisting the headers")
                headers = json.dumps(cherrypy.request.headers)
                f.write(headers)
            last_id = body[[x for x in body][-1]]
            response_dic = {"valid_count": len(body), "last_reject_id": None, "reject_count": 0,
                            "last_valid_id": last_id, "timestamp": datetime.utcnow().isoformat(),
                            "valid": True, "reject_errors": [], "id": last_id}
            cherrypy.response.status = 201
            cherrypy.response.body = response_dic
        except Exception as e:
            print("The error is %s", e)
            cherrypy.response.status = 500



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
