import cherrypy
import os
import time
import json

class RDP1Server:

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
        cl = cherrypy.request.headers['Content-Length']
        file_name = self.generate_name(cherrypy.request.headers)
        rawbody = cherrypy.request.body.read(int(cl))
        body = json.loads(rawbody)
        with open(os.path.join(self.data_store, file_name), 'wb') as f:
            print("Persisting the file %s", file_name)
            f.write(rawbody)
        with open(os.path.join(self.data_store, file_name + 'headers'), 'w') as f:
            print("Persisting the headers")
            headers = json.dumps(cherrypy.request.headers)
            f.write(headers)

    def start_server(host, port, data_store):
        cherrypy.config.update({'server.socket_port': port, 'server.socket_host': host})
        server = RDP1Server(data_store)
        cherrypy.tree.mount(server, "")
        cherrypy.engine.start()
        return server

    def generate_name(self, headers):
        time_now = time.time()
        length = headers['Content-Length']
        key_name = headers['X-Sm-Api-Key']
        final_name = ':'.join([str(time_now), length, key_name[:min(10, len(key_name))]])
        return final_name
