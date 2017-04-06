from abc import ABCMeta, abstractmethod
import cherrypy

class BaseRDPServer(metaclass=ABCMeta):

    @abstractmethod
    def upload(payload):
        pass
    upload.exposed = True

    @staticmethod
    def start_server(host, port):
        cherrypy.config.update({'server.socket_port': port, 'server.socket_host': host})
        cherrypy.quickstart(BaseRDPServer)
