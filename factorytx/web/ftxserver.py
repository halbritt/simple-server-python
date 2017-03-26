import cherrypy

class FTXServer(object):

    @cherrypy.expose
    def index(self):
        return "Welcome to the FTX Server"

    @cherrypy.expose
    def upload(self):
        return """<html>
          <head></head>
          <body>
            <form method="get" action="generate">
              <input type="text" value="8" name="length" />
              <button type="submit">Give it now!</button>
            </form>
          </body>
        </html>"""

if __name__ == '__main__':
    cherrypy.quickstart(FTXServer())
