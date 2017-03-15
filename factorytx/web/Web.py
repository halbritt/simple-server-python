import logging
import multiprocessing

from flask import Flask, jsonify, url_for

from factorytx.managers.PluginManager import component_manager

log = logging.getLogger(__name__)
plugin_manager = component_manager()['dataplugins']
app = Flask(__name__)


# Helper Functions
def _has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


# Views
@app.route('/')
def main():
    links = []
    for rule in app.url_map.iter_rules():
        # Filter out rules we can't navigate to in a browser
        # and rules that require parameters
        if "GET" in rule.methods and _has_no_empty_params(rule):
            url = url_for(rule.endpoint)
            # links.append((url, rule.endpoint))
            links.append(url)
    # links is now a list of url, endpoint tuples
    return jsonify(sitemap=links)


# DEPRECATED - TO SUPPORT OLDER VERSIONS OF SIMPLEADMIN
def populate_plugin_list():
    return plugin_manager.plugin_schemas


@app.route('/api/plugins/')
def plugins():
    return jsonify({"plugins": plugin_manager.plugin_schemas})


class Web(multiprocessing.Process):
    '''
    This is used to run as the web instance for
    the REST API endpoints for factorytx
    '''

    def __init__(self, bindall=False):
        super(Web, self).__init__()
        self.bindall = bindall

    def run(self):
        if self.bindall:
            app.run(host='0.0.0.0', port=10101)
        else:
            app.run(port=10101)
