""" This contains the PluginManager and associated manager code.

"""
import os
import logging
import glob
import pkg_resources

import yaml
import semantic_version

import factorytx

LOG = logging.getLogger(__name__)


class PluginManager(object):
    """ This manager manages the plugins in this session of factorytx.

    """

    def __init__(self, entry_points, search_directories):
        """ Returns a new PluginManager

        :param str entry_points: CSV string of entrypoints
        :param str search_directories: CSV string of directories
        """
        self.plugins = {}
        self.plugin_schemas = {}
        pts = entry_points.split(",")
        if len(pts) == 1:
            pkg = pkg_resources.iter_entry_points(pts[0])
            self.plugins.update({p.name: p for p in pkg})
            self.search_directories = search_directories
            LOG.info('Loaded %s plugins: %s', len(self.plugins), self.plugins.keys())
        else:
            for entry_point in pts:
                pkg = pkg_resources.iter_entry_points(entry_point)
                new_dict = {p.name: p for p in pkg}
                self.plugins.update(new_dict)
                LOG.info('Loaded %s plugins: %s', len(self.plugins), self.plugins.keys())
            self.search_directories = search_directories

    def get_plugins_path(self):
        """ Returns the path of the plugins

        """
        return self.search_directories

    def plugins_directory_dict(self):
        # () -> dict
        """ Return the dictionary of the plugin directories.

        """
        plugin_paths = self.get_plugins_path()
        plg_dirs = dict()
        for plg_path in plugin_paths:
            plg_path = plg_path[0]
            for directory in os.listdir(plg_path):
                if os.path.isdir(os.path.join(plg_path, directory)):
                    plg_dirs[directory] = os.path.join(plg_path, directory)
        return plg_dirs

    def load_schemas(self):
        """ Loads the plugin schemas that are currently available.

        """
        self.plugin_schemas = {}
        for plg, directory in self.plugins_directory_dict().items():
            schemas_path = os.path.abspath(os.path.join(directory, 'schemas'))
            gdir = os.path.join(schemas_path, '*.schema')
            schema_files = glob.glob(gdir)
            if len(schema_files) > 0:
                name = plg
                schemas = []
                for schema_file in schema_files:
                    with open(schema_file) as schema_fle:
                        schemas.append(yaml.load(schema_fle))

                self.plugin_schemas[name] = schemas

        # Sort by semantic version desc
        for name, schemas in self.plugin_schemas.items():
            for schema in schemas:
                schema['$schema-version'] = semantic_version.Version(
                    schema['$schema-version'])

            self.plugin_schemas[name] = sorted(
                schemas, key=lambda x: x['$schema-version'], reverse=True)
            for schema in schemas:
                schema['$schema-version'] = str(schema['$schema-version'])

    def get_plugin_schema(self, name, version):
        # (str, str) -> dict?
        """ Returns the plugin schema associated with a certain name and version, and the latest
            version if no version is specified

        :param str name: The name of the plugin.
        :param str version: The version of the plugin.
        """
        if len(self.plugin_schemas) == 0:
            self.load_schemas()
        latest_schema = None
        latest_version = 0
        for schema in self.plugin_schemas.get(name, []):
            v_str = "".join(schema['$schema-version'].split('.'))
            v_num = .01 * int(v_str)
            new_v = max(latest_version, v_num)
            if not latest_version == new_v:
                latest_version = new_v
                latest_schema = schema
            if schema['$schema-version'] == version:
                return schema
        return latest_schema

    def get_plugin(self, name):
        # (str) -> Plugin
        """ Return the specified loaded plugin.

        :param str name: The name of the plugin desired.
        :returns: The loaded plugin
        """
        plugin = self.plugins.get(name)
        if plugin:
            return plugin.load()

DATA_DIRECTORIES = [ 'factorytx.components.dataplugins,factorytx.reservecomponents.dataplugins',
                    'factorytx.components.transforms,factorytx.reservecomponents.factorytx',
                    'factorytx.components.filters,factorytx.reservecomponents.filters',
                    'factorytx.components.tx,factorytx.reservecomponents.tx']
SUBDATA_DIRECTORIES = ['factorytx.components.dataplugins.parsers,factorytx.reservecomponents.dataplugins.parsers',
                       'factorytx.components.dataplugins.transports,factorytx.reservecomponents.dataplugins.transports']

COMPONENT_MANAGER = { p.split(',')[0][21:]: PluginManager(p, [getattr(factorytx, 'R_' + p.split(',')[0][21:].upper() + '_DIRS'), getattr(factorytx, p.split(',')[0][21:].upper() + '_DIRS')]) for p in DATA_DIRECTORIES }
for entry in SUBDATA_DIRECTORIES:
    COMPONENT_MANAGER[entry.split(',')[0][33:]] = PluginManager(entry, [getattr(factorytx, 'R_' + entry.split(',')[0][33:].upper() + '_DIRS'), getattr(factorytx, entry.split(',')[0][33:].upper() + '_DIRS')])


def component_manager():
    """ Returns all of the components I manage.

    """
    return COMPONENT_MANAGER
