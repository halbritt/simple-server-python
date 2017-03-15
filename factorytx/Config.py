import os
import glob
import logging
import multiprocessing

import yaml
from jsonschema import validate
from boto.s3.connection import S3Connection

import factorytx
from factorytx.managers.PluginManager import component_manager

log = logging.getLogger(__name__)
components = component_manager()
parser_manager = components['parsers']


class ConfigError(Exception):
    pass


class Config(dict):
    """ This object holds the current configuration of this instance of factorytx.

    """
    def __init__(self):
        # () -> Config
        """ Returns a new config object.

        """
        self.plugin_conf_files = []
        self.plugin_confs = {}
        self.plugin_conf_list = []

    def load(self, conf_file=None, directory=None):
        # (str, str) -> bool
        """ Loads some collection of file(s).

        :param str conf_file: The configuration file to load
        :param str directory: The directory to load.
        """
        if conf_file:
            return self.load_file(conf_file)
        else:
            return self.load_directory(directory)

    def load_directory(self, directory):
        # (str) -> bool
        """ Loads the specified directory

        :param str directory: The directory to load.
        """
        if os.name == 'nt':
            default_directory = os.getcwd()
        else:
            default_directory = '/etc/sightmachine/factorytx/'

        file_name = 'factorytx.conf'

        if directory:
            # from provided directory
            filepath = os.path.join(os.path.abspath(directory), file_name)
        else:
            # from default path
            filepath = os.path.join(os.path.abspath(default_directory),
                                    file_name)
            if not os.path.exists(filepath):
                # load configuration file from program directory
                filepath = os.path.join(factorytx.MODULE_DIR, file_name)

        if not os.path.exists(filepath):
            log.error('no configuration file found at {}'.format(filepath))
            return False

        return self.load_file(filepath)

    def load_file(self, conf_file):
        # (str) -> bool
        """ Loads the specified file

        :param str conf_file: The configuration file that is needed to be loaded.
        """
        log.info('loading config file {}'.format(conf_file))
        try:
            with open(conf_file, 'r') as fp:
                c = yaml.load(fp)
                self.clear()
                self.update(c)
        except Exception as e:
            log.error('error loading configuration file: {}'.format(e))
            return False

        databuffer = self.get('plugins', {}).get('data')
        if not databuffer:
            log.error('no data buffer specified in the configuration file')
            return False

        # make sure the plugin directory specified is valid
        confd = self.get('plugins', {}).get('conf.d')
        if not confd:
            log.error('plugins conf.d not specified in the configuration file')
            return False

        # load plugin confs. conf.d can itself be a glob, ex. sd/tenants/*/
        confs_glob = os.path.abspath(confd) + '/*.cfg'
        self.plugin_conf_files = glob.glob(confs_glob)
        if len(self.plugin_conf_files) == 0:
            log.error('There are no config files in {}'.format(confd))
            return False

        log.info('Found {} plugin config file(s): '
                 '{}'.format(len(self.plugin_conf_files),
                             self.plugin_conf_files))

        self.plugin_confs.clear()
        for fname in self.plugin_conf_files:
            try:
                with open(fname, 'r') as f:
                    self.plugin_confs[fname] = yaml.load(f)
            except Exception as e:
                log.error("Can't parse yaml config: \"%s\": %s", fname, e)
                return False

        log.info('finished loading configuration')
        return True

    def validate_configs(self):
        # () -> bool
        """ Validates that my configs follow the right paradigms.

        :rtype: bool
        :returns: True if my config is valid and False otherwise
        """
        watcher_type = 'remotedatapost'

        cfg_errors_count = 0
        for cfg_file, cfg in self.plugin_confs.items():
            # TODO: Move all of this logic into the plugins and just call
            #       loadParameters() here instead of duplicating it all inline.
            pipeline_cfg = cfg.get('pipeline', [])
            for category in pipeline_cfg:
                next_cat = [keys for keys in category.keys()]
                no_parsers = False
                for dataplugin in category:
                    manager = components[dataplugin]
                    available_components = manager.plugins.keys()
                    for plugin in category[dataplugin]:
                        if 'type' not in plugin:
                            parsers = data_cfg.get('parsers')
                            log.error('Plugin without a type in file %s', cfg_file)
                            cfg_errors_count += 1
                            continue
                        if plugin['type'] not in available_components:
                            log.error('Unknown plugin type "{}" in file {}'.format(
                                      plugin['type'], cfg_file))
                            cfg_errors_count += 1
                            continue

                        plgn_cfg = plugin['config']
                        plgn_type = plugin['type']
                        if 'version' in plugin:
                            plgn_ver = plugin['version']
                        else:
                            plgn_ver = plugin['config']['version']
                        plgn_cfg['source'] = 'unknown'  # add source to config
                        plgn_cfg['version'] = plgn_ver
                        plgn_cfg['type'] = plgn_type
                        plgn_schema = manager.get_plugin_schema(plgn_type, plgn_ver)

                        if not plgn_schema:
                            log.error('Cant find schema for plugin "{}" '
                                      'with version {}'.format(plgn_type, plgn_ver))
                            cfg_errors_count += 1
                            continue

                        datasources = plgn_cfg.get('datasources')
                        # Try to validate datasources (polling services)
                        for datasource in datasources:
                            data_cfg = datasource.get('config')
                            if not data_cfg: data_cfg = {}
                            if 'type' in datasource:
                                data_cfg.update({'name':datasource['name'], 'type':datasource['type'], 'plugin_type':plgn_type})
                            else:
                                data_cfg.update({'name':datasource['name'], 'type':plgn_type, 'plugin_type':plgn_type})
                            #remove = []
                            #for dta in datasources:
                            #    if dta['name'] != datasource['name']:
                            #        remove.append(dta)
                            #datasources = [x for x in datasources if x not in remove]
                            #data_cfg['datasources'] = datasources
                        parsers = plgn_cfg.get('parsers')
                        # Try to validate the parsers for this plugin
                        if parsers and isinstance(parsers, list):
                            for fp in parsers:
                                fp_name = fp['type']
                                fp_cfg = fp['config']
                                fp_ver = fp_cfg['version']
                                fp_cfg.update({'name':fp_name, 'version':fp_ver, 'plugin_type':plgn_type})
                                fp_schema = parser_manager.get_plugin_schema(fp_name, fp_ver)
                                if not fp_schema:
                                    log.error('Cant find schema for parser "{}" with version {}'.format(fp_name, fp_ver))
                                    cfg_errors_count += 1
                                    continue
                                try:
                                    validate(fp_cfg, fp_schema)
                                except Exception as e:
                                    log.error('Error in config for parser "{}" in file: {}'.format(fp_name, fp_ver))
                                    log.error(str(e))
                                    cfg_errors_count += 1
                                    continue
                        elif not parsers:
                           no_parsers = True
                           continue

                        try:
                            validate(plgn_cfg, plgn_schema)
                        except Exception as e:
                            log.error('Error in config for plugin "{}" in file: {}'.format(plgn_type, cfg_file))
                            log.error(str(e))
                            cfg_errors_count += 1
                            continue
                        log.info("Parsers, so appending %s.", next_cat[0])
                        self.plugin_conf_list.append((next_cat[0], plgn_type, plgn_cfg))
                    if no_parsers:
                        for manager in category:
                            log.info("Loading %s", manager)
                            for entry in category[manager]:
                                log.info("Loading the %s with type %s.", entry['name'], entry['type'])
                            self.plugin_conf_list = [(next_cat[0], manager, category)] + self.plugin_conf_list

            for watcher_cfg in cfg.get('watchers', []):
                wtchr_ver = watcher_cfg['version']
                wtchr_schema = plugin_manager.get_plugin_schema(watcher_type,
                                                                wtchr_ver)

                if not wtchr_schema:
                    log.error('Cant find schema for plugin "{}" '
                              'with version {}'.format(watcher_type,
                                                       wtchr_ver))
                    cfg_errors_count += 1
                    continue

                try:
                    validate(watcher_cfg, wtchr_schema)
                except Exception as e:
                    log.error('Error in config for watcher in file: '
                              '{}'.format(cfg_file))
                    log.error(str(e))
                    cfg_errors_count += 1
                    continue

                self.plugin_conf_list.append((watcher_type, watcher_cfg))

        if cfg_errors_count:
            log.error('Found {} error(s) in config files'
                      ''.format(cfg_errors_count))
            return False
        else:
            log.info('No errors found, with a total of %s plugins found', len(self.plugin_conf_list))
            return True

    @staticmethod
    def _fetch_config(aws_access_key=None, aws_secret_access_key=None,
                      aws_bucket=None, aws_path_prefix=None, filename=None):
        # (str, str, str, str, str) -> str
        """ This method is responsible for getting all the configs.

        :param str aws_access_key: The AWS access key string
        :param str aws_secret_access_key: The secret access key.
        :param str aws_bucket: The bucket to use.
        :param str aws_path_prefix:
        :param str filename:
        """
        if (not aws_access_key
                or not aws_secret_access_key
                or not aws_bucket
                or not aws_path_prefix):
            raise ConfigError("not enough aws configuration "
                              "specified to update")

        try:
            conn = S3Connection(aws_access_key, aws_secret_access_key)
            bucket = conn.get_bucket(aws_bucket, validate=False)
            file_path = "{}/{}".format(aws_path_prefix, filename)
            key = bucket.get_key(file_path)
            file_contents = key.get_contents_as_string()
        except Exception as e:
            log.error("Unexpected error while downloading file: "
                      "{}".format(filename))
            raise e

        return file_contents

    @staticmethod
    def _save_config(directory, file_contents, filename):
        # (str, str, str) -> None
        """ Saves the config represented by the arguments.

        :param str directory: The directory to save to
        :param str file_contents: The contents to write
        :param str filename: The name of the saved contents
        """
        try:
            with open(os.path.join(directory, filename), "w") as fp:
                fp.write(file_contents)
        except:
            raise ConfigError("error writing contents to file "
                              "{}".format(os.path.join(directory, filename)))

    def update_config(self, files):
        # (list) -> None
        """ Load in new configurations according to changes to my environment

        :param list files: The files to get and update the config for
        """
        aws_access_key = self.get('s3', {}).get('aws_access_key')
        aws_secret_access_key = self.get('s3', {}).get('aws_secret_access_key')
        aws_bucket = self.get('s3', {}).get('aws_bucket')
        aws_path_prefix = self.get('s3', {}).get('aws_path_prefix')
        directory = self.get('plugins').get('conf.d')

        for filename in files:
            file_contents = self._fetch_config(
                aws_access_key=aws_access_key,
                aws_secret_access_key=aws_secret_access_key,
                aws_bucket=aws_bucket,
                aws_path_prefix=aws_path_prefix,
                filename=filename)
            self._save_config(directory=directory,
                              file_contents=file_contents,
                              filename=filename)

    def get_nprocs(self):
        """ Returns the number of processes that I am currently in charge of

        :rtype: int
        :returns: The number of processes
        """
        nprocs = 1  # start with 1 - main process
        for (plgn_type, plgn_cfg) in self.plugin_conf_list:
            PluginCls = plugin_manager.get_plugin(plgn_type)
            if issubclass(PluginCls, multiprocessing.Process):
                nprocs += 1
        return nprocs

config = Config()
def get_config():
    return config
