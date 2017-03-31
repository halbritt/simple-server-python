import logging
import yaml
import os
import time
from jsonschema import validate
from factorytx.managers.GlobalManager import global_manager
from factorytx.managers.PluginManager import component_manager
from factorytx.Global import setup_log
from factorytx import utils

global_manager = global_manager()
try:
    import ujson as json
except:
    import json

log = logging.getLogger(__name__)

if global_manager.get_encryption():
    from cryptography.fernet import Fernet

components = component_manager()


class DataPipeline(dict):
    """
    Data pipeline is a framework for aggregating dataplugins and transforming then tx the
    results.

    """
    __version__ = "0.1"

    def __init__(self, name='unknown'):
        self.name = name
        self.dataplugins = []
        self.transforms = []
        self.tx = []

    def add_data_plugin(self, plugin_dict):
        """ 
        When adding data plugin, will also be adding transport blocks and parser blocks, need to check schemas, etc. 
        before adding to config file 
        """
        parser = False
        parser_good = False
        datasource = False
        datasource_good = False
        if len(plugin_dict) > 0:
            version = plugin_dict['version']
            plgn_type = plugin_dict['type']
            template_schema = self.get_schema('dataplugins', plgn_type, version)
            if self.validate_schema(plugin_dict, template_schema):
                if 'parsers' in plugin_dict['config'].keys():
                    parser = True
                    parsers = plugin_dict['config']['parsers']
                    plgn_type = parsers[0]['type']
                    version = parsers[0]['config']['version']
                    template_schema = self.get_schema('parsers', plgn_type, version)
                    if self.validate_schema(parsers, template_schema):
                        parser_good = True
                if 'datasources' in plugin_dict['config'].keys():
                    datasource = True
                    datasources = plugin_dict['config']['datasources']
                    plgn_type = datasources[0]['type']
                    version = datasources[0]['config']['version']
                    template_schema = self.get_schema('transports', plgn_type, version)
                    if self.validate_schema(datasources, template_schema):
                        datasource_good = True
                if ((datasource and datasource_good) and (parser and parser_good)) or ((not datasource) and (parser and parser_good)) or ((not parser) and (datasource and datasource_good)):
                    """
                        To ensure the order is correct in the config file, put every element of the dictionary by looping into the list 
                    """
                    for key, value in plugin_dict.items():
                        self.dataplugins.append([key, value])
                return self.dataplugins

    def add_transform(self, transform_dict):
        if len(transform_dict) > 0:
            version = transform_dict['config']['version']
            plgn_type = transform_dict['type']
            template_schema = self.get_schema('transforms', plgn_type, version)
            if self.validate_schema(transform_dict['config'], template_schema):
                """
                    To ensure the order is correct in the config file, put every element of the dictionary by looping into the list
                """
                for key, value in transform_dict.items():
                    self.transforms.append([key, value])
                return self.transforms

    def add_tx(self, tx_dict):
        if len(tx_dict) > 0:
            version = tx_dict['config']['version']
            plgn_type = tx_dict['type']
            template_schema = self.get_schema('tx', plgn_type, version)
            if self.validate_schema(tx_dict, template_schema):
                """ To ensure the order is correct in the config file, put every element of the dictionary by looping into the list
                """
                for key, value in tx_dict.items():
                    self.tx.append([key, value])
                return self.tx

    @classmethod
    def insert_datasource(cls, service, datasource):
        """ add datasource and then return service """
        if 'type' in datasource.keys():
            plgn_type = datasource['type']
            template_schema = cls.get_schema('transports', plgn_type)
            if clas.validate_schema(datasource, template_schema):    
            service.update({'datasources':datasource})
        return service

    @classmethod
    def insert_parser(cls, plugin, parser):
        """ add parser and then return plugin """
        if 'type' in parser.keys():
            plgn_type = parser['type']
            template_schema = cls.get_schema('parsers', plgn_type) 
            if cls.validate_schema(parser, template_schema):
                plugin.update({'parsers':parser})
        print('plugin', plugin)
        return plugin

    @classmethod
    def insert_pollingservice(cls, plugin, pollingservice):
        """ add pllingservice and then return plugin """
        plugin.update({'polling service':pollingservice})
        return plugin
    
    @staticmethod
    def create_dataplugin_template(name, version=None):
        """ return the template validate"""
        plgn_template = components['dataplugins'].get_plugin_schema(name, version)
        print("dataplugin template", plgn_template)
        return plgn_template
    
    @staticmethod 
    def create_transform_template(name, version=None):
        """ return the template validate """
        transform_template  = components['transforms'].get_plugin_schema(name, version)
        print("transform_template", transform_template)
        return transform_template

    @staticmethod
    def create_tx_template(name, version=None):
        """ return the template validate """
        tx_template  = components['tx'].get_plugin_schema(name, version)
        print("tx_template", tx_template)
        return tx_template

    @staticmethod
    def plugin_template(component_type, plugin_name):
        """ The (sub)component types are dataplugin, parser, transport, transform, tx, filters 
            N.B. tranport, parsers are subcomponents """
        plgn = components[component_type].get_plugin(plugin_name)
        return plgn

    @staticmethod
    def get_schema(component_type, plugin_name, version='1.0.0'):
        """ Get the schema for the relavent plugin name """
        manager = components[component_type]
        print("The manager is %s", manager, component_type, plugin_name)
        manager.load_schemas()
        schema = manager.get_plugin_schema(plugin_name, version)
        if not schema:
            return ("Cannot find schema for", component_type, plugin_name, version)
        return schema

    @staticmethod
    def validate_schema(new_schema, template_schema):
        """ Returns true exactly when the new schema conforms to the template schema. """
        try:
            print("Trying to validate %s, %s", new_schema, template_schema)
            validate(new_schema, template_schema)
            return True
        except Exception as e:
            print ("Error validating schema: ", e)
            return False

    @staticmethod
    def write_config(config_dict, output_directory):
        """ return a confirmation dictionary with some nice values, like write time, size, name, etc.
            some helpful error message if there is a problem
        """
        conformation_dict = {}
        isfile = False
        try:
            if os.path.isfile(output_directory):
                isfile = True
                object_file = open(output_directory, "w")
                object_file.write(yaml.dump(config_dict, indent=2, default_flow_style=False))
                object_file.close()
            else:
                print ("Error: File", output_directory, "does not appear to exist")
        except IOError as e:
            print ("Error: File", output_directory, "does not appear to exist")

        if isfile:
            conformation_dict['Time written'] = time.ctime(os.path.getmtime(output_directory))
            conformation_dict['File size in bytes'] = os.path.getsize(output_directory)
            conformation_dict['Name of config file'] = output_directory.split('/').pop()
            print(conformation_dict)
            return conformation_dict

    def create_config_dict(self):
        config_dict = {}
        config_dict['pipeline'] = []
        dataplugins = {}
        dataplugins['dataplugins'] = self.dataplugins
        transforms = {}
        transforms['transforms'] = self.transforms
        tx = {}
        tx['tx'] = self.tx
        if len(self.dataplugins) > 0:
            config_dict['pipeline'].append(dataplugins)
        if len(self.transforms) > 0:
            config_dict['pipeline'].append(transforms)
        if len(self.tx) > 0:
            config_dict['pipeline'].append(tx)
        if len(config_dict) == 0:
            return ("There is nothing in the configuration dictionary")
        else:
            return config_dict


    @staticmethod
    def create_config_file():
        """ Just initialize and return Pipeline object """
        pipeline  = DataPipeline()
        return pipeline

