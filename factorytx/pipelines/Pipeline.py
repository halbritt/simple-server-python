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
    Data pipeline is a framework for aggregating dataplugins and 
    transforming then tx the results.

    """
    __version__ = "0.1"

    def __init__(self, name='unknown'):
        self.name = name
        self.dataplugins = []
        self.transforms = []
        self.tx = []

    def add_data_plugin(self, plugin_dict: dict) -> list:
        """ 
        This function will add a data plugin to a class list of data 
        plugins that are ultimately added to the outputed configuration 
        file.  It validates the schema given by plugin_dict before 
        adding the plugin to the class list.  Also validates the parsers 
        and datasources schemas within the dictionary before adding. 
        Returns the dataplugins list.

        """
        parser = False
        parser_good = False
        datasource = False
        datasource_good = False
        if len(plugin_dict) > 0:
            version = plugin_dict['version']
            plgn_type = plugin_dict['type']
            template_schema = self.get_schema('dataplugins', plgn_type,\
                    version)
            if self.validate_schema(plugin_dict, template_schema):
                if 'parsers' in plugin_dict['config'].keys(): 
                    # check that parsers are actually in the config
                    parser = True
                    parsers = plugin_dict['config']['parsers']
                    plgn_type = parsers[0]['type']
                    version = parsers[0]['config']['version']
                    template_schema = self.get_schema('parsers', plgn_type,\
                            version)
                    if self.validate_schema(parsers, template_schema):
                        parser_good = True
                if 'datasources' in plugin_dict['config'].keys():
                    # check that datasources are actually in the config
                    datasource = True
                    datasources = plugin_dict['config']['datasources']
                    plgn_type = datasources[0]['type']
                    version = datasources[0]['config']['version']
                    template_schema = self.get_schema('transports', plgn_type,\
                            version)
                    if self.validate_schema(datasources, template_schema):
                        datasource_good = True
                if ((datasource and datasource_good) and \
                        (parser and parser_good)) or ((not datasource) and \
                        (parser and parser_good)) or ((not parser) and \
                        (datasource and datasource_good)):
                    self.dataplugins.append(plugin_dict)
                    return self.dataplugins

    def add_transform(self, transform_dict: dict) -> list:
        """ 
        This function will add a transform to a class list of transforms 
        that are ultimately addedto the outputed configuration file.  
        It validates the schema given by transform_dict before adding 
        the transform to the class list. Returns the transforms list.

        """
        if len(transform_dict) > 0:
            version = transform_dict['config']['version']
            plgn_type = transform_dict['type']
            template_schema = self.get_schema('transforms', plgn_type, version)
            if self.validate_schema(transform_dict['config'], template_schema):
                self.transforms.append(transform_dict)
                return self.transforms

    def add_tx(self, tx_dict: dict) -> list:
        """ 
        This function will add a tx to a class list of tx that are 
        ultimately added to the outputed configuration file.  It 
        validates the schema given by tx_dict before adding the tx to 
        the class list. Returns the tx list.

        """
        if len(tx_dict) > 0:
            version = tx_dict['config']['version']
            plgn_type = tx_dict['type']
            template_schema = self.get_schema('tx', plgn_type, version)
            if self.validate_schema(tx_dict, template_schema):
                self.tx.append(tx_dict)
                return self.tx

    @classmethod
    def insert_datasource(cls, service, datasource: dict):
        """ 
        This function inserts a datasource into a specified service.
        It returns the updated service.

        """
        if 'type' in datasource.keys(): 
            plgn_type = datasource['type']
            template_schema = cls.get_schema('transports', plgn_type)
            if cls.validate_schema(datasource, template_schema):    
                service.update({'datasources':datasource})
        return service

    @classmethod
    def insert_parser(cls, plugin, parser: dict):
        """ 
        This function inserts a parser into a specific plugin.
        It returns the updated plugin.
        
        """
        if 'type' in parser.keys():
            plgn_type = parser['type']
            template_schema = cls.get_schema('parsers', plgn_type) 
            if cls.validate_schema(parser, template_schema):
                plugin.update({'parsers':parser})
        print('plugin', plugin)
        return plugin

    @classmethod
    def insert_pollingservice(cls, plugin, pollingservice: dict):
        """
        This function inserts a pollingservice into a specific plugin.
        It returns the updated plugin.
        
        """
        plugin.update({'polling service':pollingservice})
        return plugin
    
    @staticmethod
    def create_dataplugin_template(name: str, version=None):
        """ 
        This function creates a dataplugin template and returns it.
        
        """
        plgn_template = components['dataplugins'].get_plugin_schema(name,\
                version)
        print("dataplugin template", plgn_template)
        return plgn_template
    
    @staticmethod 
    def create_transform_template(name: str, version=None):
        """
        This function creates a transform tempalte and returns it.
        
        """
        transform_template  = components['transforms'].get_plugin_schema(name,\
                version)
        print("transform_template", transform_template)
        return transform_template

    @staticmethod
    def create_tx_template(name: str, version=None):
        """
        This function creates a tx template and returns it.
        
        """
        tx_template  = components['tx'].get_plugin_schema(name, version)
        print("tx_template", tx_template)
        return tx_template

    @staticmethod
    def plugin_template(component_type, plugin_name):
        """ 
        Creates a plugin template, returns it.

        """
        plgn = components[component_type].get_plugin(plugin_name)
        return plgn

    @staticmethod
    def get_schema(component_type, plugin_name, version='1.0.0'):
        """ 
        Get the schema for the relavent plugin name. Returns the schema
        or error.

        """
        manager = components[component_type]
        print("The manager is %s", manager, component_type, plugin_name)
        manager.load_schemas()
        schema = manager.get_plugin_schema(plugin_name, version)
        if not schema:
            return ("Cannot find schema for", component_type, plugin_name,\
                    version)
        return schema

    @staticmethod
    def validate_schema(new_schema: dict, template_schema: dict) -> bool:
        """ 
        This function is used to validate the new_schema (inputed dict) 
        and the template_schema (retrieved schema, using the get_schema
        function). This is a helper function whenever a schema needs to 
        be validated.  Returns bool and error message if validation 
        fails.

        """
        try:
            print("Trying to validate %s, %s", new_schema, template_schema)
            validate(new_schema, template_schema)
            return True
        except Exception as e:
            print ("Error validating schema: ", e)
            return False

    @staticmethod
    def write_config(config_dict: dict, output_directory: str) -> dict:
        """ 
        This function dumps the created config_dict to specified yaml
        file location.  Returns a conformation dictionary that states 
        the time the file was written, file size and the name of the 
        file.  See client.cfg for example desired output. Returns error
        if the output directory does not exist.

        """
        conformation_dict = {}
        isfile = False
        try:
            if os.path.isfile(output_directory):
                isfile = True
                object_file = open(output_directory, "w")
                object_file.write(yaml.dump(config_dict, indent=2,\
                        default_flow_style=False))
                object_file.close()
            else:
                print ("Error: File", output_directory,\
                        "does not appear to exist")
        except IOError as e:
            print ("Error: File", output_directory,\
                    "does not appear to exist")

        if isfile:
            conformation_dict['Time written'] = \
                    time.ctime(os.path.getmtime(output_directory))
            conformation_dict['File size in bytes'] = \
                    os.path.getsize(output_directory)
            conformation_dict['Name of config file'] = \
                    output_directory.split('/').pop()
            print(conformation_dict)
            return conformation_dict

    def create_config_dict(self) -> dict:
        """ 
        This function creates a final pipeline dictionary that 
        will be dumped to a yaml file in the write_config() function.
        Returns a dictionary unless there are no elements in any of the
        lists, it returns an error message.
        
        """
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
