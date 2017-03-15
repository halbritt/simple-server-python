import logging
import yaml
import os
from jsonschema import validate
from FactoryTx.managers.GlobalManager import global_manager
from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.Global import setup_log
from FactoryTx import utils

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
        if len(plugin_dict) > 0:
            version = plugin_dict['version']
            plgn_type = plugin_dict['type']
            template_schema = self.get_schema('dataplugins', plgn_type, version)
            if self.validate_schema(plugin_dict, template_schema):
                self.dataplugins.append(plugin_dict)
                # TODO: Add validations for the parsers and datasources in this dataplugin.
                return self.dataplugins

    def add_transform(self, transform_dict):
        if len(transform_dict) > 0:
            print ("TRANSFORM DICT", transform_dict)
            version = transform_dict['config']['version']
            plgn_type = transform_dict['type']
            template_schema = self.get_schema('transforms', plgn_type, version)
            if self.validate_schema(transform_dict, template_schema):
                self.transforms.append(transform_dict)
                return self.transforms

    def add_tx(self, tx_dict):
        if len(tx_dict) > 0:
            version = tx_dict['version']
            plgn_type = tx_dict['type']
            template_schema = self.get_schema('tx', plgn_type, version)
            if self.validate_schema(tx_dict, template_schema):
                self.tx.append(tx_dict)
                return self.tx

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
            return ("Cannot find schema")
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
        # config_file = yaml.dump(config_dict, yaml_file)
        directory = '/opt/sightmachine/factorytx/factorytx/pipelines/conf.d'
        out_file = os.path.join(directory, "test.cfg")
        print (out_file)
        if not os.path.exists(out_file):
            os.makedirs(out_file)
        object_file = open(out_file, 'w')
        object_file.write(config_file)
        object_file.close()

    @staticmethod
    def create_config_file():
        """ Just initialize and return Pipeline object """
        pipeline  = DataPipeline()
        return pipeline
