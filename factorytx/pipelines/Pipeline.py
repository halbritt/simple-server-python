import logging
import yaml
import os
from jsonschema import validate
from factorytx.managers.PluginManager import component_manager
from factorytx import utils

try:
    import ujson as json
except:
    import json

log = logging.getLogger(__name__)

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
            print("TRANSFORM DICT", transform_dict)
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
            print("Error validating schema: ", e)
            return False

    @staticmethod
    def write_config(config_dict, output_directory):
        """ return a confirmation dictionary with some nice values, like write time, size, name, etc.
            some helpful error message if there is a problem
        """
        conformation_dict = {}
        isfile = False
        try:
            # Currently if the file doesn't exist you get the error, so I'm adding the new object
            # write
            if os.path.isfile(output_directory):
                isfile = True
                object_file = open(output_directory, "w")
                # Lines can be 120 characters long ................................................>|
                object_file.write(yaml.dump(config_dict, indent=2, default_flow_style=False))
                object_file.close()
            else:
                if os.path.exists(output_directory):
                    print("Error: The output path %s doesn't appear to be a file.", output_directory)
                else:
                    log.info("Writing a new object_file to", output_directory)
                    with open(output_directory, "w") as object_file:
                        object_file.write(yaml.dump(config_dict, indent=2, default_flow_style=False))
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
        This function simply creates a final pipeline dictionary that 
        will be dumped to a yaml file in the write_config() function.

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
