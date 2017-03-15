import os
import unittest

from jsonschema import validate, ValidationError
import yaml

from FactoryTx.managers.PluginManager import component_manager

plugins = component_manager()['dataplugins']

class ExampleConfigTestCase(unittest.TestCase):

    def test_example_configs(self):
        pm = plugins
        pm.load_schemas()
        self.assertTrue(len(pm.plugin_schemas) != 0)
        plugin_dirs = pm.plugins_directory_dict()

        for plugin_name, schemas in pm.plugin_schemas.iteritems():
            plugin_path = plugin_dirs[plugin_name]

            def get_schema_for_version(version):
                for schema in schemas:
                    if schema['$schema-version'] == version:
                        return schema
                raise Exception('Cant find schema for plugin "{}" '
                                'with version {}'.format(plugin_name, version))

            # get .cfg files
            cfgs = (cfg for cfg in os.listdir(plugin_path)
                    if cfg.endswith('.cfg'))
            for cfg_name in cfgs:
                cfg_path = os.path.join(plugin_path, cfg_name)
                with open(cfg_path, 'r') as f:
                    cfg = yaml.load(f.read())

                example_configs = (p['config'] for machine in cfg['machines']
                                   for p in machine['plugins']
                                   if p['type'] == plugin_name)
                for ex_conf in example_configs:
                    try:
                        validate(ex_conf,
                                 get_schema_for_version(ex_conf['version']))
                    except ValidationError as e:
                        print('Error in example config for plugin "{}" '
                              'in file: {}'.format(plugin_name, cfg_path))
                        print(e)
                        self.fail(str(e))
                    except Exception as e:
                        print('Error while validating example config for '
                              'plugin "{}" in file: {}'.format(plugin_name,
                                                               cfg_path))
                        print(e)
                        self.fail(str(e))


if __name__ == '__main__':
    unittest.main()
