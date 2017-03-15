from FactoryTx.managers.GlobalManager import global_manager
from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.Global import setup_log
from FactoryTx import utils
import threading
import pytest
import logging
import yaml
from jsonschema import validate
from FactoryTx.pipelines.Pipeline import DataPipeline
from pytest import fixture

global_manager = global_manager()
try:
    import ujson as json
except:
    import json

log = logging.getLogger(__name__)

if global_manager.get_encryption():
    from cryptography.fernet import Fernet

components = component_manager()

@fixture(scope='module')
def get_schema_candidates():
    return [('dataplugins', 'file'), ('dataplugins', 'pollipc'), ('dataplugins', 'sql'),
            ('transports', 'ftp'), ('transports', 'localfile'), ('transports', 'ftp'),
            ('parsers', 'spreadsheetparser'), ('parsers', 'knifedataparser'), ('parsers', 'testparser')]

@fixture(scope='module')
def bad_schema_candidates():
    return[('dataplugins', 'bad'), ('dataplugins', 'extrabad'), ('dataplugins', 'superbad'),
            ('transports', 'bad'), ('transports', 'extrabad'), ('transports', 'superbad')]

def test_get_schema(get_schema_candidates):
    for candidate in get_schema_candidates:
       assert(DataPipeline.get_schema(candidate[0], candidate[1]))

def test_bad_schema(bad_schema_candidates):
    for candidate in bad_schema_candidates:
        assert(DataPipeline.get_schema(candidate[0], candidate[1]) == "Cannot find schema")

def test_full_plugin():
    pipeline = DataPipeline.create_config_file()
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client.cfg', 'r') as f:
        config = yaml.load(f)['pipeline'][0]
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client.cfg', 'r') as f:
        config1 = yaml.load(f)['pipeline'][1]
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client.cfg', 'r') as f:
        config2 = yaml.load(f)['pipeline'][2]

    plugins = config['dataplugins']
    for plugin in plugins:
        print("Adding the plugin", plugin)
        pipeline.add_data_plugin(plugin)

    transforms = config1['transforms']
    for transform in transforms:
        print("Adding the transform", transform)
        pipeline.add_transform(transform)

    txs = config2['tx']
    for tx in txs:
        print("Adding the tx", tx)
        pipeline.add_tx(tx)

    assert 0

"""def test_good_plugin():
    plugin_dict = {'type': 'file','version': '1.0.0'}
    assert(DataPipeline().add_data_plugin(plugin_dict))

def test_bad_plugin():
    plugin_dict = {'type': 'bad', 'schema-things': 'things'}
    assert(DataPipeline().add_data_plugin(plugin_dict) == None)

def test_good_transform():
    transform_dict = {'type': 'sslogtransform', 'version': '1.0.0','actions': []}
    assert(DataPipeline().add_transform(transform_dict))

def test_bad_transform():
    transform_dict = {'type' : 'bad', 'schema-things': 'things'}
    assert(DataPipeline().add_transform(transform_dict) == None)

def test_good_tx():
    tx_dict = {'type': 'localtx', 'version': '1.0.0'}
    assert(DataPipeline().add_tx(tx_dict))

def test_bad_tx():
    tx_dict = {'type': 'bad', 'schema-things': 'things'}
    assert(DataPipeline().add_tx(tx_dict) == None)


def test_create_config():
    # add to plugin, transport, tx
    pipeline = DataPipeline()
    plugin_dict = {'type': 'file', 'version': '1.0.0'}
    pipeline.add_data_plugin(plugin_dict)
    pipeline.create_config_file()
    assert 0

def test_write_config():
    # create config file
    # write
    # check in location that the config file exists

    pipeline = DataPipeline()
    pipeline.write_config(config_file)"""


""" create config and writing config testing need to happen here """
