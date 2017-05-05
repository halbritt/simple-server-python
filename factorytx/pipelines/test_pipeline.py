from factorytx.managers.PluginManager import component_manager
from factorytx.Global import setup_log
from factorytx import utils
import pytest
import logging
import yaml
from jsonschema import validate
from factorytx.pipelines.Pipeline import DataPipeline
from pytest import fixture

try:
    import ujson as json
except:
    import json

log = logging.getLogger(__name__)
components = component_manager()

@fixture(scope='module')
def get_schema_candidates():
    """
    Candidates for testing get_schema function.

    """
    return [('dataplugins', 'file'), ('dataplugins', 'pollipc'), ('dataplugins', 'sql'),
            ('tx', 'ftp'), ('tx', 'localfile'), ('tx', 'ftp'),
            ('parsers', 'spreadsheetparser'), ('parsers', 'knifedataparser'), ('parsers', 'testparser')]

def test_get_schema(get_schema_candidates):
    """
    Testing the get_schema function with valid candidates

    """
    for candidate in get_schema_candidates:
       assert(DataPipeline.get_schema(candidate[0], candidate[1]))

def test_full_plugin():
    """
    Testing the entire work flow from adding plugins, transforms, and 
    tx's.  Creates the config_dict and writes to a file called
    'test.cfg' in the conf.d directory.

    """
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

    config_dict = pipeline.create_config_dict()
    output_directory = '/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/test.cfg'
    pipeline.write_config(config_dict, output_directory)

    assert 0

def test_full_plugin2():
    pipeline = DataPipeline.create_config_file()
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client2.cfg', 'r') as f:
        config = yaml.load(f)['pipeline'][0]
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client2.cfg', 'r') as f:
        config1 = yaml.load(f)['pipeline'][1]
    with open('/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/client2.cfg', 'r') as f:
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

    config_dict = pipeline.create_config_dict()
    output_directory = '/opt/sightmachine/factorytx/factorytx/pipelines/conf.d/test.cfg'
    pipeline.write_config(config_dict, output_directory)

    assert 0

def test_template_creation():
    """
    Testing the template create functions.

    """
    tx_template = DataPipeline.create_tx_template('localtx')
    dataplugin_template = DataPipeline.create_dataplugin_template('file')
    transforms_template = DataPipeline.create_transform_template('sslogtransform')
    assert 0

def test_inserts():
    """
    Testing the insert functions.
    
    """
    dataplugin_template = DataPipeline.create_dataplugin_template('file')
    parser = {'type': 'spreadsheetparser','version':'1.0.0', 'parse_options':[]}
    DataPipeline.insert_parser(dataplugin_template, parser)

    """tx_template = DataPipeline.create_tx_tempalte('localtx')
    pollingservice = {}
    DataPipeline.insert_pollingservice(tx_template, pollingservice)"""

    # entire service, add datasources

    assert 0
