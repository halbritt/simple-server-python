""" This module has important database fixture functions

"""
from pytest import fixture
import mock
import os
from FactoryTx.DataService import DataService


@fixture(scope='module')
def dataplugin_test_data():
    """ Returns a dictionary of shell connection generating functions

    :rtype: function
    :return: the setup and teardown logic for a dataplugin test
    """
    conf = {}
    conf["glob_patcher"] = mock.patch('FactoryTx.managers.PluginManager.glob')
    conf["glob_mock"] = conf["glob_patcher"].start()
    conf["glob_mock"].glob.return_value = [
        '1.0.0.schema'
    ]

    conf["os_patcher"] = mock.patch('FactoryTx.managers.PluginManager.os')
    conf["os_mock"] = conf["os_patcher"].start()
    conf["os_mock"].path.join = os.path.join  # use real
    conf["os_mock"].path.abspath = os.path.abspath
    conf["os_mock"].path.basename = os.path.basename
    conf["os_mock"].path.dirname = os.path.dirname
    conf["os_mock"].listdir.return_value = [
        'example',
        'example2',
    ]

    conf["open_mock"] = mock.mock_open()
    conf["open_patcher"] = mock.patch('FactoryTx.managers.PluginManager.open',
                                   conf["open_mock"],
                                   create=True)
    conf["open_patcher"].start()

    conf["example_schema"] = {
       '$schema-version': '1.0.0',
       '$schema': 'http://json-schema.org/schema',
       '$plugin-type': 'acquisition',
       'properties': {
           'runtime': {
               'minimum': 1,
               'type': 'integer'
           }
       },
       'required': ['runtime']
    }
    conf["example2_schema"] = {
       '$schema-version': '1.0.0',
       '$schema': 'http://json-schema.org/schema',
       '$plugin-type': 'acquisition',
       'properties': {
           'max_retry': {
               'minimum': 1,
               'type': 'integer'
           }
       },
       'required': ['max_retry']
    }

    conf["yaml_patcher"] = mock.patch('FactoryTx.managers.PluginManager.yaml')
    conf["yaml_mock"] = conf["yaml_patcher"].start()
    conf["yaml_mock"].load.side_effect = [
        conf["example2_schema"],
        conf["example_schema"],
    ]

    conf["example_plugin_mock"] = mock.Mock(DataService)
    conf["example_plugin_mock"].__version__ = '1.0.0'
    conf["example_plugin_mock"].return_value = mock.Mock(DataService)
    conf["example_plugin_mock"].return_value.__version__ = '1.0.0'

    conf["example_ep_mock"] = mock.Mock()
    conf["example_ep_mock"].name = 'example'
    conf["example_ep_mock"].load.return_value = conf["example_plugin_mock"]

    conf["example_plugin_mock2"] = mock.Mock(DataService)
    conf["example_plugin_mock2"].__version__ = '1.0.0'
    conf["example_plugin_mock2"].return_value = mock.Mock(DataService)
    conf["example_plugin_mock2"].return_value.__version__ = '1.0.0'

    conf["example_ep_mock2"] = mock.Mock()
    conf["example_ep_mock2"].name = 'example2'
    conf["example_ep_mock2"].load.return_value = conf["example_plugin_mock2"]

    conf["pkg_resources_patcher"] = mock.patch(
        'FactoryTx.managers.PluginManager.pkg_resources')
    conf["pkg_resources_mock"] = conf["pkg_resources_patcher"].start()
    conf["pkg_resources_mock"].iter_entry_points.return_value = [
        conf["example_ep_mock"],
        conf["example_ep_mock2"],
    ]
    yield conf
    conf["glob_patcher"].stop()
    conf["os_patcher"].stop()
    conf["open_patcher"].stop()
    conf["yaml_patcher"].stop()
    conf["pkg_resources_patcher"].stop()
