import os
import unittest

import mock

from FactoryTx.managers.PluginManager import component_manager
from FactoryTx.DataService import DataService
from tests.fixtures.pipeline_fixtures import dataplugin_test_data

component_manager = component_manager()

def test_plugins(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plugs = {'example': dataplugin_test_data["example_ep_mock"],
             'example2': dataplugin_test_data["example_ep_mock2"]}
    if not plugs == plgn_mng.plugins:
        error_fixture.report_error("incorrect_plugin_lists", [plugs, plgn_mng.plugins])
    if not plgn_mng.plugin_schemas == {}:
        error_fixture.report_error("incorect_plugin_schemas", [{}, plgn_mng.plugin_schemas])
    iep = dataplugin_test_data["pkg_resources_mock"].iter_entry_points
    iep.assert_call_count(2)

def test_get_plugins_path(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_path = plgn_mng.get_plugins_path()
    if not plgn_path == "":
        error_fixture.report_error("incorrect_plugin_path")

def test_plugins_directory_dict(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_dir_dict = plgn_mng.plugins_directory_dict()
    if not len(plgn_dir_dict) == 2:
        error_fixture.report_error("incorrect_plugin_directory_length", [2, plugin_dir_dict])

def test_load_schemas(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_mng.load_schemas()
    example1 = {'example': dataplugin_test_data["example_ep_mock"],
                'example2': dataplugin_test_data["example_ep_mock2"]}
    example2 = {'example': [dataplugin_test_data["example_schema"]],
                'example2': [dataplugin_test_data["example2_schema"]]}
    if not plgn_mng.plugins == example1:
        error_fixture.report_error("incorrect_plugin_loaded_schemas", [example1, plgn_mng.plugins])
    if not plgn_mng.plugin_schemas == example2:
        error_fixture.report_error("incorrect_plugin_loaded_schemas", [example2, plgn_mng.plugin_schemas])

def test_get_plugins_schema(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_mng.load_schemas()
    schema = plgn_mng.get_plugin_schema('example', '1.0.0')
    if not schema == dataplugin_test_data['example_schema']:
        error_fixture.report_error("schemas_dont_match", [dataplugin_test_data['example_schema'], schema])

def test_get_plugins_schema_bad_version(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_mng.load_schemas()
    if not plgn_mng.get_plugin_schema('example', '0.0.0') is None:
        error_fixture.report_error("bad_object", [None])

def test_get_plugins_schema_bad_plugin(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_mng.load_schemas()
    if not plgn_mng.get_plugin_schema('unknown', '1.0.0') is None:
        error_fixture.report_error("bad_object", [None])


def test_get_plugin(dataplugin_test_data, error_fixture):
    plgn_mng = component_manager['dataplugins']
    plgn_mng.load_schemas()
    self.assertEqual(plgn_mng.get_plugin('example'),
                     dataplugin_test_data["example_plugin_mock"])
    self.assertEqual(plgn_mng.get_plugin('example2'),
                     dataplugin_test_data["example_plugin_mock2"])
    self.assertIsNone(plgn_mng.get_plugin('example3'))
