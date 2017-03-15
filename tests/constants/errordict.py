""" The dictionary of error strings to be used by the testing module.

"""

ERRDICT = {
    'network_errors': {
        'login_failure': "login_failure: url:{}, payload:{}, message:{}",
        'misc_api_failure': "Undefined api error: errors:{}",
        'http_get_failure': 'http_get_failure: url:{}, message:{}',
        'http_post_failure': 'http_post_failure: url:{}',
        'http_delete_failure': 'http_delete_failure: url:{}'
    },
    'config_errors': {
        'machinetype_yaml_error': (" Unexpected configuration value: {}"),
        'factory_shifts_error': (" NO SHIFTS ASSOCIATED WITH: {}")
    }, # "ERROR in aggregate_endtime(): " + key + " is not populated"
    'api_errors': {
        'status_code_error': ("Unexpected status code encountered: tenant:{}, code_received:{},"
                              " code_expected:{}, test_description:{}, api_endpoint:{}"),
        'json_response_error': ("Unexpected JSON response: tenant:{}, received:{},"
                                " code_expected:{}, test_description:{}, api_endpoint:{}"),
        'json_field_error': ("Unexpected JSON field or value: field_id:{}, field_type:{}, "
                             "type_expected:{}, api/json:{}"),
        'json_payload_error': "Unexpected JSON payload: api:{}, \ncorrect_output:{}, \nrecieved:{}",
        'json_value_error': ("JSON field has incorrect value: field_id:{}, field_value:{}, "
                             "value_expected:{}, api:{}"),
        'json_generic_error': ("{}")
    },
}
