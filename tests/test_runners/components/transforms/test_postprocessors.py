import logging
import unittest

import testfixtures
from testfixtures import Comparison as C, StringComparison as S, compare

from FactoryTx import postprocessors


class CollectEventsTestCase(unittest.TestCase):
    def test_collect_events(self):
        event_specs = [
            {
                'output': 'FireAlarms',
                'fields': ['FireAlarm-A1', 'FireAlarm-A2', 'FireAlarm-B1',
                           'FireAlarm-A1',  # Duplicates are suppressed.
                           'FireAlarm-C1',  # Missing fields are ignored.
                          ],
                'when': True,
            },
            {
                'output': 'SevereAlarms',
                'fields': ['FireAlarm-A2', 'GasLeak-Everywhere'],
                'when': False,
            }
        ]

        sslog = {
            'source': 'AA_BB_Robot_1',
            'version': '1.0.0',
            'plugin': "mitsubishiplc",
            'timestamp': '2016-01-08T17:07:01.000',
            'counter': 1012,
            'running': 'Auto',
            'fieldvalues': {
                'Counter': 1012,
                'FireAlarm-A1': {'value': True, 'units': 'AlarmCode'},
                'FireAlarm-A2': {'value': True, 'units': 'AlarmCode'},
                'FireAlarm-B1': {'value': False, 'units': 'AlarmCode'},
                'Hydraulic-Pressure': {'value': 104, 'units': 'PSI'},
                'GasLeak-Everywhere': {'value': False, 'units': 'AlarmCode'},
            }
        }
        result = postprocessors.collect_events(event_specs, sslog)
        compare(result, {
            'source': 'AA_BB_Robot_1',
            'version': '1.0.0',
            'plugin': "mitsubishiplc",
            'timestamp': '2016-01-08T17:07:01.000',
            'counter': 1012,
            'running': 'Auto',
            'fieldvalues': {
                'Counter': 1012,
                'FireAlarms': {'value': set(['FireAlarm-A1', 'FireAlarm-A2']),  # Ignore list order.
                               'units': 'Events'},
                'Hydraulic-Pressure': {'value': 104, 'units': 'PSI'},
                'SevereAlarms': {'value': set(['GasLeak-Everywhere']), 'units': 'Events'}
            }
        })


class SplitSslogsTestCase(unittest.TestCase):
    def setUp(self):
        self.sslog = {
            '_id': '5702dcdcef5042e63ce4250c',
            'source': None,
            'counter': None,
            'version': '1.0.0',
            'timestamp': '2016-04-04T14:21:03.281',
            'fieldvalues': {
                '1_InputCount': {'value': 18, 'units': None},
                '1_Temperature': {'value': 23, 'units': 'Celsius'},
                '1_Pressure': {'value': 'STP', 'units': 'ASCII'},
                '2_InputCount': {'value': 2, 'units': None},
                '2_Speed': {'value': 3500, 'units': 'RPM'},
                '2A_MaterialWidth': {'value': 12834010, 'units': 'beardseconds'},
                'NotMatched': {'value': 42, 'units': None},
            },
        }

    @testfixtures.log_capture(level=logging.WARNING)
    def check_split_sslogs(self, logged, keep_original, cache):
        source_specs = [
            {
                'source': 'AA_BB_TestMachine_1',
                'counterfield': '1_InputCount',
                'fieldprefixes': [{'prefix': '1_', 'replace_with': ''}],
            },
            {
                'source': 'AA_BB_TestMachine_2A',
                'counterfield': '2_InputCount',
                'fieldprefixes': [
                    {'prefix': '2_', 'replace_with': '2_'},
                    {'prefix': '2A_', 'replace_with': 'Z_'}
                ],
            },
            {
                'source': 'AA_BB_TestMachine_2B',
                'counterfield': '2_InputCount',
                'fieldprefixes': [
                    {'prefix': '2_', 'replace_with': '2_'},
                    {'prefix': '2B_', 'replace_with': 'Z_'}
                ],
            },
        ]
        comparators = [
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_1',
                'counter': 18,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': {
                    'InputCount': {'value': 18, 'units': None},
                    'Temperature': {'value': 23, 'units': 'Celsius'},
                    'Pressure': {'value': 'STP', 'units': 'ASCII'},
                    'NotMatched': {'value': 42, 'units': None},
                },
            },
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_2A',
                'counter': 2,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': {
                    '2_InputCount': {'value': 2, 'units': None},
                    '2_Speed': {'value': 3500, 'units': 'RPM'},
                    'Z_MaterialWidth': {'value': 12834010, 'units': 'beardseconds'},
                    'NotMatched': {'value': 42, 'units': None},
                },
            },
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_2B',
                'counter': 2,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': {
                    '2_InputCount': {'value': 2, 'units': None},
                    '2_Speed': {'value': 3500, 'units': 'RPM'},
                    'NotMatched': {'value': 42, 'units': None},
                },
            },
        ]
        if keep_original:
            source_specs.append({
                'source': 'AA_BB_TestMachine_0',
                'counterfield': '1_InputCount',
                'fieldprefixes': [{'prefix': '', 'replace_with': ''}],
            })
            comparators.insert(0, {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_0',
                'counter': 18,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': {
                    '1_InputCount': {'value': 18, 'units': None},
                    '1_Temperature': {'value': 23, 'units': 'Celsius'},
                    '1_Pressure': {'value': 'STP', 'units': 'ASCII'},
                    '2_InputCount': {'value': 2, 'units': None},
                    '2_Speed': {'value': 3500, 'units': 'RPM'},
                    '2A_MaterialWidth': {'value': 12834010, 'units': 'beardseconds'},
                    'NotMatched': {'value': 42, 'units': None},
                },
            })
        new_sslogs = postprocessors.split_sslog(source_specs, self.sslog, cache)
        compare(new_sslogs, comparators)
        compare(len(new_sslogs), len(set(s['_id'] for s in new_sslogs)),
                prefix="Split sslog IDs should be distinct.")
        logged.check()  # Not expecting any log messages.

    def test_split_sslogs_drop_original(self):
        self.check_split_sslogs(keep_original=False, cache={})

    def test_split_sslogs_keep_original(self):
        self.check_split_sslogs(keep_original=True, cache={})

    def test_split_sslogs_with_cache(self):
        cache = {}
        self.check_split_sslogs(keep_original=True, cache=cache)
        self.check_split_sslogs(keep_original=True, cache=cache)

    @testfixtures.log_capture(level=logging.WARNING)
    def test_split_sslogs_single_source(self, logged):
        # If only a single source is configured, then all fieldvalues
        # should be associated with that source.
        source_specs = [
            {
                'source': 'AA_BB_TestMachine_1',
                'counterfield': '1_InputCount',
                'fieldprefixes': [{'prefix': 'One_', 'replace_with': ''}],  # matches nothing.
            },
        ]
        new_sslogs = postprocessors.split_sslog(source_specs, self.sslog, {})
        compare(new_sslogs, [
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_1',
                'counter': 18,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': {
                    '1_InputCount': {'value': 18, 'units': None},
                    '1_Temperature': {'value': 23, 'units': 'Celsius'},
                    '1_Pressure': {'value': 'STP', 'units': 'ASCII'},
                    '2_InputCount': {'value': 2, 'units': None},
                    '2_Speed': {'value': 3500, 'units': 'RPM'},
                    '2A_MaterialWidth': {'value': 12834010, 'units': 'beardseconds'},
                    'NotMatched': {'value': 42, 'units': None},
                },
            },
        ])
        logged.check()  # Not expecting any log messages.

    @testfixtures.log_capture(level=logging.WARNING)
    def test_split_sslogs_missing_counter(self, logged):
        # sslogs with fieldvalues but no counter should generate a warning.
        # sslogs without a counter should be skipped.
        source_specs = [
            {
                'source': 'AA_BB_TestMachine_A',
                'counterfield': 'Nosuch_InputCount',
                'fieldprefixes': [{'prefix': '1_', 'replace_with': ''}],
            },
            {
                'source': 'AA_BB_TestMachine_B',
                'counterfield': '2_InputCount',
                'fieldprefixes': [
                    {'prefix': '2_', 'replace_with': ''},
                    {'prefix': '2A_', 'replace_with': ''},
                ],
            },
        ]

        new_sslogs = postprocessors.split_sslog(source_specs, self.sslog, {})
        compare(new_sslogs, [
            # AA_BB_TestMachine_A should be skipped
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_B',
                'counter': 2,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': C(dict),
            },
        ])
        # A warning should be emitted for the missing counter.
        logged.check((S(r'FactoryTx\..+'), 'WARNING', S(r'.+')))

    @testfixtures.log_capture(level=logging.WARNING)
    def test_split_sslogs_missing_fieldvalues(self, logged):
        # sslogs without fieldvalues should be silently skipped.
        source_specs = [
            {
                'source': 'AA_BB_TestMachine_A',
                'counterfield': 'Nosuch_InputCount',
                'fieldprefixes': [{'prefix': 'Nosuch_', 'replace_with': ''}],
            },
            {
                'source': 'AA_BB_TestMachine_B',
                'counterfield': '2_InputCount',
                'fieldprefixes': [
                    {'prefix': '1_', 'replace_with': ''},
                    {'prefix': '2_', 'replace_with': ''},
                    {'prefix': '2A_', 'replace_with': ''},
                    {'prefix': 'NotMatched', 'replace_with': ''},
                ],
            },
        ]

        new_sslogs = postprocessors.split_sslog(source_specs, self.sslog, {})
        compare(new_sslogs, [
            # AA_BB_TestMachine_A should be silently skipped
            {
                '_id': S('.+'),
                'source': 'AA_BB_TestMachine_B',
                'counter': 2,
                'version': self.sslog['version'],
                'timestamp': self.sslog['timestamp'],
                'fieldvalues': C(dict),
            },
        ])
        logged.check()


class ComputeFieldsTestCase(unittest.TestCase):
    @testfixtures.log_capture(level=logging.DEBUG)
    def test_no_fieldvalues(self, logged):
        # The original sslog should not be modified if it has no fieldvalues.
        original_sslog = {"source": "Foo"}
        base_sslog = original_sslog.copy()
        expr_specs = [
            {"name": "TotalCount", "expression": "Count1 + Count2"},
        ]
        postprocessors.compute_fields(base_sslog, expr_specs)
        logged.check()
        compare(original_sslog, base_sslog)

    @testfixtures.log_capture(level=logging.DEBUG)
    def test_simple_expression(self, logged):
        # Calculated fields should be added to the original sslog.
        base_sslog = {
            "source": "Foo",
            "fieldvalues": {
                "A": {"value": 1},
                "B": {"value": 2.3},
            }
        }
        expr_specs = [
            {"name": "C", "expression": "A + 2 * B", "units": "mm/s"},
        ]
        postprocessors.compute_fields(base_sslog, expr_specs)
        logged.check()
        compare(base_sslog, {
            "source": "Foo",
            "fieldvalues": {
                "A": {"value": 1},
                "B": {"value": 2.3},
                "C": {"value": 5.6, "units": "mm/s"},
            }
        })

    def check_configuration_error(self, invalid_expr):
        # Errors should be logged without stopping the evaluation process.
        base_sslog = {
            'source': 'Foo',
            'fieldvalues': {'A': {'value': 1}},
        }
        expr_specs = [
            {'name': 'X', 'expression': 'A'},
            {'name': 'Invalid', 'expression': invalid_expr},
            {'name': 'Y', 'expression': 'A * 1.5'},
        ]
        postprocessors.compute_fields(base_sslog, expr_specs)
        compare(base_sslog, {
            'source': 'Foo',
            'fieldvalues': {
                'A': {'value': 1},
                'X': {'value': 1, 'units': None},
                'Y': {'value': 1.5, 'units': None},
            },
        })

    @testfixtures.log_capture(level=logging.DEBUG)
    def test_syntax_error(self, logged):
        # Syntax errors should be logged without stopping evaluation.
        self.check_configuration_error(invalid_expr='A+')
        logged.check(
            ('FactoryTx.postprocessors', 'WARNING',
             S(r'Failed to compute field "Invalid" as "A\+": .*invalid syntax.*')),
        )

    @testfixtures.log_capture(level=logging.DEBUG)
    def test_single_missing_field(self, logged):
        # Missing fields should be logged without stopping evaluation.
        self.check_configuration_error(invalid_expr='MissingField')
        logged.check(
            ('FactoryTx.postprocessors', 'WARNING',
             S(r'Failed to compute field "Invalid" as "MissingField": missing field "MissingField"')),
        )

    @testfixtures.log_capture(level=logging.DEBUG)
    def test_in_order_execution(self, logged):
        # Expressions should be evaluated in the order they are defined. This
        # allows later expressions to reuse earlier outputs.
        base_sslog = {
            'source': 'Foo',
            'fieldvalues': {'A': {'value': 1}},
        }
        expr_specs = [
            {'name': 'X', 'expression': 'A'},
            {'name': 'Y', 'expression': 'A + 3 * X'},
            {'name': 'Z', 'expression': 'A + X * Y'},
        ]
        postprocessors.compute_fields(base_sslog, expr_specs)
        logged.check()
        compare(base_sslog, {
            'source': 'Foo',
            'fieldvalues': {
                'A': {'value': 1},
                'X': {'value': 1, 'units': None},
                'Y': {'value': 4, 'units': None},
                'Z': {'value': 5, 'units': None},
            },
        })
