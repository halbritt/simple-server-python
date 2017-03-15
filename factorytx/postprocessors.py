import ast
import logging

import bson
from simpleeval import simple_eval

log = logging.getLogger(__name__)


def collect_events(event_specs, sslog):
    """Scans sslog fields and converts specified fields into list of field
    names that match a specific value. The scanned fields are removed from
    the sslog.

    Given the following configuration:

        collect_events:
        - output: Alarms
          fields: [F1, F2, F3]
          when: true

    and the initial fieldvalues

        {
          'Counter': {'value': 10, 'units': 'Binary'},
          'F1': {'value': false, 'units': 'AlarmCode'},
          'F2': {'value': true, 'units': 'AlarmCode'},
          'F3': {'value': false, 'units': 'AlarmCode'}
        }

    factorytx would output the following fieldvalues:

        {
          'Counter': {'value': 10, 'units': 'Binary'},
          'Alarms': {'value': ['F2'], 'units': 'Events'}
        }

    Events are collected after sslogs are split, so you will only need one
    set of events that will be applied to both machines. Events that only
    apply to one machine should still be listed in the shared collect_events
    list.

    :param event_specs: a list of dicts representing event specifications. Each
        must contain an `output` key specifying the field name to output, a
        `fields` key indicating which fields to consume, and a `when` key, which
        specifies the value that the fields must have for their field names to
        be included in the output list.
    :param sslog: a dict representing a raw sslog. It must contain a
        `fieldvalues` key, with one subkey per raw field of polled data.

    :returns: a new sslog with all collected fields replaced by the output
        fields.

    """

    result_sslog = sslog.copy()
    result_sslog['fieldvalues'] = sslog['fieldvalues'].copy()

    for event_spec in event_specs:
        events = []
        for field_name in set(event_spec['fields']):  # Ignore duplicates.
            field_value = sslog['fieldvalues'].get(field_name, {}).get('value')
            if field_value == event_spec['when']:
                events.append(field_name)
        result_sslog['fieldvalues'][event_spec['output']] = {
            'value': events,
            'units': 'Events',
        }

    # A field may be referenced by multiple event_specs, so pop fields last.
    for event_spec in event_specs:
        for field_name in event_spec['fields']:
            result_sslog['fieldvalues'].pop(field_name, None)

    return result_sslog


def _get_field_destinations(source_specs, key, cache):
    """Returns a list of (source, output field) pairs to save a target key to."""

    if key in cache:
        return cache[key]

    destinations = []
    longest_prefix = ''

    for source_spec in source_specs:
        for fieldprefix in source_spec['fieldprefixes']:
            prefix = fieldprefix['prefix']
            if key.startswith(prefix):
                field_name = fieldprefix['replace_with'] + key[len(prefix):]
                destinations.append((source_spec['source'], field_name))
                if len(prefix) > len(longest_prefix):
                    longest_prefix = prefix

    # If the longest match is blank, then either (a) no prefix matched
    # or (b) a source was explicitly configured with prefix = ''. In both
    # cases, we want to apply the field to all machines.
    if longest_prefix == '':
        for source_spec in source_specs:
            destinations.append((source_spec['source'], key))

    cache[key] = destinations
    return destinations


def split_sslog(source_specs, sslog, cache):
    """Splits an input sslog into several output sslogs for differing sources.

    :param source_specs: a list of dicts, each of which represents a data
        source to extract. Each dict should specify a `source` key, a
        `fieldprefixes` key containing a list of prefixes to match, and
        a `counterfield` key specifying the field name of the source's counter.
    :param sslog: a raw sslog, which must contain a `fieldvalues` key with one
        subkey per field of polled data.
    :param cache: a dict which will be used to cache field mappings between
        invocations.
    :returns: a list of new sslog dicts, one per source in the counter map.

    For example, given the following source specs (as YAML):

    - source: A
      counterfield: A_Count
      fieldprefixes:
      - {prefix: A_, replace_with: ''}
      - {prefix: a_, replace_with: z_}
    - source: B
      counterfield: B_Count
      fieldprefixes:
      - {prefix: B_, replace_with: ''}

    and the input sslog

    {
        "source": "unused",
        "fieldvalues": {
            "A_Count": {"value": 1},
            "A_field": {"value": "q"},
            "a_field": {"value": "r"},
            "B_Count": {"value": 2},
        },
    }

    split_sslog would output the following two sslogs:

    [
        {
            "source": "A",
            "counter": 1,
            "fieldvalues": {"field": {"value": "q"}, "z_field": {"value": "r"}},
        },
        {
            "source": "B",
            "counter": 2,
            "fieldvalues": {"not_matched": {"value": "common"},
        },
    ]

    Additionally a source may be configured with the fieldprefix ''; this will
    cause it to match all fields, but will still allow unmatched fields to be
    split between all of the other sources.

    """

    sslogs_by_source = {}
    for source_spec in source_specs:
        source = source_spec['source']
        counterfield = source_spec['counterfield']

        # HACK: Use the base counter if counter is None. Needed to work around
        #       pollipc not storing parsed filename data to fieldvalues.
        if counterfield is None:
            counter = sslog['counter']
        else:
            counter = sslog['fieldvalues'].get(counterfield, {})
            if isinstance(counter, dict):
                counter = counter.get('value')

        sslogs_by_source[source] = sslog.copy()
        # HACK: Reset the sslog ID after splitting so that the sslogs
        #       don't end up overwriting each other.
        #       This can be fixed by moving ID generation later in the
        #       sslog generation pipeline.
        sslogs_by_source[source]['_id'] = str(bson.ObjectId())
        sslogs_by_source[source]['source'] = source
        sslogs_by_source[source]['fieldvalues'] = {}
        sslogs_by_source[source]['counter'] = counter

    for key, value in sslog['fieldvalues'].items():
        destinations = _get_field_destinations(source_specs, key, cache)
        for source, field_name in destinations:
            sslogs_by_source[source]['fieldvalues'][field_name] = value

    new_sslogs = []
    for new_sslog in sslogs_by_source.values():
        if new_sslog['counter'] is None and len(new_sslog['fieldvalues']) > 0:
            log.warning('Dropped sslog for source "%s"; its counter was missing',
                        new_sslog['source'])
        elif len(new_sslog['fieldvalues']) > 0:
            new_sslogs.append(new_sslog)

    return sorted(new_sslogs, key=lambda s: s['source'])


def _get_free_names(expr):
    # type: (str) -> Set[str]
    """Returns the set of distinct free variables occurring in a Python
    expression string. Raises SyntaxError if parsing fails.

    """

    root = ast.parse(expr)
    names = set()
    for node in ast.walk(root):
        if isinstance(node, ast.Name):
            names.add(node.id)
    return names


class MissingFieldError(Exception):
    def __init__(self, field_name):
        super(MissingFieldError, self).__init__('Missing field: "{}"'.format(field_name))
        self.field_name = field_name


def compute_fields(sslog, expr_specs):
    # type: (Dict[str, Any], List[Dict[str, str]]) -> None
    """Fills in derived fields for an sslog dict using a series of expressions.

    :param sslog: sslog dict. It must contain a "fieldvalues" key.
    :param expr_specs: list of dicts containing the following items:
        `name`: name of the field to create / update.
        `expr`: Python expression string used to calculate the field.
        `units`: of the derived expression. (OPTIONAL)
    :raises KeyError: if any of the inputs lack required keys.

    """

    if 'fieldvalues' not in sslog:
        return

    fieldvalues = sslog['fieldvalues']

    for expr_spec in expr_specs:
        name = expr_spec['name']
        expr = expr_spec['expression']
        units = expr_spec.get('units')

        try:
            # Binding all of the (potentially hundreds) of fieldvalues to names
            # can take a lot of our limited CPU budget on the DCNs. Instead, we
            # extract a list of free variables and only bind those.
            free_names = _get_free_names(expr)
            names = {}
            for free_name in free_names:
                if free_name not in fieldvalues:
                    raise MissingFieldError(free_name)
                bound_value = fieldvalues[free_name]
                if isinstance(bound_value, dict):
                    bound_value = bound_value['value']
                names[free_name] = bound_value
            value = simple_eval(expr, names=names)
            fieldvalues[name] = {'value': value, 'units': units}

        except MissingFieldError as e:
            # TODO: Discriminate computed fields by sslog type and suppress
            #       warnings when the sslog type doesn't match. This will let
            #       us avoid spamming messages about fields that are expected
            #       to be absent.
            log.warning('Failed to compute field "%s" as "%s": missing field "%s"',
                        name, expr, e.field_name)
        except Exception as e:
            log.warning('Failed to compute field "%s" as "%s": %r', name, expr, e)
