import re
import json
from bson import json_util
import os
import pytz
import numbers
from FactoryTx.utils import as_timezone
from FactoryTx.utils import naive_to_local
from datetime import datetime, timedelta, date
from math import floor

import sqlalchemy as sqla
import pandas as pd
from decimal import Decimal

from FactoryTx.Global import setup_log
from FactoryTx.components.dataplugins.DataPlugin import DataPlugin
from FactoryTx.utils import hash4b

log = setup_log('sql')


class ConnectionManager(object):

    def __init__(self):
        super(ConnectionManager, self).__init__()
        self.engines = {}
        self.pool_size = 25

    def create_engine(self, url, connect_args, file_mode = False):
        params = dict(connect_args=connect_args)

        if not file_mode:
            params['pool_size'] = self.pool_size

        if url not in self.engines:
            self.engines[url] = sqla.create_engine( url, **params)
            log.info("Params: %s", params)
            log.info('Created engine for: {}'.format(url))
        return self.engines[url]


# Singleton
connection_manager = ConnectionManager()


class SQL(DataPlugin):
    """
    Supports ssl option only with postgresql.

    Supported engines:
        * postgresql+psycopg2
        * oracle+cx_oracle
    """

    __version__ = '1.0.0'

    reconnect_timeout = 5  # seconds
    reconnect_attempts = -1  # Forever
    logname = 'SQLDataPlugin'

    def __init__(self):
        super(SQL, self).__init__()
        self.cache = {'last_id': -1, 'last_timestamp': {'value': -1, 'count': 0}}
        self.engine = None
        self.filter = None
        self.order_by = None
        self.ssl = True
        self.parsetimestamp = None
        self.serial_field_name = None
        self.db_url = None
        self.file_mode = False
        self.unique_mode = False

    def _init_cache_filename(self):
        # build cache filename:
        # cachedirectory + cachefilename from config file
        self.cache_file = os.path.join(self.cachedirectory, self.cachefilename)

    def _grep_table_name(self):
        match = re.search(r'from\s+(\S+)', self.query.lower())
        if match:
            return match.groups()[0]
        self.log.warn('failed to find table name in query')
        return ''

    def load_parameters(self, sdconfig, schema, config):
        super(SQL, self).load_parameters(sdconfig, schema, config)
        if self.db_url:
           db_url = r'{}'.format(self.db_url)
        else:
            if hasattr(self, 'docker'):
                if self.docker == True:
                    db_url = r'{}://{}:{}@{}'.format(self.db_type,
                            self.db_user, self.db_pass, self.host)
            else:
                db_url = r'{}://{}:{}@{}:{}/{}'.format(self.db_type, self.db_user, self.db_pass,
                                                       self.host, self.port, self.db_name)

        args = {}
        if self.ssl and self.db_type.startswith('postgresql'):
            args['sslmode'] = 'require'
        self.engine = connection_manager.create_engine(
            db_url, connect_args=args, file_mode = self.file_mode)

        self._init_cache_filename()
        self.load_cache()

        self.exec_args = {}
        self.sql_query = self.query
        if hasattr(self, 'stored_procedure') and self.stored_procedure:
            self.sql_query += ' ' + self.stored_procedure  # feed in parameter to stored procedure
        else:
            self.sql_query += ' WHERE {} > :last_id'.format(self.id_field)
        self.exec_args['last_id'] = self.cache['last_id']

        if self.filter:
            self.sql_query += ' AND ' + self.filter

        if hasattr(self, 'groupby'):
            self.sql_query += ' GROUP BY {}'.format(self.groupby)

        if self.order_by:
            self.sql_query += ' ORDER BY {}'.format(self.order_by)
        elif hasattr(self, 'stored_procedure') and self.stored_procedure:
            pass  # stored procedures don't allow for ordering
        else:
            self.sql_query += ' ORDER BY {}'.format(self.id_field)

        if self.limit:
            if self.engine.dialect.name == 'oracle':
                self.sql_query = ''.join(('SELECT * FROM ( ',
                                          self.sql_query,
                                          ' ) WHERE ROWNUM <= :limit'))
            elif self.engine.dialect.name == 'mssql':
                 self.sql_query = self.sql_query.replace("SELECT", "SELECT TOP :limit", 1)
            else:
                self.sql_query += ' LIMIT :limit'
            self.exec_args['limit'] = self.limit

        # Default the configs, because configs are not loading properly at this point
        if not hasattr(self, "cycle_timespan"):
            self.cycle_timespan = {}
        if "enable" not in self.cycle_timespan:
            self.cycle_timespan['enable'] = False
        if "max_timespan_minutes" not in self.cycle_timespan:
            self.cycle_timespan['max_timespan_minutes'] = 0
        if "set_time_minutes" not in self.cycle_timespan:
            self.cycle_timespan['set_time_minutes'] = 120
        if "set_time_seconds" not in self.cycle_timespan:
            self.cycle_timespan['set_time_seconds'] = 0

    def read(self):
        log.info(self.engine.pool.status())
        try:
            resp = self.engine.execute(sqla.text(self.sql_query), **self.exec_args)
        except Exception as e:
            log.error(vars(self.engine.logger))
            log.error("Error while excecuting query")
            return

        records = resp.fetchall()
        log.info('Received {} records from {}'.format(len(records), self._getSource()))

        if not records:
            # if cycle timespan feature enabled and if the data saved in the cache file doesn't match what is current
            # in the self.cache var update the cache file. This is necessary since process() updates the cache after
            # read() returns, and we don't want to call save_cache() in process since each record is processed separately.
            if self.cycle_timespan['enable'] and not self.cache_data_current():
                self.save_cache()

            return []

        # find last id
        if hasattr(self, 'stored_procedure') and self.stored_procedure and hasattr(self, 'id_field'):
            #  using column defined in id_field as the last_id column for stored procedures.
            #  would probably be smart to have this as a default behavior if id_field is defined but I don't want to
            #  break any pre-existing configurations.
            self.cache['last_id'] = max(map(lambda a: a[self.id_field], records))
        else:
            self.cache['last_id'] = max(map(lambda a: a[0], records))

        if isinstance(self.cache['last_id'], Decimal):
            self.cache['last_id'] = float(self.cache['last_id'])

        self.exec_args['last_id'] = self.cache['last_id']
        self.save_cache()

        # replace column names with those defined in the config.
        if hasattr(self, 'alias_columns'):
            self.keys = [self.alias_columns.get(key, key) for key in resp.keys()]
        else:
            self.keys = resp.keys()

        # if you are paging through time series data make sure you don't miss data
        # assumes sorted in asc order by timestamp
        # and that all rows with the same timestamp are inserted at the same moment.
        if getattr(self, 'time_based_truncate', False):
            time_column_position = self.keys.index(self.time_field_name)
            max_time = records[-1][time_column_position]

            if records[0][time_column_position] == max_time and len(records) == self.limit:
                self.cache['last_id'] -= timedelta(seconds=1)
                self.exec_args['last_id'] = self.cache['last_id']
                self.save_cache()
                log.warn('All data points returned have the same timestamp, cannot page. Please increase limit. '
                         'Cache, has been adjusted so there is no data loss.')
                return []
            elif records[0][time_column_position] == max_time and len(records) < self.limit:
                pass
            else:
                while records[-1][time_column_position] == max_time:
                    del records[-1]
                log.info('adjusting cache for time based truncation')
                self.cache['last_id'] = records[-1][time_column_position]
                self.exec_args['last_id'] = records[-1][time_column_position]
                self.save_cache()

        if hasattr(self, 'pd_pivot'):
            # uses the pandas pivot method
            # converts the records to dataframe, performs the pivot, and converts the records to a list of lists`
            df = pd.DataFrame(records, columns=self.keys)
            df = df.pivot(index=self.pd_pivot['index'], columns=self.pd_pivot['columns'], values=self.pd_pivot['values'])
            df[self.pd_pivot['index']] = df.index  # make the index a column as well
            df = df.fillna(method='ffill') if self.pd_pivot.get('fill_forward', False) else df  # fill forward
            df = df.where((pd.notnull(df)), None)  # replace NaN with None
            self.keys = df.columns.tolist()  # reset column keys
            records = df.values.tolist()  # convert to a list of lists
            if type(records[0][-1]) is pd.Timestamp:  # convert the timestamps to datetime.datetime from pandas.Timestamp
                records = map(lambda x: x[:-1] + [x[-1].to_datetime()], records)
            if len(self.keys) != len(records[0]):
                log.warn('Column length does not match record width')

        #  replace reserved characters in fieldnames
        if hasattr(self, 'replace_fieldname_characters'):
            for c in self.replace_fieldname_characters:
                c_key = c.keys()[0]
                c_val = c[c_key]
                self.keys = [k.replace(c_key, c_val) if c_key in k else k for k in self.keys]

        #  convert byte overflows to there corresponding positive integer
        if hasattr(self, 'signed_int_overflow_fix') and self.signed_int_overflow_fix['datafield'] in self.keys:
            column_position = self.keys.index(self.signed_int_overflow_fix['datafield'])
            max_val = pow(2, self.signed_int_overflow_fix['byte_size'] - 1) - 1  # maximum possible value
            min_val = pow(2, self.signed_int_overflow_fix['byte_size'] - 1) * -1  # minimum possible value
            corr_form = lambda x: 1 + x - min_val + max_val  # correction formula
            for record in records:
                if isinstance(record[column_position], numbers.Number) and record[column_position] < 0:
                    record[column_position] = corr_form(record[column_position])

        #  interpolate values
        if hasattr(self, 'linear_interpolation') and self.linear_interpolation['x_fieldname'] in self.keys and self.linear_interpolation['y_fieldname'] in self.keys:
            records = self._linear_interpolation(records)

        return records

    def _linear_interpolation(self, _records):

        def _handle_interpolation():
            '''
            requires x_diff, y_diff, x1, x2, y1, and y2 be set properly first
            :return: list of lists -- interolated records
            '''

            add_recs = []
            if not self._validate_diffs(x_diff, y_diff, x_max_diff, y_max_diff):
                return []
            for v in xrange(1, int(x_diff)):
                new_row = [None] * len_row
                new_row[x_loc] = v + x1
                new_row[y_loc] = y_func(v, x1, x2, y1, y2)
                add_recs.append(new_row)
            return add_recs

        x_loc = self.keys.index(self.linear_interpolation['x_fieldname'])
        y_loc = self.keys.index(self.linear_interpolation['y_fieldname'])
        x_max_diff = self.linear_interpolation['x_max_diff']
        y_max_diff = self.linear_interpolation['y_max_diff']
        x = self.cache.get('interp_x', None)
        y = self.cache.get('interp_y', None)
        y_func = lambda i, _x1, _x2, _y1, _y2: i * (_y2 - _y1) / int(_x2 - _x1) + _y1  # mx + b
        len_row = len(self.keys)
        additional_records = []

        for record in _records:
            x_new = record[x_loc]
            y_new = record[y_loc]

            # check to see if a diff is possible, if not then some part is invalid so skip to the next record
            try:
                x_diff = x_new - x
                y_diff = y_new - y
            except TypeError:
                continue
            finally:
                x1, x2, y1, y2 = x, x_new, y, y_new  # set values used for calculations
                x, y = x_new, y_new  # overwrite for next iteration

            if self.linear_interpolation.get('x_max', False) and x2 < x1:
                # if the code enters here it means that the x has crossed.
                # the approached used to handle this is to break the x points from [start, end] => [start, 0), [0, end]

                # calcuate the span for the two spans
                x_span1 = (self.linear_interpolation['x_max'] - x1)
                x_span2 = x2 - 0
                total_x_span = x_span1 + x_span2

                # interpolation calculations for the first span
                x2 = self.linear_interpolation['x_max'] + 1
                y2 = int(x_span1) * y_diff / int(total_x_span) + y1
                x_diff = x2 - x1
                y_diff = y2 - y1
                first_add_records = _handle_interpolation()

                # calculations for the second span
                x1, x2 = 0, x + 1
                y1, y2 = y - int(x_span2) * y_diff / int(total_x_span), y_new
                x_diff = x2 - x1
                y_diff = y2 - y1
                second_add_records = _handle_interpolation()

                additional_records += first_add_records + second_add_records  # combine interpolations for both spans

            else:
                #  interpolate new records and add them to additional records
                additional_records += _handle_interpolation()

        # update and save cache
        self.cache['interp_x'] = x
        self.cache['interp_y'] = y
        self.save_cache()
        if additional_records:
            log.info('adding {} additional interpolated records'.format(len(additional_records)))
        _records = _records + additional_records if additional_records else _records  # combine records
        return sorted(_records, key=lambda i: i[self.keys.index(self.time_field_name)])  # sort records

    def process(self, record):
        data = {}
        if 'source' in self.keys:
            data['source'] = record[self.keys.index('source')]
        else:
            data['source'] = self._getSource()
        data['fieldvalues'] = {}

        for i, value in enumerate(record):
            key = self.keys[i]

            if hasattr(self, 'regex') and key in self.regex.keys():
                pattern = re.compile(self.regex[key])
                match = pattern.match(value)
                if match:
                    if match.groups():
                        value = list(match.groups())
                    if match.groupdict():
                        value = match.groupdict()

            if self.parsetimestamp and self.parsetimestamp.get(key):
                try:
                    value = datetime.strptime(value, self.parsetimestamp[key])
                except ValueError:
                    log.warn("Could not parse field {} with value '{}' with format '{}'".format(key, value, self.parsetimestamp[key]))
                except TypeError:
                    log.warn("Could not parse field {} of type {}".format(key, type(value)))

            if key == self.time_field_name:
                if not isinstance(value, datetime):
                    log.error("Field {} isn't of datestamp type: '{}', dropping record!".format(key, value))
                    return None

                # handles timezones and daylight savings if activated
                if hasattr(self, 'timezone'):
                    local_dt = naive_to_local(value, pytz.timezone(self.timezone))
                    utc_dt = as_timezone(local_dt, pytz.utc)
                    naive_dt = utc_dt.replace(tzinfo=None)
                    data['timestamp'] = naive_dt
                else:
                    data['timestamp'] = value

                if self.unique_mode == True:
                    # Can't compare delta aware DT with non-delta aware DT
                    dt_stamp = data['timestamp'] + timedelta(milliseconds=0)

                    try:
                        if dt_stamp == self.cache['last_timestamp']['value']:
                            data['timestamp'] += timedelta(milliseconds=self.cache['last_timestamp']['count'])
                            self.cache['last_timestamp']['count'] += 1
                        else:
                            self.cache['last_timestamp']['count'] = 0

                        self.cache['last_timestamp']['value'] = dt_stamp
                    except Exception as e:
                        data['timestamp'] = dt_stamp
                        log.error("Unique timestamp {} could not be converted, reverting to original timestamp! Error: {}".format(dt_stamp, e))

                continue

            if key == self.serial_field_name:
                data['serial'] = [value]

            if key == getattr(self, 'counter_field', False):
                data['counter'] = value

            if hasattr(self, 'counter_round_down') and data.get('counter', False):
                data['counter'] = int(floor(data['counter']/self.counter_round_down)) * self.counter_round_down

            if getattr(self, 'sticky_counter', False):
                if data.get('counter') is not None:
                    self.cache['counter'] = data['counter']
                elif self.cache.get('counter') is not None:
                    data['counter'] = self.cache['counter']

            drop_null_values = getattr(self, 'drop_null_values', False)
            if not drop_null_values or (drop_null_values and (value is not None)):
                data['fieldvalues'][key] = {}
                data['fieldvalues'][key]['value'] = value

        if self.cycle_timespan['enable']:
            self._handle_cycle_timespan(data)

        if getattr(self, 'sticky_counter', False):
            self.save_cache()

        return data

    def _handle_cycle_timespan(self, data):
        """
        Function to handle the cycle_timespan inside of the config file.

        The cycle option is used for if you want to use your data.timestamp as the end time and either manually define
        the starttime based on a timespan or use the previous records endtime as the starttime of this record.
        :param data:
        :return:
        """
        def _validate_cached_starttime(s):
            """
            function to validate whether or not a potential cached starttime is valid based on the endtime and
            parameters from the config
            :param s: machine source
            :return: bool
            """
            if 'last_endtime' not in self.cache:
                self.cache['last_endtime'] = {}
                return False
            elif s not in self.cache['last_endtime']:
                return False
            elif end_time <= self.cache['last_endtime'][s].replace(tzinfo=None):
                return False
            elif (end_time - self.cache['last_endtime'][s].replace(tzinfo=None)).total_seconds() / 60 > self.cycle_timespan[
                'max_timespan_minutes']:
                return False
            else:
                return True

        # add end_time to fieldvalues
        end_time = data['timestamp']
        data['fieldvalues'][u'endtime'] = {'value': end_time}
        default_starttime = {'value': (end_time - timedelta(minutes=self.cycle_timespan['set_time_minutes'],
                                                            seconds=self.cycle_timespan['set_time_seconds']))}

        try:
            source = data['fieldvalues']['__source__']['value']
        except:
            source = self.source

        # Scenario #1: in the last run the end_time of the record was stored and fits the proper time range so it
        # is used as the this records start time.
        if _validate_cached_starttime(source):
            start_time = self.cache['last_endtime'][source]
            data['fieldvalues'][u'starttime'] = {'value': start_time}

        else:
            data['fieldvalues'][u'starttime'] = default_starttime

        self.cache['last_endtime'][source] = end_time

    def save_cache(self):
        if not os.path.exists(self.cachedirectory):
            os.makedirs(self.cachedirectory)
        json_cache = json.dumps(self.cache, default=json_util.default, sort_keys=True)
        try:
            with open(self.cache_file, 'w') as f:
                f.write(json_cache)
        except:
            log.error('Failed to save cache into {}'.format(self.cache_file))
        else:
            pass
            log.debug('Saved cache: {}; Source: {}'.format(self.cache,
                                                          self._getSource()))

    def load_cache(self):
        cache_file = self.cache_file

        # if new pointer is not found - we try to find old one and load data from it
        if not os.path.exists(self.cache_file):
            cache_file = self.find_old_cache_file()
            log.info('Found old cache file {}'.format(cache_file))

        try:
            with open(cache_file, 'r') as f:
                cache = json.loads(f.read(), object_hook=json_util.object_hook)
        except:
            log.error('Failed to load cache from {}'.format(cache_file))
        else:
            self.cache = cache

    def cache_data_current(self):
        """
        Checks to see if the cached data in the cache file matches the current cache info
        :return: boolean
        """
        cache_file = self.cache_file
        if not os.path.exists(self.cache_file):
            cache_file = self.find_old_cache_file()
        try:
            with open(cache_file, 'r') as f:
                cache = f.read()
        except Exception as e:
            log.error('Failed to load cache from {} - {}'.format(cache_file, e))
        else:
            if json.dumps(self.cache, default=json_util.default, sort_keys=True) == cache:
                return True
            else:
                return False

    def find_old_cache_file(self):
        # try to find old cache pointers in cache directory
        # corresponding to the current db config

        # take db config values
        s = ''.join((
            str(self.host),
            str(self.port),
            self.db_type,
            self.db_name,
            self._grep_table_name(),
        ))

        # make a small hash
        h = hash4b(s)
        # pattern to search old pointer: the most priority template should be first in list
        pattern = ['cache_{}_{}.json'.format(self._getSource(), h),
                   'cache_{}.json'.format(self._getSource()),]

        for root, directories, files in os.walk(self.cachedirectory):
            for patt in pattern:
                for cache_file_old in files:
                    if cache_file_old == patt:
                        return os.path.join(self.cachedirectory, cache_file_old)
            break

    @staticmethod
    def _validate_diffs(xd, yd, xmd, ymd):
        #  make sure this is valid to interpolate
        try:
            xd = xd.total_seconds()
        except AttributeError:
            pass
        try:
            yd = yd.total_seconds()
        except AttributeError:
            pass

        if ymd < yd:
            return False
        elif xmd < xd:
            return False
        elif xd == 0:
            return False
        elif yd == 0:
            return False
        else:
            return True
