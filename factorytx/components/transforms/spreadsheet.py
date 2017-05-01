import os
import pandas as pd
import difflib
import fnmatch
import math
import re
import filecmp
import pytz
from itertools import izip_longest

from datetime import datetime
from dateutil import parser
from FactoryTx.components.dataplugins.parsers.base import BaseParser
from FactoryTx.compoonents.dataplugins.DataPlugin import CodesMixin


def file_size(filename):
    with open(filename) as fd:
        fd.seek(0, 2)
        return fd.tell()


# XXX: Why don't we just izip the two byte streams and find the first line
#      that doesn't match? Faster than running a full patience diff...
#
# XXX: Is this a spreadsheet parser, or a CSV parser?
def find_diff_row(file_1, file_2):
    with open(file_1) as fd1, open(file_2) as fd2:
        text_1 = fd1.read().splitlines()
        text_2 = fd2.read().splitlines()
        d = difflib.Differ()
        diff = d.compare(text_1, text_2)
        for line, row in enumerate(diff):
            if not row.startswith(' '):
                return line if line == len(text_2) else -1


def compare_file_stat(file1, file2):
        """ Compare the file stats of a file to another.  Not using filecmp.cmp because it always checks
            the contents of the file after the stats comparison fails

        :param string file1: path to a file
        :param string file2: path to the comparison file
        ;rtype: int
        :returns: returns -1 if the files are not the same, 0 if they are the same
        :raises Exception: whatever os.stat returns
        """
        stat1 = os.stat(file1)
        stat2 = os.stat(file2)
        if stat1.st_mode != stat2.st_mode or\
           stat1.st_size != stat2.st_size or\
           stat1.st_mtime != stat2.st_mtime:
            return -1
        return 0


class SpreadSheetParser(BaseParser, CodesMixin):
    # Default to parsing CSVs in child parsers without sheet_type in their schema.
    sheet_type = 'csv'

    def __init__(self):
        super(SpreadSheetParser, self).__init__()

    def loadParameters(self, schema, conf):
        super(SpreadSheetParser, self).loadParameters(schema, conf)

        # check required fields
        if not hasattr(self, 'source'):
            raise Exception("Missing 'source' configuration value")

        if not hasattr(self, 'version'):
            raise Exception("Missing 'version' configuration value")

        # convert the conditional yaml into a dictionary of column:list[{code:conditional}]
        self.codescond = {}
        codes = getattr(self, 'codes', [])
        for code_cond in codes:
            column = code_cond.get('column')
            code = code_cond.get('code')
            cond = code_cond.get('conditional', None)
            if column and code:
                if not column in self.codescond:
                    self.codescond[column] = []
                self.codescond[column].append([code, cond])
            else:
                msg = "Incomplete codes {!r}; missing required column and/or code field"
                raise Exception(msg.format(code_cond))

    def parse_sheet(self, sheet, file_name):
        """Returns parsed sheet into sslog-appropriate dicts
        :param sheet: the panda processed csv now
        :param file_name: the name of the file where the sheet came from
        """
        return self.sheet_to_sslogs(sheet)

    """ abs BaseParser """
    def can_parse(self, remote_path):
        """Returns true if this parser is able to handle files named `remote_path`, or false otherwise.
        :param remote_path: the 'remote path' to the file to process.  in the case of SMB this is the relative
        directory from the root of the mount
        """

        # the file should always come in as a path, even in the case of SMB it should contain a '/' in front of it
        _, filename = os.path.split(remote_path)

        for report_params in self.parse_options:
            if fnmatch.fnmatch(filename, report_params['report_pattern']):
                return True
        return False

    def read_csv(self, fileobject, **kwargs):
        """ Generic function to read a csv file (using Panda) with optional parameter list and returns a DataFrame

        :param fileobject: path to a file (in this case a copy of the original)
        :param kwargs: this is the 'parse' options defined in the config file
            Yaml sample 'parse_options' Configuration
            - {
                'report_pattern': '*raw*.txt',
                'localtz': 'Asia/Ho_Chi_Minh',
                'load': {'parse': {'error_bad_lines': false,
                           'header': null,
                           'names': ['Col0', 'Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7'],
                           'sep': "|",
                           'index_col': false,
                           'parse_dates': ['Col0']},
                'counter': False,
                'actions':[{'set_index': ['Col0']}]
                'regex':
                 -
                   source: STAMPED_ID_VALUE
                   target: serial
                   pattern: '^.+(\d\d\d\dH\d+)\s+$'
            }}
        ;rtype: DataFrame
        :returns: returns the panda DataFrame of the csv file
        :raises Exception: if parsing fails unexpectedly.
        """
        def safe_strptime(date_str, pattern):
            if type(date_str) not in [str, unicode]:
                return None
            try:
                return datetime.strptime(date_str, pattern)
            except ValueError as e:
                self.log.warn('Unable to parse date {}: {}'.format(date_str, e))
                return datetime(1970,1,1)

        def safe_parse_date(date_str, **kwargs):
            if type(date_str) not in [str, unicode]:
                return None
            dayfirst = kwargs.get('dayfirst', False)
            try:
                return parser.parse(date_str, dayfirst=dayfirst)
            except ValueError as e:
                self.log.warn('Unable to parse date {}: {}'.format(date_str, e))
                return None

        def safe_localize(naive_dt, local_tz):
            if pd.isnull(naive_dt):
                return naive_dt
            try:
                return local_tz.localize(naive_dt).astimezone(pytz.utc)
            except Exception as e:
                self.log.warn('Unable to convert {} to UTC: {}'.format(naive_dt, e))
                return naive_dt

        sheetparams = kwargs.get('load', {})

        # Duplicate the parsing parameters so that we can remove
        # options that pandas doesn't understand (ex. parse_dates.)
        parse_params = sheetparams.get("parse", {}).copy()
        if not parse_params.get('skiprows'):
            parse_params['skiprows'] = 0
        if kwargs.get('skiprows'):
            parse_params['skiprows'] = kwargs.get('skiprows')
        parse_dates = parse_params.pop('parse_dates', None)
        parse_timestamps = parse_params.pop('parse_timestamps', None)
        filter_nanidx = parse_params.pop('filter_nanidx', True)

        localtz = kwargs.get('localtz')
        actions = sheetparams.get("actions", [])
        counter = sheetparams.get("counter", False)
        regex = sheetparams.get("regex", [])

        if sheetparams.get('skipparse', False):
            return pd.DataFrame([{'timestamp': datetime.utcnow()}])

        self.log.info('Time to read {}, skipping {}'.format(fileobject, parse_params['skiprows']))

        try:
            if self.sheet_type == 'csv':
                sheet = pd.read_csv(fileobject, **parse_params)
            elif self.sheet_type == 'xlsx':
                sheet = pd.read_excel(fileobject, **parse_params)
            else:
                self.log.error("Unsupported sheet type for file {}: {}".format(fileobject, self.sheet_type))
                raise
        except Exception as e:
            self.log.error("File is empty or couldn't import from file. {}".format(e))
            raise

        # when read_csv is called it expects the data to be in TAB delimited format, NOT comma.  If
        # the format is wrong, instead of multiple columns, you will get 1 column string with all the column names
        # maybe un-needed check but might be helpful in debugging
        # if len(sheet.columns) == 1:
        #   self.log.error("File is improperly configured, needs to be TAB delimited")
        #   raise Exception("Invalid file format, is not tab delimited format")
        # Note: will fail the first call to sheet[column_name], with column name in the exception message

        sheet.columns = [unicode(col).strip() for col in sheet.columns]

        self.log.info('Columns for {} are {}'.format(fileobject, sheet.columns))

        if parse_dates is not None:
            for field in parse_dates:
                sheet[field] = sheet[field].apply(lambda x: safe_parse_date(x, **parse_params))
                if localtz:
                    local = pytz.timezone(localtz)
                    sheet[field] = sheet[field].apply(lambda x: safe_localize(x, local))

        if parse_timestamps is not None:
            print("Parsing the timestamps", parse_timestamps)
            for field, pattern in parse_timestamps.items():
                print("Applying the field and patterns", field, pattern)
                sheet[field] = sheet[field].apply(lambda x: safe_strptime(x, pattern))
                if localtz:
                    local = pytz.timezone(localtz)
                    sheet[field] = sheet[field].apply(lambda x: safe_localize(x, local))

        if counter:
            sheet['counter'] = pd.Series(range(len(sheet)))

        if actions:
            for action in actions:
                for functionname, params in action.items():
                    if functionname in dir(sheet) and hasattr(getattr(sheet, functionname), "__call__"):
                        if isinstance(params, dict):
                            sheet = getattr(sheet, functionname)(**params)
                        else:
                            sheet = getattr(sheet, functionname)(*params)

                        self.log.info("Performed {} on sheet".format(functionname))

        if regex:
            for substitution in regex:
                target = substitution.get('target')
                pattern = substitution.get('pattern')
                source = substitution.get('source')

                if target is None or pattern is None or source is None:
                    # Should this trigger an exception?
                    raise Exception("Incomplete regex configuration; missing target, pattern, or source")
                else:
                    src_column = None
                    try:
                        src_column = sheet[source]
                    except Exception as e:
                        self.log.error('Invalid regex configuration source {}, doesnt exists in table.'
                                       'Possibly missing header index or invalid sep value?'
                                       ' Exception:{}'.format(source, e))
                        raise

                    if src_column is None:
                        self.log.error('Invalid regex configuration, skipping')
                    else:
                        sheet[target] = src_column.apply(lambda x: re.sub(pattern,
                                                                         substitution.get("repl", "\\1"),
                                                                         x) if type(x) == str else x)
        self.buildCodesCond()
        #technically, this will get executed the first time codes render, so is unnecessary
        #but putting it here because all the unit tests look to read_csv for config compliance

        if filter_nanidx:
            sheet = sheet[map(lambda x: not pd.isnull(x), sheet.index)]

        self.log.info('Read sheet has {} lines'.format(len(sheet)))

        return sheet

    def find_first_new_row(self, filename, old_path, new_path):
        with open(old_path, 'rb') as old_fp, open(new_path, 'rb') as new_fp:
            for i, (old_line, new_line) in enumerate(izip_longest(old_fp, new_fp)):
                if old_line is None and new_line is not None:
                    return i
                if new_line is None:
                    self.log.warning('"%s" has been truncated on line %s',
                                     filename, i)
                    return -1
                if old_line.strip() != new_line.strip():
                    self.log.warning('Unexpected change in "%s" on line %s:\n- %r\n+ %r',
                                     filename, i, old_line, new_line)
                    return i
        return -1

    def check_old_version(self, filename, old_path, new_path):
        self.log.info('Previously processed {}'.format(filename))
        if compare_file_stat(old_path, new_path) != 0:
            start_diff = self.find_first_new_row(filename, old_path, new_path)
            if start_diff >= 0:
                self.log.info('Diff in "%s" starts from line %s', filename, start_diff)
                return start_diff
            return -1
        return 0

    def parse(self, remote_path, local_path, completed_path):
        """Returns a list of sslogs parsed from a single file.

        :param remote_path: original path to the file on the remote system.
        :param local_path: path to a local destination file containing the parsed contents of the
            remote file. The name is not necessarily related to `remote_path`.
        :param completed_path: path to a file containing the prior parsed contents of the
            last file at `remote_path` that was parsed, or None if this is the
            first time that the remote path has been seen.
        ;rtype: list
        :returns: a list of dicts representing sslogs.  See comment in base.py
        :raises Exception: if parsing fails unexpectedly.
        """

        _, filename = os.path.split(remote_path)

        for report_params in self.parse_options:
            # only parse using the correct params that match the filename
            if not fnmatch.fnmatch(filename, report_params['report_pattern']):
                continue

            # determine in the last parsed 'csv' file which lines should be excluded
            if completed_path is not None:
                append = self.check_old_version(filename, completed_path, local_path)
            else:
                append = None

            if append > 0:
                offset = 0
                load_opt = self.parse_options[0].get('load')
                if load_opt:
                    load_parse_opt = load_opt.get('parse')
                    if load_parse_opt:
                        if load_parse_opt.get('header'):
                            offset = load_parse_opt.get('header')+1
                skip = [i for i in range(offset, append)]
                self.log.info('Setting skip to {} based on diff at {} and offset {}'.format(skip,append,offset))
            elif append == -1:
                self.log.info('File {} already parsed, skipping it.'.format(remote_path))
                continue
            else:
                skip = None

            try:
                parsed_data = self.read_csv(local_path, skiprows=skip, **report_params)
            except Exception as e:
                self.log.error('Failed to read_csv data from file {}. {}'.format(
                    remote_path, e))
                raise

            if parsed_data.empty:
                # when the frame is empty, just ignore the data, return as if it succeeded (sslogs will be empty)
                self.log.warning('Failed to read_csv data from file {}. Data is empty'.format(remote_path))
                return

            try:
                sslogs = self.parse_sheet(parsed_data, remote_path)
                if sslogs is not None:
                    for sslog in sslogs:
                        yield sslog
            except Exception as e:
                self.log.error('Failed to parse_sheet data from file {}. {}'.format(
                    remote_path, e))
                raise

    def sheet_to_sslogs(self, sheet, inc_rec_columns = []):
        """ Takes a sheet and processes it to a list of sslogs
                :param DataFrame sheet: parsed sheet
                :param list inc_rec_columns: a list of columns to include as part of the sslogentry from the record
                :rtype: list
                :returns: a list of dicts representing sslogs.  See comment in base.py
                :raises Exception: none
        """
        hascounter = 'counter' in sheet.columns
        hasserial = 'serial' in sheet.columns
        hasbatch = 'batch_no' in sheet.columns
        hasrunning = 'running' in sheet.columns
        hassslog_type = 'sslog_type' in sheet.columns
        hassource = 'source' in sheet.columns

        # sanity check on data
        if len(sheet) > 0:
            row = sheet[0:0 + 1].dropna(axis=1)
            if not isinstance(row.index[0], datetime):
                raise Exception("Spreadsheet parser requires sheet data first row to be a valid datetime column.\
                                Try set_index on a datetime column.")

        for idx in xrange(0, len(sheet)):

            row = sheet[idx:idx + 1].dropna(axis=1)
            record = row.to_dict(orient="records")[0]
            if type(row.index[0]) == datetime:
                timestamp = row.index[0]
            else:
                try:
                    timestamp = row.index[0].to_datetime()
                except Exception as e:
                    self.log.warn('Unable to convert timestamp {}: {}'.format(row.index[0], e))
                    timestamp = datetime(1970, 1, 1)


            sslogentry = {"source": self.source,
                          "configuration": {"version": self.version},
                          "timestamp": timestamp,
                          "fieldvalues": {k.replace(".", "_"): {"value": v, 'units': None}
                                          for k, v in record.items()
                                          if type(v) != float or (type(v) == float and not math.isnan(v))}}

            if hascounter:
                sslogentry['counter'] = int(record['counter'])

            if hasserial and record.get('serial'):
                sslogentry['serial'] = [record['serial']]

            if hasbatch and record.get('batch_no'):
                if not isinstance(record['batch_no'], list):
                    record['batch_no'] = [str(record['batch_no'])]
                sslogentry['batch_no'] = record['batch_no']

            if hasrunning:
                sslogentry['running'] = record['running']

            if hassource:
                sslogentry['source'] = record.pop('source')

            if hasattr(self, 'sslog_type'):
                sslogentry['sslog_type'] = self.sslog_type
            elif hassslog_type:
                sslogentry['sslog_type'] = record['sslog_type']

            if inc_rec_columns:
                for inc_column in inc_rec_columns:
                    if inc_column in sheet.columns:
                        sslogentry[inc_column] = record[inc_column]

            self.applyCodes(sslogentry, record, use_fieldvalues = False)

            yield sslogentry
