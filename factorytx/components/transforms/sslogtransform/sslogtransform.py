import time
import math
import os
import re
import fnmatch
import json
import threading
import filecmp
from bson import ObjectId
import pandas as pd
from factorytx import utils
import pytz
import logging
from factorytx.managers.GlobalManager import global_manager

from dateutil import parser
from datetime import datetime, timedelta
from factorytx.components.transforms.transformbase import TransformBase

global_manager = global_manager()

log = logging.getLogger(__name__)

class CodesMixin(object):
    """CodesMixin allows for both plugins and parsers to have "codes" functionality


    Note that each individual plugin needs to still have the codes block in its schema
    """
    codescond = None

    def getRecordValue(self, record, field, use_fieldvalues):
        if not use_fieldvalues:
            return record.get(field, None)

        return record.get('fieldvalues', {}).get(field, {}).get('value', None)


    def buildCodesCond(self):
        # convert the conditional yaml into a dictionary of column:list[{code:conditional}]
        self.codescond = {}
        if hasattr(self, 'codes'):
            for code_cond in self.codes:
                column = code_cond.get('column')
                code = code_cond.get('code')
                cond = code_cond.get('conditional', None)
                if column and code:
                    if not column in self.codescond:
                        self.codescond[column] = []
                    self.codescond[column].append([code, cond])
                else:
                    raise Exception("Incomplete codes; missing required column and/or code field")

    def applyCodes(self, sslogentry, record, use_fieldvalues = True):
        #apply the code conditionals from a raw data record into an sslog{codes} format
        if self.codescond == None:
            self.buildCodesCond()

        if not self.codescond:
            return

        codes = {}
        for code_column, code_cond_list in self.codescond.items():
            # check if the column even exists
            if self.getRecordValue(record, code_column, use_fieldvalues) == None:
                continue

            for code_cond_item in code_cond_list:
                code = code_cond_item[0]
                cond = code_cond_item[1]
                # check if the column is a 0/1
                if not cond:
                    # conditional doesnt exists
                    if self.getRecordValue(record, code_column, use_fieldvalues):
                        codes.update({code: 1})
                    else:
                        codes.update({code: 0})
                else:
                    try:
                        # format '{0} conditional string'
                        evalstr = cond.format(self.getRecordValue(record, code_column, use_fieldvalues))

                        # 0/False returns 0, everything else is considered True/1
                        if simple_eval(evalstr) == 0:
                            codes.update({code: 0})
                        else:
                            codes.update({code: 1})
                    except SyntaxError as se:
                        self.log.error('Syntax error on conditional expression {}. {}'.format(evalstr, se))
                        code_cond_item[1] = None
                        raise
                    except ValueError as ve:
                        self.log.error('Value error on conditional expression {}. {}'.format(evalstr, ve))
                        code_cond_item[1] = None
                        raise
                    except Exception as e:
                        self.log.error('Exception on conditional expression {}. {}'.format(evalstr, e))
                        code_cond_item[1] = None
                        raise

                # why is there duplicate codes printed???
                sslogentry['codes'] = codes
                sslogentry['defect_codes'] = codes

class SSLogTransform(TransformBase, CodesMixin):

    __version__ = '1.0.0'
    logname = "SSLogTransform"
    log = log

    counter = 1

    def apply_postprocessing(self, frame):
        processed_data = self.frame_to_sslogs(frame)
        merged_data = (self._create_shared_data_record(data)
                       for data in processed_data if data != None)
        filtered_data = filter(self._filter_only_new, merged_data)
        dump_data = []
        for chunk in utils.chunks(filtered_data, self.records_per_file):
            combined_data = self._combine_data(chunk)
            combined_data = self._serialize_timestamp_key(combined_data)
            dump_data += [combined_data]
            #if not os.path.exists(self.outputdirectory):
            #    os.makedirs(self.outputdirectory)
            #  
            #timestamp = min(d['timestamp'] for d in combined_data.values())
            #guid = utils.make_guid()
            #fname = '_'.join((timestamp, self._getSource(), guid))
            #fname = os.path.join(self.outputdirectory, fname)
            #dst_fname = fname + '.sm.json'
            #tmp_fname = fname + '.jsontemp'
            #if os.name == 'nt':
            #    fname = fname.replace(":", "_")
            #
            #try:
            #    with open(tmp_fname, 'wb') as f:
            #        f.write(json_data)
            #    # rename .jsontemp to .sm.json
            #    os.rename(tmp_fname, dst_fname)
            #except Exception as e:
            #    log.error('Failed to save data into {} {} {}'.format(
            #        self.outputdirectory, fname, e))
            #    raise
            #else:
            #    log.info('Saved data into {}'.format(fname))
        return dump_data

    def applyCodes(self, sslogentry, record, use_fieldvalues = True):
        #apply the code conditionals from a raw data record into an sslog{codes} format
        if self.codescond == None:
            self.buildCodesCond()

        if not self.codescond:
            return

        codes = {}
        for code_column, code_cond_list in self.codescond.items():
            # check if the column even exists
            if self.getRecordValue(record, code_column, use_fieldvalues) == None:
                continue

            for code_cond_item in code_cond_list:
                code = code_cond_item[0]
                cond = code_cond_item[1]
                # check if the column is a 0/1
                if not cond:
                    # conditional doesnt exists
                    if self.getRecordValue(record, code_column, use_fieldvalues):
                        codes.update({code: 1})
                    else:
                        codes.update({code: 0})
                else:
                    try:
                        # format '{0} conditional string'
                        evalstr = cond.format(self.getRecordValue(record, code_column, use_fieldvalues))

                        # 0/False returns 0, everything else is considered True/1
                        if simple_eval(evalstr) == 0:
                            codes.update({code: 0})
                        else:
                            codes.update({code: 1})
                    except SyntaxError as se:
                        self.log.error('Syntax error on conditional expression {}. {}'.format(evalstr, se))
                        code_cond_item[1] = None
                        raise
                    except ValueError as ve:
                        self.log.error('Value error on conditional expression {}. {}'.format(evalstr, ve))
                        code_cond_item[1] = None
                        raise
                    except Exception as e:
                        self.log.error('Exception on conditional expression {}. {}'.format(evalstr, e))
                        code_cond_item[1] = None
                        raise

                # why is there duplicate codes printed???
                sslogentry['codes'] = codes
                sslogentry['defect_codes'] = codes


    # Purpose: if 'only_if_new' is set, remove this entry if not new
    def _filter_only_new(self, data):
        if hasattr(self, 'only_if_new'):
            fieldvals = data.values()[0].get('fieldvalues', {})
            if not fieldvals:
                fieldvals = {}
            if self.lastvalue == fieldvals.get(
                    self.only_if_new, {}).get('value'):
                # Nope, same as last time... nothing to do here
                log.info('No new {} received for {}.  Not writing log file.'
                         ''.format(self.only_if_new, self._getSource()))
                return False
            elif fieldvals.get(self.only_if_new, {}).get('value') is None:
                log.info('Required field {} is None.  Not writing log file.'
                         ''.format(self.only_if_new))
                return False
            else:
                # The value has changed.  Record new value.
                self.lastvalue = fieldvals.get(
                    self.only_if_new, {}).get('value')
        return True

    # Purpose: ensure records do not have the same timestamp, so increment by 1ms from any duplicates
    def _combine_data(self, data):
        cdata = {}
        for d in data:
            for ts, value in d.items():
                while ts in cdata:
                    # add 1 microsecond in case if there is such
                    # key in combined data
                    ts += timedelta(microseconds=1) #add
                cdata[ts] = value
        return cdata

    # Purpose: converts
    def _serialize_timestamp_key(self, data,
                                 strf_pttrn='%Y-%m-%dT%H:%M:%S.%f'):
        serialized_data = {}
        for ts, value in data.items():
            value['timestamp'] = value['timestamp'].strftime(strf_pttrn)
            serialized_data[ts.strftime(strf_pttrn)] = value
        return serialized_data

    def _getSource(self):
        return self.source if hasattr(self, 'source') else "Unknown"

    # Purpose: modifies each sslog record to ensure consistency, ID, and other fields
    def _create_shared_data_record(self, data):
        merged_data = {}
        global_manager_dict = global_manager.get_dict()
        if hasattr(self, 'shared_sources'):
            dependency_dict = {}
            for dependency_key in self.shared_sources:
                if dependency_key in global_manager_dict:
                    dependency = global_manager_dict[dependency_key]

                    if dependency:
                        first_item = dependency

                        mappings = self.shared_sources[dependency_key]
                        for src, dest in mappings.items():
                            dependency_dict[dest] = first_item.get(src, None)
            merged_data = self.makeDictTree(dependency_dict)

            for k, v in merged_data.items():
                # it's possible after the merge that things
                # have moved "up" from fieldvalues
                try:
                    merged_data[k] = v['value']
                except:
                    pass

        merged_data = utils.combineDicts(data, merged_data)
        if merged_data.get('source'):
            if merged_data['source'] not in self._getSource().split(","):
                log.warning("Source received from plugin: {} is not specified among sources: {}".format(merged_data['source'], self._getSource()))
        else:
            merged_data['source'] = self._getSource()
        merged_data['_id'] = str(ObjectId())

        self.applyCodes(merged_data, data)

        if 'counter' not in merged_data:
            if 'counter' in merged_data.get('fieldvalues', {}).keys():
                merged_data['counter'] = merged_data['fieldvalues']['counter'].get('value')
                self.counter = merged_data['fieldvalues']['counter'].get('value')
            else:
                merged_data['counter'] = self.counter
                self.counter += 1
                if self.counter > 100000000:
                    self.counter = 1

        if 'timestamp' not in merged_data and 'timestamp' in merged_data.get('fieldvalues', {}).keys():
            merged_data['timestamp'] = merged_data['fieldvalues']['timestamp']['value']

        if 'serial' not in merged_data and 'serial' in merged_data.get('fieldvalues', {}).keys():
            serial = merged_data['fieldvalues']['serial']
            merged_data['serial'] = serial.get('value')

        if hasattr(self, 'sslog_type'):
            merged_data['sslog_type'] = self.sslog_type

        return {merged_data['timestamp']: merged_data}

    def frame_to_sslogs(self, frame, inc_rec_columns = []):
        """ Takes a frame and processes it to a list of sslogs
                :param DataFrame frame: parsed sheet
                :param list inc_rec_columns: a list of columns to include as part of the sslogentry from the record
                :rtype: list
                :returns: a list of dicts representing sslogs.  See comment in base.py
                :raises Exception: none
        """
        hascounter = 'counter' in frame.columns
        hasserial = 'serial' in frame.columns
        hasbatch = 'batch_no' in frame.columns
        hasrunning = 'running' in frame.columns
        hassslog_type = 'sslog_type' in frame.columns
        self.log.debug("The counter is %s", frame['counter'])

        # sanity check on data
        if len(frame) > 0:
            row = frame[0:0 + 1].dropna(axis=1)
            self.log.info("The frame is %s", list(row))
            self.log.info("The first row of data is %s", row.columns.values)
            if not isinstance(row.index[0], datetime):
                raise Exception("Spreadsheet parser requires sheet data first row to be a valid datetime column.\
                                Try set_index on a datetime column.")

        for idx in range(0, len(frame)):

            row = frame[idx:idx + 1].dropna(axis=1)
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
                try:
                    sslogentry['counter'] = int(record['counter'])
                except Exception as e:
                    print("The problem is %s, with exception %s", record, row, e)
                    raise

            if hasserial and record.get('serial'):
                sslogentry['serial'] = [record['serial']]

            if hasbatch and record.get('batch_no'):
                if not isinstance(record['batch_no'], list):
                    record['batch_no'] = [str(record['batch_no'])]
                sslogentry['batch_no'] = record['batch_no']

            if hasrunning:
                sslogentry['running'] = record['running']

            if hasattr(self, 'sslog_type'):
                sslogentry['sslog_type'] = self.sslog_type
            elif hassslog_type:
                sslogentry['sslog_type'] = record['sslog_type']

            if inc_rec_columns:
                for inc_column in inc_rec_columns:
                    if inc_column in frame.columns:
                        sslogentry[inc_column] = record[inc_column]

            # TODO: Figure out if we need the dataplugin addon
            if self.codescond:
                codes = {}
                for code_column, code_cond_list in self.codescond.items():
                    # check if the column even exists
                    if record.get(code_column):
                        for code_cond_item in code_cond_list:
                            code = code_cond_item[0]
                            cond = code_cond_item[1]
                            # check if the column is a 0/1
                            if not cond:
                                # conditional doesnt exists
                                if record[code_column]:
                                    codes.update({code: 1})
                                else:
                                    codes.update({code: 0})
                            else:
                                try:
                                    # format '{0} conditional string'
                                    evalstr = cond.format(record[code_column])

                                    # 0/False returns 0, everything else is considered True/1
                                    if simple_eval(evalstr) == 0:
                                        codes.update({code: 0})
                                    else:
                                        codes.update({code: 1})
                                except SyntaxError as se:
                                    self.log.error('Syntax error on conditional expression {}. {}'.format(evalstr, se))
                                    code_cond_item[1] = None
                                    raise
                                except ValueError as ve:
                                    self.log.error('Value error on conditional expression {}. {}'.format(evalstr, ve))
                                    code_cond_item[1] = None
                                    raise
                                except Exception as e:
                                    self.log.error('Exception on conditional expression {}. {}'.format(evalstr, e))
                                    code_cond_item[1] = None
                                    raise

                # why is there duplicate codes printed???
                sslogentry['codes'] = codes
                sslogentry['defect_codes'] = codes

            yield sslogentry

