import os
import datetime
import time
import xml.etree.ElementTree as ET
from json import JSONEncoder

from FactoryTx.parsers.base import BaseParser


class PollIPCParser(BaseParser):

    def __init__(self):
        super(PollIPCParser, self).__init__()

    def parse_xml(self, file_name):
        try:
            tree = ET.parse(file_name)
            root = tree.getroot()
            jsondata = dict()
            for child in root:
                jsondata[child.tag] = {}
                if self.xml:
                    # Custom int/float cleansing required
                    if child.tag in self.xml['fields'].keys():
                        val = child.text
                        if self.xml['fields'][child.tag] == 'int':
                            val = int(val)
                        elif self.xml['fields'][child.tag] == 'float':
                            val = float(val)
                        jsondata[child.tag]['value'] = val
                        jsondata[child.tag]['units'] = child.attrib.get('unit',"")
                    else:
                        jsondata[child.tag]['value'] = child.text
                        jsondata[child.tag]['units'] = child.attrib.get('unit',"")
                    if child.get('unit', None) == "File":
                        jsondata[child.tag]['value'] = self.source + "_" + \
                                                       jsondata[child.tag]['value']

            cur_time = self.file_creation_time_stamp(file_name)
            ret = {
                'source': self.source,
                'version': self.version,
                'plugin': "pollipc",
                'fieldvalues': jsondata,
                'timestamp': cur_time,
                'counter': self._get_counter_from_xml(jsondata, file_name),
                'running': self.runningfield,
                'isXMLGenerated': True,
                'isBinary': True,
                'original_filename': file_name,
                'outputdirectory': self.outputdirectory,
                'filename': file_name
            }
            finaljson = dict()
            finaljson[cur_time] = ret

            basefile = os.path.join(self.outputdirectory, self.source + '_' + cur_time)
            json_string = JSONEncoder().encode(finaljson)
            self.log.info('Creating JSON log: {}'.format(basefile))
            with open(basefile + '.tmp', 'w') as obj:
                obj.write(json_string)

            os.rename(basefile + '.tmp', basefile + "_" +
                      os.urandom(16).encode('hex') + '.sm.json')
            return True

        except Exception as e:
            self.log.exception('XML File failed to parse to json. '
                                 'Please check setup configuration '
                                 'or incoming data format change. {}'.format(e))

    @staticmethod
    def file_creation_time_stamp(cur_file):
        ts = datetime.datetime.fromtimestamp(os.path.getmtime(cur_file)).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return ts

    @staticmethod
    def time_stamp():
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f')
        return str(st)

    @staticmethod
    def tsplit(string, delimiters):
        """Behaves str.split but supports multiple delimiters."""
        delimiters = tuple(delimiters)
        stack = [string]

        for delimiter in delimiters:
            for i, substring in enumerate(stack):
                substack = substring.split(delimiter)
                stack.pop(i)
                for j, _substring in enumerate(substack):
                    stack.insert(i+j, _substring)
        return stack

    def split_filename_metadata(self, basefilename):
        return self.tsplit(basefilename, self.binaryDelimiter)

    def _get_counter_from_xml(self, jsondata, file_name):
        if self.counterfield['field'] in jsondata:
            return int(jsondata[self.counterfield['field']]['value'])
        else:
            filemetadata = {}
            self._populate_filename_metadata_parts(filemetadata, file_name)
            return int(filemetadata.get(self.counterfield['field'], -1))

    def _populate_filename_metadata_parts(self, jsondata, file_name):
        extensions = file_name.split(os.extsep)
        basefile = os.path.basename(extensions[0])
        fileparts = self.split_filename_metadata(basefile)

        for cur_part in self.filenamemetadata:
            try:
                jsondata[cur_part['name']] = fileparts[cur_part['address']]
            except Exception as e:
                self.log.warn('Image metadata update mismatch. '
                              'Check IPC filename structure '
                              'against configuration. {}'.format(e))
                pass

        if self.counterfield:
            try:
                jsondata['counter'] = int(jsondata[self.counterfield['field']])
            except Exception as e:
                self.log.warn('Image counter update mismatch. '
                              'Check IPC filename structure '
                              'against configuration. {}'.format(e))
                pass

    def write_data_json(self, file_name):
        cur_time = self.file_creation_time_stamp(file_name)
        jsondata = {
            'source': self.source,
            'version': self.version,
            'plugin': "pollipc",
            'timestamp': cur_time,
            'original_filename': file_name,
            'outputdirectory': self.outputdirectory,
            'filename': file_name,
            'isBinary': True
        }

        self._populate_filename_metadata_parts(jsondata, file_name)

        basefile = os.path.join(self.outputdirectory, self.source + '_' +
                                cur_time + "_" + os.urandom(16).encode('hex'))
        json_string = JSONEncoder().encode(jsondata)
        self.log.info('Creating JSON log: {}'.format(basefile))
        with open(basefile + '.tmp', 'w') as obj:
            obj.write(json_string)
        os.rename(basefile + '.tmp', basefile + '.sm.json')
        return True

    def process(self, fn):
        _, extension = os.path.splitext(fn)

        ret = False
        if extension.lower() not in ['.tmp', '.db']:
            if extension.lower() == '.xml':
                ret = self.parse_xml(fn)
            else:
                # If not xml, write corresponding json metadata for uploading generic binary file
                ret = self.write_data_json(fn)
        return True if ret else False
