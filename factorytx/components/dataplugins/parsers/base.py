import abc
import logging

from factorytx.utils import merge_schema_defaults


class BaseParser(object):
    """Base class for file parsers, which are responsible for transforming files
    into sslogs.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(BaseParser, self).__init__()

    def loadParameters(self, schema, conf):
        if conf is None:
            conf = {}
        self.__dict__.update(conf)
        merge_schema_defaults(schema, self.__dict__)

    def setup_log(self, logname):
        self.log = logging.getLogger(self.plugin_type + ': ' + logname)

    @abc.abstractmethod
    def can_parse(self, remote_path):
        """Returns true if this parser is able to handle files named
        `remote_path`, or false otherwise.

        """
        pass

    @abc.abstractmethod
    def parse(self, remote_path, local_path, completed_path):
        """Returns a list of sslogs parsed from a single file.

        :param remote_path: original path to the file on the remote system.
        :param local_path: path to a local file containing the contents of the
            remote file. The name is not necessarily related to `remote_path`.
        :param completed_path: path to a file containing the contents of the
            last file at `remote_path` that was parsed, or None if this is the
            first time that the remote path has been seen.
        :returns: an iterable of dicts representing sslogs.  See comment below.
        :raises Exception: if parsing fails unexpectedly.

        Example of sslogs[] = {dict{'timestamp':{datetime}}.  Must have a 'timestmap' field in {datetime} format
        sslogs = {list} <type 'list'>: [{'timestamp': datetime.datetime(2016, 2, 25, 7, 9, 6), 'counter': -1, 'source': 'T2_VT_OscillatingKnife_4', 'running': 0.0, 'configuration': {'version': '1.0.0'}, 'fieldvalues': {'Description': {'units': None, 'value': 'ON'}, 'counter': {'u
 __len__ = {int} 56
            00 = {dict} {'timestamp': datetime.datetime(2016, 2, 25, 7, 9, 6), 'counter': -1, 'source': 'T2_VT_OscillatingKnife_4', 'running': 0.0, 'configuration': {'version': '1.0.0'}, 'fieldvalues': {'Description': {'units': None, 'value': 'ON'}, 'counter': {'units': None, 'va
  __len__ = {int} 6
                'configuration' (139917509123800) = {dict} {'version': '1.0.0'}
                'counter' (139917579036304) = {int} -1
                'fieldvalues' (139917523357584) = {dict} {'Description': {'units': None, 'value': 'ON'}, 'counter': {'units': None, 'value': -1}, 'End Time': {'units': None, 'value': Timestamp('2016-02-25 07:09:06')}, 'running': {'units': None, 'value': 0.0}, 'Activity': {'units': None, 'value': 'MACHINE_ON_OFF'
                'running' (139917509148320) = {float} 0.0
                'source' (139917604669024) = {str} 'T2_VT_OscillatingKnife_4'
                'timestamp' (139917567996336) = {datetime} 2016-02-25 07:09:06
        """
        pass
