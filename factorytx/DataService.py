import abc
import os
import tempfile
import logging

from factorytx.Global import setup_log

class DataService(object):
    '''
    DataService is a Base Class for the many various services to extend
    being a base class it enforces particular functions to be implemented
    by subclass as they are used in the ServiceManager.

    Subclasses will be responsible for the handling of data, this class is
    just used to handle monitoring of services and alerting if they are
    having issues.
    '''
    __metaclass__ = abc.ABCMeta
    __version__ = '0.1'

    ##########################################################################
    # Abstract Property declarations
    ##########################################################################

    @abc.abstractproperty
    def __version__(self):
        '''
        This is an abstract property that must be overridden by subclass
        '''
        pass

    @abc.abstractproperty
    def name(self):
        '''
        This is an abstract property that must be overridden by subclass
        '''
        pass

    @abc.abstractproperty
    def connected(self):
        '''
        This is an abstract property that must be overridden by subclass
        '''
        pass

    ###########################################################################
    # Abstract Method declarations
    # These methods have to be overriden by the subclass
    ###########################################################################

    @abc.abstractmethod
    def connect(self):
        '''
        This is an abstract method that must be overridden by subclasss
        '''
        pass

    @abc.abstractmethod
    def reconnect(self):
        '''
        This is an abstract method that must be overridden by subclass
        '''
        pass

    # These enforce having to use either a process or a thread with the service
    @abc.abstractmethod
    def start(self):
        '''
        This is an abstract method that must be overridden by subclass
        '''
        pass

    @abc.abstractmethod
    def is_alive(self):
        '''
        This is an abstract method that must be overridden by subclass
        '''
        pass

    @abc.abstractmethod
    def join(self):
        '''
        This is an abstract method that must be overridden by subclass
        '''
        pass

    ###########################################################################
    # Standard function defintions - These are optional overrides
    ###########################################################################

    def insert_pipes(self, pipes):
        if len(pipes) == 2:
            self.in_pipe = pipes[0]
            self.out_pipe = pipes[1]
        elif len(pipes) == 1:
            self.pipe = pipes[0]

    def _load_plugin(self, manager, cfg):
        if 'config' in cfg:
            cfg['config'].update({'source': self.name})
            obj = manager.get_plugin(cfg['type'])()
            schema = manager.get_plugin_schema(cfg['type'], cfg['config']['version'])
            obj.loadParameters(schema, cfg['config'])
        return obj

    def loadParameters(self, sdconfig, schema, conf):
        '''
        This function will load up parameters and append that particular config
        to the object as variables
        '''
        self.__dict__.update(conf)
        if not schema: schema = {}
        for key, value in schema.get('properties', {}).items():
            if value.get('default', None) != None:
                if ((not hasattr(self, key))
                        or (hasattr(self, key)
                            and getattr(self, key) == None)):
                    setattr(self, str(key), str(value.get('default')))

        plugin_name = str(self.__class__).split('.')[-2]  # Strip plugin name
        if hasattr(self, 'source'):
            plugin_name = "{}-{}".format(plugin_name, self.source)
        self.log = setup_log(plugin_name, conf['log_level'])
        self.root_dir = sdconfig.get('plugins', {}).get('data')
