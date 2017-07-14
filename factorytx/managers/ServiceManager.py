""" This is the logic for the service manager and associated code.

"""
import os
import logging
import time

from multiprocessing import Manager
from factorytx.utils import lock_dict
from factorytx.Config import get_config
from factorytx.managers.GlobalManager import global_manager
from factorytx.managers.PluginManager import component_manager

if os.name == 'nt':
    import win32api
    import win32con
    import win32job

LOG = logging.getLogger(__name__)
CONFIG = get_config()
GLOBAL_MANAGER = global_manager()
components = component_manager()
PLUGIN_MANAGER = components['dataplugins']


class ServiceManager(object):
    """
    This is the service manager. It's main purpose is to start-up plugins
    that are defined based around a Abstract Base Class. The service
    requires the plugins to use the base class as it enforces some structure,
    version control, and forces the plugin to run as a process or thread.
    """
    __version__ = '0.1'

    def __init__(self):
        super(ServiceManager, self).__init__()
        self.poll_loop = True
        self.services = []
        self.manager = Manager()
        self.transform_queue = self.manager.Queue()
        self.tx_queue = self.manager.Queue()
        self.callback_queue = self.manager.dict()

    def set_logging(self, level):
        self.log_level = level

    def start_services(self):
        '''
        This function starts up services in the defined in the config file
        '''
        self.services = []

        for i, (component, plgn_type, _) in enumerate(CONFIG.plugin_conf_list):
            LOG.info("Starting a %s service of the component %s", plgn_type, component)
            threads = self.generate_threads(component)
            plgn = self.load_service(i, self.log_level)

            # Start plugin process
            time.sleep(1)
            self.start_service(plgn, threads)

            if hasattr(plgn, 'pid'):
                LOG.info('The process id for service (%s-%s) is: %s', plgn_type, plgn, plgn.pid)

            self.services.append((i, plgn_type, plgn))

    @staticmethod
    def load_service(index, log_level):
        """ Given the index of the service to load, goes and loads that service.

        :param int index: The index of the service to load
        :returns: The desired plugin.
        """
        LOG.debug("Loading the service %s", index)
        category, plgn_type, plgn_cfg = CONFIG.plugin_conf_list[index]
        manager = components[category]
        if 'version' in plgn_cfg:
            plgn_ver = plgn_cfg['version']
            plgn_schema = manager.get_plugin_schema(plgn_type, plgn_ver)
            plugin_cls = manager.get_plugin(plgn_type)
        else:
            plgn_ver = 'None'
            plgn_schema = manager.get_plugin_schema(category, plgn_ver)
            plugin_cls = manager.get_plugin(category)
        plgn_cfg['log_level'] = log_level
        print("The plugin is", plugin_cls)
        plgn = plugin_cls()
        plgn.load_parameters(CONFIG, plgn_schema, plgn_cfg)
        return plgn

    @staticmethod
    def start_service(plgn, threads=[]):
        """ Given a plugin, starts it.

        :param plgn: The plugin to start.
        """
        if not threads:
            plgn.start()
        else:
            plgn.insert_pipes(threads)
            plgn.start()

    def poll(self):
        '''
        This service checks on the various services it has started so
        it can cleanly exit when done
        '''
        while self.poll_loop:
            for i, name, service in self.services:
                if not service.is_alive():
                    LOG.error('%s service has died.', service)
                    try:
                        service.stop()
                    except:
                        pass
                    service.join(1)

                    LOG.info('Restarting service "%s"', service)
                    category = CONFIG.plugin_conf_list[i][0]
                    threads = self.generate_threads(category)
                    service = self.load_service(i, self.log_level)
                    self.start_service(service, threads)
                    self.services[i] = i, name, service

            time.sleep(1)

    def generate_threads(self, category):
            transform_queue = self.transform_queue
            tx_queue = self.tx_queue
            callback_queue = self.callback_queue
            threads = []
            if category == 'transforms':
                threads += [transform_queue, tx_queue]
            elif category == 'tx':
                threads += [tx_queue, callback_queue]
            elif category == 'dataplugins':
                threads += [callback_queue, transform_queue]
            return threads

class Win32ServiceManager(ServiceManager):
    """ Adds some functionality required on win32 to terminate services
        when factorytx does
    """

    def __init__(self):
        super(Win32ServiceManager, self).__init__()

        # windows only, create a process job
        # that will terminate when this one does
        self.hjob = win32job.CreateJobObject(None, "")
        extended_info = win32job.QueryInformationJobObject(
            self.hjob,
            win32job.JobObjectExtendedLimitInformation)
        bli = extended_info['BasicLimitInformation']
        bli['LimitFlags'] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        win32job.SetInformationJobObject(
            self.hjob,
            win32job.JobObjectExtendedLimitInformation,
            extended_info)

        LOG.info("Created WinNT job for child processes")

    def start_services(self):
        perms = win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA
        process = win32api.OpenProcess(perms, False, GLOBAL_MANAGER.pid)
        win32job.AssignProcessToJobObject(self.hjob, process)

        super(Win32ServiceManager, self).start_services()

    def load_service(self, index):
        plgn = super(Win32ServiceManager, self).load_service(index)

        # sanitize for Win32's fragile faux-fork
        # via pushing pickled context over POpen
        plgn.log = None
        plgn.__global_dict = GLOBAL_MANAGER.get_dict()
        return plgn

    def start_service(self, plgn):
        """ Given a plugin, goes ahead and starts the service for the plugin.

        """
        super(Win32ServiceManager, self).start_service(plgn)

        if hasattr(plgn, 'pid'):
            perms = win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA
            process = win32api.OpenProcess(perms, False, plgn.pid)
            win32job.AssignProcessToJobObject(self.hjob, process)


if os.name == 'nt':
    SERVICE_MANAGER = Win32ServiceManager()
else:
    SERVICE_MANAGER = ServiceManager()
def service_manager():
    """ Returns my service manager.

    """
    return SERVICE_MANAGER
