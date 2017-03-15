import contextlib
import os
import sys
import signal
import logging
import tempfile
import hashlib
import multiprocessing

from factorytx.Config import get_config
from factorytx.managers.GlobalManager import global_manager
from factorytx.managers.SignalManager import signal_manager
from factorytx.managers.PluginManager import component_manager
from factorytx.managers.ServiceManager import service_manager
from factorytx import locks

log = logging.getLogger(__name__)
config = get_config()
components = component_manager()
global_manager = global_manager()
signal_manager = signal_manager()
service_manager = service_manager()
dataplugin_manager = components['dataplugins']
parser_manager = components['parsers']
transport_manager = components['transports']
transformation_manager = components['transforms']
filter_manager = components['filters']
tx_manager = components['tx']

if os.name == 'nt':
    default_logfolder = "logs\\"
else:
    default_logfolder = "/var/log/sightmachine/factorytx"

def global_state():
    """ Retrives my global state.

    """
    managers = {'global_manager': global_manager,
                'signal_manager': signal_manager,
                'transport_manager': transport_manager,
                'service_manager': service_manager,
                'plugin_manager': dataplugin_manager,
                'transformation_manager': transformation_manager,
                'tx_manager': tx_manager,
                'filter_manager': filter_manager,
                'parser_manager': parser_manager}
    return managers

def init_logger(log_level='INFO'):
    fmt = '%(asctime)s - %(levelname)s - %(name)-12s - %(message)s'
    log_level = getattr(logging, log_level)
    logging.basicConfig(level=log_level, datefmt='%Y-%m-%d %H:%M:%S',
                        format=fmt)
    logging.getLogger("watchdog").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    return log_level


@contextlib.contextmanager
def lock_or_die(lockfile_path):
    """Locks the specified file, or logs an error and exits."""

    # Use append mode so that we don't truncate if we fail to grab the lock.
    lockfile = open(lockfile_path, 'a')

    if not locks.lock(lockfile, locks.LOCK_EX | locks.LOCK_NB):
        log.error('Failed to lock "%s", terminating.', lockfile_path)
        sys.exit(1)

    try:
        lockfile.truncate(0)
        lockfile.write('{}\n'.format(os.getpid()))
        lockfile.flush()
        yield
    finally:
        # Avoid races by clearing the lockfile instead of deleting it.
        lockfile.truncate(0)
        locks.unlock(lockfile)
        lockfile.close()

def has_access(log_folder, log_path=None):
    if os.path.isdir(log_folder) and os.access(log_folder, os.W_OK):
        if log_path is not None and os.path.isfile(log_path):
            if os.access(log_path, os.W_OK):
                return True
            else:
                print("Logfile {} either does not exist or is not writable"
                      "".format(log_path))
                return False
        else:
            return True
    else:
        print("Folder {} either does not exist or is not writable"
              "".format(log_folder))
        return False


def setup_log(name, log_level):
    '''
    This function configures the log.
    The name will typically be the source name
    '''
    log_folder = config.get('log_folder', default_logfolder)
    tmp_log_folder = tempfile.gettempdir()
    log_path = os.path.join(log_folder, '{}.log'.format(name))

    if not has_access(log_folder, log_path):
        print("Cannot write to log file {}, trying /tmp".format(log_path))

        log_folder = tmp_log_folder
        log_path = os.path.join(log_folder, '{}.log'.format(name))

        if not has_access(log_folder, log_path):
            raise Exception("Unable to write to log file {}, quitting")

    log = logging.getLogger(name)
    log.setLevel(log_level)
    fh = logging.FileHandler(log_path)
    fh.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)-12s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    print("{} Log is writing to file: {}".format(name, log_path))
    return log
