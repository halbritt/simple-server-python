""" This is the signal manager and associated logic.

"""
import sys
import signal
import logging
import multiprocessing

from factorytx.Config import get_config

LOG = logging.getLogger(__name__)
CONFIG = get_config()


class SignalManager(object):
    """ The signal manager interprets common os signals.

    """
    def __init__(self):
        """ Initializes a new SignalManager.

        """
        super(SignalManager, self).__init__()
        self.callbacks = []

    def register_signals(self):
        """ Registers the usual signals to listen for.

        """
        signals = ["SIGTERM", "SIGINT", "SIGBREAK", "SIGQUIT"]
        signals = [signl for signl in signals if signl in dir(signal)]
        for signame in signals:
            try:
                signal.signal(getattr(signal, signame), self._sighandler)
            except RuntimeError as runtime_error:
                LOG.warn('Unable to register handler for %s. %s', signame, runtime_error)

    def _sighandler(self, sig, frame):
        # TODO are these arguments not used?
        '''
        This is a signal handler to be able to kill a process
        '''
        cur_proc = multiprocessing.current_process()
        if not cur_proc.__class__.__name__ == '_MainProcess':
            LOG.warn("%s received a shutdown signal", cur_proc.name)
            sys.exit(-1)

        LOG.warn("Main process received a shutdown signal, "
                 "beginning shutdown...")

        # can be helpfull to do some stuff before exiting
        for callback in self.callbacks:
            try:
                callback()
            except:
                pass

        sys.exit(-1)

    def on_terminate(self, callback):
        """ Appends the callback to my callback stack.

        """
        self.callbacks.append(callback)

SIGNAL_MANAGER = SignalManager()
def signal_manager():
    """ Returns my signal manager.

    """
    return SIGNAL_MANAGER
