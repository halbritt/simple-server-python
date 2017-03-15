""" This represents the global manager of a factorytx session.

"""
import logging
import hashlib
import multiprocessing

from factorytx.Config import get_config

LOG = logging.getLogger(__name__)
CONFIG = get_config()

class GlobalManager(object):
    """ The global manager is responsible for things like encryption of a session, etc.

    """
    def __init__(self):
        """ Returns a new GlobalManager object.

        """
        super(GlobalManager, self).__init__()
        self.manager = multiprocessing.Manager()
        self.dict = self.manager.dict()
        self.encryption = None

    def get_dict(self):
        """ Return my dictionary of processes.

        """
        return self.dict

    def init_encryption(self):
        """ Given that an encryption key is specified in the config, does the thing.

        """
        if "encryption_key" in CONFIG:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding

            encryption_padding = padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA1()),
                algorithm=hashes.SHA1(),
                label=None)

            encryption_public_key = serialization.load_ssh_public_key(
                CONFIG['encryption_key'],
                backend=default_backend())
            public_bytes = encryption_public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            encryption_key_sha1 = hashlib.sha1(public_bytes).hexdigest()

            self.encryption = (
                encryption_public_key,
                encryption_padding,
                encryption_key_sha1
            )

            LOG.info('Encryption succesfully enabled.')
        else:
            LOG.info('No encryption_key in config file. Encryption disabled.')

    def get_encryption(self):
        # () -> ?
        """ Return the encryption of my session

        """
        return self.encryption

    @property
    def pid(self):
        # () -> int
        """ Get the process pid of this manager

        """
        return self.manager._process.pid

GLOBAL_MANAGER = GlobalManager()
def global_manager():
    # () -> GlobalManager
    """ Returns the global manager

    :rtype: GlobalManager
    """
    return GLOBAL_MANAGER
