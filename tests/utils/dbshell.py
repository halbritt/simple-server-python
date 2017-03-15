""" The abstract class and fixture which is the unified database object.

"""
from abc import ABCMeta, abstractmethod


class DBShell:
    """ The abstract class which represents a database connection.

    """
    __metaclass__ = ABCMeta
    test_stats = {}

    @abstractmethod
    def get_stats(self):
        """ gets and parses the status call to my database.

        :rtype: iterable
        :return: iterable of the stats key-value pairs in my dabase now.
        """
        pass

    @abstractmethod
    def clear_stats(self):
        """ sets my status dictionary to empty.

        """
        pass

    @abstractmethod
    def record_pretest_stats(self, test_name):
        """ TODO: sets the pretest stat call.

        """
        pass

    @abstractmethod
    def record_posttest_stats(self):
        """ TODO: sets the posttest stat call.

        """
        pass

    @abstractmethod
    def __init__(self, tenant, host):
        """ creates a database connection with a particular tenant and host.

        :param tenant: the tenant database to use
        :param host: the host in this environment to the right mongo
        """
        pass
