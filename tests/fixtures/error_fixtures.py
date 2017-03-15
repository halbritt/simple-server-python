""" Meant to represent error reporting in a consistent manner.

"""
from pytest import fixture
from tests.utils.error_utils import ErrorReporter


@fixture(scope='function', autouse=True)
def error_fixture(request):
    # () -> ErrorReporter
    """ Returns the error reporting fixture

    :rtype: ErrorReporter
    :returns: The error reporting fixture.
    """
    reporter = ErrorReporter(request)
    reporter.log_info()
    return ErrorReporter(request)
