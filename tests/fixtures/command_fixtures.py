""" The fixtures involved with extracting command line parameters.

"""
from pytest import fixture


def pytest_addoption(parser):
    # (parser) -> None
    """ Sets the configuration of a parser.

    :param parser parser: The parser for the command line arguments.
    """
    parser.addoption("--env", action="store", help=("Optional 'env' to use.  Default is read from "
                                                    "ACTIVE_ENVS setting in validationconf.py "))
    parser.addoption("--pipeline", action="store", help=("Optional pipeliness to run against. "
                                                       "Comma-seperated list. Default is read from "
                                                       "ALL_PIPELINES setting in validationconf.py"))


@fixture(scope="session")
def option_env(request):
    # (Request) -> str
    """ Given a request, returns the env command line argument.

    :param request request: The request object with parameters
    :rtype: str
    :returns: a string with the env command line argument
    """
    return request.config.getoption("--env")


@fixture(scope="session")
def option_pipeline(request):
    # (Request) -> str
    """ Given a request, returns the pipeline command line argument.

    :param request request: The request object with parameters
    :rtype: str
    :returns: a string with the pipeline command line argument
    """
    return request.config.getoption("--pipeline")
