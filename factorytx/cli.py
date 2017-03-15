""" This is the logic in the command line invocation of factorytx.

"""
import logging
import sys

import click

from factorytx.Global import global_state, init_logger, lock_or_die
from factorytx.Config import get_config
from factorytx.web.Web import Web
from factorytx import utils

LOG = logging.getLogger(__name__)
CONFIG = get_config()
MANAGERS = global_state()


@click.command()
@click.option('-n', '--nocore', default=False, is_flag=True,
              help='This flag will disable core from running')
@click.option('-w', '--web', default=False, is_flag=True,
              help='This flag will run the web service for factorytx')
@click.option('-b', '--bindall', default=False, is_flag=True,
              help='This flag will tell flask to bind '
                   'to all interfaces 0.0.0.0')
@click.option('-d', '--directory', default=None,
              type=click.Path(exists=True, file_okay=False, readable=True,
                              resolve_path=True),
              help='Specify a directory to look for factorytx configuration')
@click.option('-c', '--config', 'conf_file', default=None,
              type=click.Path(exists=True, dir_okay=False, readable=True,
                              resolve_path=True),
              help='Specify a configuration file (Overrides -d/--directory)')
@click.option('-u', '--update', default=False, is_flag=True,
              help='Update factorytx')
@click.option('-f', '--files', default=None,
              help='Specify configuration files to fetch')
@click.option('--check-configs', default=False, is_flag=True,
              help='Perform config check and exit')
@click.option('--version', default=False, is_flag=True,
              help='Print version information and exit')
@click.option('--log_level', default='INFO', help='Logging level to use')


def main(nocore: bool, web: bool, bindall: bool, directory: click.Path, conf_file: click.Path, update: bool, files,
        check_configs: bool, version, log_level) -> ():
    """ This function is invoked by a call to factorytx

    :param bool nocore: Run without the core platform
    :param bool web: Run the web platform
    :param bool bindall: Bind everything to 0.0.0.0
    :param click.path directory: The directory to look for
    :param click.path conf_file: The configuration to use.
    :param bool update: Update the config for factorytx
    :param ?(list, str) files: The config files to fetch.
    :param bool check_configs: Perform config check and exit.
    :param bool/str version: The version to use.
    """
    log_level = init_logger(log_level)
    build_version = utils.get_build_version()

    if version:
        print(build_version)
        exit(0)

    LOG.info("Starting factorytx. Build: %s", build_version)
    MANAGERS['parser_manager'].load_schemas()
    MANAGERS['plugin_manager'].load_schemas()
    MANAGERS['transport_manager'].load_schemas()
    MANAGERS['tx_manager'].load_schemas()
    MANAGERS['transformation_manager'].load_schemas()
    MANAGERS['filter_manager'].load_schemas()
    MANAGERS['service_manager'].set_logging(log_level)

    if not CONFIG.load(conf_file=conf_file, directory=directory):
        sys.exit(1)
    if not CONFIG.validate_configs():
        sys.exit(1)

    if check_configs:
        sys.exit(0)

    if 'logging' in CONFIG:
        logging.config.dictConfig(CONFIG['logging'])
        logging.info('Logging setup for factorytx core')

    MANAGERS['global_manager'].init_encryption()

    MANAGERS['signal_manager'].register_signals()

    # TODO: rewrite 'update' option as a subcommand
    # e.g. factorytx update file1.cfg file2.cfg file3.cfg
    if update is True:
        if files is not None:
            CONFIG.update_config(files.split(","))
        return

    if web:
        click.echo('Running Web...')
        webservice = Web(bindall)
        webservice.start()
    if not nocore:
        click.echo('Running Core...')
        lockfile_path = CONFIG.get('lockfile', '/tmp/factorytx.lock')
        with lock_or_die(lockfile_path):
            print('starting services')
            MANAGERS['service_manager'].start_services()
            print('polling')
            MANAGERS['service_manager'].poll()
    if web:
        webservice.join()


if __name__ == '__main__':
    main()
