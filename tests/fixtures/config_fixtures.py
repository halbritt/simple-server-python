# pylint: skip-file
""" This is my current solution for host dependency injection, with the demand that
    there is a new test for each host desired to test against.

"""
import os
import imp
from yaml import load
from pytest import fixture, skip
from tests.utils.dependency_utils import parametrize_hosts
from tests.validationconf import *


class ValidationConfig(object):
    """ Makes a host configuration object in order to parametrize hosts.

    """
    def read_config(self):
        """ Reads the configuration files into the config dictionary

        """
        for subdir, _, files in os.walk('envs/'):
            for fle in files:
                if fle.endswith('cfg.py'):
                    env_name = fle[:-7]
                    env_path = os.path.join(subdir, fle)
                    env_mod = imp.load_source(fle, env_path)
                    try:
                        env_dic = getattr(env_mod, 'ENVS')
                        if 'defaults' in env_dic:
                            defaults = env_dic['defaults']
                            # TODO: Are there defaults to set for FTX?
                        self.envs[env_name] = env_dic
                    except Exception:
                        #print vars(env_mod)
                        self.active_envs = getattr(env_mod, 'ACTIVE_PIPELINES')
            keys = [env_name for env_name in self.envs]
            for env_name in keys:
                if env_name not in self.active_envs:
                    del self.envs[env_name]
                else:
                    for pipeline in self.envs[env_name]:
                        if 'config_source' in self.envs[env_name][pipeline]:
                            source = self.envs[env_name][pipeline]['config_source']
                            with open(source) as f:
                                yml = load(f)
                                self.envs[env_name][pipeline]['config'] = yml


    def __init__(self): # Bring in some defaults:start=DEFAULT_START, end=DEFAULT_END
        # (dict, str, str) -> ValidationConfig
        """ Sets my configuration environments and pipelines

        :param dict active_envs: the environment dictionary
        :param str start: start time string of data range for validation
        :param str end: end time of string data range for validation
        """
        self.envs = {}
        self.hosts = None
        self.host_keys = []


@fixture(scope='module')
def parametrized_envs():
    """ we need to load in the defaults and create the keys only once per module.

    :rtype: ValidationConfig
    :returns: the right configuration object with the appropriately parametrized environments.
    """
    config = ValidationConfig()
    config.read_config()
    print("The config is", config)
    config.hosts = parametrize_hosts(config)
    config.host_keys = [':'.join(x) for x in config.hosts.keys()]
    print(vars(config))
    return config

PARAMS = parametrized_envs()
HOST_KEYS = PARAMS.host_keys


@fixture(scope='module', params=HOST_KEYS)
def pipeline_environment(request, parametrized_envs, option_env, option_pipeline):
    # (str, ValidationConfig, str, str) -> str, str, dict
    """ Parametrizes the hosts needed for the testing suite

    :param request: the request fixture of parametrized hosts.
    :param ValidationConfig parametrized_envs: the correctly parametrized environment dictionary
    :param option_env: fixture return value of --env command-line argument
    :param option_pipeline: fixture return value of --pipeline command-line argument
    :rtype: tuple
    :returns: the pipeline, host, environment dictionary tuple
    """
    filtered_env_keys = option_env.split(',') if option_env else []
    filtered_pipeline_keys = option_pipeline.split(',') if option_pipeline else []

    hosts = parametrized_envs.hosts
    pipeline, host = tuple(request.param.split(':'))
    outdict = {}

    # Filter out environments if we are supposed to filter them out
    if filtered_env_keys and (host not in filtered_env_keys):
        skip("Host isn't available")
    elif filtered_pipeline_keys and (pipeline not in filtered_pipeline_keys):
        skip("pipeline isn't available")
    else:
        outdict = hosts[(pipeline, host)]

    return pipeline, host, outdict
