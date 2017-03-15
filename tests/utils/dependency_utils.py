""" This module helps to load in the right configurations for our pipelines and hosts.

"""
import imp
from os import getcwd
from os import path
from copy import copy
from dateparser import parse

def parametrize_hosts(config):
    # (ValidationConfig) -> dict
    """ Given the config dictionary, loads in the defaults for each pipeline but doesn't override
        pipeline specific values.

    :param dict config: the configuration dictionary.
    :rtype dict: the config dictionary with defaults loaded
    :returns: the default-free dictionary.
    """
    auth_path_str = path.join(getcwd(), 'envs', 'authentication.cfg.py')
    auth_mod = imp.load_source('authentication.cfg.py', auth_path_str)
    creds = getattr(auth_mod, 'AUTHS')
    for envs in config.envs:
        env = config.envs[envs]
        if 'defaults' in env:
            default = env['defaults']
            if 'pipelines' in default:
                for pipeline in default['pipelines']:
                    if pipeline in env:
                        continue
                    env[pipeline] = {}
        else:
            env['defaults'] = {}
        for pipeline in env:
            if pipeline == 'defaults':
                continue
            default_dict = get_default_dict(env)
            default_dict.update(env[pipeline])
            env[pipeline] = default_dict
            # TODO: Are there default specific things we need to do for FTX
            #inject_default_fields = ['start_time', 'end_time', etc.]
            #for f in inject_default_fields:
            #    if f in default_dict:
            #        default_dict[f] = inject_date(default_dict[f])
            # Possibly get database information?
            #if 'postgres_host' in default_dict:
            #    if not envs in creds:
            #        username = 'postgres'
            #        password = 'postgres'
            #    else:
            #        host = creds[envs]['postgres']
            #        username = host['username']
            #        password = host['password']
            #    pg_host = default_dict['postgres_host']
            #    pg_string = 'postgresql+psycopg2://' + username + ":" + password + '@' + pg_host
            #    default_dict['postgres_host'] = pg_string
        del env['defaults']
    flattened = {(pipeline, env): config.envs[env][pipeline] for env in config.envs \
                    for pipeline in config.envs[env]}
    return flattened

def inject_date(time):
    # (str) -> str
    """ Returns the correctly formatted date string.

    :param str time: the config enabled date string.
    :rtype: str
    :returns: the string that is the correctly formatted date
    """
    final_time = parse(time)
    return final_time.strftime("%Y-%m-%dT%H:%M:%S")

def get_default_dict(env):
    # (dict) -> dict
    """ Returns the default dict of this particular environment.

    :param dict env: the environment dictionary
    :rtype: dict the default dictionary
    :returns: the defaults if they exist.
    """
    if 'defaults' in env:
        default_dict = copy(env['defaults'])
    else:
        default_dict = {}
    return default_dict
