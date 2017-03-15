import json
import os
import shutil
import yaml

# http://stackoverflow.com/questions/24700242/how-to-unit-test-mkdir-function-without-file-system-access
class MockMkdir(object):
    def __init__(self):
        self.received_args = None

    def __call__(self, *args):
        self.received_args = args


def read_yaml_config(config_filename):
    if os.path.isfile(config_filename):
        return yaml.load(open(config_filename))
    else:
        return {}


def delete_dir(path):
    if os.path.isdir(path):
        shutil.rmtree(path)


def delete_and_mkdir(path):
    delete_dir(path)
    os.makedirs(path)


def load_sslogs(directory):
    """Returns a list of sslogs loaded from the given directory."""
    sslogs = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            with open(path, 'rb') as fp:
                new_sslogs = json.load(fp)
            for sslog in new_sslogs.values():
                sslogs.append(sslog)
    sslogs = sorted(sslogs, key=lambda s: (s['timestamp'], s['counter']))
    return sslogs
