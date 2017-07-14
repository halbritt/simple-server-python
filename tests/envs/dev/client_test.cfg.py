ENVS = {}
ENVS['defaults'] = {
    'docker': False,
    'username': 'demo@ftx.com',
    'password': '1234',
    'site.domain': 'ftx_dev',
    'pipelines': ['client', 'passthrough', 'file_template']
}
ENVS['client'] = {
    'config_source': 'pipelines/dev/client.cfg'
}

ENVS['passthrough'] = {
    'config_source': 'pipelines/dev/passthrough.cfg'
}

ENVS['file_template'] = {
    'config_source': 'pipelines/dev/file_template.cfg'
}
