ENVS = {}
ENVS['defaults'] = {
    'docker': False,
    'username': 'demo@ftx.com',
    'password': '1234',
    'site.domain': 'ftx_dev',
    'pipelines': ['client'], #, client_scout, client_cloud]
}
ENVS['client'] = {
    'config_source': 'pipelines/dev/client.cfg'
}

