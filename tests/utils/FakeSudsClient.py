class FakeSudsExecuteDataSourceRequest(object):
    DataSourceKey = None
    InputParameters = None

    def __init__(self):
        pass


class FakeSudsInputParameterSubArray(object):
    InputParameter = None

    def __init__(self):
        self.InputParameter = []


class FakeSudsInputParameter(object):
    Value = None
    Name = None
    Required = None
    Output = None

    def __init__(self):
        pass


class FakeSudsFactory(object):
    def __init__(self):
        pass

    def create(self, type):
        if type == 'ArrayOfInputParameter':
            return FakeSudsInputParameterSubArray()
        elif type == 'InputParameter':
            return FakeSudsInputParameter()
        elif type == 'ExecuteDataSourceRequest':
            return FakeSudsExecuteDataSourceRequest()


class FakeSudsService(object):
    fake_response = None

    def __init__(self, fake_response):
        self.fake_response = fake_response

    def ExecuteDataSource(self, data):
        return self.fake_response


class FakeSudsClient(object):
    factory = None

    def __init__(self, host, username, password, service_fake_response, faults=False):
        self.host = host
        self.port = None
        self.username = username
        self.password = password
        self.faults = faults
        self.factory = FakeSudsFactory()
        self.service = FakeSudsService(service_fake_response)

    def set_options(self, service, port):
        pass
