class FakeDBResponse(object):
    def __init__(self, fake_records, fake_keys):
        self.fake_records = fake_records
        self.fake_keys = fake_keys

    def fetchall(self):
        return self.fake_records

    def keys(self):
        return self.fake_keys


class FakeDBConnection(object):
    def __init__(self, fake_records, fake_keys):
        self.fake_records = fake_records
        self.fake_keys = fake_keys

    def close(self):
        pass

    def execute(self, query, **kwargs):
        return FakeDBResponse(self.fake_records, self.fake_keys)


class FakeDBEngine(object):
    def __init__(self, fake_records, fake_keys):
        self.fake_records = fake_records
        self.fake_keys = fake_keys

    def connect(self):
        return FakeDBConnection(self.fake_records, self.fake_keys)
