class FakeResponseRequests(object):
    resp = {}

    def __init__(self, status_code, resp):
        self.status_code = status_code
        self.text = resp

    def read(self):
        return self.text

    def close(self):
        return

    def raise_for_status(self):
        return None


class FakeResponseURLLib(object):
    resp = {}

    def __init__(self, resp):
        self.resp = resp

    def read(self):
        return self.resp

    def close(self):
        return
