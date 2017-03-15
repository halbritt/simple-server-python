class FakePLCSocketObj(object):
    def __init__(self, af, socktype, proto, fake_recv_data, fail_on_connect=False):
        self.af = af
        self.socktype = socktype
        self.proto = proto
        self.timeout = 10
        self.fail_on_connect = fail_on_connect
        self.fake_recv_data = fake_recv_data

    def settimeout(self, timeout):
        self.timeout = timeout

    def connect(self, sa):
        if self.fail_on_connect:
            raise Exception("FakePLCSocketObj failed to connect")
        pass

    def setblocking(self, val):
        pass

    def setsockopt(self, *args):
        pass

    def sendall(self, msg):
        pass

    def recv(self, bufferlength):
        retstr = self.fake_recv_data
        return retstr[0:bufferlength]

    def close(self):
        pass

def fake_socket_socket(fake_recv_data, fail_on_connect):
    return FakePLCSocketObj("af", "socktype", "proto", fake_recv_data, fail_on_connect)
