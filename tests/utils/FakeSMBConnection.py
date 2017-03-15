import os
import shutil


class FakeFileObj(object):
    def __init__(self, filename, isdir):
        self.filename = filename
        self.isDirectory = isdir


class FakeSMBConnection(object):
    def __init__(self, in_fake_obj, username, password, name, hostname, use_ntlm_v2=True, is_direct_cp=True):
        self.username = username
        self.password = password
        self.name = name
        self.hostName = hostname
        self.use_ntlm_v2 = use_ntlm_v2
        self.is_direct_cp = is_direct_cp
        self.inFakeObj = in_fake_obj

    def connect(self, host, port, timeout):
        return True

    def retrieveFile(self, shared_folder, filepath, file_obj):
        shutil.copy("%s%s" % (shared_folder, filepath), file_obj.name)
        statinfo = os.stat(file_obj.name)
        return statinfo.st_mode, statinfo.st_size

    def deleteFiles(self, shared_folder, filepath):
        fpath = "%s/%s" % (shared_folder, filepath)
        if os.path.exists(fpath):
            os.remove(fpath)

    def listPath(self, shared_folder, filepath):
        relfn1 = "524_AutoNoSewCamera1_2014-10-07_14-30-00.bmp"
        relfn2 = "sample_dcn.xml"

        f1 = FakeFileObj(relfn1, False)
        f2 = FakeFileObj(relfn2, False)

        i1 = FakeFileObj(".", True)
        i2 = FakeFileObj("..", True)

        return [i1, i2, f2, f1]

    def close(self):
        return
