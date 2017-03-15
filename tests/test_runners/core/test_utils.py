import unittest

import testfixtures

from FactoryTx import utils


class BuildVersionTestCase(unittest.TestCase):
    def setUp(self):
        self.replacer = testfixtures.Replacer()
        self.tempdir = testfixtures.TempDirectory()
        # Fake running in a python environment rooted at `self.tempdir`.
        self.replacer.replace("FactoryTx.utils.sys.prefix", self.tempdir.path)

    def tearDown(self):
        self.tempdir.cleanup()
        self.replacer.restore()

    def fake_running_in_virtualenv(self):
        self.replacer.replace("FactoryTx.utils.sys.real_prefix", "/usr", strict=False)

    def fake_running_outside_virtualenv(self):
        self.replacer.replace("FactoryTx.utils.sys.real_prefix",
                              testfixtures.not_there, strict=False)

    def test_not_in_virtualenv(self):
        self.fake_running_outside_virtualenv()
        self.tempdir.write("build", "some random file at /usr/build\n")
        build_version = utils.get_build_version()
        self.assertRegexpMatches(build_version, "^simpledata-unknown")

    def test_absent_build_version(self):
        self.fake_running_in_virtualenv()
        build_version = utils.get_build_version()
        self.assertRegexpMatches(build_version, "^simpledata-unknown")

    def test_present_build_version(self):
        self.fake_running_in_virtualenv()
        expected_build_version = "simpledata-12.04-x86_64-fake-build-SD_167.tgz"
        self.tempdir.write("build", expected_build_version + "\n")
        build_version = utils.get_build_version()
        self.assertEqual(build_version, expected_build_version)
