import functools
import multiprocessing
import os
import signal
import sys
import unittest

import mock
from testfixtures import TempDirectory

from FactoryTx.managers.GlobalManager import GlobalManager
from FactoryTx.Global import lock_or_die, init_logger


class CoreTestCase(unittest.TestCase):

    def test_logger(self):
        init_logger()


enc_key = """\
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDDu/XRP1kyK6Cgt36gts9XAk
FiiuJLW6RU0j3KKVZSs1I7Z3UmU9/9aVh/rZV43WQG8jaR6kkcP4stOR0DEtll
PDA7ZRBnrfiHpSQYQ874AZaAoIjgkv7DBfsE6gcDQLub0PFjWyrYQUJhtOLQEK
vY/G0vt2iRL3juawWmCFdTK3W3XvwAdgGk71i6lHt+deOPNEPN2H58E4odrZ2f
sxn/adpDqfb2sM0kPwQs0aWvrrKGvUaustkivQE4XWiSFnB0oJB/lKK/CKVKuy
///ImSCGHQRvhwariN2tvZ6CBNSLh3iQgeB0AkyJlng7MXB2qYq/Ci2FUOryCX
2MzHvnbv testkey@localhost
"""


class GlobalManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.gm = GlobalManager()

    def test_init(self):
        self.assertIsNotNone(self.gm.manager)
        self.assertIsNotNone(self.gm.dict)
        self.assertIsNotNone(self.gm.get_dict())
        self.assertIsNotNone(self.gm.pid)
        self.assertIsNone(self.gm.encryption)
        self.assertIsNone(self.gm.get_encryption())

    @mock.patch.dict('FactoryTx.Global.config', {})
    def test_init_encryption_no_conf(self):
        self.assertIsNone(self.gm.encryption)
        self.gm.init_encryption()
        self.assertIsNone(self.gm.encryption)

    @mock.patch.dict('FactoryTx.Global.config', encryption_key=enc_key)
    def test_init_encryption(self):
        self.assertIsNone(self.gm.encryption)
        self.gm.init_encryption()
        self.assertIsNotNone(self.gm.encryption)
        pk, padding, key = self.gm.encryption
        self.assertIsNotNone(pk)
        self.assertEqual(padding.name, 'EME-OAEP')
        self.assertEqual(key, 'f0b9c754430ab6cef89601d67aa85c3cae57ae0a')


# We need to run a separate process to grab the lock file -- flock(2)
# can still grab the lock if it's held by a thread in the same process.
def simulated_lock_task(path, started_event, stop_event, stopped_event):
    # Simulate SignalManager's handling of signals on the master.
    signal.signal(signal.SIGTERM, lambda *args: sys.exit(1))
    with lock_or_die(path):
        started_event.set()
        stop_event.wait()
    stopped_event.set()


class LockFileTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = TempDirectory()
        self.path = self.tempdir.getpath('lockfile')

        started_event = multiprocessing.Event()
        stop_event = multiprocessing.Event()
        stopped_event = multiprocessing.Event()

        self.process = multiprocessing.Process(
            target=simulated_lock_task,
            args=(self.path, started_event, stop_event, stopped_event),
        )
        self.process.start()
        started_event.wait()

        self.stop_event = stop_event
        self.stopped_event = stopped_event

    def tearDown(self):
        if self.process.is_alive():
            self.stop_event.set()
        self.process.join()
        self.tempdir.cleanup()

    def check_pid(self, expected):
        if os.name == 'nt':
            return  # Can't read locked files on Windows
        with open(self.path, 'r') as fp:
            pid = fp.read().strip()
        self.assertEqual(pid, str(expected))

    def stop_process(self):
        self.stop_event.set()
        self.stopped_event.wait()

    def kill_process(self, signum):
        os.kill(self.process.pid, signum)
        self.process.join()

    def test_first_run(self):
        # The first time a lock is requested it should be granted, and the
        # process's PID should be written to the lock file.
        self.assertTrue(self.process.is_alive())
        self.check_pid(expected=self.process.pid)
        self.stop_process()
        self.check_pid(expected="")

    def test_mutual_exclusion(self):
        # Two processes should not be able to acquire the lock simultaneously.
        # Failing to acquire the lock should not overwrite the PID.
        self.check_pid(expected=self.process.pid)
        with self.assertRaises(SystemExit) as cm:
            with lock_or_die(self.path):
                pass
        self.assertNotIn(cm.exception.code, (0, None))
        self.check_pid(expected=self.process.pid)

    def check_lock_released(self):
        lock_body_ran = False
        with lock_or_die(self.path):
            lock_body_ran = True
            self.check_pid(expected=os.getpid())

        self.assertTrue(lock_body_ran, "Silently failed to obtain lock!")
        self.check_pid(expected="")

    def test_clean_shutdown(self):
        # The lock should be released and the PID erased on process exit.
        self.stop_process()
        self.check_pid(expected="")
        self.check_lock_released()

    def test_sigterm(self):
        # The lock should be released and the PID erased on SIGTERM.
        # This assumes that SimpleData calls `sys.exit` from the signal handler.
        self.kill_process(signal.SIGTERM)
        self.check_pid(expected="")
        self.check_lock_released()

    @unittest.skipIf(os.name == 'nt', "Windows does not support SIGKILL")
    def test_hard_kill(self):
        # The lock should be released on SIGKILL (or power loss, etc.) even if
        # the PID is not erased.
        self.kill_process(signal.SIGKILL)
        self.check_pid(expected=self.process.pid)
        self.check_lock_released()


if __name__ == '__main__':
    unittest.main()
