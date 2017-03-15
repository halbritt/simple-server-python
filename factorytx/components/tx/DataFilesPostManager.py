import glob
import os
import shutil
import json
import time
import fnmatch
import queue
import threading

import requests
from watchdog.observers import Observer
from watchdog import events


class WatchdogManager(object):

    def __init__(self):
        super(WatchdogManager, self).__init__()
        self.queues = {}
        self.watches = {}
        self.observer = Observer()
        self.observer.daemon = True
        self.observer_started = False
        self.lock = threading.RLock()

    def _get_queue(self, pathname):
        for path, queue in self.queues.iteritems():
            if fnmatch.fnmatchcase(pathname, path):
                return queue

    def dispatch(self, event):
        # filter out directory events
        if event.is_directory:
            return

        if event.event_type == events.EVENT_TYPE_MODIFIED:
            fname = event.src_path
        elif event.event_type == events.EVENT_TYPE_MOVED:
            fname = event.dest_path
        else:
            return

        with self.lock:
            queue = self._get_queue(fname)
        if queue is None:
            return

        queue.put(fname)

    def get_new_files_queue(self, path):
        with self.lock:
            if path in self.queues:
                raise Exception('Queue for {} is already created'.format(path))

            if not self.observer_started:
                self.observer.start()
                self.observer_started = True

            # create watch
            dirname = os.path.dirname(path)
            self.watches[path] = self.observer.schedule(self, dirname)

            # create queue
            queue = queue.Queue()
            self.queues[path] = queue
        return queue

    def stop_watching_queue(self, path):
        try:
            with self.lock:
                if self.queues.pop(path, None):
                    self.observer.unschedule(self.watches[path])
        except Exception as e:
            self.log.error("failed to stop watchdog for {}".format(path))
            raise e

# Singleton
watchdog_manager = WatchdogManager()


class DataFilesPostManager(object):

    def __init__(self):
        super(DataFilesPostManager, self).__init__()
        self.queues = {}

    def load_files(self, folder, extensions, reload_files=False):
        files = []
        full_paths = [folder + ext for ext in extensions]
        for path in full_paths:
            if reload_files:
                self.queues.pop(path, None)
                watchdog_manager.stop_watching_queue(path)
            if path not in self.queues:
                # load files that was already in the folder
                files += glob.glob(path)

                # create inotify watcher for this path
                self.queues[path] = watchdog_manager.get_new_files_queue(path)

            else:
                # load new files that was created after starting of factorytx
                queue = self.queues[path]
                # get all files from queue
                try:
                    while True:
                        files.append(queue.get_nowait())
                except queue.Empty:
                    pass

        return sorted(files)

    def read_json_file(self, file):
        try:
            # Load it up as a json object
            with open(file) as f:
                return json.load(f)
        except Exception:
            self.log.exception("ERROR: Unable to parse json file {}"
                               "".format(file))

    def transformInputData(self, input):
        pass

    def postfunc(self, **kwargs):
        pass

    def submitData(self, timeout=10, retries=3, **kwargs):
        retry = 0
        while retry < retries:
            try:
                response = self.postfunc(**kwargs)
                return response
            except requests.RequestException as e:
                msg = "Unable to post data to Service - {}".format(e)
                self.log.error(msg)
            except Exception as e:
                self.log.exception("Unable to post data to Service")
            time.sleep(timeout)
            retry += 1
            if retry < retries:
                self.log.info("retrying on failure")
            else:
                raise e

    def validateFilePostResponse(self, response):
        pass

    def _move_file(self, file, dest_folder):
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
        destfile = os.path.join(dest_folder, os.path.basename(file))
        shutil.move(file, destfile)

    def _handle_file(self, file, cond, folder):
        if cond:
            os.remove(file)
        else:
            self._move_file(file, folder)

    def handle_file_onerror(self, file):
        try:
            cond = self.postprocess_file_handling.get(
                'remove_on_failure', False)
            folder = self.postprocess_file_handling.get(
                'postprocess_failure_folder', '')
            self._handle_file(file, cond=cond, folder=folder)
        except:
            self.log.exception("handle_file_onerror for {} failed with "
                               "exception. Moving to next file post"
                               "".format(file))

    def handle_file_onsuccess(self, file):
        try:
            cond = self.postprocess_file_handling.get(
                'remove_on_success', True)
            folder = self.postprocess_file_handling.get(
                'postprocess_success_folder', '')
            self._handle_file(file, cond=cond, folder=folder)
        except:
            self.log.exception("handle_file_onsuccess for {} failed with "
                               "exception. Moving to next file post"
                               "".format(file))
