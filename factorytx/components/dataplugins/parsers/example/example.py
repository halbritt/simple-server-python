from datetime import datetime

from FactoryTx.parsers.base import BaseParser


class ExampleParser(BaseParser):
    def can_parse(self, remote_path):
        return True

    def parse(self, remote_path, local_path, completed_path):
        with open(local_path, 'rb') as fp:
            contents = fp.read()
        if completed_path is None:
            old_contents = None
        else:
            with open(completed_path, 'rb') as fp:
                old_contents = fp.read()
        self.log.info('Remote: "%s"\nCompleted: "%s"\n\n%s',
                      remote_path, completed_path, contents)
        sslog = {
            "counter": 1,
            "sslog_type": "cycle",
            "timestamp": datetime.utcnow(),
            "fieldvalues": {
                "contents": {"value": contents, "units": None},
                "old_contents": {"value": old_contents, "units": None},
            },
        }
        return [sslog]
