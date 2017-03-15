from datetime import datetime
import uuid

from FactoryTx.components.dataplugins.DataPlugin import DataPlugin


class DataGenerator(DataPlugin):
    """Simple plugin for end-to-end testing of the factorytx core.

    Generates an sslog every `poll_rate` seconds, where `poll_rate` is specified
    by the plugin config.

    """

    __version__ = "1.0.0"
    logname = __name__

    def read(self):
        timestamp = datetime.now()
        counter = int((timestamp - datetime(1970, 1, 1)).total_seconds() // 10)
        record = {
            "timestamp": timestamp,
            "counter": counter,
            "fieldvalues": {
                "Year": timestamp.year,
                "Month": timestamp.month,
                "Day": timestamp.day,
                "Hour": timestamp.hour,
                "Minute": timestamp.minute,
                "Seconds": timestamp.second,
                "UUID": uuid.uuid4().hex,
            }
        }
        return [record]

    def process(self, record):
        return record
