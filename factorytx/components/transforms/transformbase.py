import time
import os
import re
import fnmatch
import json
import threading
import filecmp
import pandas as pd
import pytz
from logging import getLogger

from factorytx.utils import merge_schema_defaults, status_var
from dateutil import parser
from datetime import datetime


class TransformBase(object):

    def load_parameters(self, schema, plgn_cfg):
        self.actions = []
        self.stractions = []
        self.regex = []
        self.__dict__.update(plgn_cfg)
        merge_schema_defaults(schema, self.__dict__)
        self.log = getLogger("Base Transform")

    def apply_preprocessing(self, frame: pd.DataFrame) -> pd.DataFrame:
        """ Apply my preprocessing function to my dataframe. """
        return frame

    def apply_postprocessing(self, frame: pd.DataFrame) -> pd.DataFrame:
        """ Apply my postprocessing function to my dataframe. """
        return frame

    def validate_frame(self, frame: pd.DataFrame) -> status_var:
        """ Goes ahead and validates the FRAME based on some general and particular criteria
            this function should contain the general criteria for dataframe validation.

        """
        # TODO: this is a nice place to do some validation
        return True

    def apply_config_actions(self, frame: pd.DataFrame) -> pd.DataFrame:
        """ This function applies the general dataframe manipulation that is a part of the core
            pandas functionality to a FRAME and is written in the config for my transform.

        """
        self.log.info("Applying preprocessing")
        validation = self.validate_frame
        if not validation:
            self.log.error("The frame has some problems with it, not transforming")
            return frame
        frame = self.apply_preprocessing(frame)
        actions = self.actions
        stractions = self.stractions
        counter = self.counter
        regex = self.regex


        # when read_csv is called it expects the data to be in TAB delimited format, NOT comma.  If
        # the format is wrong, instead of multiple columns, you will get 1 column string with all the column names
        # maybe un-needed check but might be helpful in debugging
        # if len(frame.columns) == 1:
        #   self.log.error("File is improperly configured, needs to be TAB delimited")
        #   raise Exception("Invalid file format, is not tab delimited format")
        # Note: will fail the first call to frame[column_name], with column name in the exception message

        frame.columns = [col for col in frame.columns]

        self.log.info('Columns for {} are {}'.format("frame", frame.columns))

        if counter:
            series = pd.Series(range(len(frame)))
            frame['counter'] = series
            print("Set the counter to %s", len(frame), pd.Series(range(len(frame))))

        self.log.info("Going into the action block with %s", vars(self))
        if actions:
            for action in actions:
                self.log.info("Applying the action %s", actions)
                for functionname, params in action.items():
                    if functionname in dir(frame) and hasattr(getattr(frame, functionname), "__call__"):
                        if isinstance(params, dict):
                            frame = getattr(frame, functionname)(**params)
                        else:
                            frame = getattr(frame, functionname)(*params)

                        self.log.info("Performed {} on frame".format(functionname))

        self.log.info("setting straction %s", stractions)
        if stractions:
            for straction in stractions:
                try:
                    print("setting straction %s", straction)
                    if straction.get('function') in dir(frame[straction['column']].str):
                        frame[straction['column']] = getattr(frame[straction['column']].str, straction.get('function'))(**straction.get('params', {}))
                        self.log.info("Performed {} on frame[{}]".format(straction['function'], straction['column']))
                except Exception as e:
                    self.log.error("Trouble setting the straction %s with the params %s: %s", straction['column'], straction['params'], e)

        if regex:
            for substitution in regex:
                target = substitution.get('target')
                pattern = substitution.get('pattern')
                source = substitution.get('source')

                if target is None or pattern is None or source is None:
                    # Should this trigger an exception?
                    raise Exception("Incomplete regex configuration; missing target, pattern, or source")
                else:
                    src_column = None
                    try:
                        src_column = frame[source]
                    except Exception as e:
                        self.log.error('Invalid regex configuration source {}, doesnt exists in table.'
                                       'Possibly missing header index or invalid sep value?'
                                       ' Exception:{}'.format(source, e))
                        raise

                    if src_column is None:
                        self.log.error('Invalid regex configuration, skipping')
                    else:
                        frame[target] = src_column.apply(lambda x: re.sub(pattern,
                                                                         substitution.get("repl", "\\1"),
                                                                         x) if type(x) == str else x)

        self.log.info("Applying Postprocessing to frame of length %s", len(frame))
        frame = self.apply_postprocessing(frame)

        return frame

