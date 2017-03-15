""" This is the structure of the error reporting object that we want for
    graceful error reporting.

"""
from logging import getLogger, basicConfig, INFO
from pytest import fail
from tests.constants.errordict import ERRDICT

basicConfig(level=INFO)


class ErrorReporter(object):
    """ This class represents the object used to report errors that are in tests.
        The reason that we want this is to make sure there is a single source of
        truth with respect to the error logging. Also we want to make the assert when we know
        the full scope of the test errors, and not just the first error that the test turns up.

    """

    def submit_error(self, error_name="custom_error", in_arg_list=[], error_level="error"):
        # (str, list, str) -> None
        """ This function adds an error to my stack of errors. If you have to do a custom error
            thats fine, but you should really use errordict.py to put in your error strings.

        :param str error_name: the name of the error that we are logging
        :param list in_arg_list: the list of the items necessary for error reporting
        :param str custom_error: for when things are short or strangely not respectful of
                                 abstraction
        """
        arg_list = []
        if in_arg_list:
            arg_list = [str(x) for x in in_arg_list]
        self.append_error_string(error_name, arg_list, error_level)

    def append_error_string(self, error_name, arg_list, error_level="error"):
        custom_string = "Custom Error--{}:"
        error_string = None
        if error_name not in self.error_string_dict:
            try:
                error_prefix = custom_string.format(error_name)
                if len(arg_list) > 0:
                    error_string = error_prefix + ', '.join(arg_list)
                elif len(arg_list) == 0:
                    error_string = error_prefix
                self.error_string_dict[error_name] = error_prefix
            except Exception:
                self.error_string_dict[error_name] = custom_string
                error_string = error_name
        else:
            try:
                if isinstance(arg_list, list):
                    error_string = self.error_string_dict[error_name].format(*arg_list)
                else:
                    error_string = self.error_string_dict[error_name].format(**arg_list)
            except Exception:
                error_string = self.error_string_dict[error_name] + ','.join(arg_list)
        if error_string:
            reported_items = self.reported_errors
            if error_level == "warning":
                reported_items = self.reported_warnings

            reported_items.append((error_name, error_string))

    def report_helper(self, reported_items, isFailure=True):
        # () -> None
        """ Reports the errors that I have collected during my test, one failure per line

        """
        if len(reported_items) > 0:
            error_dic = {}
            for error in reported_items:
                name, error = error
                break_index = error.find(":")
                if break_index != -1:
                    explanation = error[:break_index]
                    content = error[break_index:]
                else:
                    explanation = error
                    content = error

                if (name, explanation) in error_dic:
                    error_dic[(name, explanation)].append(content)
                else:
                    error_dic[(name, explanation)] = [content]

            for key in sorted(error_dic):
                logger = getLogger(key[0])

                if isFailure:
                    logfunc = logger.error
                else:
                    logfunc = logger.warning

                logfunc(key[1])
                for value in error_dic[key]:
                    if key[1] == value:
                        continue
                    logger.info(value)

            if isFailure:
                fail("Reporting all errors for this test.")
            else:
                self.log_warn_message("Reporting all warnings for this test.")

    def report_errors(self):
        # () -> None
        """ Reports the errors that I have collected during my test, one failure per line

        """
        self.report_warnings()
        self.report_helper(self.reported_errors)

    def report_warnings(self):
        # () -> None
        """ Reports the errors that I have collected during my test, one failure per line

        """
        self.report_helper(self.reported_warnings, False)

    def report_info(self):
        # () -> None
        """ Reports the infos that I have collected during my test, one failure per line

        """
        self.report_helper(self.report_info, True)

    def assert_and_fail(self, error_name, error_args=[], condition=False):
        # (bool, str) -> None
        """ Asserts failure with a conditional statement

        :param bool condition: a statement that can be evaluated to a truth value
        :param str error_message: the error message to drop if the condition is false.
        """
        if not condition:
            self.append_error_string(error_name, error_args)
            self.report_errors()

    def log_info(self):
        """ Logs the relevant test info for this test function.

        """
        logger = getLogger("Test information")
        request = self.request
        test_name = request._parent_request._pyfuncitem._obj.__doc__
        logger.info(test_name)

    def log_info_message(self, message):
        # (str) > ()
        """ Logs any message

        """
        logger = getLogger("VERIFY:")
        logger.info(message)

    def log_warn_message(self, message):
        # (str) > ()
        """ Logs any message

        """
        logger = getLogger("WARNING:")
        logger.warning(message)

    def __init__(self, request):
        """ Sets up my error dictionary so that I have all the strings that are in the different
            error genres.

        """
        self.request = request
        self.reported_errors = []
        self.reported_warnings = []
        self.report_info = []
        self.error_string_dict = {}
        for error_type in ERRDICT:
            self.error_string_dict.update(ERRDICT[error_type])
