
"""
 Status Class is used to create a standard method of handling and checking for errors.

 usage function returns a status
    return Status(0) # means this is a success
    return status(1000, StdError.get(1000))

"""
from std_errors.c_codes import Codes
from std_errors.c_std_error import StdError
import logging

SUCCESS = 'Success'
ERROR = 'Error'
INFO = 'Info'


class Status:
    def __init__(self, code=-1, message=None, log=False):
        self.code = code

        if self.code == 0:
            self.message = SUCCESS
        else:
            self.message = 'N/A'
        self.log = log

        if isinstance(code, Codes):
            e_code: Codes = code
            self.code = e_code.code
            self.message = e_code.msg

        if message is None and code != 0:
            self.message = StdError.get(str(self.code))

        if message is not None:
            self.message = message

        self._log_info(message=self.message, log=log)

    @property
    def error(self):
        return self.code != 0

    @property
    def ok(self):
        return self.code == 0

    def _log_error(self, message=None, log=False):
        if log:
            self._log_message(message, ERROR)

    def _log_info(self, message=None, log=False):
        if log:
            self._log_message(message, INFO)

    def _log_message(self, message=None, level=INFO):
        if message is None:
            message = self.message

        logging.log(message)


    """
        True if status is an error
    """

    def is_error(self, message=None, log=False):
        if self.error:
            self._log_error(message=message, log=log)
        return self.error

    """
        True if status is ok
    """

    def is_ok(self, message=None, log=False):
        if self.ok:
            self._log_info(message=message, log=log)
        return self.ok

    def __iter__(self):
        for key in (self.code, self.message):
            yield key, getattr(self, key)
