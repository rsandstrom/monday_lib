from status.c_status import Status
from std_errors.c_codes import Codes
import logging
from typing import Generic, TypeVar

T = TypeVar("T")


class Result(Generic[T]):
    def __init__(self, code=-1, message=None, data=None, log=False, status=None):
        if status is None:
            self.status = Status(code=code, message=message, log=log)
        else:
            self.status = status

        if code is None:
            code = -1
        else:
            if isinstance(code, Codes):
                e_code: Codes = code
                if message is None:
                    self.status = Status(code=e_code.code, message=e_code.msg, log=log)
                else:
                    self.status = Status(code=e_code.code, message=message, log=log)

        self.data: T = data

    # check status code is zero and optionally log with an optional message
    def is_ok(self, log=False, message=None):
        return self.status.is_ok(message=message, log=log)

    # check status code is not zero and optionally log with an optional message
    def is_error(self, log=False, message=None):
        return self.status.is_error(message=message, log=log)

    @property
    def message(self):
        return self.status.message

    @message.setter
    def message(self, x):
        self.status.message = x

    @property
    def show_status(self):
        return f"status code = [{self.status.code}], status message = [{self.status.message}]"

    def __iter__(self):
        for key in (self.status.code, self.status.message, self.data):
            yield key, getattr(self, key)


if __name__ == '__main__':
    x = Result()
    logging.info(x.status.code)
