"""
Codes are used for standard errors, can not use status here because status uses standard errors
"""


class Codes:
    def __init__(self, code=-1, msg='N/A'):
        self.code = code
        self.msg = msg