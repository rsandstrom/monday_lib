
"""
Monday class
"""
from collections import namedtuple

from monday.c_board import Board
from monday.c_column import Column
from monday.c_monday_factory import MondayFactory
from result.c_result import Result

MondayCache = namedtuple('MondayCache', 'board_id timestamp monday')


class Monday:
    def __init__(self):
        self.factory: MondayFactory = MondayFactory()


