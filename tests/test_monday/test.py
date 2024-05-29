import logging
import unittest

from monday.c_board import Board
from monday.c_monday_factory import MondayFactory
from monday.c_row import Row
from monday.c_subitem import SubItem

import logging
from std_utility.c_datetime import DateTime



if __name__ == '__main__':
    monday: MondayFactory = MondayFactory()

    # def test_select_group_big(self):
    #     pid = 4459085243
    #     search_values = ['Ready']
    #     board = self.monday.board(board_id=pid, clear_cache=True)
    #     board.select(group='COACHES', col_name='Status', values=search_values)
    #     assert board.row_count > 0, "No Rows"

    pid = 2154004550

    board = monday.board(pid, monday_account="automation@threestep.com")

    result = board.select(groups=['RTG'])
    logging.info(result)