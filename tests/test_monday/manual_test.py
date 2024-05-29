import logging
import unittest

from monday.c_board import Board
from monday.c_monday_factory import MondayFactory
from monday.c_row import Row
from monday.c_subitem import SubItem

import logging
from std_utility.c_datetime import DateTime


def assertTrue(a, b=None):

    if a:
        logging.debug(f"OK, {b}")
    else:
        logging.debug(f"FAIL: {b}")


def unit_test():
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
    assertTrue(result.is_ok(), result.message)

    row_count = board.row_count

    # test adding a row
    result = board.add_row(row_name='test 5 insert row', group_name='RTG')
    assertTrue(result.is_ok())
    m_row: Row = result.data
    m_row.set('Status', 'Done')
    m_row.set('Date', '2020-02-01')
    result = m_row.update()

    assertTrue((result.is_ok()))
    assertTrue(row_count == board.row_count - 1)

    # testing adding a subitem to that row
    sub_result = m_row.add_subitem("Rons test")
    assertTrue(sub_result.is_ok())
    s_row: SubItem = sub_result.data
    s_row.set('Status', 'Done')
    s_row.set('Date', '2020-05-10')
    result = s_row.update()
    assertTrue(result.is_ok())


if __name__ == '__main__':
    unit_test()
