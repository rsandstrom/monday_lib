import unittest
from threestep.monday.c_monday import Monday
from threestep.monday.c_monday_factory import MondayFactory
from threestep.monday.c_row import Row
from threestep.monday.c_subitem import SubItem


class TestMondayInsertRow(unittest.TestCase):
    def test_monday_insert_row(self):
        pid = 2154004550
        monday: MondayFactory = MondayFactory()
        monday_board = monday.board(pid, monday_account="automation@threestep.com")

        # test adding a row
        result = monday_board.add_row(row_name='test 5 insert row', group_name='RTG')
        self.assertTrue(result.is_ok())
        m_row: Row = result.data
        m_row.set('Status', 'Done')
        m_row.set('Date', '2020-02-01')
        result = m_row.update()

        self.assertTrue((result.is_ok()))
        self.m_row = m_row

        # testing adding a subitem to that row
        sub_result = self.m_row.add_subitem("Rons test")
        self.assertTrue(sub_result.is_ok())
        s_row: SubItem = sub_result.data
        s_row.set('Status', 'Done')
        s_row.set('Date', '2020-05-10')
        result = s_row.update()
        self.assertTrue(result.is_ok())


if __name__ == '__main__':
    unittest.main()
