import unittest

from threestep.authentication.c_credentials import Credentials
from threestep.monday.c_board import Board
from threestep.monday.c_group import Group
from threestep.monday.c_monday_factory import MondayFactory
from threestep.monday.c_row import Row
from threestep.monday.c_subitem import SubItem
from threestep.netsuite.c_expense_account_master import ExpenseAccounts


class TestMonday(unittest.TestCase):
    monday: MondayFactory = MondayFactory()

    # def test_select_group_big(self):
    #     pid = 4459085243
    #     search_values = ['Ready']
    #     board = self.monday.board(board_id=pid, clear_cache=True)
    #     board.select(group='COACHES', col_name='Status', values=search_values)
    #     assert board.row_count > 0, "No Rows"

    pid = 2154004550

    # when possible we want to use the same monday board vs create a new instance each time

    monday_board = monday.board(board_id=pid, clear_cache=True)

    def test_activity_log(self):
        result = self.monday_board.get_column_activity_log(column_names=['Status'], match='Done', user_info=True)
        assert result.is_ok() and len(result.data) == 1, f"Too many results: {result.message}"
        print(result.message)

    def test_add_colunm(self):
        result = self.monday_board.add_column('add test column', 'add a test column', 'text')
        assert result.is_ok()
        print(result.message)

    def test_rename_test_column(self):
        result = self.monday_board.rename_column('add test column', 'new_name')
        assert result.is_ok()
        print(result.message)

    def test_create_row(self):
        pid = 2154004550

        board = self.monday.board(board_id=pid, clear_cache=True)

        result = board.select(groups=['RTG'])
        self.assertTrue(result.is_ok(), result.message)

        row_count = board.row_count

        start_count = self.monday_board.row_count

        # test adding a row
        result = board.add_row(row_name='test for delete - insert row', group_name='RTG', no_strip=True)
        self.assertTrue(result.is_ok())

        m_row: Row = result.data
        m_row.set('Status', 'Done')
        m_row.set('Date', '2020-02-01')
        result = m_row.update()

        self.assertTrue((result.is_ok()))
        self.assertTrue(row_count == board.row_count - 1)
        self.m_row = m_row

        # testing adding a subitem to that row
        sub_result = self.m_row.add_subitem("Rons test")
        self.assertTrue(sub_result.is_ok())
        s_row: SubItem = sub_result.data
        s_row.set('Status', 'Done')
        s_row.set('Date', '2020-05-10')
        result = s_row.update()
        self.assertTrue(result.is_ok())

        result = board.select(groups=['RTG'])
        self.assertTrue(result.is_ok(), result.message)

        end_count = self.monday_board.row_count
        assert (end_count - start_count == 1)
        return m_row

    # to test delete, we need to add a row then remove it.
    def test_delete(self):
        pid = 2154004550

        # when possible we want to use the same monday board vs create a new instance each time

        self.monday_board = self.monday.board(board_id=pid, clear_cache=True)
        result = self.monday_board.select(groups=['RTG'])
        start_count = self.monday_board.row_count
        m_row = self.create_row()
        result = self.monday_board.delete(m_row.row_id)
        self.assertTrue(result.is_ok())
        end_count = self.monday_board.row_count
        assert (end_count - start_count == 0)

    def test_create_group(self):
        result = self.monday_board.create_group(group_name="Test Group Create")
        assert result.is_ok()
        print(result.message)

    def test_get_assets(self):
        result = self.monday_board.get_assets(row_id=2500174012)
        assert result.is_ok()
        print(result.message)

    def test_get_item_ids(self):
        group_obj: Group = self.monday_board.get_group_id('RTG').data
        group_id = group_obj.group_id
        result = self.monday_board.get_item_ids(group_id=group_id)
        assert result.is_ok()
        print(result.message)

    def test_monday_sub_items(self):
        sub_counter = 0
        row_counter = 0

        result = self.monday_board.select(groups='Subitems Test', fields=['Status', 'Subitems'])
        self.assertTrue(result.is_ok(), msg="`Subitems Test` Group no longer exists")

        for row in self.monday_board.rows:
            row.get_subitems()
            if row.sub_items:
                row_counter += 1
                for sub in row.sub_items:
                    sub_counter += 1
        self.assertEqual(2, row_counter)
        self.assertEqual(3, sub_counter)

    def test_monday_insert_row(self):
        pid = 2154004550

        # when possible we want to use the same monday board vs create a new instance each time

        self.monday_board = self.monday.board(board_id=pid, clear_cache=True)
        result = self.monday_board.select(groups=['RTG'])

        start_count = self.monday_board.row_count
        m_row = self.create_row()
        end_count = self.monday_board.row_count
        assert(end_count - start_count == 1)
        result = self.monday_board.delete(m_row.row_id)
        result = self.monday_board.select(groups=['RTG'])
        end_count = self.monday_board.row_count
        assert (end_count - start_count == 0)

    def test_monday_drop_down(self):

        # test that the copy worked
        board: Board = self.monday.board(board_id=self.pid, clear_cache=False, make_copy=True)
        self.assertNotEqual(board, self.monday_board)
        self.assertEqual(0, board.row_count)

        result = board.select(col_name='Status', values=['Done'], fields=['Status'], update_rows=False)
        self.assertTrue(result.is_ok())
        self.assertEqual(0, board.row_count)

        # select from the copy
        result = board.select(col_name='Status', values=['Done'], fields=['Status'])
        self.assertTrue(result.is_ok())

        # self.assertEqual(board.row_count, 14)

    def test_monday_load_single_row_with_one_column(self):
        # row_id = 3528470879
        # row = self.monday_board.load_row(row_id, fields=['Status'])
        # self.assertEqual(len(row), 1)

        result = self.monday_board.select(group='RTG', col_name='Name', values=['Item 1'])
        self.assertEqual(self.monday_board.row_count, 1)
        self.assertEqual(len(result.data), 1)

    def test_load_monday_users(self):
        result = self.monday_board.select(col_name='Status', values=['Done'])
        self.assertTrue(result.is_ok())

        result = self.monday_board.load_all_users()
        self.assertTrue(result.is_ok())

    def test_monday_get_person_column(self):
        result = self.monday_board.select(col_name='Status', values=['Done'])
        self.assertTrue(result.is_ok())

        m_row: Row = self.monday_board.rows[0]
        person = m_row.get_value('People')
        self.assertEqual(person.value, 'Ron Sandstrom')

    def test_monday_select_groups(self):

        result = self.monday_board.select_group(group='RTG')
        self.assertTrue(result.is_ok())
        group_selection_count = self.monday_board.row_count

        result = self.monday_board.select(group='RTG', col_name='Status', values=['Done', 'Stuck'])
        self.assertTrue(result.is_ok())
        status_count = self.monday_board.row_count

        self.assertTrue(group_selection_count >= status_count)

    def test_monday_select_big_groups(self):
        """ Needs a borard with more than 200 entries to test. """
        pid = 3967950448
        big_monday: MondayFactory = MondayFactory()
        # when possible we want to use the same monday board vs create a new instance each time

        monday_board = big_monday.board(board_id=pid, clear_cache=True)

        # q_filter = monday_board.gen_filter([('VENDOR STATUS', ['Ready', 'Override'],)])
        #
        # result = monday_board.select_group(group='READY TO BE PROCESSED', q_filter=q_filter)
        # self.assertTrue(result.is_ok())
        # group_selection_count = monday_board.row_count

        result = monday_board.select(group='READY TO BE PROCESSED', col_name='VENDOR STATUS', values=['Ready', 'Override'])
        self.assertTrue(result.is_ok())
        status_count = monday_board.row_count

        # self.assertTrue(group_selection_count >= status_count)

    def test_add_link(self):
        result = self.monday_board.select_group(group='RTG')
        self.assertTrue(result.is_ok())
        for row in result.data:
            result = row.update(column_name=row.title.link,
                                value="https://forms.threestep.com/class-finder",
                                value2='Class Finder')
            self.assertTrue(result.is_ok())

    def test_get_board_name(self):
        t_request = Credentials.get_monday_token('automation@threestep.com')
        if t_request.is_error():
            raise Exception(f"Unable to load monday board error = {t_request.message}")
        else:
            monday_token = t_request.data.token
        headers = {"Authorization": monday_token}
        result = Board.check_access(1589538355, '', headers)
        assert result.is_error(), result.message

    def test_access(self):
        pid = 1589538355
        monday_board = self.monday.board(board_id=pid, clear_cache=True)
        assert monday_board.status.is_error(), monday_board.status.message

    def test_payment_request_expense_type(self):
        # used in payment request
        x = ExpenseAccounts().get_id('Advertising and Marketing')
        self.assertTrue(x == 980)
        x = ExpenseAccounts().get_account('Advertising and Marketing')
        self.assertTrue(x == 53010)

    def test_netsuite_expense_type(self):
        # used in reimbursement board
        x = ExpenseAccounts().get_id('Travel : Rental Cars-Truck Rentals')
        self.assertTrue(x == 1035)
        x = ExpenseAccounts().get_account('Travel : Rental Cars-Truck Rentals')
        self.assertTrue(x == 54050)



    def test_update_a_single_cell(self):
        # get a count of rows that have a status of done
        result = self.monday_board.select(col_name='Status', values=['Done'])
        self.assertTrue(result.is_ok())
        row_count = self.monday_board.row_count

        # change a value of a row to stuck
        row = self.monday_board.rows[0]
        row.update_column(column_name='Status', value='Stuck')

        # get a count of rows that have a status of done
        result = self.monday_board.select(col_name='Status', values=['Done'])
        self.assertTrue(result.is_ok())

        # check to see that the count is one less
        self.assertTrue(row_count - 1, self.monday_board.row_count)

        # reset the column to old value
        row.update_column(column_name='Status', value='Done')

        # check the counts again
        result = self.monday_board.select(col_name='Status', values=['Done'])
        self.assertTrue(result.is_ok())

        self.assertTrue(row_count, self.monday_board.row_count)

        result = self.monday_board.load_all_users()
        self.assertTrue(result.is_ok())

    # def test_cleanup_forms_db(self):
    #     board_id = 3476521463
    #     dbc: DatabaseFactory = DatabaseFactory()
    #     forms_table = dbc.table(application='v8_forms', table='forms_control_board')
    #     monday: MondayFactory = MondayFactory()
    #     board = monday.board(board_id, make_copy=True)
    #     board.select(groups=['Active Forms'], fields=['Status'], col_name='Status', values=['Ready'])
    #     monday_row_ids = {}
    #     db_row_ids = []
    #     for row in board.rows:
    #         monday_row_ids[row.row_id] = row.row_id
    #
    #     db_result = forms_table.select(fields=['row_id'])
    #     if db_result.is_ok():
    #         for row in db_result.data:
    #             db_row_ids.append(row.get('row_id'))
    #
    #     for db_id in db_row_ids:
    #         if str(db_id) not in monday_row_ids:
    #             logging.info(f"Need to deletes {db_id}")
    #
    #
    # def test_update_columns_list(self):
    #     # get a count of rows that have a status of done
    #     result = self.monday_board.select(col_name='Status', values=['Done'])
    #     self.assertTrue(result.is_ok())
    #     row_count = self.monday_board.row_count
    #
    #     # change a value of a row to stuck
    #     row = self.monday_board.rows[0]
    #     row.get('Text').value = 'Stuck'
    #     row.get('Status').value = 'Stuck'
    #     row.update(columns=['Status'])
    #
    #     # get a count of rows that have a status of done
    #     result = self.monday_board.select(col_name='Status', values=['Done'])
    #     self.assertTrue(result.is_ok())
    #
    #     # check to see that the count is one less
    #     self.assertTrue(row_count - 1, self.monday_board.row_count)
    #
    #     # reset the column to old value
    #     row.update_column(column_name='Status', value='Done')
    #
    #     # check the counts again
    #     result = self.monday_board.select(col_name='Status', values=['Done'])
    #     self.assertTrue(result.is_ok())
    #
    #     self.assertTrue(row_count, self.monday_board.row_count)
    #
    #     result = self.monday_board.load_all_users()
    #     self.assertTrue(result.is_ok())

    def test_minus_sign(self):
        monday: MondayFactory = MondayFactory()
        monday_board = monday.board(board_id=3560435494, clear_cache=True)
        rows = monday_board.select(groups=['test'])


if __name__ == '__main__':
    unittest.main()
