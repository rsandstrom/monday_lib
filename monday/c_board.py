"""
Board Class
"""
import json
import logging
from datetime import datetime

from conversion.c_format import Format
from monday.c_column import Column
from monday.c_required import RequiredElements
from monday.c_select import MondaySelect
from monday.c_verify import VerifyBoard
from result.c_result import Result

from monday.c_cell import Cell
from monday.c_functions import MondayFunctions
from monday.c_row import Row
from std_utility.c_datetime import DateTime

"""
 Args:
     board_id: The monday board id
     monday_token: REQUIRED
     unique_key: key constraint for the rows, will cause an exception if duplicate key is found.
     lookup: Future: A dictionary key value pairs with field names to limit the number of columns and find data
     monday_timeout_seconds: amount of time before we retry a request to monday. large monday boards may take up 
     to 60 seconds or more to load
     monday_account: If a monday token is not passed in, this account will be used to lookup access to a board

 Raises:
     Unique Key Violation: Unable to load monday board
 """


class Board(MondayFunctions):
    def __init__(self, board_id, monday_token, monday_timeout_seconds=5, monday_account=None, fields=None,
                 verify_columns: [Column] = None, alert_to: [] = None):
        assert board_id is not None, "Board ID is required to initialize a board"

        super().__init__(board_id, monday_token, monday_account, monday_timeout_seconds, fields)

        self.was_altered = False
        self.missing_columns = []
        self.missing_labels = []
        self._temp_rows = []

        # load columns and group info
        self.status: Result = self.load_monday_column_and_group_info()

        # needs to exist, it is used when we create queries and convert from field names to field id's
        self.set_col_map(self.column_info_map)

        self.verify_columns = verify_columns
        self.alert_to = alert_to
        if self.verify_columns is not None:
            self.verify(required=self.verify_columns, alert_to=self.alert_to)

        logging.debug(f"Successful load of the monday board {self.name}")

    @property
    def keys_valid(self):
        """ True if the key map is valid for use."""
        return len(self.row_key_map) > 0

    @property
    def has_subitems(self):
        return 'Subitems' in self.column_info_map or 'Subitems2' in self.column_info_map

    def verify(self, required: RequiredElements = None, alert_to: [] = None) -> Result:
        board_checker = VerifyBoard(self)
        return board_checker.verify_required(required, alert_to)

    def add_row(self, row_name: str = None, group_name: str = None, no_strip=False):
        """Creates a new empty row and Inserts it.

        Args:
            row_name: the left most column on the board is the row name
            group_name: the name of the group this row will belong to

        Returns: a Row class object
        todo: look for new row from json

        """
        new_row = self.new_row(row_name, group_name, no_strip)
        result = new_row.insert()
        if result.is_ok():
            data = result.data.get('data').get('create_item')
            monday_board_json = {'data': {'items': [data, ]}}
            new_rows = self.create_rows_from_json(
                monday_board_json,
                filter_column=None,
                filter_values=None,
                load_subitems=False)
            new_row = new_rows[0]
            self.rows.append(new_row)
            result.data = new_row
        return result

    class ColumnActivityLog:
        def __init__(self, row):

            self.user_id = row.get('user_id')
            self.user_name = None
            self.user_email = None
            self.timestamp = int(row.get('created_at'))
            self.date_time = None
            self.created = None
            if self.timestamp is not None:
                self.date_time = datetime.utcfromtimestamp(self.timestamp / 10000000).strftime('%c')
                self.created = DateTime(dt=self.date_time)
            self.event = row.get('event')
            self.column_name = None
            data = row.get('data')
            if data is not None:
                jdata = json.loads(data)
                self.column_name = jdata.get('column_title')
                self.new_value = jdata.get('value').get('label').get('text')
                pass

    def get_column_activity_log(self, column_names: [str] = None, limit=2, match=None, user_info=False):
        response = Result(code=-1, message="Unable to locate activity log record")
        try:
            if column_names is None:
                column_names = []

            column_ids = []
            for c in column_names:
                if c in self.col_map:
                    column_ids.append(self.col_map.get(c).id)

            column_id_str = '' if len(column_ids) == 0 else ','.join(column_ids)

            response = self.monday_load_column_activity_log(column_id_str, limit)
            log = []
            if response.is_ok():
                activity_logs = response.data.get('data').get('boards')[0].get('activity_logs')
                for v in activity_logs:
                    entry = self.ColumnActivityLog(row=v)
                    if match is not None:
                        if entry.new_value == match:
                            if user_info:
                                u_result = self.monday_get_user(uid=entry.user_id)
                                if u_result.is_ok():
                                    j_data = u_result.data;
                                    entry.user_name = j_data[0].get('name')
                                    entry.email = j_data[0].get('email')
                            log.append(entry)
                            break
                    else:
                        log.append(entry)
                response.data = log
        except Exception as ex:
            logging.warning(ex)

        return response

    def update_column_in_group(self, group=None, column_name=None, column_values: [] = None, update_value: str = None):
        """
        update column in group
        requires a group name, a column name and an update value
        optional is the select_list used to filter records and update only those that match.
        we skip updates for fields that are already updated, saving time and energy
        """
        # result = self.get_mini_rows_from_group(group=group, column_name=column_name, col_values=select_list)
        result = self.select(group=group, col_name=column_name, col_values=column_values,
                             fields=[column_name], update_rows=False)
        if result.is_ok():
            for row in result.data:
                if row.get(column_name).value == update_value:
                    continue
                update_result = self.update_single_column(row.row_id, column_name=column_name,
                                                          column_value=update_value)
                logging.debug(f"row id = {row.row_id}: update was: {update_result.message}")

        return result

    def select(self,
               groups=None,
               fields=None,
               col_name=None,
               operator=None,
               col_values=None,
               values=None,
               group=None,
               limit=100,
               update_rows=True,
               q_filter=None) -> Result:
        """
        v4 select uses the new rate limiting model for monday.com to return and filter rows.
        groups may be passed as a single group name or an array of group names. If no group then all groups are processed.
        fields limit the number of fields returned in the row set, fields may be a single string or an array of names
        col_name, and col_vals are used for filtering. if operator is specified, then it is used to compare each row
        against a row value and one of the comparison columns.  If any condition matches, the row is added to result
        set, if no operator then the row value in col_name must match one of the col_vals.
        """
        result = Result(-1, message="N/A")

        if col_values is None and values is not None:
            col_values = values

        if groups is None and group is not None:
            groups = group

        if isinstance(groups, str):
            groups = [groups]

        select_rows = []
        if groups is None and (operator is not None or col_name is None):
            groups = MondaySelect.get_group_ids(self, groups)

        # ok we need to do this because the API does not allow selections and groups in the same query
        # if there is no group but there is a selection let monday do the work.
        # if we have a custom selection using an operator, then loop through all the groups and filter the data
        if groups is None:
            result = MondaySelect.group(self,
                                        self.board_id,
                                        groups=None,
                                        fields=fields,
                                        col_name=col_name,
                                        operator=operator,
                                        col_values=col_values,
                                        limit=limit,
                                        q_filter=q_filter)

            if result.is_ok():
                select_rows = result.data

        else:
            for group_id in groups:
                result = MondaySelect.group(self,
                                            self.board_id,
                                            groups=group_id,
                                            fields=fields,
                                            col_name=col_name,
                                            operator=operator,
                                            col_values=col_values,
                                            limit=limit,
                                            q_filter=q_filter)
                if result.is_ok():
                    select_rows.extend(result.data)

        result.data = select_rows

        if update_rows:
            self.rows = select_rows

        return result

    def gen_filter(self, filters):
        if filters:
            if not isinstance(filters, list):
                filters = [filters]
            for f in filters:
                filter_col = f[0]
                if filter_col not in self.col_map:
                    continue
                col_id = self.col_map[filter_col].id
                label_map = self.col_map[filter_col].label_map
                filter_values = f[1]
                if not isinstance(filter_values, list):
                    filter_values = [filter_values]
                values = []
                for f1 in filter_values:
                    values.append(label_map[f1])

            f_query = f', query_params: {{rules: [{{column_id: "{col_id}", compare_value: {values} }}]}}'
        else:
            f_query = ''

        print(f_query)
        return f_query

    def select_all_matching_callback(self, m_row, *args):
        col_name = args[0]
        if col_name is None:
            self._temp_rows.append(m_row)
            return

        values = args[1]
        try:
            val = m_row.get(col_name).value
        except Exception as ex:
            logging.warning(f"Unable to locate the column name [{col_name}] in the list of columns -> {ex}")
            return

        if len(values) > 0:
            if not isinstance(values[0], str):
                logging.warning('Field Values in select_all_matching must be of type str')

        if val in values:
            self._temp_rows.append(m_row)

    def select_old1(self, group=None, col_name=None, values=None) -> Result:
        if len(self.fields) > 0 and col_name is not None \
                and self.column_info_map.get(col_name).id not in self.fields:
            msg = f"Unable to locate the column name [{col_name}] in the list of columns"
            logging.warning(msg)
            return Result(-1, message=msg, data=[])

        item_count = self.get_item_count()
        logging.info(item_count)

        self.rows = []
        try:
            self._temp_rows = []
            if group is not None:
                self.select_group_via_callback(group, self.select_all_matching_callback, col_name, values)
            else:
                self.select_all_via_callback(self.select_all_matching_callback, col_name, values)
            self.rows = self._temp_rows
        except Exception as ex:
            logging.warning(ex)
            return Result(-1, message=ex)

        return Result(0, data=self.rows)

    def select_all_via_callback(self, callback=None, *args):
        ids = []
        for group in self.group_map:
            group_ids = self.select_group_via_callback(group, callback, *args)
            ids.extend(group_ids)
        return ids

    def select_group_via_callback(self, group, callback, *args) -> []:
        """
        executes callback (function) for each row in the group
        call this using board.process_group_generic(group='sample', process_row, arg1....)
        returns a complete list of row ids when done.
        """
        col_name = args[0]
        values = args[1]
        assert callback is not None, "Process group via call back is missing a call back function"
        # row_ids = []
        logging.info(f"Fetching group [{group}].  this could take a moment")
        # group_result = self.select_group(group=group, row_ids_only=True)
        group_result = self.get_item_ids(group_id=group, column_id=col_name, col_values=values)
        return self.do_callback_rows(group_result, callback, *args)

    def select_via_callback(self, group=None, col_name=None, values=None, callback=None, *args) -> []:
        """
        executes callback (function) for each row in the group
        call this using board.process_group_generic(group='sample', process_row, arg1....)
        returns a complete list of row ids when done.
        """

        assert callback is not None, "Process group via call back is missing a call back function"
        # row_ids = []
        logging.info(f"Fetching all row ids for group [{group}].  this could take a moment")
        the_result = self.select(group=group, col_name=col_name, values=values, row_ids_only=True)
        return self.do_callback_rows(the_result, the_callback=callback, *args)

    @staticmethod
    def row_ids_only_values(result) -> Result:
        if isinstance(result.data, list):
            return result

        row_ids = result.data.get('data').get('items')
        ids = []
        for item in row_ids:
            ids.append(item.get('id'))
        return Result(0, data=ids)

    def select_group(self, group=None, col_name=None, values=None, row_ids_only=False, q_filter=None):
        return self.select(groups=group, col_name=None, values=None, q_filter=q_filter)

    def gen_field_list_for_monday_query(self) -> str:
        """ gets the list of columns, then using the field list creates a string to be inserted into the monday query
        :return string in the form of ["id1", "id2", "id3", ]:
        """
        use_fields = ''
        try:
            col_list = self.board_info_json.get('data').get('boards')[0].get('columns')
            ids = self.convert_fields_to_ids(self.fields, col_list)
            use_fields = '['
            for field in ids:
                use_fields += f'"{field}",'
            use_fields += ']'
        except Exception as ex:
            logging.warning(ex)
        return use_fields

    # creates a new empty row but does not insert it.
    def new_row(self, row_name, group_name=None, no_strip=False) -> Row:
        """Creates a new empty row but does not insert it.

        Args:
            row_name: The name of the new row
            group_name: The name of the group this rows belongs to or None for the default group

        Returns: A Row object

        Raises:
            Exception: Unable to find group name (if group name is not None)

        """

        result = self.get_group_id(group_name)
        if result.status.is_error():
            raise Exception(result.message)

        group_id: str = result.data.group_id
        group_name = result.data.group_name

        row = Row(self)
        row.init_empty(group_id=group_id, group_name=group_name, row_name=row_name,
                       column_info_map=self.column_info_map, no_strip=no_strip)
        return row

    def get_db_fields(self):
        return ', '.join('`{0}`'.format(Format(k).snake_case) for k in self.col_map.keys())

    def get_db_parameters(self):
        return ', '.join('%s'.format(k) for k in self.col_map.keys())
