"""
Monday Functions
"""
from conversion.c_format import Format
from result.c_result import Result
import logging
from std_utility.c_maps import Maps
from std_utility.c_utility import Utility

from monday.c_callbacks import MondayCallBacks
from monday.c_cell import Cell
from monday.c_column import Column
from monday.c_group import Group
from monday.c_required import RequiredElements
from monday.c_verify import VerifyBoard


class MondayFunctions(MondayCallBacks):
    def __init__(self, board_id, monday_token, monday_account, monday_timeout_seconds=5, fields=None):
        super().__init__(board_id, monday_token, monday_account, monday_timeout_seconds, fields)

    # gets the group id and name
    def get_group_id(self, group_name=None) -> Result:
        """ Gets the group id and name.

        Note: default group in Monday is topics

        Args:
            group_name: The name of the group to lookup

        Returns: a Result class object

        """

        if group_name is None:
            group_name = 'Default Group'

        logging.debug("Insert_row group: " + group_name)

        group_id = self.group_map.get(group_name)
        if len(group_id) == 0:
            return Result(message=f"group {group_name} not found")

        return Result(code=0, data=Group(group_id=group_id, group_name=group_name))

    def load_monday_column_and_group_info(self) -> Result:
        """ function reads one row from monday and returns all column info and group data.  used to create column maps
        and allow for easy lookup from display column name to internal cryptic id (ids are needed for everything)
        """
        # load columns and group info
        result = self.load_column_and_group_info_as_json()
        if result.is_ok():
            #     return result
            #     raise Exception(f"Unable to load monday board error = {result.status.message}")
            #
            # if result.data is not None:
            #     check_data = result.data.get('data')
            #     if check_data is None:
            #         raise Exception(f"Unable to load monday board error = {result.status.message}")
            #     check_boards = check_data.get('boards')
            #     if check_boards is None or len(check_boards) == 0:
            #         raise Exception(f"Access to boards may be an issue, Zero boards returned from Monday")

            self.board_info_json = result.data
            # create board maps from json
            self.column_info_map = self.create_column_info_map_from_json(self.board_info_json)
            self.column_id_map = self.create_column_id_map_from_info(self.column_info_map)
            self.group_map = self.create_group_map_from_json(self.board_info_json)
            self.rows = self.create_rows_from_json(self.board_info_json)

            # gets the board name from the json
            self.name = self.get_board_name(self.board_info_json)
            self.permissions = self.get_board_permissions(self.board_info_json)
            self.rows = []
            self.row_multimap = {}
            result = Result(0)

        return result

    def sub_item_add_row(self, parent_row=None, sub_row_dict: dict = None):
        assert parent_row is not None, "Parent Row is required to add a sub item row"
        assert sub_row_dict is not None or not isinstance(sub_row_dict, dict), "Missing data for the sub item row"
        """
        creates a sub row from monday sub row data returned.
        Args:
            row: parent row
            sub_row_dict:
            sub_board_id:

        Returns:

        """
        sub_id = sub_row_dict.get('id')
        sub_board_id = sub_row_dict.get('board').get('id')
        sub_name = Format(sub_row_dict.get('name')).name
        sub_row = sub_row_dict.get('column_values')
        sub_assets = sub_row_dict.get('assets')
        from monday.c_subitem import SubItem
        sub_item = SubItem(board=self, parent_row=parent_row,
                           row_id=sub_id, row_name=sub_name,
                           row_data=sub_row,
                           col_map=self.column_info_sub_map,
                           assets=sub_assets,
                           sub_board_id=sub_board_id)
        sub_item.on_monday = True

        sub_item.update_cell_db_map()
        sub_item.update_cell_map()
        sub_item.parent_row.sub_items.append(sub_item)
        Maps.add_to_map_array(sub_item.parent_row.sub_multimap, sub_item.row_name, sub_item)
        return sub_item

    def find_id(self, obj_dictionary, item):
        if isinstance(obj_dictionary, dict) and item in obj_dictionary:
            return obj_dictionary.get(item)

        if isinstance(obj_dictionary, list):
            for o in obj_dictionary:
                return self.find_id(o, item)

        if isinstance(obj_dictionary, dict) and item not in obj_dictionary:
            for v in obj_dictionary.values():
                return self.find_id(v, item)

        return None

    def verify(self, required: RequiredElements = None, alert_to: [] = None) -> Result:
        board_checker = VerifyBoard(self)
        return board_checker.verify_required(required, alert_to)

    def lookup_email_address(self, name):
        if len(self.email_map) == 0:
            self.load_all_users()
        retval = self.user_map.get(name)
        if retval is None:
            retval = self.email_map.get(name)
        return retval

    def load_all_users(self) -> Result:
        result = self.load_monday_all_users_as_json()
        if result.is_ok():
            for u in result.data:
                self.user_map[u.get('name')] = u.get('email')
                self.email_map[u.get('email')] = u.get('name')
        return result

    def update_single_column(self, row_id=None, column_name: str = '', column_value: str = '') -> Result:
        """ Note if you call this function with no column value the column will be emptied.
                  Function is used to update a single column value for a row, such as a status or a dropdown.
              """
        if row_id is None:
            return Result(-1, "Row Id is required")

        cell: Cell = self.column_info_map.get(column_name)
        if cell is None:
            return Result(-1, message=f"Unable to locate col name [{column_name}]")
        column_id = cell.id
        return self.update_single_column_value(row_id, column_id, column_value)

    @staticmethod
    def convert_fields_to_ids(fields, col_list):
        if fields is None:
            fields = []
        col_map = {}
        ids = []
        try:
            for col in col_list:
                c_id = col.get('id')
                c_name = Utility.clean_name(col.get('title'))
                col_map[c_name] = c_id
            for field in fields:
                ids.append(col_map.get(field))
        except Exception as ex:
            logging.warning(ex)
        return ids

    # fetches the groups from the monday board json and creates a lookup table of group name -> group id
    @staticmethod
    def create_group_map_from_json(monday_board_json: dict):
        """Fetches the groups from the monday board json and creates a lookup table of group name -> group id.

        Args:
            monday_board_json: From the Monday board Api

        Returns: A group map dictionary { group_name : group_id ... }

        """

        try:
            data = monday_board_json.get('data')
            boards = data.get('boards')
            group_json = (boards[0]).get('groups')

        except KeyError as e:
            logging.warning(e)
            return None

        # [{'id': 'topics', 'title': 'Default Group'}]
        retval = {}
        for group in group_json:
            try:
                _id = group.get('id')
                _title = Utility.clean_name(group.get('title'))
                retval[_title] = _id

            except KeyError as e:
                logging.warning(e)

        return retval

        # create a column map for all columns, note the column class parses and creates the column info

    @staticmethod
    def create_column_map_from_json(monday_board_json: dict) -> dict:
        """
        create a column map for all columns, note the column class parses and creates the column info
        @param monday_board_json:
        @return:
        """
        col_map = {}
        try:
            index = 0
            columns_json = monday_board_json.get('data', {}).get('boards', [{}])[0].get('items')[0].get('column_values')
            for _col in columns_json:
                new_column = Column()
                new_column.from_json(index, _col)
                dup_col_idx = 0
                while new_column.name in col_map:
                    new_column.name = f"{Utility.clean_name(new_column.name)}_{dup_col_idx}"
                    dup_col_idx += 1
                col_map[new_column.name] = new_column
                index += 1

        except KeyError:
            logging.warning("Looks like there are no columns")
            return {}

        return col_map

    # create a column map for all columns, note the column class parses and creates the column info
    @staticmethod
    def create_column_info_map_from_json(monday_board_json: dict) -> dict:
        """
        create a column map for all columns, note the column class parses and creates the column info
        @param monday_board_json:
        @return:
        """
        col_map = {}
        try:
            index = 0
            columns_json = monday_board_json.get('data', {}).get('boards', [{}])[0].get('columns')
            for _col in columns_json:
                new_column = Column()
                new_column.from_json(index, _col)
                dup_col_idx = 1
                while new_column.name in col_map:
                    new_column.name = f"{Utility.clean_name(new_column.name)}_{dup_col_idx}"
                    dup_col_idx += 1
                col_map[new_column.name] = new_column
                index += 1

        except KeyError:
            logging.warning("Looks like there are no columns")
            return {}

        return col_map

    @staticmethod
    def get_columns_from_json(monday_board_json: dict) -> [Column]:
        """
        gets columns from json Used to read monday board columns
        @param monday_board_json:
        @return:
        """
        new_columns = []
        try:
            # get the monday board columns
            _columns = monday_board_json.get('data', {}).get('boards', [{}])[0].get('items')[0].get('column_values')

            # add the columns to the columns array
            index = 0
            for _col in _columns:
                new_column = Column()
                new_column.from_json(index, _col)
                new_columns.append(new_column)
                index += 1

        except KeyError:
            logging.warning("Looks like there are no columns")

        return new_columns

    # Gets the board permissions from a monday board query result
    @staticmethod
    def get_board_permissions(monday_board_json: dict) -> str:
        """
        Gets the board name from a monday board query result
        @param monday_board_json:
        @return:
        """
        default = {'boards': [{'name': ''}]}
        name: str = monday_board_json.get('data', default).get('boards')[0].get('permissions')
        return name

    # Gets the board name from a monday board query result
    @staticmethod
    def get_board_name(monday_board_json: dict) -> str:
        """
        Gets the board name from a monday board query result
        @param monday_board_json:
        @return:
        """
        default = {'boards': [{'name': ''}]}
        name: str = monday_board_json.get('data', default).get('boards')[0].get('name')
        return Format(name).name

    # Gets the board id from a monday board query result
    @staticmethod
    def get_board_id(monday_board_json):
        """
        Gets the board id from a monday board query result
        @param monday_board_json:
        @return:
        """
        default = {'boards': [{'id': None}]}
        return monday_board_json.get('data', default).get('boards')[0].get('id')

    @staticmethod
    def create_column_id_map_from_info(_column_info_map) -> dict:
        id_map = {}
        for k, v in _column_info_map.items():
            id_map[v.id] = v
        return id_map
