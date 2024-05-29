"""
MondayCore  Class that holds core data structures for use.
"""
from conversion.c_format import Format
from monday.c_connection import MondayConnection
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_maps import Maps
from std_utility.c_utility import Utility

from monday.c_cell import Cell
from monday.c_column import Column
from monday.c_row import Row


class MondayCore(MondayConnection):
    def __init__(self, board_id, monday_token, monday_account, monday_timeout_seconds=5, fields=None):
        super().__init__(board_id, monday_token, monday_account, monday_timeout_seconds)

        self.name = None
        self.permissions = None
        self.rows = []

        self.board_info_json = {}
        self.monday_board_json = {}
        self.column_info_map = {}
        self.column_info_sub_map = {}
        self.column_id_map = {}
        self.group_map = {}
        self.row_key_map = {}
        self.user_map = {}
        self.email_map = {}
        self.row_multimap = {}
        if fields is None:
            self.fields = []
        else:
            self.fields = fields

    @property
    def field_ids(self):
        ids = []
        if self.fields is None:
            self.fields = []
        else:
            for field in self.fields:
                if field in self.col_map:
                    f_id = self.col_map.get(field).id
                    ids.append(f_id)
        return ids

    @property
    def field_ids_str(self):
        use_values = '['
        for v in self.field_ids:
            use_values += f'"{v}",'
        use_values += ']'
        return use_values

    @property
    def has_fields(self):
        # fields in the connection class is a string that contains a list of ids used for queries.
        if isinstance(self.fields, list):
            return len(self.fields) > 0
        return False

    @property
    def row_count(self):
        return len(self.rows)

    @property
    def group_count(self):
        return len(self.group_map)

    @property
    def column_count(self):
        return len(self.column_info_map)

    def create_rows_from_json(self,
                              monday_board_json,
                              filter_column=None,
                              filter_values=None,
                              load_subitems=False) -> [Row]:
        """

        """
        """Creates a set of rows from the monday board json response.

        @filter_column: filter columns
        Args:
            monday_board_json: Json obtained by calling the Monday API for a board

        Returns: An Array of rows
        """
        do_filter = filter_column is not None and filter_values is not None

        rows = []
        try:
            # monday_board_json.get('data', {}).get('boards', [{}])[0].get('items'):
            # items = self.find_id(monday_board_json, 'items')
            items = self.get_board_items(monday_board_json)
            if len(items) == 0:
                return []
            # assert len(items) > 0, "this query generated zero rows"

            # remove any rows that do not match our filters (only if there is a filter)
            if do_filter:
                remove_row_list = []
                for this_row in items:
                    row_name = Utility.clean_name(this_row.get('name'))
                    keep = False
                    columns_json = this_row.get('column_values')
                    if filter_column.lower() == 'name' or filter_column.lower() == 'item':
                        if row_name in filter_values:
                            continue
                    for this_cell in columns_json:
                        col_name = Utility.clean_name(this_cell.get('title'))
                        col_val = this_cell.get('text')
                        if col_name.lower() == filter_column.lower():
                            if col_val in filter_values or filter_values == ['*']:
                                keep = True
                    if not keep:
                        remove_row_list.append(this_row)
                for remove_row in remove_row_list:
                    items.remove(remove_row)

            # now process the rows that remain
            for this_row in items:
                new_row = Row(self)
                new_row.on_monday = True
                new_row.group_id = this_row.get('group', {}).get('id')
                new_row.group_name = Format(this_row.get('group', {}).get('title')).name
                new_row.row_id = this_row.get('id')
                new_row.row_name = Format.as_ascii(this_row.get('name'))
                new_row.assets = this_row.get('assets')
                columns_json = this_row.get('column_values')
                Maps.add_to_map_array(self.row_multimap, new_row.row_name, new_row)

                # add the item to the cells and update maps
                new_cell = Cell(new_row)

                new_cell.row_id = new_row.row_id
                new_cell.id = 'name'
                new_cell.name = self.column_id_map.get(new_cell.id).name
                new_cell.labels = self.column_id_map.get(new_cell.id).labels
                new_cell.value = new_row.row_name
                new_cell.type = 'text'
                new_cell.index = len(columns_json) + 1
                new_cell.parent_row = new_row
                new_cell.modified = False
                new_cell.previous_value = new_cell.value
                new_row.cells.append(new_cell)
                new_row.cell_map[new_cell.name] = new_cell
                new_row.cell_db_map[new_cell.id] = new_cell
                setattr(new_row.readonly, Utility.db_name(new_cell.name), new_cell.value)
                setattr(new_row.title, Utility.db_name(new_cell.name), new_cell.name)

                # add the column info, need to do it here because it does not exist when we read the columns
                new_column_info = Column(new_cell.index, new_cell.id, new_cell.name, new_cell.type)
                self.column_info_map[new_cell.name] = new_column_info

                # now get the cells from the json columns (from monday), create cells and stor them in a row
                for this_cell in columns_json:
                    new_cell = Cell(new_row)
                    new_cell.source = this_cell
                    new_cell.row_id = new_row.row_id
                    new_cell.id = this_cell.get('id')
                    # since monday renames the first field, and it could conflict with other names, we need to get the
                    # updated name from the column info map that handles duplicate names.
                    column: Column = self.column_id_map.get(new_cell.id)
                    if column is None:
                        continue
                    new_cell.name = column.name
                    # Utility.clean_name(this_cell.get('title'))

                    column_info: Column = self.column_info_map.get(new_cell.name)
                    new_cell.type = column_info.type
                    new_cell.labels = column_info.labels
                    new_cell.index = column_info.index
                    new_cell.parent_row = new_row
                    new_cell.has_labels = column_info.has_labels
                    if column_info.type == 'date' or column_info.type == 'datetime':
                        new_cell._value = DateTime(this_cell.get('text', '1970-01-01 00:00:00'))
                    else:
                        new_cell._value = this_cell.get('text')
                    new_cell._modified = False
                    # needs lookup to ensure new cell is saved with the correct name

                    new_row.cell_map[new_cell.name] = new_cell
                    new_row.cells.append(new_cell)
                    setattr(new_row.readonly, Utility.db_name(new_cell.name), new_cell.value)
                    setattr(new_row.title, Utility.db_name(new_cell.name), new_cell.name)

                    if new_cell.id == 'subitems' or new_cell.id == 'subitems2':
                        new_row.has_subitems = True
                        sub_id = new_cell.id
                        if load_subitems:
                            new_row.load_sub_items(sub_id)

                new_row.cell_db_map = new_row.update_cell_db_map()

                rows.append(new_row)

        except KeyError as ex:
            logging.error(ex)

        return rows

    # def process_rows(self, items):
    #     rows = []
    #     try:
    #         # remove any rows that do not match our filters (only if there is a filter)
    #         load_subitems = False
    #         # now process the rows that remain
    #         for this_row in items:
    #             new_row = Row(self)
    #             new_row.on_monday = True
    #             new_row.group_id = this_row.get('group', {}).get('id')
    #             new_row.group_name = Format(this_row.get('group', {}).get('title')).name
    #             new_row.row_id = this_row.get('id')
    #             new_row.row_name = Format(this_row.get('name')).name
    #             new_row.assets = this_row.get('assets')
    #             columns_json = this_row.get('column_values')
    #             Maps.add_to_map_array(self.row_multimap, new_row.row_name, new_row)
    #
    #             # add the item to the cells and update maps
    #             new_cell = Cell(new_row)
    #
    #             new_cell.row_id = new_row.row_id
    #             new_cell.id = 'name'
    #             new_cell.name = self.column_id_map.get(new_cell.id).name
    #             new_cell.labels = self.column_id_map.get(new_cell.id).labels
    #             new_cell.value = new_row.row_name
    #             new_cell.type = 'text'
    #             new_cell.index = len(columns_json) + 1
    #             new_cell.parent_row = new_row
    #             new_cell._modified = False
    #             new_cell.previous_value = new_cell.value
    #             new_row.cells.append(new_cell)
    #             new_row.cell_map[new_cell.name] = new_cell
    #             new_row.cell_db_map[new_cell.id] = new_cell
    #
    #             # add the column info, need to do it here because it does not exist when we read the columns
    #             new_column_info = Column(new_cell.index, new_cell.id, new_cell.name, new_cell.type)
    #             self.column_info_map[new_cell.name] = new_column_info
    #
    #             # now get the cells from the json columns (from monday), create cells and stor them in a row
    #             for this_cell in columns_json:
    #                 new_cell = Cell(new_row)
    #                 new_cell.source = this_cell
    #                 new_cell.row_id = new_row.row_id
    #                 new_cell.id = this_cell.get('id')
    #                 # since monday renames the first field, and it could conflict with other names, we need to get the
    #                 # updated name from the column info map that handles duplicate names.
    #                 column: Column = self.column_id_map.get(new_cell.id)
    #                 if column is None:
    #                     continue
    #                 new_cell.name = column.name
    #                 # Utility.clean_name(this_cell.get('title'))
    #
    #                 column_info: Column = self.column_info_map.get(new_cell.name)
    #                 new_cell.type = column_info.type
    #                 new_cell.labels = column_info.labels
    #                 new_cell.index = column_info.index
    #                 new_cell.parent_row = new_row
    #                 new_cell.has_labels = column_info.has_labels
    #                 if column_info.type == 'date' or column_info.type == 'datetime':
    #                     new_cell._value = DateTime(this_cell.get('text', '1970-01-01 00:00:00'))
    #                 else:
    #                     new_cell._value = this_cell.get('text')
    #                 new_cell._modified = False
    #                 # needs lookup to ensure new cell is saved with the correct name
    #
    #                 new_row.cell_map[new_cell.name] = new_cell
    #                 new_row.cells.append(new_cell)
    #                 if new_cell.id == 'subitems' or new_cell.id == 'subitems2':
    #                     new_row.has_subitems = True
    #                     sub_id = new_cell.id
    #                     if load_subitems:
    #                         new_row.load_sub_items(sub_id)
    #
    #             new_row.cell_db_map = new_row.update_cell_db_map()
    #
    #             rows.append(new_row)
    #
    #     except KeyError as ex:
    #         logging.error(ex)
    #
    #     return rows

    # Gets the board name from a monday board query result
    @staticmethod
    def get_board_items(monday_board_json: dict) -> []:
        """
        Gets the board name from a monday board query result
        @param monday_board_json:
        @return:
        """
        items = []
        data = monday_board_json.get('data')
        if data is not None:
            items = data.get('items')
            if items is None:
                boards = data.get('boards')
                if boards is not None and len(boards) > 0:
                    items = boards[0].get('items')

        if items is None:
            items = []
        return items
