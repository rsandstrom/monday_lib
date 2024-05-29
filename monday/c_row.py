"""
  ***********************************************
    Row: Contains rows and row column values

        Example: {
        'name': 'Test 2',
        'id': '2008832605',
        'column_values':
            [ {'id': 'text', 'title': 'Team Name', 'text': 'Team 2' } ],
        'group': {
                'id': 'west16886',
                'title': 'West'
                }
        }

    This is a critical component of this monday board model, the row knows how to update, insert, delete and create
    itself, functions are designed to set up a row from json or generate an empty row with empty cells.
  ***********************************************
"""
import logging

from conversion.c_format import Format
from monday.c_field import FieldValue
from monday.c_title import Title
from result.c_result import Result
from std_utility.c_datetime import DateTime
from std_utility.c_maps import Maps

from monday.c_cell import Cell
from monday.c_column import Column
from monday.c_subitem import SubItem


class Row:
    def __init__(self, board):
        self.on_monday = False
        self.group_id = None
        self.board = board
        self.group_name = None
        self.row_id = None
        self.row_name = None
        self.sub_items = []
        self.sub_multimap = {}
        self.assets = []
        self.cells: [Cell] = []
        self.cell_map = {}
        self.cell_db_map = {}
        self.key = None

        # if the different types of boards have different default group names and id's we can fix that here
        self._set_default_group()

        self.readonly = FieldValue()
        self.title = Title()

    @staticmethod
    def get_asset_from_json(d) -> []:
        """
        get the assets from a board row and return it (used in download)
        """
        try:
            return d.get('data').get('boards')[0].get('items')[0].get('assets')
        except Exception as ex:
            logging.warning(ex)
        return []

    def get_subitems(self):
        """
        get subitems is used when a row wants to get or refresh its subitems
        """
        if 'Subitems' in self.cell_map or 'Subitems2' in self.cell_map:
            sub_id = self.cell_map.get('Subitems')
            if sub_id is None:
                sub_id = self.cell_map.get('Subitems2')

            assert sub_id is not None, 'unable to locate the subitem id'
            self.load_sub_items(sub_id.id)

    def _add_sub_row(self, sub_row_dict: dict, sub_board_id):
        """
        creates a sub row from monday sub row data returned.
        Args:
            sub_row_dict:
            sub_board_id:

        Returns:

        """
        sub_id = sub_row_dict.get('id')
        sub_name = Format(sub_row_dict.get('name')).name
        sub_row = sub_row_dict.get('column_values')
        sub_assets = sub_row_dict.get('assets')
        sub_item = SubItem(board=self.board, row_id=sub_id, row_name=sub_name,
                           row_data=sub_row, col_map=self.board.column_info_sub_map,
                           assets=sub_assets, sub_board_id=sub_board_id)
        sub_item.on_monday = True

        sub_item.update_cell_db_map()
        sub_item.update_cell_map()
        self.sub_items.append(sub_item)
        Maps.add_to_map_array(self.sub_multimap, sub_item.row_name, sub_item)
        return sub_item

    def load_sub_row(self, sub_ids, sub_board_id=None):
        result = self.board.get_sub_rows(sub_ids)
        sub_item = None
        if result.is_ok():
            data = result.data
            for r in data.get('data').get('items'):
                sub_item = self._add_sub_row(r, sub_board_id)

            if sub_item is None:
                return Result(-1, message="Unable to create subitem")
            else:
                sub_item.on_monday = True
                return Result(0, data=sub_item)

    def load_sub_rows(self, sub_ids):
        result = self.board.get_sub_rows(sub_ids)
        if result.is_ok():
            data = result.data
            col_map = SubItem.create_column_info_map(data)
            if len(col_map) > 0:
                self.board.column_info_sub_map = col_map
            self.sub_items = []
            for r in data.get('data').get('items'):
                sub_id = r.get('id')
                sub_name = r.get('name')
                sub_row = r.get('column_values')
                # asset_list = self.connection.get_assets(sub_id)
                # sub_assets = self.connection.get_asset_from_json(asset_list.data)
                sub_assets = r.get('assets')
                subitem = SubItem(board=self.board, row_id=sub_id, row_name=sub_name,
                                  row_data=sub_row, col_map=col_map, assets=sub_assets)

                subitem.update_cell_db_map()
                subitem.update_cell_map()
                self.sub_items.append(subitem)
                Maps.add_to_map_array(self.sub_multimap, subitem.row_name, subitem)

    # https://3step-sports.monday.com/boards/2371736570/pulses/2371737124
    def load_sub_items(self, sub_id):
        # if self.cell_map.get('Subitems') is not None:
        if self.board.has_subitems:
            result = self.board.get_sub_item_ids(self.row_id, sub_id)
            if result.is_ok():
                sub_ids = result.data
                # added check to not create sub item entries when none exist
                if len(sub_ids) == 0:
                    return

                self.load_sub_rows(sub_ids)

    @property
    def cell_count(self):
        """Get the cell count for this row"""
        return len(self.cells)

    def find_sub_rows(self, name, index):
        return Maps.multimap_get(self.sub_multimap, name, index)

    def update_cell_map(self):
        """used to on demand update the cell map"""
        self.cell_map = {}
        for a_cell in self.cells:
            a_cell: Cell = a_cell
            if a_cell.name not in self.cell_map:
                self.cell_map[a_cell.name] = a_cell
            if a_cell.db_name not in self.cell_db_map:
                self.cell_db_map[a_cell.db_name] = a_cell
        return self.cell_map

    def update_cell_db_map(self):
        """used to on demand update the cell db map"""
        self.cell_db_map = {}
        for a_cell in self.cells:
            a_cell: Cell = a_cell
            if a_cell.db_name not in self.cell_db_map:
                self.cell_db_map[a_cell.db_name] = a_cell
        return self.cell_db_map

    def new_cell(self, name=None, value=None):
        """creates a new cell container for generating new rows"""
        return Cell().new(c_value=value, column_info=self.board.column_info_map.get(name))

    def new_sub_cell(self, name=None, value=None):
        """creates a new cell container for generating new rows"""
        if self.board.column_info_sub_map.get(name) is None:
            self.get_subitems()
        return Cell().new(c_value=value, column_info=self.board.column_info_sub_map.get(name))

    def add_subitem(self, subitem_name, q_data=None) -> Result:
        result = self.board.insert_subitem(self.row_id, subitem_name, q_data)
        if result.is_ok():
            sub_row_json = result.data
            sub_row_dict = sub_row_json.get('data').get('create_subitem')
            sub_item = self.board.sub_item_add_row(parent_row=self, sub_row_dict=sub_row_dict)
            return Result(0, data=sub_item)
        return result

    def insert(self) -> Result:
        """Insert this row on Monday.com  cells are updated if they are marked modified and the key is valid"""
        result = Result()
        if self.on_monday is False:

            result = self.board.insert(self.group_id, self.row_name, self.cells)
            if result.is_ok():
                self.on_monday = True
                self.row_id = result.data.get('data').get('create_item').get('id')
        else:
            # update the record if it is already on monday
            self.update()

        # at this point the record is either inserted or updated on monday
        if result.is_ok():
            self.board.row_key_map[self.key] = self
            self.on_monday = True

        logging.debug(f"Insert status = {result.message}")

        return result

    def update_single_column(self, row_id=None, column_name: str = '', column_value: str = '') -> Result:
        """ Note if you call this function with no column value the column will be emptied.
            Function is used to update a single column value for a row, such as a status or a dropdown.
        """
        if row_id is None:
            row_id = self.row_id

        cell: Cell = self.cell_map.get(column_name)
        if cell is None:
            return Result(-1, message=f"Unable to locate col name [{column_name}]")
        column_id = cell.id
        return self.board.update_single_column_value(row_id, column_id, column_value)

        # update this row on Monday.com

    def update_column(self, column_name=None, value=None, value2=None, add_missing_labels=False ) -> Result:
        result = Result(-1, message="Either the column name is not in the cell map or value is missing")
        if column_name in self.cell_map and value is not None:
            try:
                self.set(column_name, value, value2)
                # self.get(column_name).previous_value = str(DateTime(now=True).as_timestamp)
                # self.get(column_name).modified = True
                result = self.board.monday_update(self.row_id, [self.get(column_name), ], add_missing_labels)
                logging.debug(f"Update status = {result.message}")
                if result.is_ok():
                    self.get(column_name).modified = False
            except Exception as ex:
                msg = f"Not able to set col [{column_name}] to value [{value}]: Err -> {ex} "
                logging.warning(msg)
                result = Result(-1, message=msg)
        return result

    def update_columns(self, cells=None, add_missing_labels=False):
        assert isinstance(cells, list), "Update Columns Requires a list of 1 or more cells"
        result = Result(-1, message="No Cells to Update")
        if cells is not None:
            result = self.board.monday_update(self.row_id, self.cells, add_missing_labels)
            if result.is_ok():
                for cell in cells:
                    if cell.modified:
                        cell.modified = False

            logging.debug(f"Update status = {result.message}")
        return result

    def warn_if_column_has_been_modified(self, names):
        if isinstance(names, str):
            names = [names, ]
        for c in self.cells:
            if c.modified and c.name not in names:
                logging.warning(f"Field [{c.name}] has been modified but is not going to be updated")

    def update(self, columns=None, column_name=None, value=None, value2=None, add_missing_labels=False) -> Result:
        """
        You can supply a single column nane and value to update
        or
        a list of columns=['Col 1', 'Col 2'] to only update those columns
        or
        no arguments that will update the entire row.

        Args:
            columns: a list of column names
            column_name: a single column name
            value: only used if the column_name has a string column name (1 of them)
            value2: only needed for fields such as links that require more than one value fields.
            add_missing_labels: If updating a label, you can add new labels if they don't exist

        Returns: a result

        """
        """Update this row on Monday.com  cells are updated if they are marked modified and the key is valid"""

        if columns is not None and isinstance(columns, list):
            assert isinstance(columns, list), "Columns must be a list of column names"
            update_cells = []
            self.warn_if_column_has_been_modified(columns)
            for column_name in columns:
                assert isinstance(column_name, str), "Column names must be a string"
                update_cells.append(self.get(column_name))

            result = self.board.monday_update(self.row_id, update_cells, add_missing_labels)
            if result.is_ok():
                # mark the cells as updated if successful
                for column_name in columns:
                    cell = self.get(column_name)
                    if cell.modified:
                        cell.modified = False
        else:
            if self.on_monday is True:
                if column_name is not None and value is not None:
                    try:
                        self.warn_if_column_has_been_modified(column_name)
                        self.set(column_name, value, value2)
                    except Exception as ex:
                        logging.warning(f"Not able to set col [{column_name}] to value [{value}]: Err -> {ex} ")

                result = self.board.monday_update(self.row_id, self.cells, add_missing_labels)
                if result.is_ok():
                    # mark the cells as updated if successful
                    for cell in self.cell_map.values():
                        if cell.modified:
                            cell.modified = False

                logging.debug(f"Update status = {result.message}")
            else:
                result = self.insert()

        return result

    def get_value(self, name, default=None):
        return self.get(name, default)

    def get(self, name, default=None):
        """
        get returns a cell or a value when default value is specified, while this overloading may seem a bit
        too much, it is actually very useful.
        """
        if default is not None:
            try:
                x = self.cell(name).value
                if x is not None:
                    return x
            except Exception as ex:
                logging.warning(ex)
            return default

        else:
            """convenience function so you can call cell or get"""
            return self.cell(name)

    def set(self, name, *args):
        v2 = None
        v = args[0]
        if len(args) > 1:
            v2 = args[1]
        c = self.get(name)
        c.value = v
        if c.type in ['link']:
            c.value2 = v2
        c.modified = True

    def cell(self, name) -> Cell:
        """lookup a cell by name and return it."""
        # got a map then use it.
        if self.cell_map is not None:
            cell_object = self.cell_map.get(name)
            if cell_object is None:
                logging.warning(f"row.cell -> Unable to locate field with the name [{name}]")
                return Cell(self)
            else:
                return cell_object
        # no map look it up
        for _col in self.cells:
            if _col.name.lower() == name.lower():
                return _col

        logging.warning(f"row.cell -> Unable to locate field with the name [{name}]")
        return Cell(self)

    def get_cells(self, column_names: []):
        """Gets a group of cells or 1 from the map

        Example: update_cells = row.get_cells(['col1','col2'...])

        Args:
            column_names: list of column names to return

        Returns: A list of cell class objects

        """
        retval = []

        if len(column_names) == 1:
            return self.get(column_names[0])

        for name in column_names:
            retval.append(self.cell_map.get(name))
        return retval

    def value(self, name):
        """lookup a cell value from this row and return it"""
        the_cell = self.get(name)
        if the_cell is None:
            return None
        return the_cell.value

    def values(self, column_names: []):
        """lookup multiple cells by name and return them

        Args:
            column_names: list of column names

        Returns: list of cells

        """
        retval = []
        for col_name in column_names:
            if 'Row Name' == col_name and col_name in column_names:
                retval.append(self.row_name)
                continue

            if 'Group Name' in col_name and col_name in column_names:
                retval.append(self.group_name)
                continue

            _cell = self.get(col_name)
            if _cell is None:
                continue
            if _cell.name.lower() == col_name.lower():
                retval.append(_cell.value)
        return retval

    def _set_default_group(self):
        """handle the possibility that defaults are different from board type to board type."""
        self.group_id = 'topics'  # it is possible that this type of board must be items for this
        self.group_name = 'Default Group'  # it is possible that this type of board must be items for this

    @staticmethod
    def gen_query(data):
        cmd = ' '.join(data.split())
        return {'query': cmd}

    # initializes a row with a set of empty cells. note, has no unique_key these will be checked on insert or update
    def init_empty(self, group_id: str = None, group_name: str = None, row_name: str = None,
                   column_info_map: dict = None, no_strip=False):
        """Creates an empty new row with cells that match the columns set

        Args:
            group_id: The group id
            group_name: The group name
            row_name: The row name
            column_info_map: The column info map (columns by name return a column class object)
            no_strip: do not strip.
        """
        self.group_id = group_id
        self.group_name = Format(group_name).name
        self.row_id = None
        if no_strip:
            self.row_name = row_name
        else:
            self.row_name = Format(row_name).name
        self.cells: [Cell] = []
        self.auto_create_empty_cells(column_info_map)
        self.cell_map = self.update_cell_map()
        self.cell_db_map = self.update_cell_db_map()
        self.key = None
        self.on_monday = False

    def auto_create_cells_json(self, row_id: str = None, row_data=None, column_info_map=None):
        """Creates cells from json requires a cell map to work

        Args:
            row_id: The row id
            row_data: The row dictionary
            column_info_map: The column info map

        Returns: Nothing, this update this row's cell set.

        """
        if column_info_map is None:
            column_info_map = {}
        for column_name in row_data.keys():
            column: Column = column_info_map.get(column_name)
            if column is not None:
                new_cell = Cell()
                new_cell.init(row_id, c_value=row_data.get(column.name), column_info=column)
                self.cells.append(new_cell)

    def auto_create_empty_cells(self, column_info_map: dict = None):
        """creates cells from a column info map

        Args:
            column_info_map: the column info map

         Returns Nothing, this update this row's cell set.

        """

        if column_info_map is None:
            column_info_map = {}

        for column_name in column_info_map.keys():
            # the row id will be filled in on first update.
            row_id = ''
            column: Column = column_info_map.get(column_name)
            if column is not None:
                new_cell = Cell(self).init(row_id, c_value=None, column_info=column)
                self.cells.append(new_cell)

    def get_row_name_value_dict(self):
        """Gets a row of cells name and values as a dict"""
        retval = {}
        for _cell in self.cells:
            c: Cell = _cell
            retval[c.name] = c.value
        return retval

    def get_row_db_name_value_dict(self):
        """Gets a row of cells db_name and values as a dict"""
        retval = {'group_name': self.group_name, 'row_name': self.row_name, 'row_id': self.row_id}
        for _cell in self.cells:
            c: Cell = _cell
            retval[c.db_name] = c.value
        return retval

    def get_cells_as_kv_pairs(self):
        """
        for use with database tables, makes it really easy to read from a board and write to a db
        """
        retval = {}
        for c in self.cells:
            col: Cell = c
            if isinstance(col.value, DateTime):
                val = col.value.db_format
                if val == '1970-01-01 00:00:00':
                    val = None
                retval[col.db_name] = val
            else:
                retval[col.db_name] = col.value
        return retval

    def as_tuple(self) -> ():
        row_tuple = ()
        for _cell in self.cells:
            c: Cell = _cell
            if isinstance(c.value, DateTime):
                value = c.value.db_format
                if value == '1970-01-01 00:00:00':
                    value = None
            else:
                value = c.value
            row_tuple = row_tuple + (value,)
        return row_tuple

    def as_db_dict(self):
        retval = {'group_name': self.group_name, 'row_name': self.row_name, 'row_id': self.row_id}
        for _cell in self.cells:
            c: Cell = _cell
            if isinstance(c.value, DateTime):
                value = c.value.db_format
                if value == '1970-01-01 00:00:00':
                    value = None
            else:
                value = c.value
            retval[Format(c.name).snake_case] = value
        return retval
