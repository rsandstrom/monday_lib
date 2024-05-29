"""
Subitem class is basically a row class modified for sub items.
subitems in monday are a list of rows that is linked to a row.
To obtain subitems, we need to query for subitems using the row id.
"""

from monday.c_cell import Cell
from monday.c_column import Column
from monday.c_field import Field, FieldValue
from result.c_result import Result
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_utility import Utility


class SubItem(Column):
    def __init__(self,
                 board=None,
                 parent_row=None, row_id=None, row_name=None,
                 row_data=None, col_map=None, assets=None,
                 sub_board_id=None):
        super().__init__()
        self.parent_row = parent_row
        self.on_monday = False
        self.connection = board
        self.sub_board_id = sub_board_id
        self.board = board
        self.row_id = row_id
        self.row_data = row_data
        self.row_name = row_name
        self.assets = assets
        self.cells: [Cell] = []
        self.cell_map = {}
        self.cell_db_map = {}
        self.key = None
        self.auto_create_cells_json(row_id, row_data, col_map)
        self.readonly = FieldValue()

    @property
    def cell_count(self):
        """Get the cell count for this row"""
        return len(self.cells)

    def update_cell_map(self):
        """used to on demand update the cell map"""
        self.cell_map = {}
        for a_cell in self.cells:
            a_cell: Cell = a_cell
            if a_cell.name not in self.cell_map:
                self.cell_map[a_cell.name] = a_cell
            if a_cell.db_name not in self.cell_db_map:
                self.cell_db_map[a_cell.db_name] = a_cell
            setattr(self.readonly, Utility.db_name(a_cell.name), a_cell.value)
        return self.cell_map

    def update_cell_db_map(self):
        """used to on demand update the cell db map"""
        self.cell_db_map = {}
        for a_cell in self.cells:
            a_cell: Cell = a_cell
            if a_cell.db_name not in self.cell_db_map:
                self.cell_db_map[a_cell.db_name] = a_cell
        return self.cell_db_map

    def insert(self) -> Result:
        """Insert this row on Monday.com  cells are updated if they are marked modified and the key is valid"""
        result = Result()
        if self.on_monday is False:
            result = self.board.insert_subitem(self.row_id, self.row_name, self.cells)
            if result.is_ok():
                sub_row_json = result.data
                sub_row_dict = sub_row_json.get('data').get('create_subitem')
                sub_item = self.board.sub_item_add_row(parent_row=self.parent_row, sub_row_dict=sub_row_dict)
                return Result(0, data=sub_item)
            # result = self.board.insert(self.group_id, self.row_name, self.cells)
        else:
            # update the record if it is already on monday
            self.update()

        # at this point the record is either inserted or updated on monday
        if result.is_ok():
            self.on_monday = True

        logging.info(f"Insert status = {result.message}")

        return result

    # update this row on Monday.com
    def update(self, column_name=None, value=None) -> Result:
        """Update this row on Monday.com  cells are updated if they are marked modified and the key is valid"""
        if self.on_monday is True:
            if column_name is not None and value is not None:
                try:
                    self.set(column_name, value)
                    self.get(column_name).previous_value = str(DateTime(now=True).as_timestamp)
                    self.get(column_name).modified = True
                except Exception as ex:
                    logging.warning(f"Not able to set col [{column_name}] to value [{value}]: Err -> {ex} ")

            result = self.board.sub_item_update(self.sub_board_id, self.row_id, self.cells)
            logging.debug(f"Update status = {result.message}")
        else:
            result = self.insert()
        return result

    def get(self, name) -> Cell:
        """convenience function so you can call cell or get"""
        return self.cell(name)

    def set(self, name, v):
        self.get(name).value = v

    def cell(self, name) -> Cell:
        """lookup a cell by name and return it."""
        # got a map then use it.
        if self.cell_map is not None:
            cell_object = self.cell_map.get(name)
            if cell_object is None:
                raise Exception(f"row.cell -> Unable to locate field with the name [{name}]")
            else:
                return cell_object
        # no map look it up
        for _col in self.cells:
            if _col.name.lower() == name.lower():
                return _col

        raise Exception(f"row.cell -> Unable to locate field with the name [{name}]")

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

    @staticmethod
    def gen_query(data):
        cmd = ' '.join(data.split())
        return {'query': cmd}

    # initializes a row with a set of empty cells. note, has no unique_key these will be checked on insert or update
    def init_empty(self):
        """Creates an empty new row with cells that match the columns set

        Args:
            group_id: The group id
            group_name: The group name
            row_name: The row name
            column_info_map: The column info map (columns by name return a column class object)
            keys: TBD

        """
        self.cells: [Cell] = []
        self.auto_create_empty_cells(self.board.column_info_map)
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
        for col in row_data:
            column_name = col.get('column').get('title')
            column_value = col.get('text')
            column_type = col.get('type')
            column_id = col.get('id')

            column_info: Column = column_info_map.get(column_name)
            if column_info is None:
                column_info = Column(c_index=0, c_id=column_id, c_name=column_name, c_type=column_type)
            if column_info is not None:
                new_cell = Cell(self)
                new_cell.init(row_id, c_value=column_value, column_info=column_info)
                new_cell.type = column_info.type
                new_cell.labels = column_info.labels
                new_cell.index = column_info.index
                new_cell.parent_row = self
                new_cell.connection = self.connection
                new_cell.has_labels = column_info.has_labels
                self.cells.append(new_cell)

    @staticmethod
    def create_column_info_map(data):
        col_map = {}
        try:
            index = 0
            target = data.get('data').get('items')
            if target is not None and isinstance(target, list) and len(target) > 0:
                columns_json = target[0].get('column_values')
                for _col in columns_json:
                    new_column = Column()
                    new_column.from_json(index, _col)
                    dup_col_idx = 0
                    while new_column.name in col_map:
                        new_column.name = f"{new_column.name}_{dup_col_idx}"
                        dup_col_idx += 1
                    col_map[new_column.name] = new_column
                    index += 1

        except Exception:
            logging.debug("Looks like there are no columns")
            return {}

        return col_map

    def auto_create_empty_cells(self, column_info_map: dict = {}):
        """creates cells from a column info map

        Args:
            column_info_map: the column info map

         Returns: Nothing, this update this row's cell set.

        """
        for column_name in column_info_map.keys():
            # the row id will be filled in on first update.
            row_id = ''
            column: Column = column_info_map.get(column_name)
            if column is not None:
                new_cell = Cell().init(row_id, c_value=None, column_info=column)
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
