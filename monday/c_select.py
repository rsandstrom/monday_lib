"""
Class Select
"""
from conversion.c_format import Format
from monday.c_cell import Cell
from monday.c_column import Column
from monday.c_query_helper import QueryHelper
from monday.c_row import Row
from result.c_result import Result
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_maps import Maps
from std_utility.c_utility import Utility


class MondaySelect(QueryHelper):

    def __init__(self):
        pass

    @staticmethod
    def monday_query_by_col_values(board_id, column_id, col_values, fields, limit, page, cursor=None, q_filter=None):
        column_values = MondaySelect.monday_format_list(col_values)
        view = MondaySelect.gen_view(fields)
        if q_filter is None:
            q_filter = ''

        if cursor is None:
            cursor = ''
        else:
            cursor = f'cursor: "{cursor}",'
            q_filter = ''                       # note can not have a cursor and a qfilter

        return """
       {

            items_page_by_column_values (
                        CURSOR
                        board_id: BOARD_ID,
                        column_id: "COLUMN_ID", 
                        column_values: COLUMN_VALUES
                        limit: LIMIT)
                {
                    group {id, title}
                    id
                    name
                     assets {public_url file_extension name}
                    VIEW {id title text}
                }
        }
        """.replace('VIEW', view) \
            .replace('BOARD_ID', str(board_id)) \
            .replace("COLUMN_ID", column_id) \
            .replace("COLUMN_VALUES", column_values) \
            .replace("LIMIT", limit).replace("FIELDS", str(fields))


    # [(col, values),]
    @staticmethod
    def monday_query_by_groups(board_id, groups=None, fields=None, limit=100, page=None,
                               cursor=None, q_filter=''):
        """ WORKS """
        view = MondaySelect.gen_view(fields)
        group = MondaySelect.gen_groups(groups)

        if q_filter is None:
            q_filter = ''

        if cursor is None:
            cursor = ''
        else:
            cursor = f'cursor: "{cursor}",'
            q_filter = ''                       # note can not have a cursor and a qfilter

        cmd = """
        {
            boards (ids: BOARD_ID) 
            {
                GROUPS
                {
                    id
                    title   
                        items_page (CURSOR limit: LIMIT FQUERY) {
                    cursor
                        items  { id name VIEW  {id column {title} text } assets {public_url file_extension name}  }
                    }
                    
                }

            }
        }
        """.replace('VIEW', view) \
            .replace('BOARD_ID', str(board_id)) \
            .replace("GROUPS", group) \
            .replace("LIMIT", str(limit)) \
            .replace("CURSOR", cursor) \
            .replace("FQUERY", q_filter)


        return MondaySelect.clean_query(cmd)

    @staticmethod
    def get_rows_from_select(full_json_response, select_by_group=True):
        cursor = None
        board_items = []
        if select_by_group:
            data = full_json_response.get('data')
            board = data.get('boards')[0]
            groups = board.get('groups')
            for group in groups:
                for item in group.get('items_page').get('items'):
                    cursor = item.get('cursor')
                    item['group'] = {
                        'id': group.get('id'),
                        'title': group.get('title')
                    }
                    board_items.append(item)
        else:
            data = full_json_response.get('data')
            board_items = data.get('items_by_multiple_column_values')
            assert 1 == 1, "To Do Implement select by column_values"
        return board_items, cursor

    @staticmethod
    def is_by_group(groups, col_id, operator):
        return groups is not None or col_id is None or col_id == 'name' or (col_id is not None and operator is not None)

    @staticmethod
    def check_group_inputs(parent_board, board_id):
        assert parent_board is not None, "Parent Board is Required for group select"
        assert board_id is not None, "Board_id is Required for group select"

    @staticmethod
    def group(parent_board, board_id, groups=None, fields=None,
              col_name=None,
              operator=None,
              col_values=None,
              limit=1000,
              q_filter=None):

        MondaySelect.check_group_inputs(parent_board, board_id)

        if isinstance(groups, str):
            groups = [groups]

        result_items = []
        finished = False
        column_id = MondaySelect.get_field_id(parent_board=parent_board, column_name=col_name)
        fields = MondaySelect.get_field_ids(parent_board=parent_board, fields=fields)
        groups = MondaySelect.get_group_ids(parent_board=parent_board, groups=groups)
        select_by_group = MondaySelect.is_by_group(groups, column_id, operator)
        page = 0
        items_cursor = None
        while not finished:
            if select_by_group:
                cmd = MondaySelect.monday_query_by_groups(board_id=board_id,
                                                          groups=groups,
                                                          fields=fields,
                                                          limit=limit,
                                                          page=page,
                                                          cursor=items_cursor,
                                                          q_filter=q_filter)
            else:
                cmd = MondaySelect.monday_query_by_col_values(board_id=board_id,
                                                              column_id=column_id,
                                                              col_values=col_values,
                                                              fields=fields,
                                                              limit=limit,
                                                              page=page,
                                                              cursor=items_cursor,
                                                              q_filter=q_filter)
            cmd = parent_board.gen_query(cmd)
            result = parent_board.execute(cmd)
            if result.is_ok():
                items_cursor = MondaySelect.get_cursor(result)
                items, cursor = MondaySelect.get_rows_from_select(result.data, select_by_group=select_by_group)
                finished = items_cursor is None
                result_items.extend(items)
                logging.debug(f"Processed Select Page {page}, and item count = {len(result_items)}")
                page = page + 1
            else:
                return result

        logging.debug(f"{len(result_items)} items using {page - 1} pages")
        rows = MondaySelect.process_rows(parent_board=parent_board, items=result_items)

        # do our own filtering here.
        if select_by_group:
            if column_id == 'name' and operator is None:
                operator = '='
            rows = MondaySelect.filter(col_name, operator, col_values, rows)
        return Result(0, data=rows)

    @staticmethod
    def get_cursor(result):
        items_cursor = None
        try:
            d = result.data.get('data')
            if d:
                b = d.get('boards')
                if b:
                    b = b[0]
                    g = b.get('groups')
                    if g:
                        g = g[0]
                        i = g.get('items_page')
                        if i:
                            items_cursor = i.get('cursor')
            # items_cursor = result.data.get('data').get('boards')[0].get('groups')[0].get('items_page').get('cursor')
        except Exception as e:
            items_cursor = None
            logging.warning(f"Unable to get cursor: {e}")
        return items_cursor

    @staticmethod
    def process_rows(parent_board, items):
        rows = []
        try:
            # remove any rows that do not match our filters (only if there is a filter)
            load_subitems = False
            # now process the rows that remain
            for this_row in items:
                new_row = Row(parent_board)

                new_row.on_monday = True
                new_row.group_id = this_row.get('group', {}).get('id')
                new_row.group_name = Format(this_row.get('group', {}).get('title')).name
                new_row.row_id = this_row.get('id')
                new_row.row_name = Format.as_ascii(this_row.get('name'))
                new_row.assets = this_row.get('assets')
                columns_json = this_row.get('column_values')
                Maps.add_to_map_array(parent_board.row_multimap, new_row.row_name, new_row)

                # add the item to the cells and update maps
                new_cell = Cell(new_row)

                new_cell.row_id = new_row.row_id
                new_cell.id = 'name'
                new_cell.name = parent_board.column_id_map.get(new_cell.id).name
                new_cell.labels = parent_board.column_id_map.get(new_cell.id).labels
                new_cell.value = new_row.row_name
                new_cell.type = 'text'
                new_cell.index = len(columns_json) + 1
                new_cell.parent_row = new_row
                new_cell.parent_row = new_row
                new_cell._modified = False
                new_row.cells.append(new_cell)
                new_row.cell_map[new_cell.name] = new_cell
                new_row.cell_db_map[new_cell.id] = new_cell
                setattr(new_row.readonly, Utility.db_name(new_cell.name), new_cell.value)
                setattr(new_row.title, Utility.db_name(new_cell.name), new_cell.name)

                # add the column info, need to do it here because it does not exist when we read the columns
                new_column_info = Column(new_cell.index, new_cell.id, new_cell.name, new_cell.type)
                parent_board.column_info_map[new_cell.name] = new_column_info

                # now get the cells from the json columns (from monday), create cells and stor them in a row
                for this_cell in columns_json:
                    new_cell = Cell(new_row)
                    new_cell.source = this_cell
                    new_cell.row_id = new_row.row_id
                    new_cell.id = this_cell.get('id')
                    # since monday renames the first field, and it could conflict with other names, we need to get the
                    # updated name from the column info map that handles duplicate names.
                    column: Column = parent_board.column_id_map.get(new_cell.id)
                    if column is None:
                        continue
                    new_cell.name = column.name
                    # Utility.clean_name(this_cell.get('title'))

                    column_info: Column = parent_board.column_info_map.get(new_cell.name)
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

    @staticmethod
    def filter(col_name=None, operator=None, col_values=None, rows=None):
        result_list = []
        if rows is None:
            rows = []
        if col_name is None or col_values is None:
            return rows

        if col_values is None:
            col_values = []

        for row in rows:
            item = row.get(col_name).value
            if operator is None:
                if item in col_values:
                    result_list.append(row)
            else:
                for val in col_values:
                    if isinstance(item, DateTime):
                        item = item.as_timestamp
                        val = DateTime(val).as_timestamp
                    if operator == '>':
                        if item > val:
                            result_list.append(row)
                    elif operator == '<':
                        if item < val:
                            result_list.append(row)
                    elif operator == '>=':
                        if item >= val:
                            result_list.append(row)
                    elif operator == '<=':
                        if item <= val:
                            result_list.append(row)
                    elif operator == '=':
                        if str(item) == str(val):
                            result_list.append(row)

        return result_list
