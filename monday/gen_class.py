import os
import re

from conversion.c_format import Format
from monday.c_monday import Monday
import logging


def get_type(t):
    if t in ['date', 'datetime']:
        return ': DateTime', 'DateTime()', 'DateTime()'

    if t in ['subtasks', 'lookup', 'text', 'pulse-updated', 'long-text',
             'person', 'color', 'name', 'dropdown']:
        return ': str', "''", "''"

    if t in ['multiple-person', 'file']:
        return ': [str]', '[]', []

    if t in [ 'boolean']:
        return ': bool', False, False

    if t in ['numeric']:
        return '', 0, 0

    return ' ' + t, 'None', None


def add_get_type_function():
    return f"""\n
def get_type(t):
    if t in ['date', 'datetime']:
        return ': DateTime', 'DateTime()', DateTime()
    
    if t in ['subtasks', 'lookup', 'text', 'pulse-updated', 'long-text', 
             'person', 'color', 'name', 'dropdown']:
        return ': str', "''", ''

    if t in ['multiple-person', 'file']:
        return ': [str]', '[]', []
        
    if t in [ 'boolean']:
        return ': bool', False, False
    
    if t in ['numeric']:
        return '', 0, 0

    return ' '+t, 'None', None
"""


def add_row_set_function():
    return f"""\n
def row_set(row, name, value):
    try:
        return row.set(name, value)
    except Exception as ex:
        logging.error(ex)
"""


def add_row_get_function():
    return f"""\n
def row_get(row, name, _map, _type):
    try:
        if row is None or name not in _map:
            return get_type(_type)[2]
      
        if _type == 'numeric':
            value = row.get(name).value
            if '.' in value:
                return float(value)
                
            if value.isnumeric():
                return int(value)
         
        return row.get(name).value
    except Exception as ex:
        logging.error(ex)
    return _type
"""


def filecase(s):
    s = s.replace("'", "")
    s = titlecase(s)
    # s =  Utility.to_db(s)
    return s


def class_case(s):
    s = re.sub('[^A-Za-z0-9 ]+', '', s)
    s = titlecase(s)
    s = "".join(s.split())
    return s


def titlecase(s):
    return Format(s).title_case
    # return re.sub(
    #     r"[A-Za-z]+('[A-Za-z]+)?",
    #     lambda word: word.group(0).capitalize(),
    #     s)


def variable_name(n):
    n = n.replace('#', 'number')
    n = n.replace('/', ' per ')
    # n = Utility.to_db(n)
    n = Format(n).snake_case

    n = 'c_'+n
    return n


def col_name(n):
    return Format(n).snake_case
    # n = n.replace('#', 'number')
    # n = n.replace('/', ' per ')
    # n = re.sub('[^A-Za-z0-9 ]+', '', n)
    # n = Utility.to_snake(n, lower=True)
    # return n


def gen_column_titles(self, file_contents):
    file_contents += f"\n        # column column display names\n"
    col_list = {}
    for col in self.column_info_map.values():
        self_col_name = Format(col_name(col.name)).snake_case
        if self_col_name not in col_list:
            col_list[self_col_name] = col.name
        else:
            step = 1
            final_name = f"{self_col_name}_{step}"
            while final_name not in col_list:
                final_name = f"{self_col_name}_{step}"
                if final_name not in col_list:
                    col_list[final_name] = col.name
                step += 1
        col.big_name = self_col_name

    for col in self.column_info_map.values():
        if self.has_subitems and 'subitems' in col.name.lower():
            if len(self.column_info_sub_map) > 0:
                file_contents += f"        {'self.col_sub_' + col.big_name} = '{col.name}'\n"
        else:
            file_contents += f"        self.col_{col.big_name} = '{col.name}'\n"

    return file_contents


def gen_monday_class(self, class_name=None, file_name=None):
    assert self.status.is_ok(), self.status.message
    file_contents = 'from std_utility.c_datetime import DateTime\n'
    file_contents += 'from std_logging.logs import logging.n'
    file_contents += "from monday.c_column import Column\n"
    sub_class_name = f"SubItems"
    if class_name is None:
        class_name = class_case(self.name)
    else:
        class_name = class_case(class_name)

    if file_name is None:
        file_name = filecase(self.name)
    else:
        file_name = filecase(file_name)

    """**********************************************************
                Create sub items class and variables
    **********************************************************"""

    if len(self.column_info_sub_map) > 0:
        file_contents += f"\n\nclass {sub_class_name}:"
        file_contents += "\n    def __init__(self, row=None):\n"
        file_contents += "        self.m_row = row\n"
        for col in self.column_info_sub_map.values():
            _type = get_type(col.type)
            getter = f" = row_get(row, '{col.name}', row.board.column_info_sub_map, '{col.type}')\n"
            file_contents += f"        self.{variable_name(col.name)}{_type[0]}{getter}"

        """**********************************************************
                Create sub items update
        **********************************************************"""

        file_contents += "\n"
        file_contents += "    def update(self):\n"

        for col in self.column_info_sub_map.values():
            if self.has_subitems and 'subitems' in col.name.lower():
                continue
            else:
                file_contents += f"        row_set(self.m_row, '{col.name}', self.{variable_name(col.name)})\n"
        file_contents += f"        return self.m_row.update()\n"

    """**********************************************************
         Create main class and variables
    **********************************************************"""

    file_contents += f"\n\nclass {class_name}:"
    file_contents += "\n    def __init__(self, row=None):\n"
    file_contents += "        self.m_row = row\n"

    file_contents += "        if row is None:\n"
    file_contents += "            group_name = None\n"
    file_contents += "            c_map = None\n"
    file_contents += "            self.c_row_id = None\n"
    file_contents += "            self.c_row_name = None\n"
    file_contents += "        else:\n"
    file_contents += "            group_name = row.group_name\n"
    file_contents += "            c_map = row.board.column_info_map\n"
    file_contents += "            self.c_row_id = row.row_id\n"
    file_contents += "            self.c_row_name = row.row_name\n"
    file_contents += "        self.c_group_name = group_name\n"
    for col in self.column_info_map.values():
        if self.has_subitems and 'subitems' in col.name.lower():
            if len(self.column_info_sub_map) > 0:
                file_contents += "        sub_array = []\n"
                file_contents += "        if self.m_row is not None and self.m_row.has_subitems:\n"
                file_contents += "            for r in self.m_row.sub_items:\n"
                file_contents += "                sub_array.append(r)\n"
                file_contents += f"        self.{variable_name(col.name)}: [{sub_class_name}] = sub_array\n"
        else:
            _type = get_type(col.type)
            getter = f" = row_get(row, '{col.name}', c_map, '{col.type}')\n"
            file_contents += f"        self.{variable_name(col.name)}{_type[0]}{getter}"

    """**********************************************************
         Create Validation groups, columns and sub columns
    **********************************************************"""

    file_contents += "\n        self.groups = [\n"
    for group_name in self.group_map:
        file_contents += f"            '{group_name}',\n"
    file_contents += f"        ]\n"

    file_contents += "\n        self.columns = [\n"
    for col in self.column_info_map.values():
        file_contents += f"            Column(c_name='{col.name}', c_labels={col.labels}),\n"
    file_contents += f"        ]\n"

    if self.has_subitems:
        file_contents += "\n        self.sub_columns = [\n"
        for col in self.column_info_sub_map.values():
            file_contents += f"            Column(c_name='{col.name}', c_labels={col.labels}),\n"
        file_contents += f"        ]\n"

    file_contents = gen_column_titles(self, file_contents)

    """**********************************************************
         Create Update Code
    **********************************************************"""

    file_contents += "\n"
    file_contents += "    def update(self):\n"

    for col in self.column_info_map.values():
        if self.has_subitems and 'subitems' in col.name.lower():
            continue

        file_contents += f"        row_set(self.m_row, '{col.name}', self.{variable_name(col.name)})\n"
    file_contents += f"        return self.m_row.update()\n"

    """**********************************************************
                Utility Functions
    **********************************************************"""
    file_contents += add_get_type_function()
    file_contents += add_row_set_function()
    file_contents += add_row_get_function()

    """
    **********************************************************
    """

    # DEFINES
    file_contents += f"\n\nclass COL:\n"
    col_list = {}
    for col in self.column_info_map.values():
        big_name = col_name(col.name).upper()
        if big_name not in col_list:
            col_list[big_name] = col.name
        else:
            step = 1
            final_name = f"{big_name}_{step}"
            while final_name not in col_list:
                final_name = f"{big_name}_{step}"
                if final_name not in col_list:
                    col_list[final_name] = col.name
                step += 1
        col.big_name = big_name

    for col in self.column_info_map.values():
        if self.has_subitems and 'subitems' in col.name.lower():
            if len(self.column_info_sub_map) > 0:
                file_contents += f"    {'SUB_'+col.big_name} = '{col.name}'\n"
        else:
            file_contents += f"    {col.big_name} = '{col.name}'\n"

    target_file = f'./c_{Format(file_name).snake_case}.py'
    with open(target_file, 'w') as f:
        f.write(file_contents)

    if os.path.exists(target_file):
        logging.info(f"Created {target_file}")
    else:
        logging.warning(f"Unable to create required columns file")


if __name__ == '__main__':
    # testing only

    board_id = 2428301908  # netsuite
    board_id = 2279397740  # basketball club teams
    board_id = 2382950888  # monday payment board - coupa
    board_id = 2279390445  # basketball master calendar
    monday_board = Monday(board_id=board_id,
                          use_select=True,
                          monday_account="automation@com")

    gen_monday_class(monday_board, class_name='NetSuiteRecord', file_name='netsuite_record')