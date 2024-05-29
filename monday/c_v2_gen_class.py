import os
import re

from conversion.c_format import Format
from monday.c_monday import Monday
from monday.c_monday_factory import MondayFactory
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_utility import Utility


class V2GenClass:
    monday: MondayFactory = MondayFactory()

    def __init__(self, board_id, class_name):
        self.class_name = Format(class_name).class_case
        self.file_name = 'c_' + Format(class_name).snake_case + '.py'
        self.col_dict = {}
        self.board = self.monday.board(board_id=board_id)
        self.body = 'from monday.c_column import Column\n'\
                    'from monday.c_cell import Cell\n'

        # gen the class
        self.add_constants(0)
        self.add_line(0, f"class {self.class_name}({self.class_name}Constants):")
        self.add_line(1, f"def __init__(self, row=None):")
        self.add_line(2, f"assert row is not None, 'row must be a monday row'")
        self.add_line(2, f"self.row = row")
        self.add_line(2, f"self.row_id = row.row_id")
        self.add_line(2, f"self.row_name = row.row_name")
        self.add_line(2, f"self.group = row.group_name")
        self.add_db_map(2)
        self.add_validation(2)

        for col in self.board.column_info_map.values():
            if Format(col.name).snake_case in ['row', 'row_id', 'row_name', 'group', 'subitems']:
                continue
            self.add_property(1, col.name)

        self.add_line(0, '')
        self.write_file()

    @staticmethod
    def col_name(name):
        name = Format(name).snake_case
        if name in ['class',]:
            name = f"c_{name}"
        return name

    def add_line(self, indent, item):
        space = '\n' + ('    ' * indent)
        self.body += (space + item)

    def add_db_map(self, indent):
        self.add_line(indent, '')
        self.add_line(indent, 'self.db_map = {')
        self.add_line(indent + 1, f"'row_id': self.row_id,")
        self.add_line(indent + 1, f"'row_name': self.row_name,")
        self.add_line(indent + 1, f"'group': self.group,")
        for col in self.board.column_info_map.values():
            if col.name.lower() in ['subitems', ]:
                continue
            c = self.col_name(col.name)
            if c in ['row', 'row_id', 'row_name', 'group']:
                continue
            if col.type in ['date', 'datetime']:
                self.add_line(indent + 1, f"'{c}': self.{c}.value.db_data,")
            else:
                self.add_line(indent + 1, f"'{c}': self.{c}.value,")
        self.add_line(indent, '}')

    @staticmethod
    def instance_name(s):
        if s.lower() in ['class']:
            s = 'c_' + s
        return f"self.{Format(s).snake_case}"

    def add_property(self, indent, name):
        if name in self.board.col_map and name not in ['Subitems', ]:
            self.add_line(indent, '')
            self.add_line(indent, f"@property")
            self.add_line(indent, f"def {self.col_name(name)}(self) -> Cell:")
            # self.add_line(indent + 1, f"if '{name}' in self.row.board.col_map:")
            self.add_line(indent + 1, f"return self.row.get('{name}')")

    def add_constants(self, indent):
        col_list = {}
        for col in self.board.column_info_map.values():
            for label in col.labels:
                label = str(label)
                if len(label) == 0:
                    continue
                if '?' in label:
                    logging.info('here')
                col_list[f"l_{Format(label).snake_case}"] = label

        for col in self.board.column_info_map.values():
            col_list[f"t_{Format(col.name).snake_case}"] = col.name

        self.add_line(indent, '')
        self.add_line(indent, f'class {self.class_name}Constants:')

        for k, v in col_list.items():
            self.add_line(indent + 1, f"{Format(k).snake_case} = '{v}'")
        self.add_line(indent, '')
        self.add_line(indent, '')

    def add_validation(self, indent):
        self.add_line(indent, '')
        self.add_line(indent, "self.groups = [")
        for group_name in self.board.group_map:
            self.add_line(indent + 1, f"'{group_name}',")
        self.add_line(indent, "]")

        self.add_line(indent, '')
        self.add_line(indent, "self.columns = [")
        for col in self.board.column_info_map.values():
            self.add_line(indent + 1, f"Column(c_name='{col.name}', c_labels={col.labels}),")
        self.add_line(indent, "]")

        if self.board.has_subitems:
            self.add_line(indent, '')
            self.add_line(indent, f"self.sub_columns = [")
            for col in self.board.column_info_sub_map.values():
                self.add_line(indent + 1, f"Column(c_name='{col.name}', c_labels={col.labels}),")
            self.add_line(indent, "]")

    def write_file(self):
        target_file = self.file_name
        with open(target_file, 'w') as f:
            f.write(self.body)

        if os.path.exists(target_file):
            logging.info(f"Created {target_file}")
        else:
            logging.warning(f"Unable to create required columns file")



def test():
    # testing only

    board_id = 2428301908  # netsuite
    board_id = 2279397740  # basketball club teams
    board_id = 2382950888  # monday payment board - coupa
    board_id = 2279390445  # basketball master calendar

    c_gen = V2GenClass(board_id=3476521463, class_name='Forms Control Board')

    return c_gen

if __name__ == '__main__':
    x = test()
    print(x.body)
    exit(0)
