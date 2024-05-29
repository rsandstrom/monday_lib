"""
    Key: Create, Assemble, Check Key functions are included in this class
"""

from result.c_result import Result
from status.c_status import Status
import logging


class Key:

    @staticmethod
    # creates a key array and returns it
    def unique(group_name=False, row_name=False, field_names=None) -> []:
        _key = []

        if group_name is True:
            _key.append("Group Name")

        if row_name is True:
            _key.append("Row Name")

        if isinstance(field_names, str):
            _key.append(field_names)

        if isinstance(field_names, list):
            for field in field_names:
                _key.append(field)

        return _key

    @staticmethod
    # assembles the key part of the lookup key used in key_maps
    def assemble_key_part(_the_key, _key_col_name, _key_value):
        if len(_the_key) == 0:
            # k = {_key_col_name: _key_value}
            # return json.dumps(k)
            return f"{_key_col_name}:{_key_value}".lower().replace(" ", "")
        else:
            # k = json.loads(_the_key)
            # k[_key_col_name] = _key_value
            # return json.dumps(k)
            return f"{_the_key}!{_key_col_name}:{_key_value}".lower().replace(" ", "")

    @staticmethod
    # check the unique_key to see if this record is unique
    def check_keys(keys: [] = None, row_key_map: dict = None, check_key=None, row_name=None):
        result = Result(0)

        if keys is None:
            return Result(0)

        if len(keys) > 0 and check_key in row_key_map:
            result.status = Status(-1, f"Duplicate key found: [{check_key}]")

        if result.is_ok() and len(keys) > 0 and check_key is None:
            result.status = Status(-1, f"Unable to process the board when "
                                       f"requiring keys = {keys} and row [{row_name}] "
                                       f"does not contain this key")
        return result

    # returns a result with the key the_key if the row has the key columns
    @staticmethod
    def create(a_row, keys) -> Result:
        result = Result(0)

        if a_row is None:
            return Result(message=f"Missing row this should not happen")

        if keys is None:
            return Result(0)

        # make a copy, so we can remove items without altering the base set of unique_key.
        key_column_names = keys.copy()

        _key: str = ''
        key_count = 0
        try:
            if 'Group Name' in key_column_names:
                key_count += 1
                _key = Key.assemble_key_part(_key, "Group Name", str(a_row.group_name))
                key_column_names.remove('Group Name')

            if 'Row Name' in key_column_names:
                key_count += 1
                _key = Key.assemble_key_part(_key, "Row Name", str(a_row.row_name))
                key_column_names.remove('Row Name')

            for _cell in a_row.cells:
                column_name = _cell.name.lower()

                # a cell with no value can not have a key part
                if _cell.value is None:
                    continue

                # for each key column name see if there is a match
                for _key_column_name in key_column_names:
                    the_key_part = str(_key_column_name).lower()
                    if the_key_part == column_name:
                        key_count += 1
                        # get the key column value ready for use
                        key_column_value = str(_cell.value).lower()
                        _key = Key.assemble_key_part(_key, _key_column_name, key_column_value)
                        key_column_names.remove(_key_column_name)
                        break
        except Exception as ex:
            logging.warning(ex)
            pass
        if key_count != len(keys):
            missing_keys = []
            found_keys = []
            for k in keys:
                if k == 'Group Name' or k == 'Row Name':
                    continue
                found = False
                for cell in a_row.cells:
                    if k.lower() == cell.name.lower():
                        if cell.value is None:
                            logging.debug(f"Key field [{k}] has no data value")
                        else:
                            found = True
                            break
                if found:
                    found_keys.append(cell.name.lower())
            for k in keys:
                if k.lower() not in found_keys:
                    if k == 'Group Name' or k == 'Row Name':
                        continue
                    missing_keys.append(k)
            result = Result(message=f"Missing key fields {missing_keys} in row [{a_row.row_name}]")
            _key = None

        if _key is not None:
            result = Result(code=0, data=_key)

        return result

    # creates a key that can be used to find a row
    @staticmethod
    def search(group_name: str = None, row_name: str = None, key_values_json: dict = None):
        key = ''
        if group_name is not None:
            key = Key.assemble_key_part(key, 'Group Name', group_name)
        if row_name is not None:
            key = Key.assemble_key_part(key, 'Row Name', row_name)

        for k, v in key_values_json.items():
            key = Key.assemble_key_part(key, k, v)
        return key


