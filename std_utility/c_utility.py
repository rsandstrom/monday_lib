import os
import re
import unicodedata
from asyncio.log import logger
from urllib.parse import urlparse, unquote

import stringcase as sc
import string
import random

from conversion.c_format import Format
from std_utility.c_datetime import DateTime


class Utility:
    def __init__(self):
        pass

    @staticmethod
    def get_basename_from_path(path):
        filename = None
        extension = ''
        try:
            filename = os.path.basename(path)
        except Exception as ex:
            logger.warning(f"unable to parse the file name err = {ex}")
        return filename

    @staticmethod
    def get_fileext_from_path(path):
        file_extension = None
        try:
            filename, file_extension = os.path.splitext(path)
        except Exception as ex:
            logger.warning(f"unable to parse the file name err = {ex}")
        return file_extension

    @staticmethod
    def get_filename_from_path(path):
        file_extension = None
        try:
            filename = os.path.basename(path)
            return filename
        except Exception as ex:
            logger.warning(f"unable to parse the file name err = {ex}")
        return None

    @staticmethod
    def get_path_from_path(path):
        try:
            base_part = Utility.get_basename_from_url(path)
            filename = os.path.basename(path)
            path_part, file_extension = os.path.splitext(path)
            path_part = path_part.replace(base_part, '')
            return path_part
        except Exception as ex:
            logger.warning(f"unable to parse the file name, err = {ex}")
        return None

    @staticmethod
    def get_basename_from_url(url):
        filename = None
        extension = ''
        try:
            url = unquote(url)
            a = urlparse(url)
            full_file_name = os.path.basename(a.path)
            filename, file_extension = os.path.splitext(full_file_name)
        except Exception as ex:
            logger.warning(f"unable to parse the file name {url} = err = {ex}")
        return filename

    @staticmethod
    def get_path_from_url(url):
        try:
            base_part = Utility.get_basename_from_url(url)
            url = unquote(url)
            a = urlparse(url)
            filename = os.path.basename(a.path)
            path, file_extension = os.path.splitext(a.path)
            path = path.replace(base_part, '')
            return path
        except Exception as ex:
            logger.warning(f"unable to parse the file name {url} = err = {ex}")
        return None

    @staticmethod
    def get_filename_from_url(url):
        try:
            url = unquote(url)
            a = urlparse(url)
            filename = os.path.basename(a.path)

            path, file_extension = os.path.splitext(a.path)
            return filename
        except Exception as ex:
            logger.warning(f"unable to parse the file name {url} = err = {ex}")
        return None

    @staticmethod
    def get_fileext_from_url(url):
        file_extension = None
        try:
            url = unquote(url)
            a = urlparse(url)
            filename, file_extension = os.path.splitext(a.path)
        except Exception as ex:
            logger.warning(f"unable to parse the file name {url} = err = {ex}")
        return file_extension

    @staticmethod
    # simple match return true or false if matched
    def match(a, b) -> bool:
        if a is None or b is None:
            return False
        return str(a).lower() == str(b).lower()

    @staticmethod
    def clean(s: str, fixup=True):
        if isinstance(s, str):
            if fixup:
                s = s.replace('\u2013', '-')
                s = s.replace('&', 'and')
            # take out any unicode characters
            s = unicodedata.normalize(u'NFKD', s).encode('ascii', 'ignore').decode('utf-8')
            # only allow valid chars
            s = re.sub('[^A-Za-z0-9 _-]+', '', s)
            # remove double spaces if any
            s = re.sub(" +", " ", s)
        return s

    @staticmethod
    def snake_case(record: dict, custom_fields: bool = True) -> dict:
        """
        ***********************************************
        snake_case(record: dict) skip any record that does not start with lower case
        ***********************************************
        """
        retval = {}
        for key, val in record.items():
            # skip any field that does not start with a lower case (the base API set).
            if custom_fields is False and len(key) > 0 and key[0].isupper() or key[0].isnumeric():
                continue
            retval[Utility.api2snake(key)] = val
        return retval

    @staticmethod
    def api2snake(name):
        """
          ***********************************************
          convert a leagueapps field name to a proper snake case name
          Note this is different than the function below
          ***********************************************
          """
        prefix = ''
        if len(name) > 0 and name[0].isupper() or name[0].isnumeric():
            prefix = 'c_'
            name = name.lower()
        return Utility.to_snake(name, prefix)

    @staticmethod
    def to_db(name):
        name = name.replace('#', 'number ').replace('/', ' ').replace(')', ' ').replace('?', '').replace('(', ' ')
        name = Format(name).snake_case
        return name

    @staticmethod
    def db_name(name):
        x = Format(name).snake_case
        if name is None:
            x = ''
        # if not isinstance(name, str):
        #     name = str(name)
        # name = re.sub('[^A-Za-z0-9 _]+', '', name)
        # x = Utility.to_snake(name, lower=True)
        if x in ['class']:
            x = 'c_' + x
        return x

    @staticmethod
    def field_value_name(name):
        name = re.sub('[^A-Za-z0-9 ]+', '', name)
        x = Utility.to_snake(name, lower=True)
        return 'fv_' + x

    @staticmethod
    def to_snake(name, _prefix='', lower=False):
        prefix = _prefix

        if lower:
            name = name.lower()

        if len(name) > 64:
            name = name[0:63]

        name = name.replace('&', 'And') \
            .replace(',', '') \
            .replace('?', '') \
            .replace('(', '') \
            .replace(')', '') \
            .replace('/', '') \
            .replace('~', '') \
            .replace('*', '') \
            .replace(':', '') \
            .replace("'", "")
        if name.isupper():
            name = name.lower()
        else:
            name = sc.titlecase(name)
        name = ''.join(name.split())
        retval = prefix + sc.snakecase(name)
        return retval

    @staticmethod
    def clean_name(s):
        if s is None:
            return None
        s = s.replace("'", "")
        # s = s.replace(':', ' ')
        # s = s.replace('#', 'number')
        # s = s.replace('/', ' per ')
        return s

    @staticmethod
    def sql_get_and_clean_up_value(row, column_name):
        try:
            val = str(row.get(column_name)).replace("'", "").replace('"', "")
            if len(val.strip()) > 0:
                return f'"{val}",'
        except Exception as ex:
            logger.error(ex)

        return "null,"

    @staticmethod
    def remove_traiing_comma(s):
        t = s[-1]
        if t == ',':
            return s[0:len(s) - 1]
        else:
            return s[0:len(s)]

    @staticmethod
    def if_none(x, y):
        """returns default value if x is None"""
        if x is None:
            return y
        return x

    @staticmethod
    def trim_all_extra_white_space(data):
        cmd = ' '.join(data.split())
        cmd = cmd.replace('\n', '')
        return cmd

    @staticmethod
    def convert_timestamp_to_row_datetime(row, field_name):
        dt = DateTime(row.get(field_name))
        if dt.date.year == 1970:
            if field_name in row:
                row.pop(field_name)
        else:
            row[field_name] = str(DateTime(row.get(field_name)).as_datetime)

    @staticmethod
    def generate_random_base31(length: int) -> str:
        """
        generates random string of n length
        :param length: number of characters in string
        :return: random string
        """
        # all non-vowel characters (don't want to generate random words that might be inappropriate)
        possible_chars = [char for char in string.ascii_letters + string.digits if char not in 'aeiouAEIOU']
        return ''.join(random.choice(possible_chars) for i in range(length))

    @staticmethod
    def match_names(source, target, fuzzy_first_name=False):
        """
        match names, matches the complete source with a target name.
        The source name can be in any order and the target name can be in any order as well
        for example, source = 'ron sandstrom'  target = 'sandstrom ron sr'  these will match
        the entire source name (after split) must exist in the target as complete word matches to declare success.
        """
        if source is None or target is None:
            return False
        source = source.replace('.', '').replace(',', '').replace(':', '').strip().lower()
        target = target.replace('.', '').replace(',', '').replace(':', '').strip().lower()

        source_parts = source.split(' ')
        target_parts = target.split(' ')

        # for each of the source name parts, make sure that they are all in the target lookup
        pos = 0
        for s in source_parts:
            s_part = s.strip()

            # if enabled allow the first part of the source name to do a partial match of any part of the target
            if fuzzy_first_name and pos == 0:
                located = False
                for t_part in target_parts:
                    if s_part in t_part:
                        located = True
                        break
                if not located:
                    return False

            # require a complete word match
            elif s_part not in target_parts:
                return False

        return True

