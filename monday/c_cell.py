"""
Cell:  Monday boards are made of rows and columns, each row contains a set of cells, one cell for each column.
The cells contain column properties and values and may have the value set as needed.  When a value is set
the cell is marked dirty and will be pushed to monday on the next update
 """

from typing import TypeVar

import stringcase as sc

from conversion.c_format import Format
from result.c_result import Result
from std_errors.c_ecode import Ecode
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_file_container import FileContainer
from std_utility.c_utility import Utility

from monday.c_column import Column

T = TypeVar("T")


class Cell:
    def __init__(self, row):
        self.row = row
        self.board = row.board
        self.row_id = None
        self.id = None
        self.index = None
        self._type = None
        self.value2 = None
        self.has_labels = False
        self.labels = []
        self.previous_value = None
        self.db_name = None
        self._value: T = None
        self._modified = False
        self._name = None
        self.source = None
        self.parent_row = None

    def init(self, row_id: str = '', c_value: object = None, column_info: Column = Column()):
        self.row_id = row_id
        self.id = column_info.id
        self._name = column_info.name
        self._type = column_info.type
        self.labels = column_info.labels
        self._value: T = c_value
        self._modified = False
        self.previous_value = None
        return self

    def new(self, c_value: object = None, column_info: Column = Column()):
        self.row_id = ''
        if column_info is not None:
            self.id = column_info.id
            self._name = column_info.name
            self._type = column_info.type
            self.labels = column_info.labels
        self._value: T = c_value
        self._modified = True
        self.previous_value = None
        return self

    @property
    def name(self):
        """ The cell name = column name shown on the Monday board."""
        return self._name

    @name.setter
    def name(self, x):
        """ Set the cell name (using the column name) and store the db name (snake case name too)"""
        self._name = x
        self.db_name = Format(x).snake_case

    @property
    def type(self):
        """ Get the cell type, can be text, number, people, date, _datetime, status or dropdown."""
        return self._type

    @type.setter
    def type(self, x):
        """Set the cell name """
        self._type = x

    @property
    def modified(self):
        return self._modified
        # """True if the cell has been modified"""
        # if not self._modified:
        #     return False
        # if self.previous_value is None or self._modified:
        #     return True
        # return str(self.previous_value).lower() != str(self.value).lower() or self.value is None

    @modified.setter
    def modified(self, x: bool = False):
        """Set the modified flag (True or False)"""
        self._modified: bool = x

    @property
    def value(self):
        """Get the cell value"""
        return self._value

    @value.setter
    def value(self, new_value):
        """Set the cell value, also copies the current value to the modified value, if different modified flag set."""

        # store the date time parts if datetime
        if self._type == 'date' or self._type == 'datetime':
            if not isinstance(new_value, DateTime):
                new_value: DateTime = DateTime(new_value)
            if self._value is not None:
                if isinstance(self._value, str) and len(self._value) < 5:
                    pass

                # no idea what this does.
                elif self._value.is_datetime:
                    self._type = 'datetime'
                else:
                    self._type = 'date'

        if self._type == 'boolean' and isinstance(self._value, str):
            if self._value == 'v':
                self._value = True
            else:
                self._value = False

        if self._type != 'dropdown' and self.has_labels and self._value is not None:
            if new_value not in self.labels and new_value != '':
                logging.warning(f"Cell Skipped, {self.name} [{new_value}] can not be found in [{self.labels}]")
                return

        self._value = new_value
        if str(self.previous_value) != str(self._value):
            self.modified = True

        self.previous_value = self._value

    def update(self, value=None, value2=None, add_missing_labels=False):
        self.row.warn_if_column_has_been_modified(self.name)

        if value == '':
            return self.row.update_single_column(column_name=self.name, column_value=value)

        if value is not None:
            self.value = value
            self.value2 = value2
            self.modified = True

        return self.row.update_column(column_name=self.name, value=self.value, value2=self.value2,
                                      add_missing_labels=add_missing_labels)

    def download_files(self, file_path='./', into_files_array=False) -> Result:
        """ (Use this)
        parm: file_path the physical location of the file after download
        parm: into_files_array - instruct the code to place the contents of the downloaded files into file buffer
              this makes it easy to email the data as an attachment or work on it directly

           Notes:  monday stores files as assets, they are passed if requested as part of the query see below:
                   assets {public_url file_extension name}
                   the assets are stored with the row and point to a file URL that is good for 1 hour but
                   usable by the API.
                   When a row has files, it is passed the private URL for those files, those private URLs are
                   only usable to logged-in users. for example, you could paste it into a browser and the file
                   is downloaded.
                   We need to use the file names listed in the cell files list to look up the matching asset in
                   the parent_row to obtain the public URL to download.
                   Because there is a lot of URL parsing and file handling, I created the file class to encapsulate
                   this and make working with the file urls in the cloud.
           query {
                   boards (ids: BOARD_ID)
                   {
                       groups (ids: GROUP_ID)  {
                       items {name id assets {public_url file_extension name} column_values {id column {title} text }
                       group {id title} }   }
                   }
        """

        try:
            if self._type != 'file':
                msg = "Only cells of type 'file' can be downloaded"
                return Result(-1, message=msg, data=[])

            if len(self._value) == 0:
                return Result(Ecode.Monday.no_files_to_download, data=[])

            if self.parent_row.assets is None:
                result = self.board.get_assets(self.row_id)
                if result.is_error():
                    return Result(Ecode.Monday.no_files_to_download, data=[])
                self.parent_row.assets = self.parent_row.get_asset_from_json(result.data)

            # get the list of files from the cell file value
            retval = []
            files = self._value.split(',')

            for file_url in files:
                file_url = file_url.strip()
                lookup_file = Utility.get_filename_from_url(file_url)
                # lookup the asset in the parent_row's asset list
                the_protected_1_hour_url = self._find_asset_by_name(lookup_file)
                url = the_protected_1_hour_url

                result = self.board.get_file(url=url, file_path=file_path)

                # create a list of files to return to the caller.
                if result.is_ok():
                    if not into_files_array:
                        f: FileContainer = result.data
                        f.write_file()
                        retval.append(result.data)
                    else:
                        retval.append(result.data)
                else:
                    return Result(-1, message=result.message)
        except Exception as ex:
            logging.warning(ex)
            return Result(-1, message=ex)

        return Result(0, data=retval)

    def download(self, file_path='./', into_files_array=False):
        """
        parm: file_path the physical location of the file after download
        parm: into_files_array - instruct the code to place the contents of the downloaded files into file buffer
              this makes it easy to email the data as an attachment or work on it directly

           Notes:  monday stores files as assets, they are passed if requested as part of the query see below:
                   assets {public_url file_extension name}
                   the assets are stored with the row and point to a file URL that is good for 1 hour but
                   usable by the API.
                   When a row has files, it is passed the private URL for those files, those private URLs are
                   only usable to logged-in users. for example, you could paste it into a browser and the file
                   is downloaded.
                   We need to use the file names listed in the cell files list to look up the matching asset in
                   the parent_row to obtain the public URL to download.
                   Because there is a lot of URL parsing and file handling, I created the file class to encapsulate
                   this and make working with the file urls in the cloud.
           query {
                   boards (ids: BOARD_ID)
                   {
                       groups (ids: GROUP_ID)  {
                       items {name id assets {public_url file_extension name} column_values {id column {title} text }
                       group {id title} }   }
                   }
        """

        if self._type != 'file':
            logging.info("Only cells of type 'file' can be downloaded")
            return []

        if len(self._value) == 0:
            return []

        # get the list of files from the cell file value
        retval = []
        files = self._value.split(',')

        for file_url in files:

            lookup_file = Utility.get_filename_from_url(file_url)
            # lookup the asset in the parent_row's asset list
            the_protected_1_hour_url = self._find_asset_by_name(lookup_file)
            url = the_protected_1_hour_url

            result = self.board.get_file(url=url, file_path=file_path, into_files_array=into_files_array)

            # create a list of files to return to the caller.
            if result.is_ok():
                if not into_files_array:
                    f: FileContainer = result.data
                    f.write_file()
                else:
                    retval.append(result.data)

        return retval

    def delete_files(self) -> Result:
        # , board_id, row_id, column_id
        return self.board.monday_delete_files(self.board.board_id, self.row_id, self.id)

    def upload_file(self, file_path, is_buffered=False, data=None) -> Result:
        if self._type != 'file':
            return Result(-1, message="Only cells of type 'file' can be uploaded to")
        return self.board.upload_file(self.parent_row.row_id, self.id, file_path,
                                      is_buffered=is_buffered, data=data)

    def _find_asset_by_name(self, in_name):
        for item in self.parent_row.assets:
            f_name = item.get('name')
            if f_name == in_name:
                return item.get('public_url')
