"""
Monday Query Class, generate queries for monday operations
"""
import io
import json
import os
from datetime import datetime
from urllib import request

from monday.c_cell import Cell
from monday.c_connection import MondayConnection
from monday.c_core import MondayCore
from networking.c_requests import Network
from result.c_result import Result
from std_errors.c_ecode import Ecode
import logging
from std_utility.c_datetime import DateTime
from std_utility.c_file_container import FileContainer
from std_utility.c_utility import Utility


class MondayQuery(MondayCore):
    def __init__(self, board_id, monday_token, monday_account, monday_timeout_seconds=5, fields=None):
        super().__init__(board_id, monday_token, monday_account, monday_timeout_seconds, fields)

    def add_fields_to_query(self, query: str) -> str:
        # column_values (ids: FIELDS)
        if self.has_fields:
            query = query.replace("column_values", f"column_values (ids: {self.field_ids_str})")

        return query

    def set_col_map(self, col_map):
        self.col_map = col_map

    @staticmethod
    def extract_item_count(result):
        count = 0
        if result.is_ok():
            try:
                count = result.data.get('data').get('boards')[0].get('items_count')
            except Exception as ex:
                logging.warning(ex)

        return count

    def get_item_count(self) -> int:
        """
                   Gets the count of items
        """

        logging.debug(f"Getting Item Count [{self.board_id}]")

        cmd = """
                         query 
                         {
                             boards (ids: BOARD_ID) 
                             { 
                                items_count 
                              }
                          }
                     """.replace('BOARD_ID', str(self.board_id))

        query = self.gen_query(cmd)
        result = self.execute(query)
        return self.extract_item_count(result)

    @staticmethod
    def get_file(url=None, file_path='.') -> Result:
        filename = ''

        try:
            filename = Utility.get_filename_from_url(url)
            # reads the data into a temporary file name, then ads the contents to retval array
            g = request.urlopen(url)
            data = g.read()
            file_bytes = io.BytesIO()
            file_bytes.write(data)
            file_bytes.name = filename
            file_obj = FileContainer(url=url, file_path=file_path, filename=filename, buffer=file_bytes)
            result = Result(0, data=file_obj)

        except Exception as ex:
            result = Result(Ecode.Monday.downloading_files, message=ex)
            logging.warning(f"Unable to download {file_path}{filename} err = {ex}")

        return result

    @staticmethod
    def gen_query(data):
        cmd = ' '.join(data.split())
        cmd = cmd.replace('\n', '')
        return {'query': cmd}

    @staticmethod
    def clean_query(data):
        cmd = ' '.join(data.split())
        cmd = cmd.replace('\n', '')
        return cmd

    @staticmethod
    def gen_mutation(data):
        cmd = ' '.join(data.split())
        cmd = cmd.replace('\n', '')
        return {'query': cmd}

    def get_sub_item_ids(self, item_id, sub_id):
        cmd = """query { items (ids: ITEM_ID) {column_values (ids:["SUB_ID"]) {value} } }""". \
            replace("ITEM_ID", str(item_id)).replace("SUB_ID", sub_id)
        query = self.gen_query(cmd)
        result = self.execute(query)
        if result.is_ok():
            try:
                ids = []
                _d = json.loads(result.data.get('data').get('items')[0].get('column_values')[0].get('value')).get(
                    'linkedPulseIds')
                for _id in _d:
                    ids.append(_id.get('linkedPulseId'))
                result.data = ids
            except Exception as ex:
                logging.debug(ex)
                result.data = []

        return result

    def get_sub_rows(self, linked_pulse_ids):
        cmd = """ query { items(ids: VALUE_RETURNED) {
                          name 
                          id 
                          assets {public_url file_extension name}
                          column_values {id column{title} value text type } } }""". \
            replace("VALUE_RETURNED", str(linked_pulse_ids))
        query = self.gen_query(cmd)
        result = self.execute(query)

        return result

    def monday_delete_files(self, board_id, row_id, column_id):
        cmd = """
                mutation 
                { 
                    change_column_value 
                    (
                            item_id: ROW_ID, 
                            column_id: "COLUMN_ID", 
                            board_id: BOARD_ID, 
                            value:"{\\"clear_all\\": true}"
                    ) { id } 
                }
        """.replace('ROW_ID', str(row_id)).replace('COLUMN_ID', str(column_id)).replace('BOARD_ID', str(board_id))

        query = {'query': cmd}
        result = self.execute(query)

        logging.debug(f"query = {query}")
        logging.debug(f"Update {row_id} was {result.status.message}")
        return result

    def upload_file(self, row_id, column_id, file_path=None, is_buffered=False, data=None):
        cmd = """
             mutation ($file: File!)
              {
                  add_file_to_column (file: $file, item_id: ROW_ID, column_id: "COLUMN_ID") { id }
              }
          """.replace('ROW_ID', row_id).replace('COLUMN_ID', column_id).replace('FILE_PATH', file_path)

        if is_buffered:
            files = [('variables[file]', (os.path.basename(file_path), data, 'contenttype'))]
        else:
            files = [('variables[file]', (os.path.basename(file_path), open(file_path, 'rb'), 'contenttype'))]
        query = self.gen_mutation(cmd)
        result = self.execute(query, files=files)

        if result.is_ok():
            rows = result.data.get('data').get('users')
            result.data = rows

        logging.debug(f"upload result is was {result.status.message}")
        return result

    # updates one or more columns in a row, uses list[Cell] or dict field name / value pairs returns result, status
    def sub_item_update(self, sub_board_id, row_id, q_data) -> Result:
        if isinstance(q_data, dict):
            data = self._generate_update_query(q_data)
        else:
            data = self._generate_update_query_v2(q_data)

        update_command = r'mutation ' \
                         r'{change_multiple_column_values ' \
                         f'(item_id: ITEM_ID, board_id: BOARD_ID, column_values: "{data}" )' \
                         r'{id} }' \
            .replace('BOARD_ID', str(sub_board_id)) \
            .replace("ITEM_ID", row_id)

        update_query = self.gen_query(update_command)

        result = self.execute(update_query)

        logging.debug(f"Update query = {update_query}")
        logging.debug(f"Update {row_id} was {result.status.message}")
        return result

    def update_single_column_value(self, row_id=None, column_id: str = None, column_value: str = '') -> Result:
        """ Updates a single column value for a row on a board using a simple string value"""
        if row_id is None or len(column_id) == 0:
            return Result(-1, message="Unable to update a column with no row id or column id")
        update_command = """
              mutation {
                  change_simple_column_value(
                       item_id: ITEM_ID, 
                       board_id: BOARD_ID, 
                       column_id: "COLUMN_ID", 
                       value: "COLUMN_VALUE") { id }
              }
          """.replace('BOARD_ID', str(self.board_id)) \
            .replace("ITEM_ID", str(row_id)) \
            .replace("COLUMN_ID", column_id) \
            .replace("COLUMN_VALUE", column_value)

        update_query = {'query': update_command}

        result = self.execute(update_query)

        logging.debug(f"Update query = {update_query}")
        logging.debug(f"Update {row_id} was {result.status.message}")
        return result

    # updates one or more columns in a row, uses list[Cell] or dict field name / value pairs returns result, status
    def monday_update(self, row_id, q_data, add_missing_labels=False) -> Result:
        if isinstance(q_data, dict):
            data = self._generate_update_query(q_data)
        else:
            data = self._generate_update_query_v2(q_data)

        if add_missing_labels:
            update_command = r'mutation ' \
                             r'{change_multiple_column_values ' \
                             f'(item_id: ITEM_ID, board_id: BOARD_ID, column_values: "{data}", ' \
                             f'create_labels_if_missing: true)' \
                             r'{id} }' \
                .replace('BOARD_ID', str(self.board_id)) \
                .replace("ITEM_ID", row_id)
        else:
            update_command = r'mutation ' \
                             r'{change_multiple_column_values ' \
                             f'(item_id: ITEM_ID, board_id: BOARD_ID, column_values: "{data}" ) ' \
                             r'{id} }' \
                .replace('BOARD_ID', str(self.board_id)) \
                .replace("ITEM_ID", row_id)

        update_query = {'query': update_command}

        result = self.execute(update_query)

        logging.debug(f"Update query = {update_query}")
        logging.debug(f"Update {row_id} was {result.status.message}")
        return result

    def insert_subitem(self, parent_row_id, subitem_name, q_data=None):
        if q_data is None:
            q_data = []
        if isinstance(q_data, dict):
            data = self._generate_update_query_v3_dict(q_data)
        else:
            data = self._generate_update_query_v3(q_data)

        mutation = """
        mutation  
        {
            create_subitem ( parent_item_id: PARENT_ROW_ID, item_name: "ROW_NAME", column_values: COL_VALUES)
            {
                id 
                board {id}
                name 
                assets {public_url file_extension name}
                column_values {id column{title} value text type }
            } 
        }
        """.replace("PARENT_ROW_ID", str(str(parent_row_id)))\
            .replace("ROW_NAME", subitem_name).replace("COL_VALUES", json.dumps(data))

        query = self.gen_query(mutation)
        logging.debug(query)

        result = self.execute(query)

        return result

    def access_check(self):
        return self.check_access(self.board_id, "", self.headers)

    @staticmethod
    def check_access(board_id, board_name, headers):
        query = """
               query {
                     boards (ids: BOARD_ID) {
                       name }}
               """.replace('BOARD_ID', str(board_id))
        data = {"query": query}
        api_endpoint = 'https://api.monday.com/v2/'
        r = Network.post(api_endpoint, data=data, headers=headers, timeout=1)
        result = MondayConnection.check_response(r)
        if result.is_ok():
            board_count = len(result.data.get('data').get('boards'))
            if board_count == 0:
                return Result(-1, message=f"Automation does not have access to board [{board_id}]: [{board_name}]")

        return result

    def get_board_name(self):
        query = """
        query {
  boards (ids: BOARD_OD) {
    name }}
        """.replace('BOARD_ID', str(self.board_id))
        data = {"query": query}
        result = self.execute(data)
        return result

    def add_column(self, title, description='', c_type='text'):
        query = """
        mutation{
          create_column(board_id: BOARD_ID, title:"TITLE", 
                description: "DESC", column_type:TYPE) {
            id
            title
            description
          }
        }
        """.replace('BOARD_ID', str(self.board_id)) \
            .replace('TITLE', str(title)) \
            .replace('DESC', str(description)) \
            .replace('TYPE', str(c_type))

        data = {"query": query}
        result = self.execute(data)
        return result

    def update_link_column(self, row_id, column_name, url, text):
        column_id = self.col_map.get(column_name).id
        assert column_id is not None, "Unable to locate Column ID"
        query = """
        mutation {
          change_multiple_column_values(item_id:ROW_ID, 
            board_id:BOARD_ID, 
            column_values: "{\"COL_ID\" : {\"url\" : \"URL", \"text\":\"URL_TEXT\"}}") {
            id
          }
        }
        """.replace('BOARD_ID', str(self.board_id))\
            .replace("ROW_ID", row_id)\
            .replace('COL_ID', column_id)\
            .replace('URL', str(url))\
            .replace('URL_TEXT', text)

        data = {"query": query}
        result = self.execute(data)
        return result

    def rename_column(self, column_name, title):
        column_id = self.col_map.get(column_name).id
        assert column_id is not None, "Unable to locate Column ID"
        query = """
        mutation {
            change_column_title (board_id: BOARD_ID, column_id: "COL_ID", title: "TITLE") {
                id
            }
        }
        """.replace('BOARD_ID', str(self.board_id)) \
            .replace('TITLE', str(title)) \
            .replace('COL_ID', str(column_id))

        data = {"query": query}
        result = self.execute(data)
        return result

    # needs testing, but I think this is the way to go.
    def insert(self, group_id='topics', row_name='delete me', q_data=None) -> Result:
        # data = self.format_data_for_query(q_data)

        if isinstance(q_data, dict):
            data = self._generate_update_query_v3_dict(q_data)
        else:
            data = self._generate_update_query_v3(q_data)
        # r' create_labels_if_missing: true ' \

        mutation = """
            mutation 
            {
                create_item 
                (
                    board_id: BOARD_ID, 
                    group_id: "GROUP_ID", 
                    item_name: "ROW_NAME",
                    column_values: COL_VALUES
                )
                {
                    id
                    name 
                    assets {public_url file_extension name}
                    column_values {id column {title} value text type }
                } 
            }
        """.replace("BOARD_ID", str(self.board_id)) \
            .replace("GROUP_ID", str(group_id))\
            .replace("ROW_NAME", row_name)\
            .replace("COL_VALUES", json.dumps(data))

        query = self.gen_query(mutation)
        logging.debug(query)

        result = self.execute(query)

        logging.debug(f"code = {result.status.code}, message = [{result.status.message}]")
        logging.debug(f"Create {row_name} was {result.status.message}")
        return result

    # needs testing, but I think this is the way to go. uses a row_id or item_id
    def delete(self, item_id) -> Result:

        query = r' mutation {' \
                r' delete_item (' \
                r' item_id: ITEM_ID ) {id}}' \
            .replace("ITEM_ID", item_id)

        data = {"query": query}
        result = self.execute(data)
        # result = self.execute(query)
        if result.is_ok():
            try:
                for i in range(len(self.rows) - 1, -1, -1):
                    if self.rows[i].row_id == item_id:
                        del self.rows[i]
                        break
            except Exception as e:
                logging.debug(e)

        logging.debug(f"Delete {item_id} was {result.status.message}")
        return result

    # def load_partial_board_json(self, column_id='XXX', lookup_value='XXX') -> Result:
    #     logging.info(f"Loading Monday.com board {self.board_id}")
    #     query = '{' \
    #             'items_by_column_values  boards (ids: BOARD-ID, column_id: "COLUMN_ID", column_value: "LOOKUP_VALUE")' \
    #             '{id name permissions tags {id name } ' \
    #             'groups {id title} ' \
    #             'items {name id column_values {id title text } group {id title}} ' \
    #             'columns {id title type settings_str }}}' \
    #         .replace("BOARD-ID", str(self.board_id)) \
    #         .replace("COLUMN_ID", column_id) \
    #         .replace("LOOKUP_VALUE", lookup_value)
    #     data = {"query": query}
    #     result = self.execute(data)
    #
    #     logging.debug(f"Load Monday Board was {result.status.message}")
    #     return result

    def load_monday_all_users_as_json(self):
        """
           Get all users and email addresses
        """

        logging.info(f"Loading user list")

        cmd = """ query {
                      users {
                      name 
                      created_at
                      email
                      }            
                  }
          """

        query = self.gen_query(cmd)
        result = self.execute(query)

        if result.is_ok():
            rows = result.data.get('data').get('users')
            result.data = rows

        logging.debug(f"Load fields only from Monday Board was {result.status.message}")
        return result

    def monday_get_user(self, uid: str = ''):
        """
           Get all users and email addresses
        """

        cmd = """ query {
                      users (ids: USER_ID) 
                      {
                          name 
                          email
                      }            
                  }
          """.replace('USER_ID', uid)

        query = self.gen_query(cmd)
        result = self.execute(query)

        if result.is_ok():
            rows = result.data.get('data').get('users')
            result.data = rows

        logging.debug(f"Load fields only from Monday Board was {result.status.message}")
        return result

    def test_query(self, cmd: str = ''):
        query = self.gen_query(cmd)
        result = self.execute(query)
        return result

    def get_mini_row_from_group(self,
                                group_id=None,
                                column_id: str = None,
                                col_name: str = None,
                                col_values: str = None):
        """
           row ids item name,
           use a group name and optional column_id as a string. "status"
        """
        if column_id is None and col_values is None:
            cmd = """ {
                            boards (ids: BOARD_ID) 
                            {
                                groups (ids: "GROUP_ID" )  
                                { 
                                    items {name id} 
                                }
                            }
                        }
                        """.replace('BOARD_ID', str(self.board_id)) \
                .replace('GROUP_ID', str(group_id))

        elif col_values is not None:
            cmd = """ {
                          items_by_column_values  
                                        (board_id: BOARD_ID , 
                                        column_id: "COLUMN_ID", 
                                        column_value: "LOOKUP_VALUE") 
                                        {
                                             group {id, title} name id column_values (ids: "COLUMN_ID") {id title text }
                                        }
                                    }
                                    """ \
                .replace('BOARD_ID', str(self.board_id)) \
                .replace('GROUP_ID', str(group_id)) \
                .replace('COLUMN_ID', str(column_id)) \
                .replace('LOOKUP_VALUE', col_values)

        else:
            cmd = """ {
                             boards (ids: BOARD_ID) 
                             {
                                 groups (ids: "GROUP_ID" )  
                                 {
                                      items 
                                      {
                                          name id column_values (ids: "COLUMN_IDS_STR")
                                          {
                                              id title text 
                                          }
                                      } 
                                 }
                             }
                         }
                 """.replace('BOARD_ID', str(self.board_id)) \
                .replace('GROUP_ID', str(group_id)) \
                .replace("COLUMN_IDS_STR", column_id)

        query = self.gen_query(cmd)
        result = self.execute(query)

        class MiniRow:
            def __init__(self, m_row=None, column_info=None):
                self.name = m_row.get('name')
                self.id = m_row.get('id')
                self.has_cell = False
                if 'column_values' in m_row:
                    self.has_cell = True
                    item = m_row.get('column_values')[0]
                    self.cell = Cell(row=m_row)
                    self.cell.init(self.id, item.get('text'), column_info)

        if result.is_ok():
            data = result.data.get('data')
            if 'items_by_column_values' in data:
                rows = []
                filtered_rows = data.get('items_by_column_values')
                for f_row in filtered_rows:
                    if f_row.get('group').get('id') == group_id:
                        f_row.pop('group')
                        rows.append(f_row)
            else:
                rows = result.data.get('data').get('boards')[0].get('groups')[0].get('items')

            result_rows = []
            col_info = self.col_map.get(col_name)
            for row in rows:
                result_rows.append(MiniRow(m_row=row, column_info=col_info))

            result = Result(0, data=result_rows)

        return result

    def get_item_ids(self, group_id=None, column_id=None, col_values=None, cursor=None):

        if cursor is None:
            cursor = ''
        else:
            cursor = f'cursor: "{cursor}",'

        cmd = """ {
                     boards (ids: BOARD_ID)
                     {
                         groups (ids: "GROUP_ID") { 
                             items_page (CURSOR limit:100) {
                                 cursor
                                 items {id} 
                             }
                         }
                     }
                 }
                 """.replace('BOARD_ID', str(self.board_id)).replace('GROUP_ID', str(group_id)).replace("CURSOR", cursor) \

        result_ids = []
        t_result = []
        page = 1
        finished = False
        cursor = 'N/A'
        while not finished:
            command = cmd.replace('PAGE', str(page))
            page += 1
            query = self.gen_query(command)
            result = self.execute(query)
            if result.is_ok():
                try:
                    cursor = result.data.get('data').get('boards')[0].get('groups')[0].get('items_page').get('cursor')
                    rows = result.data.get('data').get('boards')[0].get('groups')[0].get('items_page').get('items')
                    result.data.get('data').pop('boards')
                    result.data.get('data')['items'] = rows
                    t_result = []
                    if len(rows) == 0:
                        finished = True
                    for row in rows:
                        t_result.append(row.get('id'))
                except Exception as ex:
                    return Result(-1, message=f"No Rows Returned Group: [{group_id}]- {ex}", data=[])
            if not finished:
                result_ids.extend(t_result)
            if cursor is None:
                finished = True
        if len(result_ids) > 0:
            return Result(0, data=result_ids)
        else:
            return Result(-1, data=[], message=f"No Rows Returned Group: [{group_id}]")

    def load_select_group_as_json(self, group_id=None, row_ids_only=False):
        """
           Get all fields from a board.
           returns all fields or no data
           groups (ids: "GROUP_ID" )  {  items(newest_first: true, limit:100, page:2) {id}   }
        """

        logging.debug(f"Loading rows using group,  {self.board_id}, group id = {group_id}")
        if row_ids_only:
            cmd = """ {
                         boards (ids: BOARD_ID)
                         {
                             groups (ids: "GROUP_ID" )  {  items(newest_first: true) {id}   }
                         }
                     }
                     """.replace('BOARD_ID', str(self.board_id)).replace('GROUP_ID', str(group_id))
        else:
            cmd = """ {
                          boards (ids: BOARD_ID) 
                          {
                              groups (ids: "GROUP_ID" )  {
                              items {name id column_values {id column {title} text } 
                              group {id title} }   }
                          }
                      }
              """.replace('BOARD_ID', str(self.board_id)).replace('GROUP_ID', str(group_id))

        query = self.gen_query(cmd)
        result = self.execute(query)

        if result.is_ok():
            try:
                rows = result.data.get('data').get('boards')[0].get('groups')[0].get('items')
                result.data.get('data').pop('boards')
                result.data.get('data')['items'] = rows
            except Exception as ex:
                return Result(-1, message=f"No Rows Returned Group: [{group_id}]- {ex}", data=[])

        logging.debug(f"Load fields only from Monday Board was {result.status.message}")
        return result

    def load_select_result_as_json(self, col_id=None, values=None, row_ids_only=False):
        """
           Get all fields from a board.
           returns all fields or no data
        """
        use_values = '['
        for v in values:
            use_values += f'"{v}",'
        use_values += ']'
        logging.info(f"Loading rows using selection col=[{col_id}], match={values},  {self.board_id}")

        if row_ids_only:

            cmd = """ query {
                             items_by_multiple_column_values 
                             ( board_id: BOARD_ID, column_id: "COLUMN_ID", column_values: COLUMN_VALUES) { id }
                         }
                     """.replace('BOARD_ID', str(self.board_id)) \
                .replace("COLUMN_ID", col_id) \
                .replace("COLUMN_VALUES", use_values)
        else:
            cmd = """ query {
                      items_by_multiple_column_values 
                      ( board_id: BOARD_ID, column_id: "COLUMN_ID", column_values: COLUMN_VALUES) {
                      id
                      name
                      column_values {id column {title} text } 
                      group {id title} }
                  }
              """.replace('BOARD_ID', str(self.board_id)) \
                .replace("COLUMN_ID", col_id) \
                .replace("COLUMN_VALUES", use_values)

        query = self.gen_query(cmd)
        result = self.execute(query)

        if result.is_ok():
            try:
                rows = result.data.get('data').get('items_by_multiple_column_values')
                result.data.get('data').pop('items_by_multiple_column_values')
                result.data.get('data')['items'] = rows
            except Exception as ex:
                return Result(-1, message=f"No Rows Returned Group: - {ex}", data=[])

        logging.debug(f"Load fields only from Monday Board was {result.status.message}")
        return result

    def get_assets(self, row_id):
        """"
            get assets for a row (used to download files)
        """

        logging.debug(f"Loading board [{self.board_id}] getting a row [{row_id}] ")

        cmd = """
                           query 
                  {
                     
                       items (ids: [ROW_ID, ]) { name id assets {public_url file_extension name}
                       column_values {id column {title} text }
                       group {id title} }
                      
                   }
           """.replace('BOARD_ID', str(self.board_id)).replace("ROW_ID", str(row_id))

        query = self.gen_query(cmd)
        result = self.execute(query)
        return result

    def monday_load_column_activity_log(self, column_ids: str=None, limit=2):
        column_id_str = ''
        if column_ids is not None:
            column_id_str = f', column_ids: "{column_ids}"'

        cmd = """
          query {
              boards (ids: BOARD_ID) 
              {
                  activity_logs (limit: LIMIT, COLUMN_IDS) 
                  {
                    user_id 
                    created_at
                    id
                    event
                    data

                  }
              }
          }
          """.replace('BOARD_ID', str(self.board_id)) \
            .replace("COLUMN_IDS", column_id_str)\
            .replace('LIMIT', str(limit))

        query = self.gen_query(cmd)
        result = self.execute(query)
        return result

    def load_activity_log(self, date_from, date_to, column_ids=None):
        column_id_str = ''
        if isinstance(date_from, datetime):
            date_from = DateTime(dt=date_from)
        if isinstance(date_to, datetime):
            date_to = DateTime(dt=date_to)
        if isinstance(date_from, DateTime):
            date_from = date_from.iso8601
        if isinstance(date_to, DateTime):
            date_to = date_to.iso8601
        if column_ids is not None:
            column_id_str = f', column_ids: "{column_ids}"'

        cmd = """
          query {
              boards (ids: BOARD_ID) 
              {
                  activity_logs (from: "DATE_FROM", to: "DATE_TO" COLUMN_IDS) 
                  {
                    user_id 
                    created_at
                    id
                    event
                    data
                    
                  }
              }
          }
          """.replace('BOARD_ID', str(self.board_id)) \
            .replace("DATE_FROM", str(date_from)) \
            .replace("DATE_TO", str(date_to))\
            .replace("COLUMN_IDS", column_id_str)

        query = self.gen_query(cmd)
        result = self.execute(query)
        return result

    # def load_rows_from_monday(self, row_id):
    #     """
    #         read a row
    #         Note: this will only return the data from the fields list if supplied.
    #     """
    #
    #     logging.debug(f"Loading board [{self.board_id}] getting a row [{row_id}] ")
    #
    #     cmd = """
    #               query
    #               {
    #                   boards (ids: BOARD_ID)
    #                   {
    #                        items (ids: [ROW_ID, ]) { name id assets {public_url file_extension name}
    #                        column_values {id title text }
    #                        group {id title} }
    #                    }
    #                }
    #           """.replace('BOARD_ID', str(self.board_id)).replace("ROW_ID", str(row_id))
    #
    #     cmd = self.add_fields_to_query(query=cmd)
    #     query = self.gen_query(cmd)
    #     result = self.execute(query)
    #     return result

    # reads a monday board returns a dict with rows and columns
    def load_monday_board_using_field_list_as_json(self, use_fields) -> Result:
        """
            Get all fields from a board.
            returns all fields or no data
        """

        logging.debug(f"Loading board [{self.board_id}] rows using field list ")

        cmd = """
                  query 
                  {
                       boards (ids: BOARD_ID) { id 
                       items { name id column_values (ids: FIELDS) {id column {title} text }
                       group {id title} } 
                       groups {id title}
                   }}
              """.replace('BOARD_ID', str(self.board_id)).replace("FIELDS", use_fields)

        query = self.gen_query(cmd)
        result = self.execute(query)

        logging.debug(f"Load fields only from Monday Board was {result.status.message}")
        return result

    # reads a monday board returns a dict with rows and columns
    def load_column_and_group_info_as_json(self) -> Result:
        """
            Get all fields from a board.
            returns all fields or no data
        """
        logging.debug(f"getting columns list from Monday.com board {self.board_id}")

        cmd = """
              query 
              {
                   boards (ids: BOARD_ID) 
                   {
                      id name permissions tags { id name } 
                      groups { id title} 
                      items_page (limit: 1) {
                        items { name id column_values {id column { title } text } }
                      }
                      columns { id title type settings_str }
                    }
              } 
              """.replace('BOARD_ID', str(self.board_id))

        query = self.gen_query(cmd)
        result = self.execute(query)

        logging.debug(f"Load one record from Monday Board was {result.status.message}")
        return result

    # reads a monday board returns a dict with rows and columns
    def load_monday_board_json(self) -> Result:
        """
            Get all fields from a board.
            returns all fields or no data
        """
        logging.info(f"Loading Monday.com board {self.board_id}")
        data = {
            "query": '{boards (ids: SOME-BOARD-ID) {id name '
                     'items {name id '
                     'column_values (ids: []) {id title text } group {id title} }'
                     ' }}'.replace('SOME-BOARD-ID', str(self.board_id))
        }
        # data = {
        #     "query": '{boards (ids: SOME-BOARD-ID) {id name permissions tags {id name } ' \
        #              'groups {id title} ' \
        #              'items {name id '
        #              'column_values (ids: []) {id title text } group {id title} }' \
        #              'columns {id title type settings_str } ' \
        #              ' }}'.replace('SOME-BOARD-ID', str(self.board_id))
        # }
        result = self.execute(data)

        logging.debug(f"Load Monday Board was {result.status.message}")
        return result

    # reads a monday board returns a dict with rows and columns
    def load_monday_board_json_v1(self) -> Result:
        """
            Get all fields from a board.
            returns all fields or no data
        """
        logging.info(f"Loading Monday.com board {self.board_id}")
        data = {
            "query": '{boards (ids: SOME-BOARD-ID) {id name permissions tags {id name } '
                     'groups {id title} '
                     'items {name id '
                     'column_values {id title text } group {id title} }'
                     'columns {id title type settings_str } '
                     ' }}'.replace('SOME-BOARD-ID', str(self.board_id))
        }
        result = self.execute(data)

        logging.debug(f"Load Monday Board was {result.status.message}")
        return result

    # converts list[Cell] or dict of field / value pairs into usable query for monday
    def format_data_for_query(self, q_data):
        if isinstance(q_data, type([Cell])):
            q_data = self._convert_cell_to_dict(q_data)
            return q_data

        data = self._generate_update_query(q_data)

        return data

        # converts the field names to the column id and creates a query

    @staticmethod
    def _generate_update_query_v3_dict(q_data: dict) -> str:
        if q_data is None:
            return '{}'
        data = '{'
        for _col_name, _col_value in q_data.items():
            if _col_value is None:
                continue
            if data != '{':
                data += ','
            data += '\"' + _col_name + '\": \"' + str(_col_value) + '\"'
        retval = data + '}'
        logging.debug(retval)
        return retval

    @staticmethod
    def _generate_update_query_v3(q_data) -> str:
        if q_data is None:
            return '{}'
        data = '{'
        for _cel in q_data:
            _cell: Cell = _cel
            if not _cell.modified:
                continue
            if _cell.value is None:
                continue

            if data != '{':
                data += ','
            try:
                if _cell.type == 'datetime':
                    _c: DateTime = _cell.value
                    dd: str = _c.to_date_str()
                    dt: str = _c.to_time_str()
                    data += ' "' + _cell.id + '" : {"date" : "' + dd + '", "time" : "' + dt + '" } '
                    continue

                if _cell.modified and _cell.type == 'boolean':
                    a_value = 'false'
                    if _cell.value is True:
                        a_value = 'true'

                    data += """ "ID": {"checked": "VALUE" } """ \
                        .replace('ID', _cell.id) \
                        .replace('VALUE', a_value)
                    continue

                the_value = str(_cell.value)

                if _cell.type == 'date':
                    if isinstance(_cell.value, DateTime):
                        _c: DateTime = _cell.value
                        the_value = _c.to_date_str()
                    if isinstance(_cell.value, str):
                        the_value = _cell.value

                if _cell.modified and _cell.value is not None:
                    data += """ "ID": "VALUE" """ \
                        .replace('ID', _cell.id) \
                        .replace('VALUE', the_value)
                    # data += '\\"' + _cell.id + '\\' + '\":' + '\\' + '\"' + the_value + '\\' + '\"'
            except Exception as ex:
                logging.error(ex)

        retval = data + '}'

        logging.debug(retval)
        return retval

    # converts the field names to the column id and creates a query
    @staticmethod
    def _generate_update_query(q_data: dict) -> str:
        if q_data is None:
            return '{}'
        data = '{'
        for _col_name, _col_value in q_data.items():
            if _col_value is None:
                continue
            if data != '{':
                data += ','
            data += '\\"' + _col_name + '\\' + '\":' + '\\' + '\"' + str(_col_value) + '\\' + '\"'
        retval = data + '}'
        logging.debug(retval)
        return retval

    @staticmethod
    def _generate_update_query_v2(q_data) -> str:
        data = '{'
        for _cel in q_data:
            _cell: Cell = _cel
            if not _cell.modified:
                continue
            if _cell.value is None:
                continue

            if data != '{':
                data += ','
            try:
                if _cell.type == 'link':
                    if _cell.source is None or not isinstance(_cell.source, dict):
                        continue
                    text = _cell.value2
                    if text is None or len(str(text)) == 0:
                        text = _cell.source.get('title')
                    url = _cell.value
                    if text is None or url is None:
                        continue
                    data += ' \\"' + _cell.id + '\\" : {\\"url\\" : \\"' + url + '\\", \\"text\\":\\"' + text + '\\" } '
                    continue

                if _cell.type == 'datetime':
                    _c: DateTime = _cell.value
                    dd: str = _c.to_date_str()
                    dt: str = _c.to_time_str()
                    data += ' \\"' + _cell.id + '\\" : {\\"date\\" : \\"' + dd + '\\", \\"time\\" : \\"' + dt + '\\" } '
                    continue

                if _cell.modified and _cell.type == 'boolean':
                    a_value = 'false'
                    if _cell.value is True:
                        a_value = 'true'

                    data += """ \\"ID\\": {\\"checked\\": \\"VALUE\\" } """ \
                        .replace('ID', _cell.id) \
                        .replace('VALUE', a_value)
                    continue

                the_value = str(_cell.value)

                if _cell.type == 'date':
                    _c: DateTime = _cell.value
                    the_value = _c.to_date_str()

                if _cell.modified and _cell.value is not None:
                    data += """ \\"ID\\": \\"VALUE\\" """ \
                        .replace('ID', _cell.id) \
                        .replace('VALUE', the_value)
                    # data += '\\"' + _cell.id + '\\' + '\":' + '\\' + '\"' + the_value + '\\' + '\"'
            except Exception as ex:
                logging.error(ex)

        retval = data + '}'

        logging.debug(retval)
        return retval

    # create a group
    def create_group(self, group_name):
        command = r'mutation ' \
                  r'{ create_group ' \
                  r'(board_id: "BOARD_ID", ' \
                  r'group_name: "GROUP_NAME") { id }}' \
            .replace("BOARD_ID", str(self.board_id)) \
            .replace("GROUP_NAME", group_name)

        return self.execute({'query': command})

    @staticmethod
    def _convert_cell_to_dict(_data: [Cell]) -> dict:
        data = {}
        for d in _data:
            if d.type == '_datetime':
                data[d.id] = d._datetime()
                continue

            if d.modified and d.value is not None:
                data[d.id] = d.value
        return data

    @staticmethod
    def check_if_deleted_board(data):
        d = data.get('data')
        assert d is not None, 'This board has been deleted'
        b = d.get('boards')
        if b is not None:
            assert len(b) > 0, 'This board has been deleted'
