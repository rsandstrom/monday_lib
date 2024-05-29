"""
MondayCallbacks optimization of Monday Calls
"""
import logging

from monday.c_query import MondayQuery
from monday.c_query_helper import QueryHelper
from result.c_result import Result


class MondayCallBacks(MondayQuery):
    def __init__(self, board_id, monday_token, monday_account,  monday_timeout_seconds=5, fields=None):
        super().__init__(board_id, monday_token, monday_account, monday_timeout_seconds, fields)

    def load_rows_from_monday(self, row_id, fields=None) -> Result:
        """
        read a row
            Note: this will only return the data from the fields list if supplied.
        Args:
            row_id:
            fields: can be a str for a single field name or a list of field names

        Returns:
            Result:

        """
        if isinstance(row_id, list):
            assert len(row_id) < 101, "As of 10/3/2022 Monday limits us to 100 item ids being returned."

        view = QueryHelper.gen_view(fields)
        item_ids = QueryHelper.monday_format_list(row_id)
        logging.debug(f"Loading board [{self.board_id}] getting a row [{row_id}] ")

        cmd = """
                  query 
                  {
                        items (ids: [ITEM_IDS ] limit: 100) { name id assets {public_url file_extension name}
                        VIEW {id column {title} text }
                        group {id title} }
                   }
              """\
            .replace('BOARD_ID', str(self.board_id))\
            .replace("ITEM_IDS", str(item_ids))\
            .replace('VIEW', view)

        cmd = self.add_fields_to_query(query=cmd)
        query = self.gen_query(cmd)
        result = self.execute(query)
        return result

    def _load_rows(self, ids: [], fields=None) -> []:
        logging.debug(f"loading [{len(ids)}] Monday rows")
        ids_str = str(ids).replace('[', '').replace(']', '').replace("'", "")
        result = self.load_rows_from_monday(ids_str, fields=fields)
        if result.is_error():
            raise Exception(f"Unable to load monday board error = {result.status.message}")
        n_rows = self.create_rows_from_json(result.data)
        return n_rows

    def load_rows(self, ids: [], fields=None) -> []:
        """
        loads rows a max of 100 records at a time. for monday update 10/3/2022
        Args:
            fields:
            ids:

        Returns: n_rows

        """
        n_rows = []

        t_ids = []
        for f_id in ids:
            t_ids.append(f_id)
            if len(t_ids) == 100:
                n_rows.extend(self._load_rows(t_ids, fields=fields))
                t_ids = []

        if len(t_ids) > 0:
            n_rows.extend(self._load_rows(t_ids, fields=fields))

        return n_rows

    def load_row(self, row_id, fields=None) -> []:
        fields = QueryHelper.get_field_ids(self, fields=fields)
        result = self.load_rows_from_monday(row_id, fields=fields)
        if result.is_error():
            raise Exception(f"Unable to load monday board error = {result.status.message}")
        n_rows = self.create_rows_from_json(result.data)
        return n_rows

    def load_one_row(self, row_id, fields=None):
        rows = self.load_row(row_id, fields)
        if rows is not None and len(rows) > 0:
            return rows[0]
        return None

    @staticmethod
    def do_callback(rows: [], callback, *args):
        """
        calls the callback function and passes each row one at a time
        *args is optional
        """
        logging.debug(f"Executing Call back for [{len(rows)}] rows")
        all_done = False

        for row in rows:
            try:
                all_done = callback(row, *args)
                if all_done:
                    break
            except Exception as ex:
                logging.warning(f"Call back Failed for row name [{row.row_name}] error = {ex}")

        return all_done

    def do_callback_rows(self, result, the_callback=None, *args) -> []:
        row_ids = []
        if result.is_ok():
            logging.info(f"     Processing {len(result.data)} rows")
            count = 0
            total = 0
            ids = []
            row_ids = result.data
            for r_id in row_ids:
                ids.append(r_id)
                count += 1
                if count % 50 == 0:
                    the_rows = self.load_rows(ids)
                    all_done = self.do_callback(the_rows, the_callback, *args)
                    if all_done:
                        ids = []
                        break
                    ids = []
                    total += count
                    logging.debug(total)
                    count = 0
            if ids:
                the_rows = self.load_rows(ids)
                self.do_callback(the_rows, the_callback, *args)

        return row_ids
