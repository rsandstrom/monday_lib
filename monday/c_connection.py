"""
  Connection: This class is used to connect to Monday.com and process api requests
"""
from time import sleep, time

from networking.c_requests import Network
from result.c_result import Result
from status.c_status import Status
from std_errors.c_ecode import Ecode
import logging


class MondayConnection(object):
    def __init__(self, board_id, monday_token, monday_account, monday_timeout_seconds=5):
        self.board_id = board_id
        self.monday_token = monday_token
        self.monday_account = monday_account
        self.headers = {"Authorization": self.monday_token, "API-Version": "2023-10"}
        self.col_map = None
        self.monday_timeout_seconds = monday_timeout_seconds

    # execute any query and get a response from monday returns a result and status
    def execute(self, query, files=None) -> Result:
        logging.debug(f"ready to execute query = [{query}]")
        if files is not None:
            api_endpoint = 'https://api.monday.com/v2/file'
        else:
            api_endpoint = 'https://api.monday.com/v2/'

        result = Result()
        try:
            wait_seconds_for_retry = 2.5
            retry_count = 0
            try_again = True
            while retry_count < 5 and try_again:
                r = None
                template = 'time()# {:0.2f}, clock()# {:0.2f}'
                start = time()
                r = Network.post(api_endpoint, data=query, headers=self.headers,
                                 timeout=self.monday_timeout_seconds, files=files)
                end = time()
                logging.debug(f'Completed Monday.com request in {round(end - start, 3)} seconds')
                result = MondayConnection.check_response(r)
                if result.status.is_error():
                    if result.status.code == 4000:
                        logging.info(f"Complexity budget exhausted, Sleeping for {int(result.data)} seconds")
                        sleep(int(result.data) + 1)
                        retry_count += 1
                        try_again = True
                    else:
                        retry_count += 1
                        try_again = True
                        logging.error(f"retry count = [{retry_count}], {result.status.message}")
                        sleep(wait_seconds_for_retry)
                else:
                    try_again = False

        except Exception as ex:
            logging.error(ex)
            result.status = Status(2003, ex)

        if result.is_ok():
            try:
                # check for access, no boards means no access
                data = result.data.get('data')
                boards = data.get('boards')
                if boards is not None:
                    if len(boards) == 0:
                        if self.monday_account is None:
                            self.monday_account = 'Automation@com'
                        return Result(-1, message=f"{self.monday_account} "
                                                  f"does not have access to board [{self.board_id}]")

                if result.data is not None \
                        and result.data.get('data') is not None \
                        and result.data.get('data').get('complexity') is not None:
                    complexity = result.data.get('data').get('complexity')
                    logging.info(
                        f"complexity query:[{complexity.get('query')}], remaining budget: [{complexity.get('after')}]")
            except Exception as ex:
                logging.error(f"{result.message} Error: [{ex}]")

        return result

        # check response and return results, status

    @staticmethod
    def check_response(r):
        error = None
        retval = Result()

        if r.status_code == 200 and r.json().get('errors') is None:
            # Connection.check_if_deleted_board(r.json())
            return Result(code=0, data=r.json())

        if r.status_code == 500:
            if r.text == '{}':
                return Result(code=Ecode.Network.internal_server_error_500, message=r.reason)

            return Result(code=Ecode.Network.internal_server_error_500, message=r.text)

        if r.status_code == 504:
            return Result(code=Ecode.Network.gateway_timeout_error_504, message=r.reason)

        # set the error code
        error_text = ''
        try:
            error_text = r.json()
        except Exception as ex:
            logging.exception(ex, exc_info=True)

        if isinstance(error_text, dict):
            error_messages = error_text.get('errors')
            if 'complexity' in r.text.lower():
                error_code = 4000
                error_txt = ''
                timeout = 60
                for m in error_messages:
                    error_txt += f"{m}\n"
                    the_message = m
                    if the_message is not None and 'reset in' in the_message:
                        break_on_next = False
                        x = the_message.split()
                        timeout = x[len(x) - 2]
                        retval = Result(error_code, message=error_txt, log=True, data=timeout)
            else:
                retval = Result(4001, message=r.text, log=True)

        else:
            retval = Result(code=r.status_code, message=str(r.json()), log=True)

        return retval


