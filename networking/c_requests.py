from time import time
from time import sleep
import requests
from requests import Timeout, Response
import logging


class Network:

    @staticmethod
    def get(url, data=None, headers=None, timeout=60, show_info=True):
        return Network._execute_get(url, 'GET', data, headers=headers, timeout=timeout, show_info=show_info)

    @staticmethod
    def get_v2(url, headers=None, timeout=60, show_info=True, params=None):
        return Network._execute_get(url, 'GET', headers=headers, timeout=timeout, show_info=show_info, params=params)

    @staticmethod
    def post(url, data=None, headers=None, timeout=60, files=None):
        return Network._execute_get(url, 'POST', params=data, headers=headers, timeout=timeout, files=files)

    @staticmethod
    def _execute_get(url, _type=None, params=None, headers=None, timeout=60, show_info=True, files=None) -> Response:

        """Executes a get request and times out if get reaches timeout

        Args:
            url: The URL to connect to
            params: The arguments
            headers: Header
            timeout: Timeout

        Returns: Response

        """
        if show_info:
            logging.debug(f"Executing [{_type}] request, for url [{url}]")
        retval: Response = Response()
        wait_seconds_for_retry = 2.5
        retry_count = 0
        try_again: bool = True
        r = None

        start = time()
        logging.debug("timer set")
        # retry in the event of complexity limit reached.
        while retry_count < 5 and try_again:
            try:
                if _type == 'GET':
                    retval = requests.get(url, params=params, headers=headers, timeout=timeout)
                else:  # POST
                    retval = requests.post(url, data=params, headers=headers, timeout=timeout, files=files)
                try_again = False
            except Timeout:
                sleep(wait_seconds_for_retry)
                retry_count += 1
                logging.info(f"[{retry_count}] = Waiting [{wait_seconds_for_retry}] seconds, and trying again ")
        end = time()
        if retval is None:
            logging.info(
                f'Failed request in {round(end - start, 3)} seconds, retval = [{retval}]')
        elif show_info:
            logging.debug(
                f'Completed request in {round(end - start, 3)} seconds, status = [{retval.status_code}]')

        return retval
