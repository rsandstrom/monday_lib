"""
MondayFactory class
"""
import copy
from collections import namedtuple
from time import sleep

from cache.c_cached_resource import CachedResource
from cache.c_singleton import Singleton
from conversion.c_conversion import Conversion
from monday._monday_token import MONDAY_TOKEN
from monday.c_board import Board
from monday.c_column import Column
import logging

MondayCache = namedtuple('MondayCache', 'board_id timestamp monday')


@Singleton
class MondayFactory(CachedResource):
    def __init__(self, enable_cache=True, cache_expire_seconds=14400):
        super().__init__(enable_cache=enable_cache, expire_seconds=cache_expire_seconds)

        self.expire_seconds = cache_expire_seconds

    def board(self, board_id, make_copy=False, fields: [] = None,
              monday_token=None, monday_timeout_seconds=5, monday_account=None,
              verify_columns: [Column] = None, alert_to: [] = None, clear_cache=False) -> Board:

        """ gets a monday board with the board_id,
            if copy is set to true, return a copy of the board.  This is useful if you need to have multiple operations
            performed on the same board, for example on the same set of rows.
            When a copy is made, the copy consists of the base monday class with no rows returned.
            If the cache timeout has not expired for this board, it will be obtained from the cache.
            if the request is new or the cache timeout has expired, then we will get it from Monday.com
            note: timestamps are the number of seconds from 1970-01-01
        """
        # safety check to ensure that the board id is always the correct format
        if isinstance(board_id, str):
            board_id = Conversion.to_int(board_id)

        if monday_account is None:
            monday_account = 'automation@com'

        if clear_cache:
            self.clear_cache()

        cached_board = self.get_cache_item(board_id)
        if cached_board is None:
            # get the token
            if monday_token is None:
                monday_token = self.get_monday_token(board_id, monday_account)

            # we are here because either the cache has expired or the board was not in the cache
            new_board: object = None
            try_again = 0
            while try_again < 5:
                try:
                    new_board: Board = Board(board_id,
                                             fields=fields,
                                             monday_token=monday_token,
                                             monday_timeout_seconds=monday_timeout_seconds,
                                             monday_account=monday_account,
                                             verify_columns=verify_columns,
                                             alert_to=alert_to)
                    break
                except Exception as ex:
                    try_again += 1
                    sleep(20)
                    logging.warning(
                        f"Sleeping for 20 seconds and will retrying Monday Board [{try_again}] error -> {ex}")

            assert new_board is not None, f"Giving up on the board {board_id}"

            logging.debug(f"** New ** Connection to board [{board_id}]: {new_board}")

            # update the cache
            cache_key = CachedResource.cache_key(board_id)
            self.update_cache_item(obj=new_board, key=cache_key, expire_seconds=self.expire_seconds)
            cached_board = new_board

        if make_copy:
            the_board = copy.deepcopy(cached_board)
        else:
            the_board = cached_board

        return the_board

    def get_monday_token(self, board_id, monday_account):
        cache_key = CachedResource.cache_key(monday_account, board_id)
        m_token = self.get_cache_item(cache_key)
        if m_token is None:
            m_token = MONDAY_TOKEN
            self.update_cache_item(obj=m_token, key=cache_key, expire_seconds=self.expire_seconds)
        return m_token

