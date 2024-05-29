from collections import namedtuple
from time import sleep

from cache.c_singleton import Singleton
from conversion.c_conversion import Conversion
from monday._monday_token import MONDAY_TOKEN
from monday.c_monday import Monday
import logging

import copy

from std_utility.c_datetime import DateTime

MondayCache = namedtuple('MondayCache', 'board_id timestamp monday')


@Singleton
class MondayConnection(object):
    def __init__(self):
        self.boards = {}
        self.monday_token = None
        self._get_monday_token()
        self._cache_timeout = 60 * 5  # 5 minutes

    def expire_cache(self, board_id=None):
        if board_id is None:
            self.boards = {}
        else:
            self.boards.pop(board_id)

    def _get_monday_token(self):
        m_token = self.monday_token
        if m_token is None:
            m_token = MONDAY_TOKEN
            self.monday_token = m_token

    def get_monday_board(self, board_id, make_copy=False):
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

        # check to see if the board is in the cache.
        if self.boards and board_id in self.boards:
            # yes, then get the cache item using the board id
            cache_item: MondayCache = self.boards.get(board_id)

            # just a double check, should never be needed that the cache item exists
            if cache_item is not None:
                # create a timestamp for now
                now = DateTime(init='now').as_short_timestamp

                # if the cache item timestamp is < the timeout then use the cache copy.
                in_use_seconds = now - cache_item.timestamp
                if in_use_seconds < self._cache_timeout:
                    # if the copy flag is set to True, then make a copy
                    if make_copy:
                        the_board = copy.deepcopy(cache_item.monday)
                    else:
                        the_board = cache_item.monday

                    # return the cached version of the board
                    logging.debug(f"Cached Connection to board [{board_id}]: {the_board}")
                    return the_board

        # we are here because either the cache has expired or the board was not in the cache

        new_board = None
        try_again = 0
        while try_again < 5:
            try:
                new_board: Monday = Monday(board_id, use_select=True, monday_token=self.monday_token, load_subitems=True)
                break
            except Exception as ex:
                try_again += 1
                sleep(20)
                logging.warning(f"Sleeping for 20 seconds and will retrying Monday Board [{try_again}]")

        if new_board is None:
            logging.warning(f"Giving up on the board {board_id}")
            return None

        # new_board = Monday(board_id, use_select=True, monday_token=self.monday_token, load_subitems=True)
        new_timestamp = DateTime(init='now').as_short_timestamp

        # create the new cache item with the board id, timestamp and monday board
        cache_item = MondayCache(board_id=board_id, timestamp=new_timestamp, monday=new_board)

        # add the cache item to the singleton cache list
        self.boards[board_id] = cache_item

        # if the copy flag is set to True, then make a copy
        if make_copy:
            the_board = copy.deepcopy(new_board)
        else:
            the_board = new_board
        logging.info(f"** New ** Connection to board [{board_id}]: {the_board}")
        return the_board

