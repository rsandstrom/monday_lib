from monday.c_events import MondayEvent
from monday.c_monday_factory import MondayFactory
from monday.c_row import Row
from result.c_result import Result
import logging


class WebHook:
    def __init__(self):
        self.the_row = None

    @staticmethod
    def is_main_item(data):
        parent_id = None
        try:
            parent_id = data.parent_item_board_id
        except AttributeError:
            pass

        try:
            if data.board_id is not None and (data.board_id == parent_id or parent_id is None):
                return True
        except AttributeError:
            pass

        return False

    def get_focused_row(self, event):
        monday: MondayFactory = MondayFactory()

        data = event.data

        if WebHook.is_main_item(data):
            board = monday.board(data.board_id, make_copy=True)
            board.rows = board.load_row(data.row_id)
            if board.row_count > 0:
                self.the_row: Row = board.rows[0]
        else:
            board = monday.board(data.parent_item_board_id, make_copy=True)
            the_parent: [Row] = board.load_row(data.parent_item_id)
            if len(the_parent) > 0:
                sub_row = the_parent[0].load_sub_row(data.row_id, data.board_id)
                if sub_row.is_ok():
                    self.the_row = sub_row.data

        return self.the_row

    @staticmethod
    def process_request(call_back, args):
        if args is None or args is None:
            return Result(-1, 'Process request args are missing')

        data = args.get('event')
        if data is not None:
            event = MondayEvent(data)

            try:
                the_row = WebHook.get_focused_row(event)
                if the_row is not None:
                    p_result = call_back(the_row)
                    return p_result

            except Exception as ex:
                log_traceback(ex)
                logging.error(ex)

        return Result(-1, 'No Data to Process')
