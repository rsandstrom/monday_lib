import unittest

from threestep.monday.c_board import Board
from threestep.monday.c_monday_factory import MondayFactory
from threestep.sendmail.c_attacchment import Attachment
from threestep.sendmail.c_support_email import SupportEmail
from threestep.status.c_status import Status
from threestep.monday.c_monday import Monday
from threestep.std_errors.c_ecode import Ecode
from threestep.std_logging.logs import init_logs, logger
from threestep.std_utility.c_file_container import FileContainer
from datetime import datetime


class TestMondayDownloadFiles(unittest.TestCase):
    monday: MondayFactory = MondayFactory()

    def test_monday_download_files(self):
        pid = 2154004550
        monday_board = self.monday.board(board_id=pid, clear_cache=True)

        result = monday_board.select(groups='RTG', col_name='Name', col_values=['Item 1'])
        self.assertTrue(result.is_ok())  # might as well test this while we are here

        # copy files to array of file object you can use for other purposes
        body = ['testing attachments']
        attachments = []
        for row in monday_board.rows:
            logger.debug(row)
            for cell in row.cells:
                if cell.type == 'file':
                    logger.info(f"Downloading files {cell.value}")
                    result = cell.download_files(into_files_array=True)
                    self.assertTrue(result.is_ok)
                    if result.status.code == Ecode.Monday.no_files_to_download.code:
                        continue

                    files: [FileContainer] = result.data

                    for file in files:
                        buff = file.getbuffer()
                        attach = Attachment(buff, file.name)
                        attachments.append(attach)
                        logger.debug(f"file buffer {file.name}")

        self.assertTrue(len(attachments) > 0)

if __name__ == '__main__':
    init_logs(console=True, the_version='2.0.0', log_to_file=False, log_level='debug')
    unittest.main()
