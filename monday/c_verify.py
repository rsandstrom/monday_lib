"""
Verify Class is used to lock down monday boards and report when critical fields are altered.
you can choose to send the email in this function by adding a list of email addresses in alert_to
or you can simply take the return code and process the message later.
"""
import os

from monday.c_column import Column
from monday.c_required import RequiredElements
from result.c_result import Result
from sendmail.c_support_email import SupportEmail
import logging


class VerifyBoard:
    def __init__(self, board):
        self.board = board

    @staticmethod
    def add_items(the_list, keyword):
        msg = ''
        msg += f"<hr>Expected {keyword} are Missing or Modified:"
        msg += f"<ul>"
        for m in the_list:
            msg += f"<li>{m}</li>"
        msg += f"</ul>"
        return msg

    def verify_required(self, required: RequiredElements = None, alert_to: [] = None) -> Result:
        missing_groups = []
        missing_columns = []
        missing_sub_columns = []
        missing_labels = []
        missing_sub_labels = []
        missing_groups_flag = False
        missing_sub_columns_flag = False
        missing_columns_flag = False
        missing_labels_flag = False
        missing_sub_labels_flag = False

        for group in required.groups:
            if group not in self.board.group_map:
                missing_groups.append(group)
                missing_groups_flag = True

        for check_column in required.columns:
            col: Column = check_column
            if col.name not in self.board.column_info_map:
                missing_columns.append(check_column.name)
                missing_columns_flag = True

            if col.has_labels:
                for lbl in check_column.labels:
                    if lbl not in self.board.column_info_map.get(col.name).labels:
                        missing_labels.append(lbl)
                        missing_labels_flag = True

        if hasattr(required, 'sub_columns'):
            for check_column in required.sub_columns:
                col: Column = check_column
                if col.name not in self.board.column_info_sub_map:
                    missing_sub_columns.append(check_column.name)
                    missing_sub_columns_flag = True

                if col.has_labels:
                    for lbl in check_column.labels:
                        if lbl not in self.board.column_info_sub_map.get(col.name).labels:
                            missing_sub_labels.append(lbl)
                            missing_sub_labels_flag = True

        message_body = '<h1>' \
                       '<div style="background-color:red;color:white;padding:2%;">ALERT Altered Monday Board</div>' \
                       '</h1><hr>' \
                       f'Monday board [{self.board.name}] ' \
                       f'url: https://3step-sports.monday.com/boards/{self.board.board_id}'

        if missing_groups_flag:
            message_body += self.add_items(missing_groups, 'Groups')

        if missing_columns_flag:
            message_body += self.add_items(missing_columns, 'Columns')

        if missing_labels_flag:
            message_body += self.add_items(missing_labels, 'Labels')

        if missing_sub_columns_flag:
            message_body += self.add_items(missing_sub_columns, 'Sub Columns')

        if missing_sub_labels_flag:
            message_body += self.add_items(missing_sub_labels, 'Sub Labels')

        if missing_groups_flag or missing_columns_flag or missing_labels_flag \
                or missing_sub_columns_flag or missing_sub_labels_flag:
            self.board.was_altered = True
            self.board.missing_labels = missing_labels
            self.board.missing_columns = missing_columns
            if alert_to is not None:
                SupportEmail(email_to=alert_to,
                             email_subject=f'Alert Missing Columns or Labels on board [{self.board.name}]',
                             email_body=message_body).send()
            logging.warning(f"Alert Missing Columns or Labels on board [{self.board.name}]")
            result = Result(-1, message='Board Alteration', data=message_body)
        else:
            result = Result(0)

        return result

    def create_required_elements_file(self, filename='required'):

        file_contents = "from monday.c_column import Column\n\n"
        file_contents += "\nclass RequiredElements:"
        file_contents += "\n    def __init__(self):"

        file_contents += "\n        self.groups = [\n"
        for group_name in self.board.group_map:
            file_contents += f"            '{group_name}',\n"
        file_contents += f"        ]\n"

        file_contents += "\n        self.columns = [\n"
        for col in self.board.column_info_map.values():
            file_contents += f"            Column(c_name='{col.name}', c_labels={col.labels}),\n"
        file_contents += f"        ]\n"

        if self.board.has_subitems:
            file_contents += "\n        self.sub_columns = [\n"
            for col in self.board.column_info_sub_map.values():
                file_contents += f"            Column(c_name='{col.name}', c_labels={col.labels}),\n"
            file_contents += f"        ]\n"

            target_file = f'./{filename}.py'
            with open(target_file, 'w') as f:
                f.write(file_contents)

            if os.path.exists(target_file):
                logging.info(f"Created {target_file}")
            else:
                logging.warning(f"Unable to create required columns file")
