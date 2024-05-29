from sendmail.c_attacchment import Attachment
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

"""
used to create a email message and add attachments if desired, supports both plain and html messages
"""


class Message:
    def __init__(self, msg_to: [str] = None, msg_cc: [str] = None, msg_bcc: [str] = None, msg_from: str = None,
                 msg_display_name: str = None, msg_subject: str = None, msg_body: str = None,
                 msg_attachments: [Attachment] = None, is_html: bool = False):

        if msg_to is None or msg_from is None or msg_subject is None or msg_body is None:
            raise Exception('Class is missing required inputs')

        self.msg_to = self.get_comma_str(msg_to)
        self.msg_from = self.get_comma_str(msg_from)
        self.msg_cc = Message.get_comma_str(msg_cc)
        self.msg_bcc = Message.get_comma_str(msg_bcc)
        self.msg_display_name = msg_display_name
        self.msg_subject = msg_subject
        self.msg_body = msg_body
        self.msg_attachments = msg_attachments

        self.message = MIMEMultipart()
        self.message["To"] = self.msg_to
        if self.msg_display_name is None:
            self.msg_display_name = self.msg_from
        self.message["From"] = f"{self.msg_display_name} <{self.msg_from}>"
        self.message["CC"] = self.msg_cc
        self.message["BCC"] = self.msg_bcc
        self.message["Subject"] = msg_subject

        if is_html:
            html_mime = MIMEText(self.msg_body, "html")
            self.message.attach(html_mime)
        else:
            text_mime = MIMEText(self.msg_body, "plain")
            self.message.attach(text_mime)

        # now add attachments if any
        if self.msg_attachments is not None:
            for attachment in self.msg_attachments:
                self.add_attachment(self.message, attachment)

    def as_str(self):
        return str(self.message)

    @staticmethod
    def get_comma_str(_msg: object = None) -> str:
        if _msg is None:
            _msg = ['']
        if isinstance(_msg, str):
            return _msg

        if isinstance(_msg, list):
            return ','.join(_msg)

        return str(_msg)

    @staticmethod
    def add_attachment(message: MIMEMultipart, item: Attachment):
        attachment = MIMEApplication(item.container)
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(item.filename)
        message.attach(attachment)
