"""
python library to send emails over office365 smtp

the message sent is a MIMEMultipart object from the sendmail library
example message:

    from sendmail.mime.multipart import MIMEMultipart
    from sendmail.mime.text import MIMEText

    RECIPIENTS = ['email1@sendmail.com', 'email2@sendmail.com']

    message = MIMEMultipart()
    message["Subject"] = 'sendmail sent from sendmail.py'
    message["To"] = ",".join(RECIPIENTS)

    html_content = dataframe.to_html()  # any html for website, example is using pandas to_html func
    message.attach(html_content, 'html')

    sendmail(smtp_user, smtp_address, smtp_password, RECIPIENTS, message
"""
import datetime
import logging
import re
from smtplib import SMTP, SMTPException, SMTPAuthenticationError
from typing import List

import pandas as pd
from pretty_html_table import pretty_html_table

from credientals.send_mail import SMTP_USERNAME, SMTP_PASSWORD, SMTP_HOST, SMTP_PORT
from sendmail.c_attacchment import Attachment
from sendmail.c_message import Message
from status.c_status import Status


class Mail:

    def __init__(self,
                 smtp_login_name: str = None,
                 smtp_password: str = None,
                 msg_from: str = None,
                 msg_display_name: str = None,
                 msg_to: [str] = None,
                 msg_cc: [str] = None,
                 msg_bcc: [str] = None,
                 msg_subject: str = None,
                 msg_body: str = None,
                 msg_attachments: [Attachment] = None,
                 is_html: bool = True):

        self.smtp_login_name = smtp_login_name
        self.smtp_password = smtp_password
        self.msg_from = Message.get_comma_str(msg_from)
        self.msg_to = Message.get_comma_str(msg_to)
        self.msg_cc = Message.get_comma_str(msg_cc)
        self.msg_bcc = Message.get_comma_str(msg_bcc)
        self.msg_subject = msg_subject
        self.msg_body = msg_body
        self.status = Status(0)
        self.msg_display_name = msg_display_name
        self.msg_attachments = msg_attachments
        self.is_html = is_html

        if self.msg_subject is None:
            self.msg_subject = str(datetime.datetime.now())

        if msg_from is None:
            self.msg_from = smtp_login_name

        if smtp_login_name is None:
            self.smtp_login_name = self.msg_from

        if self.smtp_password is None:
            self.smtp_login_name = SMTP_USERNAME
            self.smtp_password = SMTP_PASSWORD
            if self.smtp_login_name is None:
                status = Status(-1, "Unable to get email username/password from database")

    def send(self, msg_to: List[str] = None, msg_from: str = None, msg_display_name: str = None, msg_cc: [str] = None,
             msg_bcc: [str] = None, msg_subject: str = None, msg_body=None,
             msg_attachments: [Attachment] = None, is_html: bool = None):

        if msg_display_name is not None:
            self.msg_display_name = msg_display_name

        if msg_from is not None:
            self.msg_from = msg_from

        if msg_cc is not None:
            self.msg_cc = msg_cc

        if msg_bcc is not None:
            self.msg_bcc = msg_bcc

        if msg_body is not None:
            self.msg_body = msg_body

        if is_html is not None:
            self.is_html = is_html

        if msg_attachments is not None:
            self.msg_attachments = msg_attachments

        if msg_to is not None:
            self.msg_to = msg_to

        if msg_subject is not None:
            self.msg_subject = msg_subject

        if self.msg_body is not None:
            self.msg_body = Message(
                msg_to=self.msg_to,
                msg_cc=self.msg_cc,
                msg_bcc=self.msg_bcc,
                msg_from=self.msg_from,
                msg_display_name=self.msg_display_name,
                msg_subject=self.msg_subject,
                msg_body=self.msg_body,
                msg_attachments=self.msg_attachments,
                is_html=self.is_html)

        status = Status(0)
        try:
            if self.smtp_password is None:
                self.smtp_login_name = SMTP_USERNAME
                self.smtp_password = SMTP_PASSWORD

            server = SMTP(SMTP_HOST, SMTP_PORT)
            server.ehlo()
            server.starttls()
            server.login(self.smtp_login_name, self.smtp_password)

            response = server.sendmail(from_addr=self.msg_from, to_addrs=self.msg_to, msg=self.msg_body.as_str())
            server.quit()
            logging.debug(f"Success: sendmail sent {self.msg_to} from {self.msg_from} -> {response}")
        except SMTPAuthenticationError:
            status = Status(-1, "SMTP Authentication Error: The username and/or password you entered is incorrect")
            logging.error(status.message)
        except SMTPException as e:
            status = Status(-1, e)
            logging.error(e)
        except Exception as e:
            status = Status(-1, e)

        return status

    @staticmethod
    def gen_msg_body(table_1st_line=None, intro_message=None, body_message=None):

        html = intro_message

        # generate html and attach to email
        df = pd.DataFrame(body_message, columns=table_1st_line)
        df.index += 1
        html = html + pretty_html_table.build_table(df, 'grey_light', font_size='small'
                                                    , font_family='Courier New', width_dict=['200px', '900px'],
                                                    index=True)
        html = re.sub(r'width: auto">(.+)</th>', r'width: 50px">\1</th>', html)
        html = re.sub(r';width: auto">(.+)</td>', r';width: 900px">\1</td> ', html)

        return html


def unit_test():
    smtp_user = SMTP_USERNAME
    smtp_pw = SMTP_PASSWORD

    attachment1 = Attachment("stuff", 'stuff.csv')
    attachment2 = Attachment("stuff2", 'stuff2.csv')
    """
    create an email object and send seperate emails with different subjects and messages
    """
    email = Mail(SMTP_HOST, msg_display_name="Tech Support", smtp_password=smtp_pw)
    if email.status.is_ok():
        status = email.send(
            msg_to=["ron.sandstrom@rlsand.com"],
            msg_subject="email test",
            msg_body='This is a test',
            msg_attachments=[attachment1, attachment2])

        if status.is_ok():
            logging.info(status.message)
        else:
            logging.error(status.message)

    csv = "'apple','test.csv'"
    """
    send an email on demand with subjects and messages
    """
    attachment1 = Attachment("stuff", 'stuff.csv')
    attachment2 = Attachment("stuff2", 'stuff2.csv')

    email2 = Mail(smtp_login_name=smtp_user,
                  smtp_password=smtp_pw,
                  msg_from='noreply@threestep.com',
                  msg_display_name='Core Tech',
                  msg_to=["ron.sandstrom@threestep.com"],
                  msg_subject="test 2 " + str(datetime.datetime.now()),
                  msg_body="test again",
                  msg_attachments=[attachment1, attachment2], is_html=True)

    if email2.status.is_ok():
        status = email2.send()
        if status.is_ok():
            logging.info(status.message)
        else:
            logging.error(status.message)


if __name__ == '__main__':
    unit_test()
