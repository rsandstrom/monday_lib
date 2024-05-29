"""
Class to send support email from our system.
emails are hard coded to come from no-reply@threestep.com
"""
from credientals.send_mail import SMTP_PASSWORD
from sendmail.c_mail import Mail
import logging


class SupportEmail:
    def __init__(self, email_to=None, email_subject=None, email_body=None, is_html: bool=True):
        self.email_to = email_to
        self.email_subject = email_subject
        self.email_body = email_body
        self.result = None
        self.smtp_pw = None
        self.is_html = is_html

        self.smtp_pw = SMTP_PASSWORD

    def send(self, email_to: [] = None, email_subject: str = None, email_body: str = None, attachments: [] = None, is_html: bool=True):
        """
        create an email object and send seperate emails with different subjects and messages
        """
        if email_subject is not None:
            self.email_subject = email_subject

        if email_to is not None:
            self.email_to = email_to

        if email_body is not None:
            self.email_body = email_body

        email = Mail('noreply@threestep.com', msg_display_name="Tech Support", smtp_password=self.smtp_pw, is_html=is_html)
        if email.status.is_ok():
            status = email.send(
                msg_to = self.email_to,
                msg_subject = self.email_subject,
                msg_body = self.email_body,
                msg_attachments=attachments)

            if status.is_ok():
                logging.info(status.message)
            else:
                logging.error(status.message)

        return email.status


