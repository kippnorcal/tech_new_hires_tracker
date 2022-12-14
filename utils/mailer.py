from smtplib import SMTP_SSL
from os import getenv
from email.mime.text import MIMEText


class Mailer:
    def __init__(self, job_name):
        self.job_name = job_name
        self.user = getenv("GMAIL_USER")
        self.password = getenv("GMAIL_PWD")
        self.server = SMTP_SSL("smtp.gmail.com", 465)
        self.from_address = "KIPP NorCal Job Notification"
        self.to_address = getenv("NOTIF_TO_ADDRESS")

    def _subject_line(self, success):
        subject_type = "Success" if success else "Error"
        return f"{self.job_name} - {subject_type}"

    def _body_text(self):
        logs = self._read_logs("app.log")
        if self.success:
            return f"{self.job_name} was successful:\n{logs}"
        else:
            return f"{self.job_name} encountered an error:\n{logs}"

    def _message(self, success):
        message = MIMEText(self._body_text())
        message["Subject"] = self._subject_line(success)
        message["From"] = self.from_address
        message["To"] = self.to_address
        return message.as_string()

    @staticmethod
    def _read_logs(filename):
        with open(filename) as f:
            return f.read()

    def notify(self, success=True):
        with self.server as s:
            s.login(self.user, self.password)
            message = self._message(success)
            recipients = [self.to_address]
            s.sendmail(self.user, recipients, message)
