from smtplib import SMTP_SSL
from os import getenv
from email.mime.text import MIMEText


class Mailer:
    def __init__(self, job_name):
        self._job_name = job_name
        self.user = getenv("GMAIL_USER")
        self.password = getenv("GMAIL_PWD")
        self.server = SMTP_SSL("smtp.gmail.com", 465)
        self.from_address = "KIPP NorCal Job Notification"
        self.to_address = getenv("NOTIF_TO_ADDRESS")

    def _subject_line(self):
        subject_type = "Success" if self.success else "Error"
        return f"{self._job_name} - {subject_type}"

    def _body_text(self):
        if self.success:
            return f"{self._job_name} was successful:\n{self.logs}"
        else:
            return f"{self._job_name} encountered an error:\n{self.logs}"

    def _message(self):
        message = MIMEText(self._body_text())
        message["Subject"] = self._subject_line()
        message["From"] = self.from_address
        message["To"] = self.to_address
        return message.as_string()

    @staticmethod
    def _read_logs(filename):
        with open(filename) as f:
            return f.read()

    def notify(self, success=True):
        self.success = success
        self.logs = self._read_logs("app.log")
        with self.server as s:
            s.login(self.user, self.password)
            message = self._message()
            recipients = [self.to_address]
            s.sendmail(self.user, recipients, message)
