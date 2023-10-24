import os
import traceback

from job_notifications import create_notifications
from pygsheets import authorize, Spreadsheet

from jobs.sla_monitor import refresh_sla_source
from jobs.refresh_tracker import tracker_refresh
from utils.arg_parser import create_parser
from utils.logger_config import get_logger

TECH_TRACKER_SHEET = os.getenv("TECH_TRACKER_SHEETS_ID")
HR_MOT_SHEET = os.getenv("HR_MOT_SHEETS_ID")
GOOGLE_CREDENTIALS = os.getenv("CREDENTIALS_FILE")

# Create Parser
ARGS = create_parser().parse_args()


logger = get_logger()


def create_sheet_connection(sheet_key: str) -> Spreadsheet:
    client = authorize(service_file=GOOGLE_CREDENTIALS)
    return client.open_by_key(sheet_key)


def main():
    tech_spreadsheet = create_sheet_connection(TECH_TRACKER_SHEET)
    if ARGS.sla_monitor_refresh:
        refresh_sla_source(tech_spreadsheet)
    else:
        school_year = ARGS.school_year[0]
        hr_mot_spreadsheet = create_sheet_connection(HR_MOT_SHEET)
        tracker_refresh(tech_spreadsheet, hr_mot_spreadsheet, school_year)


if __name__ == "__main__":
    if ARGS.sla_monitor_refresh:
        notifications = create_notifications("Tech On-boarding Tracker - SLA Monitor Refresh",
                                             "mailgun", logs="app.log")
    else:
        notifications = create_notifications(f"Tech On-boarding Tracker - {ARGS.school_year[0]}",
                                             "mailgun", logs="app.log")
    try:
        notifications.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        notifications.notify(error_message=stack_trace)
