import os
import traceback

from pygsheets import authorize, Spreadsheet

from jobs.sla_monitor import refresh_sla_source
from jobs.refresh_tracker import tracker_refresh
from utils.arg_parser import create_parser
from utils.logger_config import get_logger
from utils.mailer import Mailer

TECH_TRACKER_SHEET = os.getenv("TECH_TRACKER_SHEETS_ID")
HR_MOT_SHEET = os.getenv("HR_MOT_SHEETS_ID")
GOOGLE_CREDENTIALS = os.getenv("CREDENTIALS_FILE")

# Create Parser
ARGS = create_parser().parse_args()


logger = get_logger()


def create_sheet_connection(sheet_key: str) -> Spreadsheet:
    client = authorize(service_file=GOOGLE_CREDENTIALS)
    return client.open_by_key(sheet_key)


def main(mailer):
    tech_spreadsheet = create_sheet_connection(TECH_TRACKER_SHEET)
    if ARGS.sla_monitor_refresh:
        mailer.job_name_extend('SLA Monitor Refresh')
        refresh_sla_source(tech_spreadsheet)
    else:
        school_year = ARGS.school_year[0]
        mailer.job_name_extend(school_year)
        hr_mot_spreadsheet = create_sheet_connection(HR_MOT_SHEET)
        tracker_refresh(tech_spreadsheet, hr_mot_spreadsheet, school_year)


if __name__ == "__main__":
    mailer = Mailer(f"Tech On-boarding Tracker")
    try:
        main(mailer)
        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        mailer.notify(success=False)