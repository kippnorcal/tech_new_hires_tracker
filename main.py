import os
import traceback
from time import sleep

from gbq_connector import DbtClient
from job_notifications import create_notifications
from pygsheets import authorize, Spreadsheet

from jobs.sla_monitor import refresh_sla_source
from jobs.refresh_tracker import tracker_refresh
from utils.arg_parser import create_parser
from utils.logger_config import get_logger

TECH_TRACKER_SHEET = os.getenv("TECH_TRACKER_SHEETS_ID")
HR_TRACKER_SHEET = os.getenv("HR_TRACKER_SHEETS_ID")
GOOGLE_CREDENTIALS = os.getenv("CREDENTIALS_FILE")

# Create Parser
ARGS = create_parser().parse_args()


logger = get_logger()


def create_sheet_connection(sheet_key: str) -> Spreadsheet:
    client = authorize(service_file=GOOGLE_CREDENTIALS)
    return client.open_by_key(sheet_key)


def _refresh_dbt() -> None:
    dbt_conn = DbtClient()
    logger.info("Refreshing dbt; sleeping for 30 seconds")
    dbt_conn.run_job()
    sleep(30)


def main(notifications):
    tech_spreadsheet = create_sheet_connection(TECH_TRACKER_SHEET)
    hr_mot_spreadsheet = create_sheet_connection(HR_TRACKER_SHEET)
    if ARGS.sla_monitor_refresh:
        notifications.extend_job_name("- SLA Monitor Refresh")
        refresh_sla_source(tech_spreadsheet)
    else:
        if ARGS.dbt_refresh:
            _refresh_dbt()
        school_year = ARGS.school_year[0]
        notifications.extend_job_name(f"- {ARGS.school_year[0]}")
        tracker_refresh(tech_spreadsheet, hr_mot_spreadsheet, school_year)


if __name__ == "__main__":
    notifications = create_notifications("Tech On-boarding Tracker", "mailgun", logs="app.log")
    try:
        main(notifications)
        notifications.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        notifications.notify(error_message=stack_trace)
