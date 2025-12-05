from datetime import date, datetime
import logging
import os
from typing import Union
from zoneinfo import ZoneInfo

from gbq_connector import BigQueryClient
import numpy as np
import pandas as pd
from pygsheets import Spreadsheet, Worksheet

logger = logging.getLogger(__name__)

# Tech Tracker Cell References
TECH_TRACKER_BASE_ROW = 4  # -1 to include
TECH_TRACKER_BASE_COL = 2
TECH_TRACKER_COL_WIDTH = 14
TECH_TIMESTAMP_CELL_REF = "A1"


# Rename fields from dbt report to match tracker headers
REPORT_COLUMN_RENAME_MAP = {
    "account_id": "account_id",
    "local_staff_id": "Employee ID",
    "staff_last_first_name": "Staff Name",
    "staff_email": "Work Email",
    "personal_email": "Personal Email",
    "work_phone": "Work Phone",
    "staff_status": "Status",
    "termination_date": "Termination Date",
    "position_name": "Position",
    "work_location_description": "Work Location",
    "pay_location_description": "Pay Location",
    "supervisor_last_first_name": "Manager Name",
    "supervisor_email": "Manager Email",
    "last_updated": "Last Updated",
}


def _create_tracker_updated_timestamp(tracker_worksheet: Worksheet) -> None:
    timestamp = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    d_stamp = timestamp.strftime("%x")
    t_stamp = timestamp.strftime("%-I:%M %p")
    tracker_worksheet.update_value(TECH_TIMESTAMP_CELL_REF, f"LAST UPDATED: {d_stamp} @ {t_stamp}")


def _filter_out_cleared_on_boarders(cleared_ids_df: pd.DataFrame, tech_tracker_df: pd.DataFrame) -> pd.DataFrame:
    result = pd.merge(
        tech_tracker_df,
        cleared_ids_df,
        indicator=True,
        how="outer",
        on=["account_id"]).query("_merge=='left_only'")
    return result.drop(["_merge"], axis=1)


def _get_and_prep_datasource(bq_conn) -> pd.DataFrame:
    dataset = os.getenv("GBQ_DATASET")
    refreshed_df = bq_conn.get_table_as_df("rpt_staff__tech_offboarding_tracker_datasource", dataset=dataset)
    refreshed_df = refreshed_df.rename(columns=REPORT_COLUMN_RENAME_MAP)
    return refreshed_df.drop_duplicates(subset=["account_id"])


def _get_and_prep_tracker_df(tracker_worksheet: Worksheet) -> pd.DataFrame:
    # Sort range first to eliminate possible blank rows
    tracker_worksheet.sort_range(
        start=(TECH_TRACKER_BASE_ROW, TECH_TRACKER_BASE_COL),
        end=(tracker_worksheet.rows, tracker_worksheet.cols),
        basecolumnindex=2
    )
    # -1 to include headers
    df = tracker_worksheet.get_as_df(
        has_header=True,
        start=(TECH_TRACKER_BASE_ROW - 1, TECH_TRACKER_BASE_COL),
        end=(tracker_worksheet.rows, TECH_TRACKER_COL_WIDTH),
        include_tailing_empty=False
    )
    df.astype(str)
    return df


def _get_cleared_tech_ids(spreadsheet: Spreadsheet) -> pd.DataFrame:
    cleared_sheet = spreadsheet.worksheet_by_title(f"Offboarding - Cleared")
    return cleared_sheet.get_as_df(has_header=True, start="C3", end=(cleared_sheet.rows, 3),
                                   include_tailing_empty=False)


def _get_new_records(tracker_df: pd.DataFrame, jobvite_df: pd.DataFrame) -> pd.DataFrame:
    ids_df = tracker_df[["account_id"]].copy()
    result = pd.merge(
        jobvite_df,
        ids_df,
        indicator=True,
        how="outer",
        on=["account_id"]).query("_merge=='left_only'")
    return result.drop(["_merge"], axis=1)


def _insert_updated_data_to_google_sheets(updated_tracker_df: pd.DataFrame, tech_tracker_sheet: Worksheet) -> None:
    tech_tracker_sheet.set_dataframe(updated_tracker_df, (TECH_TRACKER_BASE_ROW, TECH_TRACKER_BASE_COL),
                                     copy_head=False)
    sheet_dim = (tech_tracker_sheet.rows, tech_tracker_sheet.cols)
    tech_tracker_sheet.sort_range((TECH_TRACKER_BASE_ROW, TECH_TRACKER_BASE_COL), sheet_dim, basecolumnindex=18,
                                  sortorder="DESCENDING")


def _update_dataframe(stale_df: pd.DataFrame, current_data_df: pd.DataFrame) -> pd.DataFrame:
    """Generalized func to update one dataframe with data from another"""
    df = stale_df.copy()
    try:
        df = df.set_index("account_id")
        current_data_df = current_data_df.set_index("account_id")
        df.update(current_data_df)
        df = df.reset_index()
    except ValueError as error:
        logger.exception(error)
        index_str = "\n".join(df["account_id"].to_list())
        raise Exception(f"Duplicates found when calling reset_index(). "
                        f"Here are the indexes from the tracker dataframe:\n{index_str}")
    return df


def refresh_offboarding_tracker(tech_tracker_spreadsheet: Spreadsheet) -> None:

    tracker_name = "Offboarding Tracker"
    bq_conn = BigQueryClient()
    refreshed_df = _get_and_prep_datasource(bq_conn)

    tech_tracker_sheet = tech_tracker_spreadsheet.worksheet_by_title(tracker_name)
    tracker_backup_df = _get_and_prep_tracker_df(tech_tracker_sheet)

    # Tech Tracker has ability to clear onboarders who have completed onboarding to an archive sheet
    # The below filters those onboarders out of the Jobvite dataset
    cleared_ids_df = _get_cleared_tech_ids(tech_tracker_spreadsheet)
    refreshed_df = _filter_out_cleared_on_boarders(cleared_ids_df, refreshed_df)
    logging.info(f"Found {len(refreshed_df)} records to add or update")

    updated_tracker_df = pd.DataFrame()

    if not tracker_backup_df.empty:
        updated_tracker_df = _update_dataframe(tracker_backup_df, refreshed_df)
        logging.info(f"Updating sheet {tracker_name} with fresh data")
    else:
        logging.info(f"Tech Tracker sheet {tracker_name} is empty")

    new_records = _get_new_records(tracker_backup_df, refreshed_df)
    if not new_records.empty:
        updated_tracker_df = pd.concat([updated_tracker_df, new_records])
        logging.info(f"Adding {len(new_records)} new records to sheet {tracker_name}")
    else:
        logging.info(f"No new records to add to tracker sheet {tracker_name}")

    if not updated_tracker_df.empty:
        _insert_updated_data_to_google_sheets(updated_tracker_df, tech_tracker_sheet)
        logger.info(f"Finished refreshing tracker sheet {tracker_name}")
    else:
        logger.info(f"No updates found. Nothing to refresh in sheet {tracker_name}")

    _create_tracker_updated_timestamp(tech_tracker_sheet)
