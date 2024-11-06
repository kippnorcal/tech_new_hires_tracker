from datetime import date, timedelta
import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
from pygsheets import Spreadsheet

logger = logging.getLogger(__name__)

COLUMN_RENAME_MAP = {
    "New, Returners, Rehire or Transfer": "NewHire_Type",
    "Cleared?": "HR_Cleared",
    "Date Added": "DateAdded",
    "Start Date": "StartDate",
    "Date Cleared": "DateCleared",
    "Start Date - Last Updated": "StartDate_LastUpdated",
    "Pay Location - Last Updated": "PayLocation_LastUpdated",
    "Main Last Updated": "Main_LastUpdated",
    "GLS Tracking #": "GLS_Tracking",
    "Assigned Technician": "AssignedTechnician",
    "Computer Type": "Computer_Type",
    "Computer Status": "Computer_Status",
    "Phone Type": "Phone_Type",
    "Phone Status": "Phone_Status",
    "Pay Location": "PayLocation",
    "Former or Current KIPP": "Former_KIPP",
    "Cleared Email Sent": "ClearedEmailSent",
    "Work Location": "WorkLocation",
    "Personal Email": "Personal_Email",
    "Completion Status": "Completion_Status"
}


def _datefields_to_dt_obj(df: pd.DataFrame) -> None:
    df['StartDate_LastUpdated'] = pd.to_datetime(df['StartDate_LastUpdated'], format="%Y-%m-%d")
    df['PayLocation_LastUpdated'] = pd.to_datetime(df['PayLocation_LastUpdated'], format="%Y-%m-%d")
    df['DateCleared'] = pd.to_datetime(df['DateCleared'], format="%m/%d/%Y")
    df['DateAdded'] = pd.to_datetime(df['DateAdded'], format="%Y-%m-%d")
    df['StartDate'] = pd.to_datetime(df['StartDate'], format="%m/%d/%Y")
    df['Main_LastUpdated'] = pd.to_datetime(df['Main_LastUpdated'], format="%Y-%m-%d")


def _compare_dates_new_col(df: pd.DataFrame, new_col: str, date_col1: str, date_col2: str) -> None:
    """Creates a new column with 1, 0 values by comparing two date columns"""
    df[new_col] = np.where(df[date_col1] < df[date_col2], 1, 0)


def _eval_sla_met(df: pd.DataFrame) -> None:
    """Evaluating if the Service Level Agreement has been met for each record.
    If SLA has been met, set value at 1
    If SLA has not been met, set value to 0
    If on-boarder has not yet started, set value to None, then replace with empty string"""

    df["TechCleared_MetSLA_Boolean"] = np.where(
        pd.isnull(df["DateCleared"]),
        np.where((date.today() + timedelta(days=1)) > df["StartDate"].dt.date, 0, None),
        np.where((df["DateCleared"] + timedelta(days=1)) <= df["StartDate"], 1, 0)
    )
    # Replacing all None values with empty strings as met SLA is still TBD
    df["TechCleared_MetSLA_Boolean"] = df["TechCleared_MetSLA_Boolean"].replace(np.nan, '')


def _eval_sla_denominator_field(df: pd.DataFrame) -> None:
    """Evaluating if record should be included in SLA denominator
    If DateCleared is not null, then set value to 1
    If DateCleared is null, then evaluate if SLA deadline has passed, If True, set value to 1"""

    df["Include_SLA_Denominator"] = np.where(
        pd.isnull(df["DateCleared"]),
        np.where((date.today() + timedelta(days=1)) > df["StartDate"].dt.date, 1, 0),
        1
    )


def _eval_tech_timeliness(df: pd.DataFrame) -> None:
    """If DateCleared, then calculate how on-time tech was provisioned"""
    df["TechCleared_Timeliness"] = np.where(
        pd.isnull(df["DateCleared"]),
        "",
        (df["DateCleared"] - df["StartDate"]).dt.days
    )


def _identify_tracker_cleared_sheets(spreadsheet: Spreadsheet) -> Tuple[List[pd.DataFrame], List[pd.DataFrame]]:
    sheet_list = spreadsheet.worksheets()
    cleared = []
    tracker = []

    for sheet in sheet_list:
        logger.info(f"Evaluating {sheet.title}")
        sheet_type = sheet.title.split(' ')[-1]
        if sheet_type in ["Cleared", "Tracker"]:
            logger.info(f"-- Sheet type is {sheet_type}")
            sheet_year = sheet.title.split(' ')[0]
            logger.info(f"-- Sheet year is {sheet_year}")
            sheet_df = sheet.get_as_df(has_header=True, start="B4", end=(sheet.rows, sheet.cols),
                                       include_tailing_empty=False)

            sheet_df["SchoolYear"] = f"20{sheet_year.split('-')[-1]}"
            if sheet_type == "Cleared":
                logger.info(f"-- Added to cleared list")
                cleared.append(sheet_df)
            elif sheet_type == "Tracker":
                logger.info(f"-- Added to tracker list")
                tracker.append(sheet_df)
        else:
            logger.info("Not a Tracker or a Cleared sheet")

    logger.info(f"Evaluated {len(cleared)} cleared and {len(tracker)} tracker "
                f"sheets")
    return cleared, tracker


def refresh_sla_source(spreadsheet: Spreadsheet) -> None:
    sla_sheet = spreadsheet.worksheet_by_title("SLA_data_source")
    cleared_dfs, tracker_dfs = _identify_tracker_cleared_sheets(spreadsheet)

    tracker_df = pd.concat(tracker_dfs)
    tracker_df['Date Cleared'] = None
    cleared_dfs.append(tracker_df)
    agg_df = pd.concat(cleared_dfs)
    logger.info("**Combined sheets into one data frame**")

    # filter out rescinded candidates
    agg_df = agg_df.drop(agg_df[agg_df.Rescinded != '--'].index)
    logger.info("Removed rescinded hires")

    # drop rescinded col
    agg_df = agg_df.drop("Rescinded", axis="columns")
    logger.info("Dropped rescinded column")

    # Rename Columns
    agg_df = agg_df.rename(columns=COLUMN_RENAME_MAP)
    logger.info("Renamed columns")

    # COMBINE FIRST AND LAST NAMES
    agg_df['Staff_Name'] = agg_df["First Name"].astype(str) + ' ' + agg_df["Last Name"].astype(str)
    agg_df = agg_df.drop(["First Name", "Last Name"], axis="columns")

    # HIRE MONTH
    _datefields_to_dt_obj(agg_df)
    agg_df["Hire_Month"] = agg_df['StartDate'].dt.strftime('%B')

    # StartDateChange_Boolean
    logger.info("Evaluating Start Date changes")
    _compare_dates_new_col(agg_df, "StartDateChange_Boolean", "DateAdded", "StartDate_LastUpdated")

    # LocationChange_Boolean
    logger.info("Evaluating Pay Location changes")
    _compare_dates_new_col(agg_df, "LocationChange_Boolean", "DateAdded", "PayLocation_LastUpdated")

    # TechCleared_MetSLA_Boolean
    logger.info("Evaluating met SLA bool field")
    _eval_sla_met(agg_df)

    # TechCleared_Timeliness
    logger.info("Evaluating Tech Cleared Timeliness")
    _eval_tech_timeliness(agg_df)

    # Include_SLA_Denominator
    logger.info("Creating SLA denominator")
    _eval_sla_denominator_field(agg_df)

    # Converting NaT values in DateCleared field to blank strings
    agg_df["DateCleared"] = agg_df["DateCleared"].dt.strftime('%Y-%m-%d')
    agg_df["DateCleared"] = agg_df["DateCleared"].replace(pd.NaT, '')

    # push to Google Sheets
    logger.info("Inserting into SLA_data_source")
    sla_sheet.clear()
    sla_sheet.set_dataframe(agg_df, "A1")
