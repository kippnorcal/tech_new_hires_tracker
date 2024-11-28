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
TECH_TRACKER_BASE_ROW = 5  # -1 to include
TECH_TRACKER_BASE_COL = 2
TECH_TRACKER_COL_WIDTH = 19
TECH_TIMESTAMP_CELL_REF = "A2"

# HR Tracker Cell References
HR_TRACKER_BASE_ROW = 5
HR_TRACKER_BASE_COL = 1
HR_TRACKER_COL_WIDTH = 55

# For filtering columns from HR Tracker
COLUMN_MAPPINGS = {
    4: "job_candidate_id",
    47: "Cleared?",
    48: "Cleared Email Sent"
}

# Rename fields from dbt report to match tracker headers
REPORT_COLUMN_RENAME_MAP = {
        "job_candidate_id": "job_candidate_id", 
        "first_name": "First Name",
        "last_name": "Last Name",
        "hire_reason": "New, Returners, Rehire or Transfer",
        "email": "Personal Email",
        "assigned_work_location": "Work Location",
        "assigned_pay_location": "Pay Location",
        "start_date": "Start Date",
        "title": "Title",
        "are_you_a_former_or_current_kipp_employee": "Former or Current KIPP", 
        "sped": "SpEd", 
    }


def _add_cleared_column_info(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """Adds cleared columns to new records"""
    tracker_df["Cleared?"] = ""
    tracker_df["Cleared Email Sent"] = ""
    return tracker_df


def _append_new_records_to_tracker(updated_tracker_df: pd.DataFrame, new_records: pd.DataFrame) -> pd.DataFrame:
    new_records = _add_cleared_column_info(new_records)
    _fill_in_rescinded_and_date_fields(new_records)
    return pd.concat([updated_tracker_df, new_records])


def _calculate_main_updated_date(df: pd.DataFrame) -> None:
    df["Main Last Updated"] = np.where((df["Start Date - Last Updated"] <= df["Pay Location - Last Updated"]),
                                       df["Pay Location - Last Updated"], df["Start Date - Last Updated"])


def _create_tracker_updated_timestamp(tracker_worksheet: Worksheet) -> None:
    timestamp = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    d_stamp = timestamp.strftime("%x")
    t_stamp = timestamp.strftime("%-I:%M %p")
    tracker_worksheet.update_value(TECH_TIMESTAMP_CELL_REF, f"LAST UPDATED: {d_stamp} @ {t_stamp}")


def _compare_date_tracked_columns(updated_tracker_df: pd.DataFrame, old_tracker_df: pd.DataFrame) -> pd.DataFrame:
    """Function that will date stamp changes to data in columns"""
    cols_to_compare = ["Start Date", "Pay Location"]
    for col in cols_to_compare:
        df_compared = _merge_for_comparison(updated_tracker_df, old_tracker_df, col)
        updated_tracker_df = _update_dataframe(updated_tracker_df, df_compared)
        updated_tracker_df.drop(
            columns=[
                "Start Date - Last Updated",
                "Pay Location - Last Updated",
                "Main Last Updated"
            ]
        )
    return updated_tracker_df


def _fill_in_rescinded_and_date_fields(df: pd.DataFrame) -> None:
    today = date.today()
    df["Rescinded"] = "--"
    df["Date Added"] = today
    df["Start Date - Last Updated"] = today
    df["Pay Location - Last Updated"] = today
    df["Main Last Updated"] = today


def _filter_candidates_for_school_year(jobvite_df: pd.DataFrame, school_year: str):
    year_2digit = school_year[-2:]
    year = int(f"20{year_2digit}")  # convert school year to 4 digit year
    start_of_year = datetime(year - 1, 6, 30)
    end_of_year = datetime(year, 7, 1)
    jobvite_df = jobvite_df[
        (jobvite_df["Start Date"] >= start_of_year) & (jobvite_df["Start Date"] < end_of_year)
        ]
    return jobvite_df


def _filter_out_cleared_on_boarders(cleared_ids_df: pd.DataFrame, tech_tracker_df: pd.DataFrame) -> pd.DataFrame:
    result = pd.merge(
        tech_tracker_df,
        cleared_ids_df,
        indicator=True,
        how="outer",
        on=["job_candidate_id"]).query("_merge=='left_only'")
    return result.drop(["_merge"], axis=1)


def _get_and_prep_jobvite_data(bq_conn, dataset, year)  -> pd.DataFrame:
    jobvite_df = _get_jobvite_data(bq_conn, dataset)
    jobvite_df = jobvite_df.rename(columns=REPORT_COLUMN_RENAME_MAP)
    jobvite_df = _filter_candidates_for_school_year(jobvite_df, year)
    return jobvite_df.drop_duplicates(subset=["job_candidate_id"])


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
        start=(TECH_TRACKER_BASE_ROW -1, TECH_TRACKER_BASE_COL),
        end=(tracker_worksheet.rows, TECH_TRACKER_COL_WIDTH),
        include_tailing_empty=False
        )
    df.astype(str)
    df["Start Date - Last Updated"] = pd.to_datetime(df["Start Date - Last Updated"], format="%Y-%m-%d").dt.date
    df["Pay Location - Last Updated"] = pd.to_datetime(df["Pay Location - Last Updated"], format="%Y-%m-%d").dt.date
    df["Start Date"] = pd.to_datetime(df["Start Date"], format="%m/%d/%Y")
    return df


def _get_cleared_tech_ids(spreadsheet: Spreadsheet, year: str) -> pd.DataFrame:
    cleared_sheet = spreadsheet.worksheet_by_title(f"{year} Cleared")
    return cleared_sheet.get_as_df(has_header=True, start="C4", end=(cleared_sheet.rows, 3),
                                   include_tailing_empty=False)


def _get_cleared_to_hire_data_from_hr_tracker(hr_worksheet: Worksheet) -> pd.DataFrame:
    hr_tracker_df = hr_worksheet.get_as_df(
        start=(HR_TRACKER_BASE_ROW, HR_TRACKER_BASE_COL),
        end=(hr_worksheet.rows, HR_TRACKER_COL_WIDTH),
        has_header=False,
        include_tailing_empty=True
        )
    hr_tracker_df = hr_tracker_df.rename(columns=COLUMN_MAPPINGS)

    #  Filtering unneeded columns
    column_filter = list(COLUMN_MAPPINGS.values())
    hr_tracker_df[column_filter].copy()
    hr_tracker_df = hr_tracker_df[column_filter].copy()

    #  Removing indexes where the job_candidate_id is blank
    empty_string_indexes = hr_tracker_df[hr_tracker_df["job_candidate_id"] == ""]
    hr_tracker_df = hr_tracker_df.drop(index=empty_string_indexes.index)

    hr_tracker_df.astype(str)
    hr_tracker_df["Cleared Email Sent"] = np.where(hr_tracker_df["Cleared Email Sent"] == "TRUE", "Yes", "No")
    return hr_tracker_df


def _get_jobvite_data(bq_conn: BigQueryClient, dataset: str) -> pd.DataFrame:
    df = bq_conn.get_table_as_df("rpt_staff__tech_onboarding_tracker_data_source", dataset=dataset)
    df["start_date"] = pd.to_datetime(df["start_date"])
    return df


def _get_new_records(tracker_df: pd.DataFrame, jobvite_df: pd.DataFrame) -> pd.DataFrame:
    ids_df = tracker_df[["job_candidate_id"]].copy()
    result = pd.merge(
        jobvite_df,
        ids_df,
        indicator=True,
        how="outer",
        on=["job_candidate_id"]).query("_merge=='left_only'")
    return result.drop(["_merge"], axis=1)


def _get_rescinded_offers(bq_conn: BigQueryClient, dataset: str) -> Union[list, None]:
    df = bq_conn.get_table_as_df("rpt_staff__tech_onboarding_tracker_rescinded_offers", dataset=dataset)
    if df is not None:
        return df["job_candidate_id"].to_list()
    else:
        return None


def _insert_updated_data_to_google_sheets(updated_tracker_df: pd.DataFrame, tech_tracker_sheet: Spreadsheet) -> None:
    tech_tracker_sheet.set_dataframe(updated_tracker_df, (TECH_TRACKER_BASE_ROW, TECH_TRACKER_BASE_COL), copy_head=False)
    sheet_dim = (tech_tracker_sheet.rows, tech_tracker_sheet.cols)
    tech_tracker_sheet.sort_range((TECH_TRACKER_BASE_ROW, TECH_TRACKER_BASE_COL), sheet_dim, basecolumnindex=18, sortorder="DESCENDING")


def _merge_for_comparison(updated_tracker_df: pd.DataFrame, old_tracker_df: pd.DataFrame, col: str) -> pd.DataFrame:
    results = pd.merge(
        updated_tracker_df,
        old_tracker_df,
        how='outer',
        on="job_candidate_id"
    )
    new_value = f"{col}_x"
    old_value = f"{col}_y"
    update_date_field = f"{col} - Last Updated_x"
    results = results[["job_candidate_id", new_value, old_value, update_date_field]]
    results.loc[results[new_value] != results[old_value], update_date_field] = date.today()
    return results.rename(columns={update_date_field: update_date_field[:-2]})


def _pull_cleared_field_from_hr_onboarding_tracker(updated_tracker_df: pd.DataFrame, hr_spreadsheet: Spreadsheet, year: str) -> pd.DataFrame:
    hr_sheet = hr_spreadsheet.worksheet_by_title(f"Main {year}")
    hr_cleared_df = _get_cleared_to_hire_data_from_hr_tracker(hr_sheet)
    return _update_dataframe(updated_tracker_df, hr_cleared_df)


def _rescind_records_from_tracker(updated_tracker_df: pd.DataFrame, rescinded_offer_ids: list) -> pd.DataFrame:
    logging.info("Identified rescinded offers")
    updated_tracker_df = _update_rescinded_col(rescinded_offer_ids, updated_tracker_df)
    for offer_id in rescinded_offer_ids:
        logging.info(f"Removing {offer_id}")
    return updated_tracker_df


def _update_dataframe(stale_df: pd.DataFrame, current_data_df: pd.DataFrame) -> pd.DataFrame:
    """Generalized func to update one dataframe with data from another"""
    try:
        df = stale_df.copy()
        df = df.set_index("job_candidate_id")
        current_data_df = current_data_df.set_index("job_candidate_id")
        df.update(current_data_df)
        df = df.reset_index()
    except ValueError as error:
        logger.exception(error)
        index_str = "\n".join(stale_df["job_candidate_id"].to_list())
        logger.info(f"Here are the indexes from the tracker dataframe:\n{index_str}")
    return df



def _update_rescinded_col(id_list: list, df: pd.DataFrame) -> pd.DataFrame:
    filtered_for_updates = df.loc[(df["job_candidate_id"].isin(id_list)) & (df["Rescinded"] == "--")]
    if not filtered_for_updates.empty:
        filtered_for_updates["Rescinded"] = f"Yes - {date.today()}"
        filtered_for_updates = filtered_for_updates.set_index("job_candidate_id")
        df = df.set_index("job_candidate_id")
        df.update(filtered_for_updates)
        df = df.reset_index()
    return df


def _update_tracker_data(tracker_backup_df: pd.DataFrame, jobvite_df: pd.DataFrame) -> pd.DataFrame:
    updated_tracker_df = _update_dataframe(tracker_backup_df, jobvite_df)
    updated_tracker_df = _compare_date_tracked_columns(updated_tracker_df, tracker_backup_df)
    _calculate_main_updated_date(updated_tracker_df)
    return updated_tracker_df


def tracker_refresh(tech_tracker_spreadsheet: Spreadsheet, hr_spreadsheet: Spreadsheet, year: str) -> None:
    dataset = os.getenv("GBQ_DATASET")
    bq_conn = BigQueryClient()
    jobvite_df = _get_and_prep_jobvite_data(bq_conn, dataset, year)

    tracker_name = f"{year} Tracker"
    tech_tracker_sheet = tech_tracker_spreadsheet.worksheet_by_title(tracker_name)
    tracker_backup_df = _get_and_prep_tracker_df(tech_tracker_sheet)

    # Tech Tracker has ability to clear onboarders who have completed onboarding to an archive sheet
    # The below filters those onboarders out of the Jobvite dataset
    cleared_ids_df = _get_cleared_tech_ids(tech_tracker_spreadsheet, year)
    jobvite_df = _filter_out_cleared_on_boarders(cleared_ids_df, jobvite_df)
    logging.info(f"Found {len(jobvite_df)} records to add or update")

    updated_tracker_df = pd.DataFrame()

    if not tracker_backup_df.empty:
        updated_tracker_df = _update_tracker_data(tracker_backup_df, jobvite_df)
        logging.info(f"Updating sheet {tracker_name} with data from Jobvite")
    else:
        logging.info(f"Tech Tracker sheet {tracker_name} is empty")

    new_records = _get_new_records(tracker_backup_df, jobvite_df)
    if not new_records.empty:
        updated_tracker_df = _append_new_records_to_tracker(updated_tracker_df, new_records)
        logging.info(f"Adding {len(new_records)} new records to sheet {tracker_name}")
    else:
        logging.info(f"No new records to add to tracker sheet {tracker_name}")

    if not updated_tracker_df.empty:
        rescinded_offer_ids = _get_rescinded_offers(bq_conn, dataset)
        if rescinded_offer_ids is not None:
            _rescind_records_from_tracker(updated_tracker_df, rescinded_offer_ids)
        
        updated_tracker_df = _pull_cleared_field_from_hr_onboarding_tracker(updated_tracker_df, hr_spreadsheet, year)
        
        # Converting Start Date field to string for insertion
        updated_tracker_df["Start Date"] = updated_tracker_df["Start Date"].dt.strftime("%m/%d/%Y")
        
        _insert_updated_data_to_google_sheets(updated_tracker_df, tech_tracker_sheet)
        logger.info(f"Finished refreshing tracker sheet {tracker_name}")
    else:
        logger.info(f"No updates found. Nothing to refresh in sheet {tracker_name}")

    _create_tracker_updated_timestamp(tech_tracker_sheet)
