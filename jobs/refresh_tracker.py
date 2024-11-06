from datetime import date, datetime
import logging
import os
from time import sleep
from zoneinfo import ZoneInfo

from gbq_connector import BigQueryClient, DbtClient
import numpy as np
import pandas as pd
from pygsheets import Spreadsheet, Worksheet

TECH_TRACKER_SHEET = os.getenv("TECH_TRACKER_SHEETS_ID")
HR_MOT_SHEET = os.getenv("HR_MOT_SHEETS_ID")
GOOGLE_CREDENTIALS = os.getenv("CREDENTIALS_FILE")

logger = logging.getLogger(__name__)

# The order of the columns below determines the order in the Tech Tracker
COLUMN_MAPPINGS = {
    3: "job_candidate_id",
    51: "Cleared?",
    52: "Cleared Email Sent"
}


def _get_cleared_mot_data(hr_mot_worksheet) -> pd.DataFrame:
    mot_df = hr_mot_worksheet.get_as_df(start=(3, 1), end=(hr_mot_worksheet.rows, hr_mot_worksheet.cols),
                                        has_header=False, include_tailing_empty=False)
    mot_df = mot_df.rename(columns=COLUMN_MAPPINGS)

    #  Filtering unneeded columns
    column_filter = list(COLUMN_MAPPINGS.values())
    mot_df[column_filter].copy()
    mot_df = mot_df[column_filter].copy()

    #  Removing indexes where the job_candidate_id is blank
    empty_string_indexes = mot_df[mot_df["job_candidate_id"] == ""]
    mot_df.drop(index=empty_string_indexes.index, inplace=True)

    mot_df.astype(str)
    mot_df["Cleared Email Sent"] = np.where(mot_df["Cleared Email Sent"] == "TRUE", "Yes", "No")
    return mot_df


def _add_cleared_column_info(tracker_df) -> pd.DataFrame:
    """Adds cleared columns to new records"""
    tracker_df["Cleared?"] = ""
    tracker_df["Cleared Email Sent"] = ""
    return tracker_df


def _get_jobvite_data(bq_conn: BigQueryClient) -> pd.DataFrame:
    dbt_conn = DbtClient()

    logging.info("Refreshing dbt; sleeping for 30 seconds")
    dbt_conn.run_job()
    sleep(30)

    dataset = os.getenv("GBQ_DATASET")
    df = bq_conn.get_table_as_df("rpt_jobvite__tracker_data_source", dataset=dataset)
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.strftime("%Y-%m-%d")
    return df


def _create_tracker_updated_timestamp(tracker_worksheet) -> None:
    timestamp = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    d_stamp = timestamp.strftime("%x")
    t_stamp = timestamp.strftime("%-I:%M %p")
    tracker_worksheet.update_value("A2", f"LAST UPDATED: {d_stamp} @ {t_stamp}")


def _update_dataframe(stale_df: pd.DataFrame, current_data_df: pd.DataFrame) -> pd.DataFrame:
    """Generalized func to update one dataframe with data from another"""
    try:
        df = stale_df.copy()
        df.set_index("job_candidate_id", inplace=True)
        current_data_df.set_index("job_candidate_id", inplace=True)
        df.update(current_data_df)
        df.reset_index(inplace=True)
    except ValueError as error:
        logger.exception(error)
        index_str = "\n".join(stale_df["job_candidate_id"].to_list())
        logger.info(f"Here are the indexes from the tracker dataframe:\n{index_str}")
    return df


def _fill_in_rescinded_and_date_fields(df) -> None:
    today = date.today()
    df["Rescinded"] = "--"
    df["Date Added"] = today
    df["Start Date - Last Updated"] = today
    df["Pay Location - Last Updated"] = today
    df["Main Last Updated"] = today


def _get_new_records(tracker_df, mot_df) -> pd.DataFrame:
    ids_df = tracker_df[["job_candidate_id"]].copy()
    result = pd.merge(
        mot_df,
        ids_df,
        indicator=True,
        how="outer",
        on=["job_candidate_id"]).query("_merge=='left_only'")
    result.drop(["_merge"], axis=1, inplace=True)
    return result


def _filter_out_cleared_on_boarders(cleared_ids_df, tech_tracker_df) -> pd.DataFrame:
    result = pd.merge(
        tech_tracker_df,
        cleared_ids_df,
        indicator=True,
        how="outer",
        on=["job_candidate_id"]).query("_merge=='left_only'")
    result.drop(["_merge"], axis=1, inplace=True)
    return result


def _filter_candidates_for_school_year(jobvite_df, school_year):
    jobvite_df["Start Date"] = pd.to_datetime(jobvite_df["Start Date"])
    year_2digit = school_year[-2:]
    year = int(f"20{year_2digit}")  # convert school year to 4 digit year
    start_of_year = datetime(year - 1, 6, 30)
    end_of_year = datetime(year, 7, 1)
    jobvite_df = jobvite_df[
        (jobvite_df["Start Date"] >= start_of_year) & (jobvite_df["Start Date"] < end_of_year)
        ]
    jobvite_df["Start Date"] = jobvite_df["Start Date"].dt.strftime("%m/%d/%Y")
    return jobvite_df


def _get_rescinded_offers(sql: MSSQL) -> list:
    df = sql.query_from_file("sql/rescinded_offers.sql")
    return df["job_candidate_id"].to_list()


def _update_rescinded_col(id_list, df) -> pd.DataFrame:
    filtered_for_updates = df.loc[(df["job_candidate_id"].isin(id_list)) & (df["Rescinded"] == "--")]
    if not filtered_for_updates.empty:
        filtered_for_updates["Rescinded"] = f"Yes - {date.today()}"
        filtered_for_updates.set_index("job_candidate_id", inplace=True)
        df.set_index("job_candidate_id", inplace=True)
        df.update(filtered_for_updates)
        df.reset_index(inplace=True)
    return df


def _compare_date_tracked_columns(updated_tracker_df, old_tracker_df) -> pd.DataFrame:
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


def _merge_for_comparison(updated_tracker_df, old_tracker_df, col: str) -> pd.DataFrame:
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
    results.rename(columns={update_date_field: update_date_field[:-2]}, inplace=True)
    return results


def _calculate_main_updated_date(df) -> None:
    df["Main Last Updated"] = np.where((df["Start Date - Last Updated"] <= df["Pay Location - Last Updated"]),
                                       df["Pay Location - Last Updated"], df["Start Date - Last Updated"])


def _get_and_prep_tracker_df(tracker_worksheet) -> pd.DataFrame:
    # Sort range first to eliminate possible blank rows
    tracker_worksheet.sort_range(start="B5", end=(tracker_worksheet.rows, tracker_worksheet.cols), basecolumnindex=2)
    df = tracker_worksheet.get_as_df(has_header=True, start="B4", end=(tracker_worksheet.rows, 19),
                                     include_tailing_empty=False)
    df.astype(str)
    df["Start Date - Last Updated"] = pd.to_datetime(df["Start Date - Last Updated"], format="%Y-%m-%d").dt.date
    df["Pay Location - Last Updated"] = pd.to_datetime(df["Pay Location - Last Updated"], format="%Y-%m-%d").dt.date
    return df


def _get_cleared_ids(spreadsheet, year) -> pd.DataFrame:
    cleared_sheet = spreadsheet.worksheet_by_title(f"{year} Cleared")
    return cleared_sheet.get_as_df(has_header=True, start="C4", end=(cleared_sheet.rows, 3),
                                   include_tailing_empty=False)


def tracker_refresh(tech_tracker_spreadsheet: Spreadsheet, hr_mot_spreadsheet: Spreadsheet, year: str) -> None:
    bq_conn = BigQueryClient()
    tech_tracker_sheet = tech_tracker_spreadsheet.worksheet_by_title(f"{year} Tracker")
    tracker_backup_df = _get_and_prep_tracker_df(tech_tracker_sheet)

    jobvite_df = _get_jobvite_data(bq_conn)
    jobvite_df = _filter_candidates_for_school_year(jobvite_df, year)
    jobvite_df.drop_duplicates(subset=["job_candidate_id"], inplace=True)

    rescinded_offer_ids = _get_rescinded_offers(bq_conn)

    # Tech Tracker has ability to clear onboarders who have completed onboarding to an archive sheet
    # The below filters those onboarders out of the Jobvite dataset
    cleared_ids_df = _get_cleared_ids(tech_tracker_spreadsheet, year)
    jobvite_df = _filter_out_cleared_on_boarders(cleared_ids_df, jobvite_df)

    updated_tracker_df = pd.DataFrame()

    if not tracker_backup_df.empty:
        updated_tracker_df = _update_dataframe(tracker_backup_df, jobvite_df)
        updated_tracker_df = _compare_date_tracked_columns(updated_tracker_df, tracker_backup_df)
        _calculate_main_updated_date(updated_tracker_df)
        logging.info("Updating Tech Tracker with data from HR's MOT")
    else:
        logging.info("Tech Tracker is empty")

    new_records = _get_new_records(tracker_backup_df, jobvite_df)
    if not new_records.empty:
        new_records = _add_cleared_column_info(new_records)
        _fill_in_rescinded_and_date_fields(new_records)
        updated_tracker_df = pd.concat([updated_tracker_df, new_records])
        logging.info(f"Adding {len(new_records)} new records to tracker")
    else:
        logging.info("No new records to add to Tech Tracker")

    if not updated_tracker_df.empty:
        if rescinded_offer_ids:
            logging.info("Checking for rescinded offers")
            _update_rescinded_col(rescinded_offer_ids, updated_tracker_df)
        hr_sheet = hr_mot_spreadsheet.worksheet_by_title(f"Master_{year}")
        hr_cleared_df = _get_cleared_mot_data(hr_sheet)
        updated_tracker_df = _update_dataframe(updated_tracker_df, hr_cleared_df)
        tech_tracker_sheet.set_dataframe(updated_tracker_df, "B5", copy_head=False)
        sheet_dim = (tech_tracker_sheet.rows, tech_tracker_sheet.cols)
        tech_tracker_sheet.sort_range("B5", sheet_dim, basecolumnindex=18, sortorder="DESCENDING")
        logger.info("Refreshed Tech Tracker")
    else:
        logger.info("No updates found. Nothing to refresh.")

    _create_tracker_updated_timestamp(tech_tracker_sheet)
