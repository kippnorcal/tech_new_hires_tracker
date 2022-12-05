from datetime import date, datetime
import logging
import os
import sys
from typing import Union
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pygsheets import authorize, Worksheet


DEBUG = int(os.getenv("DEBUG", default=0))
TECH_TRACKER_SHEET = os.getenv("TECH_TRACKER_SHEETS_ID")
HR_MOT_SHEET = os.getenv("HR_MOT_SHEETS_ID")

logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="./app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p %Z",
)
logger = logging.getLogger(__name__)

COLUMN_MAPPINGS = {
    3: 'job_candidate_id',
    4: 'First Name',
    5: 'Last Name',
    19: 'Personal Email',
    7: 'Work Location',
    8: 'Pay Location',
    9: 'Start Date',
    13: 'Title',
    16: 'Former or Current KIPP',
    1: 'New, Returners, Rehire or Transfer',
    15: 'SpEd',
    50: 'Cleared?',
    51: 'Cleared Email Sent',
#    54: 'Rescinded'
}


def _create_sheet_connection(sheet_key: str, worksheet_name: str) -> Union[Worksheet, None]:
    # pygsheets.exceptions.WorksheetNotFound
    # googleapiclient.errors.HttpError
    worksheet = None
    try:
        client = authorize(service_file='./service_account_credentials.json')
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.worksheet_by_title(worksheet_name)
    except Exception as e:
        logger.warning(e)
    return worksheet


def create_tracker_updated_timestamp(tracker: Worksheet) -> None:
    timestamp = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    d_stamp = timestamp.strftime('%x')
    t_stamp = timestamp.strftime('%-I:%M %p')
    tracker.update_value('A2', f'LAST UPDATED: {d_stamp} @ {t_stamp}')


def _create_updated_df(tech_tracker_df, mot_df):
    df = tech_tracker_df.copy()
    df.set_index('job_candidate_id', inplace=True)
    mot_df.set_index('job_candidate_id', inplace=True)
    df.update(mot_df)
    df.reset_index(inplace=True)
    df.drop(columns=['Start Date - Last Updated', 'Pay Location - Last Updated', 'Main Last Updated'])
    return df


def _get_cleaned_mot_df(hr_mot_sheet):
    # Todo: update hr_mot_sheet for prod
    mot_df = hr_mot_sheet.get_as_df(start=(3, 1), end=(hr_mot_sheet.rows, 56), has_header=False,
                                    include_tailing_empty=False)
    mot_df = mot_df.rename(columns=COLUMN_MAPPINGS)

    #  Filtering unneeded columns
    column_filter = list(COLUMN_MAPPINGS.values())
    mot_df[column_filter].copy()
    mot_df = mot_df[column_filter].copy()

    #  Removing indexes where the job_candidate_id is blank
    empty_string_indexes = mot_df[mot_df['job_candidate_id'] == '']
    mot_df.drop(index=empty_string_indexes.index, inplace=True)

    mot_df.astype(str)
    return mot_df


def _fill_in_date_fields(df):
    today = date.today()
    df['Date Added'] = today
    df['Start Date - Last Updated'] = today
    df['Pay Location - Last Updated'] = today
    df['Main Last Updated'] = today


def get_new_records(tracker_df, mot_df):
    ids_df = tracker_df[["job_candidate_id"]].copy()
    result = pd.merge(
        mot_df,
        ids_df,
        indicator=True,
        how="outer",
        on=["job_candidate_id"]).query('_merge=="left_only"')
    result.drop(["_merge"], axis=1, inplace=True)
    if not result.empty:
        result['Rescinded'] = '--'
        _fill_in_date_fields(result)
    return result


def _get_rescinded_offers(hr_mot_wksht):
    """HR strikesthrough rescinded offers"""
    rescinded_offer_ids = list()
    col = hr_mot_wksht.get_col(4, include_tailing_empty=False, returnas='cell')
    for cell in col[2:]:
        if cell is not None:
            if cell.text_format is not None:
                strike_through = cell.text_format.get('strikethrough', False)
                if strike_through:
                    rescinded_offer_ids.append(cell.value_unformatted)

    return rescinded_offer_ids


def _update_rescinded_col(id_list, df):
    filtered_for_updates = df.loc[(df['job_candidate_id'].isin(id_list)) & (df['Rescinded'] == '--')]
    if not filtered_for_updates.empty:
        filtered_for_updates['Rescinded'] = f'Yes - {date.today()}'
        filtered_for_updates.set_index('job_candidate_id', inplace=True)
        df.set_index('job_candidate_id', inplace=True)
        df.update(filtered_for_updates)
        df.reset_index(inplace=True)
    return df


def _compare_dfs(updated_tracker_df, old_tracker_df):
    cols_to_compare = ['Start Date', 'Pay Location']
    for col in cols_to_compare:
        df_compared = _merge_for_comparison(updated_tracker_df, old_tracker_df, col)
        updated_tracker_df = _create_updated_df(updated_tracker_df, df_compared)

    return updated_tracker_df


def _merge_for_comparison(updated_tracker_df, old_tracker_df, col):
    results = pd.merge(
        updated_tracker_df,
        old_tracker_df,
        how='outer',
        on="job_candidate_id"
    )
    new_value = f"{col}_x"
    old_value = f"{col}_y"
    update_date_field = f"{col} - Last Updated_x"
    results = results[['job_candidate_id', new_value, old_value, update_date_field]]
    #  print(results.head())
    results.loc[results[new_value] != results[old_value], update_date_field] = date.today()
    #  print(results.head())
    # results[['job_candidate_id', update_date_field]]
    results.rename(columns={update_date_field: update_date_field[:-2]}, inplace=True)
    return results


def _evaluate_datapoints(x, new_v, old_v):
    pass


def calculate_main_updated_date(df):
    # df.where(df["Start Date - Last Updated"] > df["Pay Location - Last Updated"], )
    df['Main Last Updated'] = np.where((df['Start Date - Last Updated'] <= df["Pay Location - Last Updated"]),
                                       df["Pay Location - Last Updated"], df['Start Date - Last Updated'])


def main():
    # getting tech worksheet and DFs
    # Todo: update tech_tracker_sheet for prod
    tech_tracker_sheet = _create_sheet_connection(TECH_TRACKER_SHEET, "2022-23 Tracker")
    tracker_backup_df = tech_tracker_sheet.get_as_df(has_header=True, start="B4", end=(tech_tracker_sheet.rows, 19),
                                                     include_tailing_empty=False)
    tracker_backup_df.astype(str)
    tracker_backup_df['Start Date - Last Updated'] = pd.to_datetime(tracker_backup_df['Start Date - Last Updated'],
                                                                    format="%Y-%m-%d").dt.date
    tracker_backup_df['Pay Location - Last Updated'] = pd.to_datetime(tracker_backup_df['Pay Location - Last Updated'],
                                                                      format="%Y-%m-%d").dt.date
    #  print('\nTRACKER DATA')
    #  print(f'{tracker_backup_df.to_string()}')
    # date_cols = [col for col in tracker_backup_df.columns.tolist() if col not in string_cols]
    # tracker_backup_df[date_cols] = pd.to_datetime(tracker_backup_df[date_cols], format='%d-%m-%Y')
    hr_mot_sheet = _create_sheet_connection(HR_MOT_SHEET, "Master_22-23")
    rescinded_offer_ids = _get_rescinded_offers(hr_mot_sheet)
    hr_mot_df = _get_cleaned_mot_df(hr_mot_sheet)
    hr_mot_df.astype(str)
    #  print('\nMOT DATA')
    #  print(f'{hr_mot_df.to_string()}')
    updated_tracker_df = pd.DataFrame()

    if not tracker_backup_df.empty:
        updated_tracker_df = _create_updated_df(tracker_backup_df, hr_mot_df)
        updated_tracker_df = _compare_dfs(updated_tracker_df, tracker_backup_df)  #  ToDo: Rename this
        calculate_main_updated_date(updated_tracker_df)

    new_records = get_new_records(tracker_backup_df, hr_mot_df)
    if not new_records.empty:
        # Todo: will need to update 'tracker_backup_df' from below
        updated_tracker_df = pd.concat([updated_tracker_df, new_records])

    if rescinded_offer_ids:
        _update_rescinded_col(rescinded_offer_ids, updated_tracker_df)

    if not updated_tracker_df.empty:
        # ToDo: Log this, and log if nothing is updated
        tech_tracker_sheet.set_dataframe(updated_tracker_df, "B5", copy_head=False)
        sheet_dim = (tech_tracker_sheet.rows, tech_tracker_sheet.cols)
        tech_tracker_sheet.sort_range('B5', sheet_dim, basecolumnindex=18, sortorder='DESCENDING')

    create_tracker_updated_timestamp(tech_tracker_sheet)


if __name__ == "__main__":
    main()
