from typing import List, Tuple
import numpy as np
import pandas as pd
from pygsheets import Worksheet

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
    "Completion Status" "Completion_Status"
}


def _datefields_to_dt_obj(df):
    df['StartDate_LastUpdated'] = pd.to_datetime(df['StartDate_LastUpdated'], format="%Y-%m-%d")
    df['PayLocation_LastUpdated'] = pd.to_datetime(df['PayLocation_LastUpdated'], format="%Y-%m-%d")
    df['DateCleared'] = pd.to_datetime(df['DateCleared'], format="%m/%d/%Y")
    df['DateAdded'] = pd.to_datetime(df['DateAdded'], format="%Y-%m-%d")
    df['StartDate'] = pd.to_datetime(df['StartDate'], format="%m/%d/%Y")
    df['Main_LastUpdated'] = pd.to_datetime(df['Main_LastUpdated'], format="%Y-%m-%d")


def compare_dates(df, new_col, date1, date2):
    df[new_col] = np.where(df[date1] < df[date2], 1, 0)


def eval_sla_met(df):
    df["TechCleared_MetSLA_Boolean"] = np.where(pd.isnull(df["DateCleared"]), "n/a", 0)
    df["TechCleared_MetSLA_Boolean"] = np.where((df["TechCleared_MetSLA_Boolean"] != "n/a") & (df["DateCleared"] <= df["StartDate"]), 1, df["TechCleared_MetSLA_Boolean"])


def eval_tech_timeliness(df):
    df["TechCleared_Timeliness"] = np.where(pd.isnull(df["DateCleared"]), "n/a", 0)
    df["TechCleared_Timeliness"] = np.where((df["TechCleared_Timeliness"] != "n/a"), (df["StartDate"] - df["DateCleared"]).dt.days, df["TechCleared_Timeliness"])


def identify_tracker_cleared_sheets(spreadsheet) -> Tuple[List[pd.DataFrame], List[pd.DataFrame]]:
    sheet_list = spreadsheet.worksheets()
    cleared = []
    tracker = []

    for sheet in sheet_list:
        sheet_type = sheet.title.split(' ')[-1]
        sheet_year = sheet.title.split(' ')[0]
        sheet_df = sheet.get_as_df(has_header=True, start="B4", end=(sheet.rows, sheet.cols),
                                   include_tailing_empty=False)

        sheet_df["SchoolYear"] = f"20{sheet_year.split('-')[-1]}"
        if sheet_type == "Cleared":
            cleared.append(sheet_df)
        elif sheet_type == "Tracker":
            tracker.append(sheet_df)

    return cleared, tracker

def refresh_sla_source(spreadsheet):
    sla_sheet = spreadsheet.worksheet_by_title("SLA_data_source")
    cleared_dfs, tracker_dfs = identify_tracker_cleared_sheets(spreadsheet)

    tracker_df = pd.concat(tracker_dfs)
    tracker_df['Date Cleared'] = None
    cleared_dfs.append(tracker_df)
    agg_df = pd.concat(cleared_dfs)
    # filter out rescinded candidates
    agg_df = agg_df.drop(agg_df[agg_df.Rescinded != '--'].index)
    # drop rescinded col
    agg_df.drop("Rescinded", axis="columns", inplace=True)

    # Rename Columns
    agg_df.rename(columns=COLUMN_RENAME_MAP, inplace=True)

    # COMBINE FIRST AND LAST NAMES
    agg_df['Staff_Name'] = agg_df["First Name"].astype(str) + ' ' + agg_df["Last Name"].astype(str)
    agg_df.drop(["First Name", "Last Name"], axis="columns", inplace=True)

    # HIRE MONTH
    _datefields_to_dt_obj(agg_df)
    agg_df["Hire_Month"] = agg_df['StartDate'].dt.strftime('%B')

    # StartDateChange_Boolean
    compare_dates(agg_df, "StartDateChange_Boolean", "DateAdded", "StartDate_LastUpdated")

    # LocationChange_Boolean
    compare_dates(agg_df, "LocationChange_Boolean", "DateAdded", "PayLocation_LastUpdated")

    # TechCleared_MetSLA_Boolean
    eval_sla_met(agg_df)

    # TechCleared_Timeliness
    eval_tech_timeliness(agg_df)

    # Include_SLA_Denominator
    agg_df["TODAY"] = pd.Timestamp.today().date()
    compare_dates(agg_df, "Include_SLA_Denominator", "StartDate", "TODAY")
    agg_df.drop("TODAY", axis="columns", inplace=True)

    # push to Google Sheets
    sla_sheet.set_dataframe(agg_df, "A1")