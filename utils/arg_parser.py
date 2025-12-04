import argparse


def create_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--school-year",
        dest="school_year",
        help="School year in YY-YY format; ex. '22-23'",
        nargs=1
    )
    parser.add_argument(
        "--sla-refresh",
        dest="sla_monitor_refresh",
        help="Refreshes Tech's SLA monitor data source",
        action="store_true"
    )
    parser.add_argument(
        "--dbt-refresh",
        dest="dbt_refresh",
        help="Refreshes dbt before running updating tracker",
        action="store_true"
    )
    parser.add_argument(
        "--off-boarding-refresh",
        dest="offboarding_refresh",
        help="Refreshes offboarding tracker",
        action="store_true"
    )

    return parser
