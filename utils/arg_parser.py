import argparse


def create_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--school-year",
        dest="school_year",
        help="School year in YYYY-YY format; ex. '2022-23'",
        action="store_true"
    )

    return parser

