import argparse


def create_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--school-year",
        dest="school_year",
        help="School year in YY-YY format; ex. '22-23'",
        nargs=1
    )

    return parser
