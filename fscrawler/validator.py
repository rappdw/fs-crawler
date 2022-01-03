import argparse
import sys
from pathlib import Path
from fscrawler.controller.graph_validator_reader import GraphReader


def validate(in_dir, basename):
    reader = GraphReader(in_dir, basename)
    print(reader.get_validation_stats())


def main():
    parser = argparse.ArgumentParser(
        description="validate-fs - Validate the data in a crawl of the FamilySearch tree",
        add_help=False,
        usage="validate-fs -b basename -i input_directory [options]",
    )
    parser.add_argument("-b", "--basename", type=str,
                        help="basename for all output files", required=True)
    parser.add_argument("-i", "--indir", type=str,
                        help="output directory", required=True)

    # extract arguments from the command line
    try:
        parser.error = parser.exit
        args = parser.parse_args()
    except SystemExit as e:
        print(f"\n\n*****\n{e}\n*****\n\n")
        parser.print_help()
        sys.exit(2)
    individuals = None

    in_dir = Path(args.indir)
    basename = args.basename

    validate(in_dir, basename)


if __name__ == "__main__":
    main()