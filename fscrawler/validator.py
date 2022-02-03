import argparse
import sys
from pathlib import Path
from fscrawler.controller import GraphValidator


def validate(in_dir, basename, save_valid_graph: bool):
    reader = GraphValidator(in_dir, basename)
    print(reader.get_validation_stats())
    if len(reader.invalid_src) < 100:
        print("\nInvalid Source Vertices\n")
        for fs_id in reader.invalid_src:
            print(fs_id)
    print("\nInvalid relationships by iteration:")
    print(reader.get_validation_histogram())
    if save_valid_graph:
        reader.save_valid_graph()


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
    parser.add_argument("-s", "--save", action="store_true", default=False,
                        help="save validated graph")

    # extract arguments from the command line
    try:
        parser.error = parser.exit
        args = parser.parse_args()
    except SystemExit as e:
        print(f"\n\n*****\n{e}\n*****\n\n")
        parser.print_help()
        sys.exit(2)

    in_dir = Path(args.indir)
    basename = args.basename

    validate(in_dir, basename, args.save)


if __name__ == "__main__":
    main()
