from __future__ import print_function
import re
import sys
import time
import getpass
import argparse

from pathlib import Path

from fscrawler.controller import FamilySearchAPI
from fscrawler\
    .model.graph import Graph

def main():
    parser = argparse.ArgumentParser(
        description="crawl-fs - Crawl the FamilySearch tree and extract vertices and edges for ingestion into RedBlackGraph",
        add_help=False,
        usage="crawl-fs -u username -p password [options]",
    )
    parser.add_argument("-u", "--username", metavar="<STR>", type=str, help="FamilySearch username", required=True)
    parser.add_argument("-p", "--password", metavar="<STR>", type=str, help="FamilySearch password", required=True)
    parser.add_argument("-i", "--individuals", metavar="<STR>", nargs="+", action="append", type=str,
        help="Starting list of individual FamilySearch IDs for the crawl")
    parser.add_argument("-h", "--hopcount", metavar="<INT>", type=int, default=4,
        help="Number of hops from the seed set")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
        help="Increase output verbosity [False]")
    parser.add_argument("-t", "--timeout", metavar="<INT>", type=int, default=60, help="Timeout in seconds [60]")
    parser.add_argument("--show-password", action="store_true", default=False,
        help="Show password in .settings file [False]")
    try:
        parser.add_argument("-o", "--outdir", type=str,
            help="output directory")
        parser.add_argument("-b", "--basename", type=str,
            help="basename for all output files")
        parser.add_argument("-l", "--logfile", metavar="<FILE>", type=argparse.FileType("w", encoding="UTF-8"), default=False,
            help="output log file [stderr]")
    except TypeError:
        sys.stderr.write("Python >= 3.4 is required to run this script\n")
        sys.stderr.write("(see https://docs.python.org/3/whatsnew/3.4.html#argparse)\n")
        sys.exit(2)

    # extract arguments from the command line
    try:
        parser.error = parser.exit
        args = parser.parse_args()
    except SystemExit:
        parser.print_help()
        sys.exit(2)
    individuals = None
    if args.individuals:
        individuals = [item for sublist in args.individuals for item in sublist]
        for fid in individuals:
             if not re.match(r"[A-Z0-9]{4}-[A-Z0-9]{3}", fid):
                sys.exit("Invalid FamilySearch ID: " + fid)

    args.username = args.username if args.username else input("Enter FamilySearch username: ")
    args.password = args.password if args.password else getpass.getpass("Enter FamilySearch password: ")

    out_dir = Path(args.outdir)
    basename = args.basename

    time_count = time.time()

    # Report settings used when getmyancestors.py is executed.
    def parse_action(act):
        if not args.show_password and act.dest == "password":
            return "******"
        value = getattr(args, act.dest)
        return str(getattr(value, "name", value))

    formatting = "{:74}{:\t>1}\n"
    settings_name = out_dir / f"{basename}.settings"
    try:
        with settings_name.open("w") as settings_file:
            settings_file.write(formatting.format("time stamp: ", time.strftime("%X %x %Z")))
            for action in parser._actions:
                settings_file.write(formatting.format(action.option_strings[-1], parse_action(action)))
    except OSError as exc:
        print(f"Unable to write {settings_name}: f{repr(exc)}", file=sys.stderr)

    # initialize a FamilySearch session and a family tree object
    print("Login to FamilySearch...")
    fs = FamilySearchAPI(args.username, args.password, args.verbose, args.logfile, args.timeout)
    if not fs.is_logged_in():
        sys.exit(2)

    # add list of starting individuals to the family tree
    graph = Graph(fs)
    if not individuals:
        individuals = [fs.get_defaul_starting_id()]
    todo = individuals
    for fsid in todo:
        graph.add_to_frontier(fsid)

    # crawl for specified number of hops
    for i in range(args.hopcount):
        if len(graph.frontier) == 0:
            break
        print(f"Downloading hop: {i}...")
        fs.process_hop(i, graph)

    graph.print_graph(out_dir, basename)

    print(f"Downloaded {str(len(graph.individuals))} individuals, {str(len(graph.frontier))} frontier,  "
          f"{str(round(time.time() - time_count))} seconds with {str(fs.get_counter())} HTTP requests.")


if __name__ == "__main__":
    main()
