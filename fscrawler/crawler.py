import asyncio
import logging
import re
import sys
import time
import getpass
import argparse
import getpass

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
    parser.add_argument("-u", "--username", metavar="<STR>", type=str,
                        help="FamilySearch username")
    parser.add_argument("-p", "--password", metavar="<STR>", type=str,
                        help="FamilySearch password")
    parser.add_argument("-i", "--individuals", metavar="<STR>", nargs="+", action="append", type=str,
                        help="Starting list of individual FamilySearch IDs for the crawl")
    parser.add_argument("-h", "--hopcount", metavar="<INT>", type=int, default=4,
                        help="Number of hops from the seed set")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Increase output verbosity [False]")
    parser.add_argument("-t", "--timeout", metavar="<INT>", type=int, default=60,
                        help="Timeout in seconds [60]")
    parser.add_argument("--show-password", action="store_true", default=False,
                        help="Show password in .settings file [False]")
    parser.add_argument("-o", "--outdir", type=str,
                        help="output directory", required=True)
    parser.add_argument("-s", "--strictresolve", action="store_true", default=False,
                        help="strict resolution of relationships")
    parser.add_argument("-b", "--basename", type=str,
                        help="basename for all output files")

    # extract arguments from the command line
    try:
        parser.error = parser.exit
        args = parser.parse_args()
    except SystemExit as e:
        print(f"\n\n*****\n{e}\n*****\n\n")
        parser.print_help()
        sys.exit(2)
    individuals = None
    if args.individuals:
        individuals = [item for sublist in args.individuals for item in sublist]
        for fid in individuals:
             if not re.match(r"[A-Z0-9]{4}-[A-Z0-9]{3}", fid):
                sys.exit("Invalid FamilySearch ID: " + fid)

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    args.username = args.username if args.username else input("Enter FamilySearch username: ")
    args.password = args.password if args.password else getpass.getpass("Enter FamilySearch password: ")

    out_dir = Path(args.outdir)
    basename = args.basename
    if not basename:
        basename = getpass.getuser()

    time_count = time.time()

    # Report settings used when crawler.py is executed.
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
        logger.error(f"Unable to write {settings_name}: f{repr(exc)}")

    # initialize a FamilySearch session and a family tree object
    logger.info("Login to FamilySearch...")
    fs = FamilySearchAPI(args.username, args.password, args.verbose, args.timeout)
    if not fs.is_logged_in():
        sys.exit(2)

    # add list of starting individuals to the family tree
    graph = Graph(fs)
    if not individuals:
        individuals = [fs.get_defaul_starting_id()]
    todo = individuals
    for fsid in todo:
        graph.add_to_frontier(fsid)

    # setup asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # crawl for specified number of hops
    for i in range(args.hopcount):
        if len(graph.frontier) == 0:
            break
        logger.info(f"Downloading hop: {i}... ({len(graph.frontier)} individuals in hop)")
        fs.process_hop(i, graph, loop)

    # now that we've crawled all of the hops, see which relationships we need to validate
    relationships_to_validate = graph.get_relationships_to_validate(args.strictresolve)
    logger.info(f"Validating {len(relationships_to_validate)} relationships...")
    fs.resolve_relationships(graph, relationships_to_validate, loop)

    graph.print_graph(out_dir, basename)

    logger.info(f"Downloaded {len(graph.individuals):,} individuals, {len(graph.frontier):,} frontier, "
          f"{(round(time.time() - time_count)):,} seconds with {fs.get_counter():,} HTTP requests.")

if __name__ == "__main__":
    main()
