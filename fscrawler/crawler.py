import asyncio
import faulthandler
import signal
import logging
import re
import sys
import time
import argparse
import getpass
import keyring

from pathlib import Path

from fscrawler.controller import FamilySearchAPI
from fscrawler.model.graph_db_impl import GraphDbImpl


def crawl(out_dir, basename, username, password, timeout, verbose, iteration_bound, individuals=None):
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG if verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    time_count = time.time()

    # initialize a FamilySearch session and a family tree object
    logger.info("Login to FamilySearch...")
    fs = FamilySearchAPI(username, password, verbose, timeout)
    if not fs.is_logged_in():
        sys.exit(2)

    # add list of starting individuals to the family tree
    graph = GraphDbImpl(out_dir, basename)
    iteration_start = 0
    # # check to see if the files already exists and if so, consider this
    # # a restart
    # if GraphIO(out_dir, basename, graph).exists():
    #     restart = True
    #     reader = GraphReader(out_dir, basename, graph)
    #     iteration_start = reader.get_max_iteration() + 1
    #     iteration_bound = iteration_start + iteration_bound
    #     logger.info(f"Loaded graph for restart: {graph.get_graph_stats()}. Running iterations {iteration_start} "
    #                 f"through {iteration_bound}.")
    # else:
    #     restart = False

    if not individuals:
        individuals = [fs.get_default_starting_id()]
    for fs_id in individuals:
        graph.add_to_frontier(fs_id)

    # setup asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # crawl for specified number of iterations
    for i in range(iteration_start, iteration_bound):
        fs.iterate(i, graph, loop)
    fs.resolve_relationships(graph, loop)

    logger.info(f"Crawl complete. Graph: {graph.get_graph_stats()}\n"
                f"duration: {(round(time.time() - time_count)):,} seconds, HTTP Requests: {fs.get_counter():,}.")


def main():
    parser = argparse.ArgumentParser(
        description="crawl-fs - Crawl the FamilySearch tree and extract vertices and edges for ingestion "
                    "into RedBlackGraph",
        add_help=False,
        usage="crawl-fs -u username -p password [options]",
    )
    parser.add_argument("-b", "--basename", type=str,
                        help="basename for all output files")
    parser.add_argument("-h", "--hopcount", metavar="<INT>", type=int, default=4,
                        help="Number of crawl iterations to run")
    parser.add_argument("-i", "--individuals", metavar="<STR>", nargs="+", action="append", type=str,
                        help="Starting list of individual FamilySearch IDs for the crawl")
    parser.add_argument("-o", "--outdir", type=str,
                        help="output directory", required=True)
    parser.add_argument("-p", "--password", metavar="<STR>", type=str,
                        help="FamilySearch password")
    parser.add_argument("--save-living", action="store_true", default=False,
                        help="When writing out csf files, save living individuals")
    parser.add_argument("--show-password", action="store_true", default=False,
                        help="Show password in .settings file [False]")
    parser.add_argument("-t", "--timeout", metavar="<INT>", type=int, default=60,
                        help="Timeout in seconds [60]")
    parser.add_argument("-u", "--username", metavar="<STR>", type=str,
                        help="FamilySearch username")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Increase output verbosity [False]")

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

    args.username = args.username if args.username else input("Enter FamilySearch username: ")
    if not args.password:
        args.password = keyring.get_password("fs-crawler", args.username)
        if not args.password:
            args.password = getpass.getpass("Enter FamilySearch password: ")

    out_dir = Path(args.outdir)
    basename = args.basename
    if not basename:
        basename = getpass.getuser()

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
        sys.stderr.write(f"Unable to write {settings_name}: f{repr(exc)}")

    crawl(out_dir, basename, args.username, args.password, args.timeout, args.verbose, args.hopcount, individuals)


if __name__ == "__main__":
    faulthandler.enable(all_threads=True)
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    main()
