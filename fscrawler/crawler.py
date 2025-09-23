import argparse
import asyncio
import faulthandler
import getpass
import json
import logging
import re
import signal
import sys
import threading
import time
from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import keyring

from fscrawler.controller import FamilySearchAPI
from fscrawler.controller.fsapi import ThrottleConfig, DEFAULT_THROTTLE, StopRequested
from fscrawler.model.graph_db_impl import GraphDbImpl


class CrawlControl:
    def __init__(self):
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self.stop_reason = None
        self.pause_reason = None
        self._pause_checkpointed = False
        self._lock = threading.Lock()

    def request_stop(self, reason: str):
        with self._lock:
            if not self._stop_event.is_set():
                self.stop_reason = reason
                self._stop_event.set()
            self._pause_event.clear()
            self._pause_checkpointed = False

    def request_pause(self, reason: str):
        with self._lock:
            self.pause_reason = reason
            self._pause_event.set()

    def clear_pause(self):
        with self._lock:
            if self._pause_event.is_set():
                self._pause_event.clear()
                self._pause_checkpointed = False
                self.pause_reason = None

    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def wait_if_paused(self, logger=None, graph=None, iteration=None):
        if not self._pause_event.is_set():
            return
        with self._lock:
            if not self._pause_checkpointed:
                if logger:
                    logger.info("Pause requested%s; checkpointing crawl state...",
                                f" ({self.pause_reason})" if self.pause_reason else "")
                if graph and hasattr(graph, "checkpoint"):
                    checkpoint_iteration = iteration if iteration is not None else getattr(graph, "starting_iter", 0)
                    graph.checkpoint(checkpoint_iteration, "pause")
                self._pause_checkpointed = True
        while self._pause_event.is_set() and not self._stop_event.is_set():
            time.sleep(1)
        with self._lock:
            if logger and self._pause_checkpointed and not self._stop_event.is_set():
                logger.info("Resuming crawl after pause request.")
            self._pause_checkpointed = False


def install_signal_handlers(control: CrawlControl, logger):
    def handle_stop(signum, _frame):
        sig_name = getattr(signal, "Signals", lambda s: s)(signum)
        name = sig_name.name if hasattr(sig_name, "name") else str(signum)
        logger.info("Received %s; requesting graceful shutdown...", name)
        control.request_stop(f"signal {name}")

    def handle_pause(signum, _frame):
        sig_name = getattr(signal, "Signals", lambda s: s)(signum)
        name = sig_name.name if hasattr(sig_name, "name") else str(signum)
        if control.should_stop():
            return
        if control._pause_event.is_set():
            logger.info("Received %s; clearing pause request.", name)
            control.clear_pause()
        else:
            logger.info("Received %s; pausing crawl...", name)
            control.request_pause(f"signal {name}")

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_stop)
    if hasattr(signal, "SIGUSR2"):
        try:
            signal.signal(signal.SIGUSR2, handle_pause)
        except ValueError:
            logger.debug("Unable to register SIGUSR2 handler (unsupported platform).")


def start_pause_watcher(pause_file: Optional[str], control: CrawlControl, logger):
    if not pause_file:
        return None
    pause_path = Path(pause_file)

    def watcher():
        last_command = None
        while not control.should_stop():
            try:
                command = pause_path.read_text().strip().lower()
            except FileNotFoundError:
                command = ""
            if command != last_command:
                if command == "pause":
                    logger.info("Pause file requested pause.")
                    control.request_pause("pause-file")
                elif command == "resume":
                    logger.info("Pause file requested resume.")
                    control.clear_pause()
                elif command == "stop":
                    logger.info("Pause file requested stop.")
                    control.request_stop("pause-file stop")
                last_command = command
            time.sleep(1)

    thread = threading.Thread(target=watcher, name="pause-watcher", daemon=True)
    thread.start()
    return thread

RUN_USAGE = "crawl-fs [run] -u username -p password [options]"


def parse_run_args(argv):
    parser = argparse.ArgumentParser(
        description="crawl-fs run - Crawl the FamilySearch tree and extract vertices and edges for ingestion into RedBlackGraph",
        add_help=False,
        usage=RUN_USAGE,
    )
    parser.add_argument("-b", "--basename", type=str, help="basename for all output files")
    parser.add_argument("--gen-sql", action="store_true", default=False,
                        help="generate sql file in addition to database")
    parser.add_argument("-h", "--hopcount", metavar="<INT>", type=int, default=4,
                        help="Number of crawl iterations to run")
    parser.add_argument("-i", "--individuals", metavar="<STR>", nargs="+", action="append", type=str,
                        help="Starting list of individual FamilySearch IDs for the crawl")
    parser.add_argument("-o", "--outdir", type=str, required=True, help="output directory")
    parser.add_argument("-p", "--password", metavar="<STR>", type=str, help="FamilySearch password")
    parser.add_argument("--show-password", action="store_true", default=False,
                        help="Show password in .settings file [False]")
    parser.add_argument("-t", "--timeout", metavar="<INT>", type=int, default=60,
                        help="Timeout in seconds [60]")
    parser.add_argument("-u", "--username", metavar="<STR>", type=str, help="FamilySearch username")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Increase output verbosity [False]")
    parser.add_argument("--requests-per-second", type=float, dest="requests_per_second",
                        help=f"Limit outbound HTTP requests per second [{DEFAULT_THROTTLE.requests_per_second}]")
    parser.add_argument("--person-batch-size", type=int, dest="person_batch_size",
                        help=f"Maximum person IDs per request [{DEFAULT_THROTTLE.person_batch_size}]")
    parser.add_argument("--max-concurrent-person-requests", type=int, dest="max_concurrent_person_requests",
                        help=f"Maximum concurrent person requests [{DEFAULT_THROTTLE.max_concurrent_person_requests}]")
    parser.add_argument("--max-concurrent-relationship-requests", type=int,
                        dest="max_concurrent_relationship_requests",
                        help=f"Maximum concurrent relationship requests [{DEFAULT_THROTTLE.max_concurrent_relationship_requests}]")
    parser.add_argument("--delay-between-person-batches", type=float, dest="delay_between_person_batches",
                        help=f"Delay between person request batches in seconds [{DEFAULT_THROTTLE.delay_between_person_batches}]")
    parser.add_argument("--delay-between-relationship-batches", type=float,
                        dest="delay_between_relationship_batches",
                        help=f"Delay between relationship request batches in seconds [{DEFAULT_THROTTLE.delay_between_relationship_batches}]")
    parser.add_argument("--max-retries", type=int, dest="max_retries",
                        help=f"Maximum retries on throttled/server errors [{DEFAULT_THROTTLE.max_retries}]")
    parser.add_argument("--backoff-base", type=float, dest="backoff_base_seconds",
                        help=f"Initial backoff delay in seconds [{DEFAULT_THROTTLE.backoff_base_seconds}]")
    parser.add_argument("--backoff-multiplier", type=float, dest="backoff_multiplier",
                        help=f"Exponential backoff multiplier [{DEFAULT_THROTTLE.backoff_multiplier}]")
    parser.add_argument("--backoff-max", type=float, dest="backoff_max_seconds",
                        help=f"Maximum backoff delay in seconds [{DEFAULT_THROTTLE.backoff_max_seconds}]")
    parser.add_argument("--pause-file", type=str,
                        help="Path to a control file that can contain 'pause', 'resume', or 'stop'")
    parser.add_argument("-?", "--help", action="help", help="Show this help message and exit")

    args = parser.parse_args(argv)
    if args.individuals:
        flattened = [item for sublist in args.individuals for item in sublist]
        for fid in flattened:
            if not re.match(r"[A-Z0-9]{4}-[A-Z0-9]{3}", fid):
                parser.error(f"Invalid FamilySearch ID: {fid}")
        args.individuals = flattened
    else:
        args.individuals = []
    return args, parser


def parse_checkpoint_args(argv):
    parser = argparse.ArgumentParser(
        description="crawl-fs checkpoint --status - Inspect or manage crawl checkpoints",
        add_help=False,
        usage="crawl-fs checkpoint --status -o OUTDIR [-b BASENAME]",
    )
    parser.add_argument("--status", action="store_true", default=False,
                        help="Show checkpoint status for the specified crawl database")
    parser.add_argument("-o", "--outdir", required=True, type=str,
                        help="output directory where the crawl database lives")
    parser.add_argument("-b", "--basename", type=str, help="crawl basename (defaults to current user)")
    parser.add_argument("-?", "--help", action="help", help="Show this help message and exit")

    args = parser.parse_args(argv)
    if not args.status:
        parser.error("checkpoint currently supports only the --status action")
    return args


def resolve_credentials(args):
    if not args.username:
        args.username = input("Enter FamilySearch username: ")
    if not args.password:
        stored = keyring.get_password("fs-crawler", args.username)
        if stored:
            args.password = stored
        else:
            args.password = getpass.getpass("Enter FamilySearch password: ")
    return args


def write_settings_file(out_dir: Path, basename: str, parser: argparse.ArgumentParser, args, command: str) -> None:
    formatting = "{:74}{:\t>1}\n"
    settings_name = out_dir / f"{basename}.settings"
    try:
        with settings_name.open("w") as settings_file:
            settings_file.write(formatting.format("time stamp: ", time.strftime("%X %x %Z")))
            settings_file.write(formatting.format("command", command))
            for action in parser._actions:
                if not action.option_strings:
                    continue
                dest = action.dest
                if dest == "help":
                    continue
                value = getattr(args, dest, None)
                if dest == "password" and not args.show_password:
                    value = "******"
                if isinstance(value, list):
                    value = ",".join(value)
                settings_file.write(formatting.format(action.option_strings[-1], value))
    except OSError as exc:
        sys.stderr.write(f"Unable to write {settings_name}: {repr(exc)}")


def build_throttle_config(args) -> ThrottleConfig:
    overrides = {}
    for field in fields(ThrottleConfig):
        value = getattr(args, field.name, None)
        if value is not None:
            overrides[field.name] = value
    return ThrottleConfig(**overrides) if overrides else ThrottleConfig()


def run_crawl(args, resume: bool, control: Optional[CrawlControl] = None):
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    control = control or CrawlControl()
    install_signal_handlers(control, logger)
    start_pause_watcher(getattr(args, "pause_file", None), control, logger)

    logger.info("Login to FamilySearch...")
    throttle_config = build_throttle_config(args)
    fs = FamilySearchAPI(args.username, args.password, args.verbose, args.timeout,
                         throttle=throttle_config, control=control)
    if not fs.is_logged_in():
        sys.exit(2)

    graph = GraphDbImpl(args.outdir, args.basename)

    run_started = datetime.now(UTC).replace(microsecond=0).isoformat()
    config_snapshot = {
        "username": args.username,
        "timeout": args.timeout,
        "hopcount": args.hopcount,
        "verbose": args.verbose,
        "resume": resume,
        "started_at": run_started,
        "throttle": asdict(throttle_config),
    }
    if args.individuals:
        config_snapshot["cli_individuals"] = args.individuals
    if getattr(args, "pause_file", None):
        config_snapshot["pause_file"] = str(args.pause_file)
    if hasattr(graph, "record_run_configuration"):
        graph.record_run_configuration(config_snapshot)

    seeds = list(args.individuals)
    if not seeds:
        seeds = [fs.get_default_starting_id()]

    seeded = graph.seed_frontier_if_empty(seeds)
    if seeded:
        logger.info("Seeded %s individual(s) into the frontier queue.", seeded)
    elif resume:
        logger.info("Frontier already populated; resuming existing crawl state.")
    else:
        logger.info("Frontier already populated; continuing existing crawl state.")

    if hasattr(graph, "record_seed_snapshot"):
        graph.record_seed_snapshot(seeds)

    if hasattr(graph, "checkpoint"):
        graph.checkpoint(graph.starting_iter, "pre-run")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    duration_seconds = 0
    stats = None
    request_count = 0
    stop_reason = None
    try:
        time_start = time.time()
        for iteration in range(graph.starting_iter, args.hopcount):
            if control.should_stop():
                stop_reason = control.stop_reason or "stop requested"
                break
            control.wait_if_paused(logger, graph, iteration)
            try:
                fs.iterate(iteration, graph, loop)
            except StopRequested as exc:
                stop_reason = str(exc)
                break
        if not stop_reason and not control.should_stop():
            fs.resolve_relationships(graph, loop)
        else:
            if hasattr(graph, "checkpoint"):
                graph.checkpoint(graph.starting_iter, "stop")
        duration_seconds = round(time.time() - time_start)
        stats = graph.get_graph_stats()
        request_count = fs.get_counter()
        if hasattr(graph, "checkpoint") and not control.should_stop():
            final_iteration = max(graph.starting_iter - 1, 0)
            graph.checkpoint(final_iteration, "post-run")
    finally:
        loop.stop()
        loop.close()
        graph.close(args.gen_sql)

    stats = stats or "unavailable"

    if stop_reason:
        logger.info(
            "Crawl stopped early (%s). Graph: %s\n" "duration: %s seconds, HTTP Requests: %s.",
            stop_reason,
            stats,
            f"{duration_seconds:,}",
            f"{request_count:,}",
        )
    else:
        logger.info(
            "Crawl complete. Graph: %s\n" "duration: %s seconds, HTTP Requests: %s.",
            stats,
            f"{duration_seconds:,}",
            f"{request_count:,}",
        )


def checkpoint_status(args):
    out_dir = Path(args.outdir)
    basename = args.basename or getpass.getuser()
    graph = GraphDbImpl(out_dir, basename)
    try:
        status = graph.get_checkpoint_status()
    finally:
        graph.close()
    print(json.dumps(status, indent=2, sort_keys=True))


def main():
    argv = sys.argv[1:]
    if argv and argv[0] == "checkpoint":
        checkpoint_args = parse_checkpoint_args(argv[1:])
        checkpoint_status(checkpoint_args)
        return

    resume = False
    if argv and argv[0] == "resume":
        resume = True
        argv = argv[1:]

    args, parser = parse_run_args(argv)
    args = resolve_credentials(args)

    args.outdir = Path(args.outdir)
    args.outdir.mkdir(parents=True, exist_ok=True)
    args.basename = args.basename or getpass.getuser()

    write_settings_file(args.outdir, args.basename, parser, args, "resume" if resume else "run")

    control = CrawlControl()
    run_crawl(args, resume=resume, control=control)


if __name__ == "__main__":
    faulthandler.enable(all_threads=True)
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    main()
