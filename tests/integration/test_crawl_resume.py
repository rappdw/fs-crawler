import logging
from types import SimpleNamespace

import pytest

from fscrawler.crawler import run_crawl, CrawlControl
from fscrawler.model.individual import Individual
from fscrawler.model.graph_db_impl import GraphDbImpl


def make_person(pid: str):
    return {
        "id": pid,
        "living": False,
        "names": [
            {
                "preferred": True,
                "nameForms": [
                    {
                        "parts": [
                            {"type": "http://gedcomx.org/Given", "value": pid},
                            {"type": "http://gedcomx.org/Surname", "value": "Tester"},
                        ]
                    }
                ],
            }
        ],
        "gender": {"type": "http://gedcomx.org/Male"},
        "display": {"lifespan": "1900-2000"},
    }


class StubFamilySearchAPI:
    def __init__(self, username, password, verbose=False, timeout=60, throttle=None, control=None, telemetry=None):
        self.username = username
        self.password = password
        self.verbose = verbose
        self.timeout = timeout
        self.throttle = throttle
        self.control = control
        self.telemetry = telemetry
        self.counter = 0
        self.logged = True
        self.fid = "P1"

    def get_counter(self):
        return self.counter

    def get_default_starting_id(self):
        return self.fid

    def is_logged_in(self):
        return self.logged

    def iterate(self, iteration, graph, loop):
        graph.start_iteration(iteration)
        for pid in list(graph.get_ids_to_process()):
            if not pid:
                continue
            self.counter += 1
            graph.add_individual(Individual(make_person(pid), iteration))
            if iteration < 4:
                graph.add_to_frontier(f"{pid}-N{iteration}")
        graph.end_iteration(iteration, 0.01)

    def resolve_relationships(self, graph, loop):
        graph.end_relationship_resolution(0, 0.0)


@pytest.fixture(autouse=True)
def quiet_logging():
    logging.getLogger().setLevel(logging.WARNING)


@pytest.fixture
def stub_familysearch_api(monkeypatch):
    monkeypatch.setattr("fscrawler.crawler.FamilySearchAPI", StubFamilySearchAPI)
    return StubFamilySearchAPI


def build_args(outdir, basename, hopcount, individuals=None):
    return SimpleNamespace(
        username="user",
        password="pass",
        verbose=False,
        timeout=5,
        outdir=outdir,
        basename=basename,
        hopcount=hopcount,
        individuals=individuals or [],
        show_password=False,
        requests_per_second=None,
        person_batch_size=None,
        max_concurrent_person_requests=None,
        max_concurrent_relationship_requests=None,
        delay_between_person_batches=None,
        delay_between_relationship_batches=None,
        max_retries=None,
        backoff_base_seconds=None,
        backoff_multiplier=None,
        backoff_max_seconds=None,
        pause_file=None,
        metrics_file=None,
        gen_sql=False,
    )


def read_status(outdir, basename):
    graph = GraphDbImpl(outdir, basename)
    try:
        return graph.get_checkpoint_status()
    finally:
        graph.close()


def test_run_and_resume(tmp_path, stub_familysearch_api):
    outdir = tmp_path / "out"
    args = build_args(outdir, "crawl", hopcount=3)
    control = CrawlControl()
    run_crawl(args, resume=False, control=control)

    status = read_status(outdir, "crawl")
    assert status["last_completed_iteration"] == 2
    assert status["starting_iteration"] == 3

    resume_args = build_args(outdir, "crawl", hopcount=5)
    control = CrawlControl()
    run_crawl(resume_args, resume=True, control=control)

    status = read_status(outdir, "crawl")
    assert status["last_completed_iteration"] == 4
    assert status["starting_iteration"] == 5
