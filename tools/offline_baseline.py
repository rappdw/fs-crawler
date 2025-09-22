#!/usr/bin/env python3
"""Run a single offline crawl iteration against canned data to capture baseline metrics."""
import asyncio
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import Dict, Iterable, List
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from fscrawler.controller.fsapi import FamilySearchAPI, PartitionedRequest
from fscrawler.model.graph_db_impl import GraphDbImpl


def _make_person(pid: str, given: str, surname: str, gender_type: str, lifespan: str) -> Dict:
    return {
        "id": pid,
        "living": False,
        "names": [
            {
                "preferred": True,
                "nameForms": [
                    {
                        "parts": [
                            {"type": "http://gedcomx.org/Given", "value": given},
                            {"type": "http://gedcomx.org/Surname", "value": surname},
                        ]
                    }
                ],
            }
        ],
        "gender": {"type": gender_type},
        "display": {"lifespan": lifespan},
    }


SAMPLE_PERSONS: Dict[str, Dict] = {
    "P0": _make_person("P0", "Pat", "Ancestor", "http://gedcomx.org/Male", "1900-1981"),
    "P1": _make_person("P1", "Alex", "Ancestor", "http://gedcomx.org/Female", "1905-1990"),
    "C0": _make_person("C0", "Casey", "Descendant", "http://gedcomx.org/Female", "1930-2010"),
}

SAMPLE_RELATIONSHIPS: List[Dict] = [
    {
        "type": "http://gedcomx.org/Couple",
        "person1": {"resourceId": "P0"},
        "person2": {"resourceId": "P1"},
    }
]

SAMPLE_CHILD_PARENT: List[Dict] = [
    {
        "id": "REL1",
        "child": {"resourceId": "C0"},
        "parent1": {"resourceId": "P0"},
        "parent2": {"resourceId": "P1"},
    }
]

SAMPLE_RELATIONSHIP_LOOKUP: Dict[str, Dict] = {
    "REL1": {
        "childAndParentsRelationships": [
            {
                "id": "REL1",
                "child": {"resourceId": "C0"},
                "parent1": {"resourceId": "P0"},
                "parent1Facts": [
                    {"type": "http://gedcomx.org/BiologicalParent"}
                ],
                "parent2": {"resourceId": "P1"},
                "parent2Facts": [
                    {"type": "http://gedcomx.org/BiologicalParent"}
                ],
            }
        ]
    }
}


class StubSession:
    def __init__(self, username: str, password: str, verbose: bool = False, timeout: int = 15):
        self.username = username
        self.password = password
        self.verbose = verbose
        self.timeout = timeout
        self.counter = 0
        self.logged = True
        self.fid = "P0"

    def get_url(self, url: str):
        self.counter += 1
        # Only "current user" lookups are expected during FamilySearchAPI initialization.
        if url.endswith("/platform/users/current.json"):
            return {
                "users": [
                    {
                        "personId": self.fid,
                        "preferredLanguage": "en",
                        "displayName": "Stub User",
                    }
                ]
            }
        return {}

    async def get_urla(self, url: str):
        self.counter += 1
        parsed = urlparse(url)
        path = parsed.path
        if path.startswith("/platform/tree/persons"):
            query = parse_qs(parsed.query)
            raw_ids: Iterable[str] = []
            if "pids" in query:
                raw_ids = query["pids"][0].split(",")
            ids = [pid for pid in raw_ids if pid]
            persons = [SAMPLE_PERSONS[pid] for pid in ids if pid in SAMPLE_PERSONS]
            return {
                "persons": persons,
                "relationships": SAMPLE_RELATIONSHIPS,
                "childAndParentsRelationships": SAMPLE_CHILD_PARENT,
            }
        if path.startswith("/platform/tree/child-and-parents-relationships"):
            rel_id = Path(path).stem
            return SAMPLE_RELATIONSHIP_LOOKUP.get(rel_id, {})
        raise ValueError(f"Unsupported URL in stub session: {url}")


def simple_partition(ids: Iterable[str], count: int, *_args, **_kwargs):
    id_list = [fsid for fsid in ids if fsid]
    if not id_list:
        return PartitionedRequest(0, [])
    batches: List[Iterable[Iterable[str]]] = [(tuple(id_list),)]
    return PartitionedRequest(len(batches), batches)


def run_baseline() -> Dict[str, float]:
    with tempfile.TemporaryDirectory() as tmpdir:
        graph = GraphDbImpl(Path(tmpdir), "baseline")
        graph.add_to_frontier("P0")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with patch("fscrawler.controller.fsapi.Session", StubSession), \
                patch("fscrawler.controller.fsapi.partition_requests", simple_partition):
            api = FamilySearchAPI("stub", "stub", verbose=False, timeout=1)

            tracemalloc.start()
            iter_start = time.perf_counter()
            api.iterate(0, graph, loop)
            iter_duration = time.perf_counter() - iter_start
            current_mem, peak_mem = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            loop.stop()
            loop.close()

        return {
            "iteration_duration_s": iter_duration,
            "requests_issued": api.get_counter(),
            "requests_per_second": api.get_counter() / iter_duration if iter_duration else 0.0,
            "current_mem_bytes": float(current_mem),
            "peak_mem_bytes": float(peak_mem),
            "vertex_count": float(graph.get_individual_count()),
            "frontier_count": float(graph.get_frontier_count()),
        }


if __name__ == "__main__":
    metrics = run_baseline()
    print("Offline baseline metrics:")
    for key, value in metrics.items():
        if key.endswith("bytes"):
            print(f"  {key}: {value:,.0f}")
        else:
            print(f"  {key}: {value:.3f}")
