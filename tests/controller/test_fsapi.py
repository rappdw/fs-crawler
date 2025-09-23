import asyncio
import json
import pathlib
from typing import Generator, Tuple, Union

import pytest

from fscrawler.controller.fsapi import FamilySearchAPI, StopRequested, ThrottleConfig, partition_requests
from fscrawler.model import RelationshipType
from fscrawler.model.graph_memory_impl import GraphMemoryImpl


def test_partition_requests():
    ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}
    expected = {
        0: ((0, 1, 2), (3, 4, 5)),
        1: ((6, 7, 8), (9, 10, 11)),
        2: ((12, 13, 14), (15, 16, 17)),
        3: ((18, 19, 20), (21, 22)),
    }

    partitioning = partition_requests(ids, len(ids), 3, 2)
    idx = 0
    for partition in partitioning.iterator:
        assert partition == expected[idx]
        idx += 1
    assert partitioning.number_of_partitions == idx

    expected = {
        0: (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        1: (10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
        2: (20, 21, 22),
    }
    partitioning = partition_requests(ids, len(ids), 1, 10)
    idx = 0
    for partition in partitioning.iterator:
        assert partition == expected[idx]
        idx += 1
    assert partitioning.number_of_partitions == idx


@pytest.fixture
def fs_api(monkeypatch):
    class DummySession:
        def __init__(self, username, password, verbose=False, timeout=60, **_):
            self.counter = 0
            self.logged = True
            self.fid = "P1"

        def is_logged_in(self):
            return self.logged

        async def get_urla(self, url):
            return {}

    monkeypatch.setattr("fscrawler.controller.fsapi.Session", DummySession)
    return FamilySearchAPI("user", "password", False)


@pytest.fixture
def persons_json(request):
    file = pathlib.Path(request.node.fspath.dirname) / 'data' / 'persons.json'
    with file.open() as fp:
        return json.load(fp)


@pytest.fixture
def bio_relationship_json(request):
    # child: KWZG-916
    # parent1: KWZQ-QZV
    # parent2: KWZQ-QZG
    # both parents biological
    file = pathlib.Path(request.node.fspath.dirname) / 'data' / 'biological_parent_relationship.json'
    with file.open() as fp:
        return json.load(fp)


@pytest.fixture
def step_relationship_json(request):
    # child: KWZG-916
    # parent1: KWZQ-QZV
    # parent2: KJDT-2VN
    # parent2: step
    file = pathlib.Path(request.node.fspath.dirname) / 'data' / 'step_parent_relationship.json'
    with file.open() as fp:
        return json.load(fp)


class GraphTest(GraphMemoryImpl):

    def end_iteration(self, iteration: int, duration: float):
        pass

    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        pass

    def get_count_of_relationships_to_resolve(self) -> int:
        pass

    def __init__(self):
        super().__init__()
        self.results = dict()

    def update_relationship(self, relationship_id: Union[str, Tuple[str, str]], relationship_type: RelationshipType):
        self.results[relationship_id] = relationship_type


def test_iterate_honors_stop_request(monkeypatch):
    class DummySession:
        def __init__(self, *_, **__):
            self.counter = 0
            self.logged = True
            self.fid = "P1"

        def is_logged_in(self):
            return True

        async def get_urla(self, url):
            return {"persons": []}

    monkeypatch.setattr("fscrawler.controller.fsapi.Session", DummySession)

    class Control:
        stop_reason = "test stop"

        def should_stop(self):
            return True

        def wait_if_paused(self, *_, **__):
            pass

    control = Control()
    throttle = ThrottleConfig(person_batch_size=1, max_concurrent_person_requests=1,
                              max_concurrent_relationship_requests=1, delay_between_person_batches=0,
                              delay_between_relationship_batches=0, requests_per_second=0)
    api = FamilySearchAPI("user", "password", False, throttle=throttle, control=control)
    graph = GraphTest()
    graph.seed_frontier_if_empty(["P1"])
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with pytest.raises(StopRequested):
        api.iterate(0, graph, loop)
    loop.stop()
    loop.close()


def test_processing_persons(fs_api, persons_json, bio_relationship_json, step_relationship_json):
    graph = GraphTest()
    fs_api.process_persons_result(persons_json, graph, 0)
    individuals = graph.get_individuals()
    count = 0
    for individual in individuals:
        assert individual.fid in ['KWZQ-QZV', 'KWZQ-QZ2', 'KWCY-KHR']
        count += 1
    assert count == 3

    relationships = graph.get_relationships()

    results = dict()
    for relationship, rel_id in relationships:
        results[relationship] = rel_id

    assert results[('KWZG-916', 'KWZQ-QZV')] == 'MHFN-X8H'
    assert results[('KWZG-916', 'KWZQ-QZG')] == 'MHFN-X8H'
    assert results[('KWZG-916', 'KJDT-2VN')] == '98F8-S5H'
    fs_api.process_relationship_result(step_relationship_json, graph)
    assert graph.results[('KWZG-916', 'KWZQ-QZV')] == RelationshipType.UNSPECIFIED_PARENT
    assert graph.results[('KWZG-916', 'KJDT-2VN')] == RelationshipType.STEP_PARENT
    fs_api.process_relationship_result(bio_relationship_json, graph)
    assert graph.results[('KWZG-916', 'KWZQ-QZV')] == RelationshipType.BIOLOGICAL_PARENT
    assert graph.results[('KWZG-916', 'KWZQ-QZG')] == RelationshipType.BIOLOGICAL_PARENT
    assert graph.results[('KWZG-916', 'KJDT-2VN')] == RelationshipType.STEP_PARENT
