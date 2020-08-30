import pytest
import json
import pathlib
from fscrawler.controller.fsapi import split_seq, partition_requests, FamilySearchAPI
from fscrawler.controller.session import FAMILYSEARCH_LOGIN, AUTHORIZATION, BASE_URL, CURRENT_USER, FSSESSIONID
from fscrawler.model.graph import Graph
from fscrawler.model.relationship_types import RelationshipType
from unittest.mock import patch, ANY
from asynctest import CoroutineMock

LOGIN_W_PARAMS = FAMILYSEARCH_LOGIN + '?ldsauth=false'
LOCATION = 'https://location_url'

def test_split_seq():
    sequence = [0, 1, 2, 3, 4, 5]
    expected = {
        0: [0, 1],
        1: [2, 3],
        2: [4, 5],
    }
    for idx, segment in enumerate(split_seq(sequence, 2)):
        assert idx in expected
        assert expected[idx] == segment

    sequence = [0, 1, 2, 3, 4]
    expected = {
        0: [0, 1],
        1: [2, 3],
        2: [4],
    }
    for idx, segment in enumerate(split_seq(sequence, 2)):
        assert idx in expected
        assert expected[idx] == segment

def test_partition_requests():
    ids = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    expected = {
        0: [[0, 1, 2], [3, 4, 5]],
        1: [[6, 7, 8], [9, 10, 11]],
        2: [[12, 13, 14], [15, 16, 17]],
        3: [[18, 19, 20], [21, 22]],
    }

    partitioning = partition_requests(ids, None, 3, 2)
    idx = 0
    for partition in partitioning:
        assert partition == expected[idx]
        idx += 1
    assert idx == 4

    expected = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        1: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        2: [20, 21, 22],
    }
    partitioning = partition_requests(ids, None, 1, 10)
    idx = 0
    for partition in partitioning:
        assert partition == expected[idx]
        idx += 1
    assert idx == 3




@pytest.fixture
def fs_api(httpx_mock):
    """Setup a mock fs_api and mock the login process"""
    httpx_mock.add_response(url=LOGIN_W_PARAMS, json={}, headers={'location': LOCATION})
    httpx_mock.add_response(url=LOCATION, data='name="params" value="012345678901234567890123456789"', headers={'Set-Cookie': f'{FSSESSIONID}=12345'})
    httpx_mock.add_response(url=AUTHORIZATION, data='', headers={'location': LOCATION})
    httpx_mock.add_response(url=BASE_URL + CURRENT_USER, json={'users': [{'personId': 'P1', 'preferredLanguage': 'English', 'displayName': 'Test User'}]})
    api = FamilySearchAPI('user', 'password', False)
    return api

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

def test_processing_persons(fs_api, persons_json, bio_relationship_json, step_relationship_json):
    graph = Graph()
    requiring_resolution = fs_api.process_persons_result(persons_json, graph, 0)
    assert 'KWZQ-QZV' in graph.individuals
    assert 'KWZQ-QZ2' in graph.individuals
    assert 'KWCY-KHR' in graph.individuals

    assert "MHFN-X8H" in requiring_resolution
    assert "98F8-S5H" in requiring_resolution

    assert graph.relationships[('KWZG-916', 'KWZQ-QZV')] == RelationshipType.UNTYPED_PARENT
    assert graph.relationships[('KWZG-916', 'KWZQ-QZG')] == RelationshipType.UNTYPED_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] == RelationshipType.UNTYPED_PARENT
    fs_api.process_relationship_result(step_relationship_json, graph)
    assert graph.relationships[('KWZG-916', 'KWZQ-QZV')] == RelationshipType.UNSPECIFIED_PARENT
    assert graph.relationships[('KWZG-916', 'KWZQ-QZG')] == RelationshipType.UNTYPED_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.UNTYPED_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.BIOLOGICAL_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.UNSPECIFIED_PARENT
    fs_api.process_relationship_result(bio_relationship_json, graph)
    assert graph.relationships[('KWZG-916', 'KWZQ-QZV')] == RelationshipType.BIOLOGICAL_PARENT
    assert graph.relationships[('KWZG-916', 'KWZQ-QZG')] == RelationshipType.BIOLOGICAL_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.UNTYPED_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.BIOLOGICAL_PARENT
    assert graph.relationships[('KWZG-916', 'KJDT-2VN')] != RelationshipType.UNSPECIFIED_PARENT
