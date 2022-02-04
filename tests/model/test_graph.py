from collections import defaultdict
from typing import Generator, Tuple

from fscrawler.model import Graph, RelationshipType, Individual, Relationship, RelationshipCounts


class GraphTest(Graph):
    def end_iteration(self, iteration: int, duration: float):
        pass

    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        pass

    def get_count_of_relationships_to_resolve(self) -> int:
        pass

    def __init__(self):
        self.results = dict()

    def start_iteration(self):
        pass

    def get_processing_count(self) -> int:
        pass

    def get_individual_count(self) -> int:
        pass

    def get_relationship_count(self) -> RelationshipCounts:
        pass

    def get_frontier_count(self) -> int:
        pass

    def get_graph_stats(self) -> str:
        pass

    def add_individual(self, individual: Individual):
        pass

    def add_to_frontier(self, fs_id: str):
        pass

    def add_parent_child_relationship(self, child: str, parent: str, rel_id: str):
        pass

    def is_individual_in_graph(self, fs_id: str) -> bool:
        pass

    def get_relationships(self) -> Generator[Tuple[Relationship, str], None, None]:
        pass

    def get_individuals(self) -> Generator[Individual, None, None]:
        pass

    def get_frontier(self) -> Generator[str, None, None]:
        pass

    def get_ids_to_process(self) -> Generator[str, None, None]:
        pass

    def get_candidate_relationships(self) -> Generator[Tuple[str, str, int], None, None]:
        pass

    def update_relationship(self, relationship_id: str, relationship_type: RelationshipType):
        self.results[relationship_id] = relationship_type


def resolve(relationships):
    current_source = None
    gender_to_relationship_map = defaultdict(lambda: set())
    results = dict()
    for relationship in relationships:
        if relationship[0] != current_source:
            calc_updates_needed(gender_to_relationship_map, results)

            current_source = relationship[0]
            gender_to_relationship_map = defaultdict(lambda: set())
            gender_to_relationship_map[relationship[2]].add((relationship[0], relationship[1]))
        else:
            gender_to_relationship_map[relationship[2]].add((relationship[0], relationship[1]))
    calc_updates_needed(gender_to_relationship_map, results)
    return results


def calc_updates_needed(gender_to_relationship_map, results):
    total_relationships = 0
    for rel_set in gender_to_relationship_map.values():
        total_relationships += len(rel_set)
    for rel_set in gender_to_relationship_map.values():
        if len(rel_set):
            if len(rel_set) == 1 and total_relationships < 3:
                rel_type = "assumed biological"
            else:
                rel_type = "resolve"
            for rel_id in rel_set:
                results[rel_id] = rel_type


def test_relationship_resolution():
    relationships = [
        ("Isabella", "9ZT2-JR1", -1),
        ("Isabella", "9ZT2-JR1", 1),
        ("Isabella", "9ZT2-QRF", -1),
        ("Daniel", "M9JK-BT5", -1),
        ("Daniel", "M9JK-BT5", 1),
        ("Barbara", "9VFV-48R", -1),
        ("Barbara", "9VFV-48R", 1),
    ]
    expected_results = {
        '9ZT2-QRF': RelationshipType.RESOLVE,
        '9ZT2-JR1': RelationshipType.RESOLVE,
        'M9JK-BT5': RelationshipType.ASSUMED_BIOLOGICAL,
        '9VFV-48R': RelationshipType.ASSUMED_BIOLOGICAL
    }
    graph = GraphTest()
    graph.determine_resolution(relationships)
    assert graph.results == expected_results
