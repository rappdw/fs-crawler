from collections import deque
from typing import Deque, Dict, Iterable, Set, Union, Generator, Tuple

from . import RelationshipType
from .graph import Graph, Relationship, RelationshipCounts
from .individual import Individual


class GraphMemoryImpl(Graph):
    """
    Graph of individuals in FamilySearch

    This class supports the iterative building of the graph by successively crawling out one link
    from persons in the seed set (self.processing). It does so by adding individuals on the "far"
    end of a relationship link to the frontier (self.frontier) which is used as the seed set for
    the next iteration
    """

    def update_relationship(self, relationship_id: Union[str, Tuple[str, str]], relationship_type: RelationshipType):
        pass

    def end_iteration(self, iteration: int, duration: float):
        pass

    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        pass

    def get_count_of_relationships_to_resolve(self) -> int:
        pass

    def __init__(self):
        self.individuals: Dict[str, Union[Individual, None]] = dict()
        self.relationships: Dict[Relationship, Union[str, None]] = dict()  # maps (src, dest) to rel_id
        self.frontier_queue: Deque[str] = deque()
        self.frontier_lookup: Set[str] = set()
        self.visited: Set[str] = set()
        self.processing_queue: Deque[str] = deque()
        self.processing_lookup: Set[str] = set()

    def get_processing_count(self):
        return len(self.processing_queue)

    def get_individuals(self) -> Generator[Individual, None, None]:
        for individual in self.individuals.values():
            if individual:
                yield individual

    def is_individual_in_graph(self, fs_id: str) -> bool:
        return fs_id in self.individuals or fs_id in self.visited

    def get_frontier(self) -> Generator[str, None, None]:
        for fs_id in self.frontier_queue:
            yield fs_id

    def peek_frontier(self, limit: int = 1) -> Tuple[str, ...]:
        if limit <= 0:
            return tuple()
        slice_end = min(limit, len(self.frontier_queue))
        items = []
        for idx, fs_id in enumerate(self.frontier_queue):
            if idx >= slice_end:
                break
            items.append(fs_id)
        return tuple(items)

    def seed_frontier_if_empty(self, fs_ids: Iterable[str]) -> int:
        if self.frontier_queue or self.processing_queue:
            return 0
        inserted = 0
        for fs_id in fs_ids:
            if fs_id and fs_id not in self.visited and fs_id not in self.frontier_lookup:
                self.frontier_queue.append(fs_id)
                self.frontier_lookup.add(fs_id)
                inserted += 1
        return inserted

    def get_relationships(self) -> Generator[Tuple[Relationship, str], None, None]:
        for rel, rel_id in self.relationships.items():
            if rel_id:
                yield rel, rel_id

    def add_visited_individual(self, fs_id: str):
        self.visited.add(fs_id)

    def add_to_frontier(self, fs_id: str):
        if fs_id and fs_id not in self.visited and fs_id not in self.processing_lookup and fs_id not in self.frontier_lookup:
            self.frontier_queue.append(fs_id)
            self.frontier_lookup.add(fs_id)

    def add_individual(self, person: Individual):
        if person.fid not in self.visited and person.fid not in self.individuals:
            self.individuals[person.fid] = person
        if person.fid in self.processing_lookup:
            self.processing_lookup.discard(person.fid)
            try:
                self.processing_queue.remove(person.fid)
            except ValueError:
                pass

    def add_parent_child_relationship(self, child, parent, rel_id):
        self.add_to_frontier(child)
        self.add_to_frontier(parent)
        if (child, parent) not in self.relationships:
            self.relationships[(child, parent)] = rel_id

    def start_iteration(self):
        # remove already processed individuals from the frontier queue
        if self.individuals:
            processed = set(self.individuals.keys())
            if processed:
                retained = deque()
                retained_lookup: Set[str] = set()
                while self.frontier_queue:
                    fs_id = self.frontier_queue.popleft()
                    if fs_id not in processed:
                        retained.append(fs_id)
                        retained_lookup.add(fs_id)
                self.frontier_queue = retained
                self.frontier_lookup = retained_lookup

        # mark processed individuals as visited and reset the in-memory individuals map
        self.visited |= set(self.individuals.keys())
        self.individuals = dict()

        # transfer the frontier queue into processing order
        self.processing_queue = deque(self.frontier_queue)
        self.processing_lookup = set(self.frontier_lookup)
        self.frontier_queue = deque()
        self.frontier_lookup = set()

    def get_graph_stats(self) -> str:
        rel_counts = self.get_relationship_count()
        return f"{self.get_individual_count():,} vertices, {self.get_frontier_count():,} frontier, " \
               f"{rel_counts.within:,} edges, {rel_counts.spanning:,} spanning edges, " \
               f"{rel_counts.frontier} frontier edges"

    def get_ids_to_process(self) -> Generator[str, None, None]:
        for fs_id in list(self.processing_queue):
            if fs_id and fs_id not in self.visited:
                yield fs_id

    def get_individual_count(self):
        return len(self.individuals) + len(self.visited)

    def get_frontier_count(self):
        return len(self.frontier_queue)

    def get_relationship_count(self) -> RelationshipCounts:
        rel_count = 0
        spanning_rel_count = 0
        frontier_rel_count = 0
        individuals = self.individuals.keys() | self.visited
        for (src, dest) in self.relationships.keys():
            src_in = src in individuals
            dest_in = dest in individuals
            if src_in and dest_in:
                rel_count += 1
            elif not src_in and not dest_in:
                frontier_rel_count += 1
            else:
                spanning_rel_count += 1
        return RelationshipCounts(rel_count, spanning_rel_count, frontier_rel_count)

    def clear_individual(self, fs_id: str):
        self.individuals[fs_id] = None

    def clear_relationship(self, relationship: Relationship):
        self.relationships[relationship] = None
