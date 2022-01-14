import csv
from collections import namedtuple
from typing import Dict, Set, Union

from .individual import Individual
from .relationship_types import RelationshipType

REL_TYPES_TO_VALIDATE = {
    RelationshipType.UNTYPED_PARENT,
    RelationshipType.BIOLOGICAL_PARENT,
    RelationshipType.UNSPECIFIED_PARENT
}

REL_TYPES_TO_REPLACE = {
    RelationshipType.UNTYPED_PARENT,
    RelationshipType.UNSPECIFIED_PARENT
}

RelationshipCounts = namedtuple("RelationshipCounts", "within spanning frontier")
Relationship = namedtuple("Relationship", "src dest")


class Graph:
    """
    Graph of individuals in FamilySearch

    This class supports the iterative building of the graph by successively crawling out one link
    from persons in the seed set (self.processing). It does so by adding individuals on the "far"
    end of a relationship link to the frontier (self.frontier) which is used as the seed set for
    the next iteration
    """

    def __init__(self):
        self.individuals: Dict[str, Union[Individual, None]] = dict()
        self.relationships: Dict[Relationship, Union[str, None]] = dict()  # maps (src, dest) to rel_id
        self.frontier: Set[str] = set()
        self.visited: Set[str] = set()
        self.processing: Set[str] = set()

    def get_individuals(self):
        return self.individuals.values()

    def is_individual_in_graph(self, fs_id: str) -> bool:
        return fs_id in self.individuals or fs_id in self.visited

    def get_frontier(self):
        return self.frontier

    def get_relationships(self):
        return self.relationships

    def get_visited_individuals(self) -> Set[str]:
        return self.visited

    def add_visited_individual(self, fs_id: str, living: bool):
        self.visited.add(fs_id)

    def add_to_frontier(self, fs_id: str):
        if fs_id not in self.visited and \
                fs_id not in self.processing:
            self.frontier.add(fs_id)

    def add_individual(self, person: Individual):
        if person.fid not in self.visited and person.fid not in self.individuals:
            self.individuals[person.fid] = person

    def add_parent_child_relationship(self, child, parent, rel_id):
        if (child, parent) not in self.relationships:
            self.relationships[(child, parent)] = rel_id

    def start_iteration(self):
        # remove from the frontier anything that has been processed in this iteration
        self.frontier -= self.individuals.keys()

        # update our visited sets with what has been processed
        self.visited |= self.individuals.keys()

        # reset the collections that are used to process
        self.individuals = dict()

        # tee up the next iteration
        self.processing = self.frontier
        self.frontier = set()

    def write_individual(self, writer: csv.writer, person: Individual, clear_on_write: bool):
        writer.writerow([person.fid, person.gender.value, f"{person.name.surname}, {person.name.given}",
                         person.iteration, person.lifespan])
        if clear_on_write:
            # by convention when a relationship is written, the individual is set to None
            self.individuals[person.fid] = None

    def write_relationship(self, writer: csv.writer, src: str, dest: str, rel_type: RelationshipType, rel_id: str,
                           clear_on_write: bool):
        writer.writerow([src, dest, rel_type.value, rel_id])
        if clear_on_write:
            # by convention when a relationship is written, the rel_id is set to None
            self.relationships[(src, dest)] = None

    def graph_stats(self) -> str:
        rel_counts = self.get_relationship_count()
        return f"{self.get_individual_count():,} vertices, {self.get_frontier_count():,} frontier, " \
               f"{rel_counts.within:,} edges, {rel_counts.spanning:,} spanning edges, " \
               f"{rel_counts.frontier} frontier edges"

    def get_ids_to_process(self) -> Set[str]:
        return self.processing - self.visited - self.individuals.keys() - {None}

    def get_individual_count(self):
        return len(self.individuals) + len(self.visited)

    def get_frontier_count(self):
        return len(self.frontier)

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
