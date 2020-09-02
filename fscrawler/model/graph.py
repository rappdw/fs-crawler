from enum import Enum
from itertools import chain
from typing import Dict, Tuple, Set, Optional

from .individual import Individual
from .relationship_types import RelationshipType

REL_TYPES_TO_VALIDATE = {RelationshipType.UNTYPED_PARENT, RelationshipType.BIOLOGICAL_PARENT,
                         RelationshipType.UNSPECIFIED_PARENT}

REL_TYPES_TO_REPLACE = {RelationshipType.UNTYPED_PARENT, RelationshipType.UNSPECIFIED_PARENT}


class EdgeConditions(Enum):
    invalid_state = 1
    restricted = 2
    writeable = 3
    spanning_and_unresolved = 4
    spanning = 5


def determine_edge_condition(p1_living: bool, p2_living: bool, p1_frontier: bool, p2_frontier: bool,
                             relationship_resolved: bool, save_living: bool, span_frontier: bool) -> EdgeConditions:
    # test for invalid conditions
    if (p2_frontier and p2_living) or (p1_frontier and p1_living):
        return EdgeConditions.invalid_state

    # if the relationship contains a living and we aren't saving living we shouldn't write
    if (p1_living or p2_living) and not save_living:
        return EdgeConditions.restricted
    # if we aren't spanning the frontier we are ok regardless of relationship type
    # (relationships will have been resolved)
    if not (p1_frontier or p2_frontier):
        return EdgeConditions.writeable
    # at this point we are spanning the frontier so make sure it's allowed first of all
    if span_frontier:
        # now check to see if we have spanning from frontier to processing set and the
        # relationship isn't resolved
        if p1_frontier and not relationship_resolved:
            return EdgeConditions.spanning_and_unresolved
        # we are OK to write the relationship
        return EdgeConditions.writeable
    if p1_frontier or p2_frontier:
        return EdgeConditions.spanning
    raise ValueError(f"unexpected conditions: {p1_living}, {p1_frontier}, {p2_living}, {p2_frontier}, "
                     f"{relationship_resolved}, {save_living}, {span_frontier}")


class Graph:
    """
    Graph of individuals in FamilySearch

    This class supports the iterative building of the graph by successively crawling out one link
    from persons in the seed set (self.processing). It does so by adding individuals on the "far"
    end of a relationship link to the frontier (self.frontier) which is used as the seed set for
    the next iteration
    """

    def __init__(self):
        self.individuals: Dict[str, Individual] = dict()
        self.living: Dict[str, Individual] = dict()
        self.relationships: Dict[Tuple[str, str], RelationshipType] = dict()
        self.next_iter_relationships: Dict[Tuple[str, str], RelationshipType] = dict()
        self.frontier: Set[str] = set()
        self.individuals_visited: Set[str] = set()
        self.living_individuals_visited: Set[str] = set()
        self.relationships_visited: Set[Tuple[str, str]] = set()
        self.individual_count: int = 0
        self.relationship_count: int = 0
        self.processing: Set[str] = set()

    def get_individual(self, fs_id: str) -> Optional[Individual]:
        if fs_id in self.individuals:
            return self.individuals[fs_id]
        if fs_id in self.living:
            return self.living[fs_id]
        return None

    def get_individual_info(self, fs_id: str) -> Tuple[bool, bool]:
        """returns info about an individual.
        The first element is True if we've visited the individual and they are living. If false
        it indicates that they are not living or that we haven't visited.
        The second element is True if the individual is in our frontier (we haven't visited).
        It is invalid for both elements of the Tuple to be True"""
        info = (fs_id in self.living or fs_id in self.living_individuals_visited, fs_id in self.frontier)
        if info[0] and info[1]:
            raise ValueError(f'Living flag for individual is set, but no individual data for: {fs_id}')
        return info

    def is_relationship_resolved(self, child: str, parent: str) -> bool:
        """determines if a relationship is resolved.
        Resolved indicates that either we have determined the nature of the relationship
        via a call to retrieve relationship facts, or there is insufficient information to
        assume that the relationship is not biological """
        rel_key = (child, parent)
        if rel_key in self.relationships_visited:
            # in a previous iteration, we have determined this relationship to be resolved
            # don't think we should ever hit this case, but include for completeness
            return True
        if rel_key in self.relationships:
            if self.relationships[rel_key] not in REL_TYPES_TO_REPLACE:
                # we've resolved the relationship to a specific type
                return True
            # final case we consider resolved is one in which we have the child in the current
            # processing set and the parent either in the current processing set or the frontier
            # if the relationship in this case is "UNTYPED" then we didn't have multiple
            # relationships and thus didn't require resolution (case handled above) so we can
            # assume absence of evidence of multiple parental type relationships so we'll consider
            # this relationship resolved assuming type biological
            if child in self.processing:
                return True
        return False

    def get_individuals(self):
        return chain(self.individuals.values(), self.living.values())

    def get_frontier(self):
        return self.frontier

    def get_relationships(self):
        return self.relationships

    def get_visited_individuals(self):
        return self.individuals_visited | self.living_individuals_visited

    def get_next_iter_relationships(self):
        return self.next_iter_relationships

    def add_visited_individual(self, fs_id: str, living: bool):
        self.individual_count += 1
        if living:
            self.living_individuals_visited.add(fs_id)
        else:
            self.individuals_visited.add(fs_id)

    def add_visited_relationship(self, rel_key: Tuple[str, str]):
        self.relationship_count += 1
        self.relationships_visited.add(rel_key)

    def add_next_iter(self, rel_key: Tuple[str, str], rel_type: RelationshipType):
        self.relationship_count += 1
        self.next_iter_relationships[rel_key] = rel_type

    def add_to_frontier(self, fs_id: str):
        if fs_id not in self.individuals_visited and \
                fs_id not in self.living_individuals_visited and \
                fs_id not in self.processing:
            self.frontier.add(fs_id)

    def add_individual(self, person: Individual):
        if person.living:
            if person.fid not in self.living_individuals_visited and person.fid not in self.living:
                self.individual_count += 1
                self.living[person.fid] = person
        else:
            if person.fid not in self.individuals_visited and person.fid not in self.individuals:
                self.individual_count += 1
                self.individuals[person.fid] = person

    def add_parent_child_relationship(self, child, parent,
                                      rel_type: RelationshipType = RelationshipType.UNTYPED_PARENT):
        rel_key = (child, parent)
        if rel_key not in self.relationships_visited and rel_key not in self.relationships:
            self.relationship_count += 1
            self.relationships[rel_key] = rel_type

    def update_relationship(self, child, parent, rel_type: RelationshipType):
        rel_key = (child, parent)
        if rel_key in self.relationships_visited:
            return
        if rel_key not in self.relationships:
            raise ValueError(f"Did not find {rel_key} in relationships collection")
        current_rel = self.relationships[(child, parent)]
        if current_rel in REL_TYPES_TO_REPLACE:
            self.relationships[(child, parent)] = rel_type

    def iterate(self):
        # remove from the frontier anything that has been processed in this iteration
        self.frontier -= self.individuals.keys()
        self.frontier -= self.living.keys()

        # update our visited sets with what has been processed
        self.individuals_visited |= self.individuals.keys()
        self.living_individuals_visited |= self.living.keys()
        self.relationships_visited |= self.relationships.keys()

        # reset the collections that are used to process
        self.individuals = dict()
        self.living = dict()
        self.relationships = self.next_iter_relationships
        self.next_iter_relationships = dict()

        # tee up the next iteration
        self.processing = self.frontier
        self.frontier = set()

    def _get_edge_condition(self, person_id1: str, person_id2: str, save_living: bool, span_frontier: bool):
        p, r = self.get_individual_info(person_id1)
        q, s = self.get_individual_info(person_id2)
        t = self.is_relationship_resolved(person_id1, person_id2)
        return determine_edge_condition(p, q, r, s, t, save_living, span_frontier)

    def end_iteration(self):
        temp = dict()
        # determine any relationships that need to be passed to the next iteration
        # for resolution. This handles the case of a parent in iteration n for a child
        # in iteration n+1 which may not be a biological parent (e.g. step, foster, etc.)
        for (src, dest), rel_type in self.relationships.items():
            edge_condition = self._get_edge_condition(src, dest, True, True)
            if edge_condition == EdgeConditions.spanning_and_unresolved:
                self.next_iter_relationships[(src, dest)] = rel_type
            else:
                temp[(src, dest)] = rel_type
        self.relationships = temp

    def graph_stats(self) -> str:
        return f"{self.individual_count:,} vertices, {self.relationship_count:,} edges, " \
               f"{len(self.frontier):,} frontier"

    def get_ids_to_process(self) -> Set[str]:
        return self.processing
