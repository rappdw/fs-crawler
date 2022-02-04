from abc import ABC, abstractmethod
from collections import namedtuple, defaultdict
from typing import Generator, Tuple, Iterable, DefaultDict, Set, Union
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

"""
    RelationshipCounts is a named tuple that holds the counts of various types
    of edges. Types include:
    
    within - edge between two vertices in the graph
    spanning - edge between a vertex in the graph and a vertex beyond the graph horizon
    frontier - edge between two vertices beyond the graph horizon
"""
RelationshipCounts = namedtuple("RelationshipCounts", "within spanning frontier")

"""
    Relationship is a named tupel that holds the source and destination vertex ids
"""
Relationship = namedtuple("Relationship", "src dest")


class Graph(ABC):
    @abstractmethod
    def start_iteration(self):
        """
        Graphs are built iteratively. A frontier of vertices is defined, those vertices are
        resolved, all aut-bound edges are determined for those vertices and the
        destination vertices for those edges are added to the frontier for the next
        iteration.

        start_iteration is called to prepare any internal state of the graph to begin
        this iteration.
        """
        ...

    @abstractmethod
    def get_processing_count(self) -> int:
        """
        Returns the number of vertices that are in the current iteration's processing
        set. This is also the same number as the previous iteration's frontier

        Returns:
            int: the number of vertices in the processing set
        """
        ...

    @abstractmethod
    def get_individual_count(self) -> int:
        """
        Returns the number of vertices that have been fully resolved in the graph

        Returns:
            int: the number of vertices in the graph
        """
        ...

    @abstractmethod
    def get_relationship_count(self) -> RelationshipCounts:
        """
        Returns the number of edges that have been fully resolved in the graph

        Returns:
            RelationshipCounts: the number of edges (for various types) in the graph
        """
        ...

    @abstractmethod
    def get_frontier_count(self) -> int:
        """
        Returns the number of vertices that are 1 hop outside the currently resolved graph

        Returns:
            int: the number of vertices outside (1 hop) the horizon of the graph
        """
        ...

    @abstractmethod
    def get_graph_stats(self) -> str:
        """
        Returns a string with key metrics of the graph

        Returns:
            str: a string that represents key graph metrics
        """
        ...

    @abstractmethod
    def add_individual(self, individual: Individual):
        """
        A vertex id is resolved to an individual via calls to FamilySearch. Once resolved
        add_individual can be called to place the fully resolved vertex in the graph

        Parameters:
            individual: Key characteristics of a vertex, e.g. name, key life events, gender, ...
        """
        ...

    @abstractmethod
    def add_to_frontier(self, fs_id: str):
        """
        Adds a vertex id to the frontier set of the graph.

        Parameters:
            fs_id: FamilySearch id of the individual
        """
        ...

    @abstractmethod
    def add_parent_child_relationship(self, child: str, parent: str, rel_id: str):
        """
        An edge in the graph is defined by a src vertex (child) a destination vertex (parent)
        as well as the type of relationship (biological, adoptive, etc.). add_parent_child_relationship
        will add an edge, which often results in placing either the child or parent into the
        frontier as a side effect.

        Parameters:
            child: fs_id of the source vertex
            parent: fs_id of the destination vertex
            rel_id: fs_id of the relationship information (which can be resolved to determine relationship type)
        """
        ...

    @abstractmethod
    def is_individual_in_graph(self, fs_id: str) -> bool:
        """
        Returns a bool indicating whether a given vertex is already in the graph

        Parameters:
            fs_id: FamilySearch id of the vertex

        Returns:
            bool: True if individual is in graph, False if individual is not
        """
        ...

    @abstractmethod
    def get_relationships(self) -> Generator[Tuple[Relationship, str], None, None]:
        """
        Returns a Generator that yields graph Relationships (source & destination vertex) along with
        the FamilySearch id of the relationship
        """
        ...

    @abstractmethod
    def get_individuals(self) -> Generator[Individual, None, None]:
        """
        Returns a Generator that yields Individuals in the graph
        """
        ...

    @abstractmethod
    def get_frontier(self) -> Generator[str, None, None]:
        """
        Returns a Generator that yields ids of the vertices in the graph frontier
        """
        ...

    @abstractmethod
    def get_ids_to_process(self) -> Generator[str, None, None]:
        """
        Returns the set of ids to process for the current iteration
        """
        ...

    @abstractmethod
    def update_relationship(self, relationship_id: Union[str, Tuple[str, str]], relationship_type: RelationshipType):
        """
        Update the type for a relationship

        Parameters:
            relationship_id: FamilySearch id of the relationship
            relationship_type: Type to update the relationship to
        """
        ...

    def determine_resolution(self, relationships: Iterable[Tuple[str, str, int]]):
        """
        Given a set of relationships determine which relationships should be resolved.

        As a result of calling this function all relationships in the graph that were passed in will be flagged as:
            RelationshipType.ASSUMED_BIOLOGICAL if resolution not required
            RelationshipType.RESOLVE if resolution required

        Parameters:
            relationships: an iterable of Tuple[source id, relationship id, destination gender value], assumed to
                be ordered by source id
        """
        current_source = None
        gender_to_relationship_map = defaultdict(lambda: set())
        for relationship in relationships:
            if relationship[0] != current_source:
                self._calc_updates_needed(gender_to_relationship_map)

                current_source = relationship[0]
                gender_to_relationship_map = defaultdict(lambda: set())
                gender_to_relationship_map[relationship[2]].add(relationship[1])
            else:
                gender_to_relationship_map[relationship[2]].add(relationship[1])
        self._calc_updates_needed(gender_to_relationship_map)

    def _calc_updates_needed(self, gender_to_relationship_map: DefaultDict[int, Set[str]]):
        total_relationships = 0
        for rel_set in gender_to_relationship_map.values():
            total_relationships += len(rel_set)
        for rel_set in gender_to_relationship_map.values():
            if len(rel_set):
                if len(rel_set) == 1 and total_relationships < 3:
                    rel_type = RelationshipType.ASSUMED_BIOLOGICAL
                else:
                    rel_type = RelationshipType.RESOLVE
                for rel_id in rel_set:
                    self.update_relationship(rel_id, rel_type)

    @abstractmethod
    def end_iteration(self, iteration: int, duration: float):
        ...

    @abstractmethod
    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        ...

    @abstractmethod
    def get_count_of_relationships_to_resolve(self) -> int:
        ...
