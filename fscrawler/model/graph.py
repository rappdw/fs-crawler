from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Generator, Tuple
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
    def add_visited_individual(self, fs_id: str):
        """
        Adds a vertex id to the visited vertex set.

        TODO: This probably shouldn't be in the interface, rather part of the memory implementation

        Parameters:
            fs_id: FamilySearch id of the individual
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
