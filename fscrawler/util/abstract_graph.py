from abc import ABC, abstractmethod
from typing import Dict, Sequence, Tuple


class VertexInfo(ABC):
    @abstractmethod
    def get_vertex_key(self) -> Dict[int,Tuple[str,str]]:
        """
        Get the "vertex key", a dictionary keyed by the vertex id (int) with values
        that are tuples of (external id, string designation)
        """
        pass


class AbstractGraphBuilder(ABC):
    def __init__(self, sparse_threshold: int = 1000):
        self.sparse_threshold = sparse_threshold

    @abstractmethod
    def init_builder(self, vertex_count: int, edge_count: int):
        """
        Initialize the builder
        """
        pass

    @abstractmethod
    def get_ordering(self) -> Sequence[int]:
        pass

    @abstractmethod
    def add_vertex(self, vertex_id: int, color: int):
        pass

    @abstractmethod
    def add_edge(self, source_id: int, dest_id: int):
        pass

    @abstractmethod
    def add_gender(self, vertex_id: int, color: int):
        pass

    @abstractmethod
    def build(self):
        pass