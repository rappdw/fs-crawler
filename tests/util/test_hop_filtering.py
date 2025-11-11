import os
from typing import Sequence

from fscrawler import RelationshipDbReader, AbstractGraphBuilder


class SimpleGraphBuilder(AbstractGraphBuilder):
    """Simple graph builder for testing hop filtering."""
    
    def __init__(self, use_tqdm=False):
        super().__init__(use_tqdm=use_tqdm)
        self.vertex_count = 0
        self.edge_count = 0
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def init_builder(self, vertex_count: int, edge_count: int):
        self._init_status(vertex_count, edge_count)
        self.vertex_count = vertex_count
        self.edge_count = edge_count
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def get_ordering(self) -> Sequence[int]:
        # Return trivial ordering
        return list(range(self.vertex_count))
    
    def add_vertex(self, vertex_id: int, color: int):
        self._track_vertex()
        self.vertices.append((vertex_id, color))
    
    def add_edge(self, source_id: int, dest_id: int):
        self._track_edge()
        self.edges.append((source_id, dest_id))
    
    def add_gender(self, vertex_id: int, color: int):
        self.genders[vertex_id] = color
    
    def build(self):
        self._build_status()
        return {
            'vertex_count': len(self.vertices),
            'edge_count': len(self.edges),
            'gender_count': len(self.genders)
        }
    
    def save_cache(self, graph, cache_path, metadata: dict):
        """Stub implementation for testing."""
        pass
    
    def load_cache(self, cache_path, expected_metadata: dict):
        """Stub implementation for testing."""
        pass


def test_hop_filtering_2():
    """Test that hop filtering with hops=2 returns vertices from iterations 0-1."""
    test_db_file = os.path.join(os.path.dirname(__file__), "resources/test.db")
    builder = SimpleGraphBuilder(use_tqdm=False)
    reader = RelationshipDbReader(test_db_file, hops=2, graph_builder=builder)
    result = reader.read()
    
    # iteration 0: 2 vertices, iteration 1: 3 vertices = 5 total
    assert result['vertex_count'] == 5, f"Expected 5 vertices with hops=2, got {result['vertex_count']}"
    assert result['gender_count'] == 5, f"Expected 5 genders with hops=2, got {result['gender_count']}"
    print(f"✓ hops=2: {result['vertex_count']} vertices, {result['edge_count']} edges")


def test_hop_filtering_3():
    """Test that hop filtering with hops=3 returns vertices from iterations 0-2."""
    test_db_file = os.path.join(os.path.dirname(__file__), "resources/test.db")
    builder = SimpleGraphBuilder(use_tqdm=False)
    reader = RelationshipDbReader(test_db_file, hops=3, graph_builder=builder)
    result = reader.read()
    
    # iteration 0: 2 vertices, iteration 1: 3 vertices, iteration 2: 3 vertices = 8 total
    assert result['vertex_count'] == 8, f"Expected 8 vertices with hops=3, got {result['vertex_count']}"
    assert result['gender_count'] == 8, f"Expected 8 genders with hops=3, got {result['gender_count']}"
    print(f"✓ hops=3: {result['vertex_count']} vertices, {result['edge_count']} edges")


def test_hop_filtering_none():
    """Test that hops=None returns all vertices (backward compatibility)."""
    test_db_file = os.path.join(os.path.dirname(__file__), "resources/test.db")
    builder = SimpleGraphBuilder(use_tqdm=False)
    reader = RelationshipDbReader(test_db_file, hops=None, graph_builder=builder)
    result = reader.read()
    
    # All vertices: 2 + 3 + 3 + 7 = 15 total
    assert result['vertex_count'] == 15, f"Expected 15 vertices with hops=None, got {result['vertex_count']}"
    assert result['gender_count'] == 15, f"Expected 15 genders with hops=None, got {result['gender_count']}"
    print(f"✓ hops=None: {result['vertex_count']} vertices, {result['edge_count']} edges")


def test_hop_filtering_1():
    """Test that hop filtering with hops=1 returns only iteration 0 vertices."""
    test_db_file = os.path.join(os.path.dirname(__file__), "resources/test.db")
    builder = SimpleGraphBuilder(use_tqdm=False)
    reader = RelationshipDbReader(test_db_file, hops=1, graph_builder=builder)
    result = reader.read()
    
    # iteration 0 only: 2 vertices
    assert result['vertex_count'] == 2, f"Expected 2 vertices with hops=1, got {result['vertex_count']}"
    assert result['gender_count'] == 2, f"Expected 2 genders with hops=1, got {result['gender_count']}"
    print(f"✓ hops=1: {result['vertex_count']} vertices, {result['edge_count']} edges")
