#!/usr/bin/env python3
"""
Demo script showing hop filtering in action.

This demonstrates how the hop parameter limits the graph to vertices
within N hops from the seed nodes.
"""
import logging
import os
import sys

# Add parent directory to path to import fscrawler
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fscrawler import RelationshipDbReader, AbstractGraphBuilder
from typing import Sequence


class DemoGraphBuilder(AbstractGraphBuilder):
    """Simple graph builder for demonstrating hop filtering."""
    
    def __init__(self, use_tqdm=False):
        super().__init__(use_tqdm=use_tqdm)
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def init_builder(self, vertex_count: int, edge_count: int):
        self._init_status(vertex_count, edge_count)
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def get_ordering(self) -> Sequence[int]:
        return list(range(len(self.vertices)))
    
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
            'vertices': len(self.vertices),
            'edges': len(self.edges),
            'genders': len(self.genders)
        }


if __name__ == '__main__':
    # Configure logging to show INFO level messages
    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )
    
    # Use the test database
    test_db = os.path.join(os.path.dirname(__file__), 
                          '../tests/util/resources/test.db')
    
    if not os.path.exists(test_db):
        print(f"Test database not found at: {test_db}")
        sys.exit(1)
    
    print("=" * 70)
    print("Hop Filtering Demo")
    print("=" * 70)
    print("\nThe test database has vertices at different iteration levels:")
    print("  - iteration 0: 2 vertices (seed nodes)")
    print("  - iteration 1: 3 vertices (1 hop from seed)")
    print("  - iteration 2: 3 vertices (2 hops from seed)")
    print("  - iteration 3: 7 vertices (3 hops from seed)")
    print("  - Total: 15 vertices, 14 edges")
    print("\n" + "=" * 70)
    
    # Test different hop values
    hop_values = [None, 1, 2, 3, 4]
    
    for hops in hop_values:
        print(f"\n{'Reading with hops=' + str(hops):.<60}")
        builder = DemoGraphBuilder(use_tqdm=False)
        reader = RelationshipDbReader(test_db, hops=hops, graph_builder=builder)
        result = reader.read()
        
        print(f"  → Result: {result['vertices']} vertices, {result['edges']} edges")
    
    print("\n" + "=" * 70)
    print("Summary:")
    print("  - hops=None: Returns entire graph (all 15 vertices)")
    print("  - hops=1: Returns only iteration 0 (2 vertices)")
    print("  - hops=2: Returns iterations 0-1 (5 vertices)")
    print("  - hops=3: Returns iterations 0-2 (8 vertices)")
    print("  - hops=4: Returns iterations 0-3 (all 15 vertices)")
    print("\nMemory usage is proportional to the filtered graph size!")
    print("=" * 70)
