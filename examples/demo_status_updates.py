#!/usr/bin/env python3
"""
Demo script showing status updates from the AbstractGraphBuilder.

This demonstrates both tqdm progress bars and logging output when building
graphs with the status update feature enabled.
"""
import logging
import os
import sys

# Add parent directory to path to import fscrawler
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fscrawler import RelationshipDbReader, AbstractGraphBuilder
from typing import Sequence


class DemoGraphBuilder(AbstractGraphBuilder):
    """Simple graph builder that demonstrates status updates."""
    
    def __init__(self, enable_status=True, status_interval=5, use_tqdm=True):
        # Use small interval for demo purposes when using logging mode
        super().__init__(enable_status=enable_status, status_interval=status_interval, 
                        use_tqdm=use_tqdm)
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def init_builder(self, vertex_count: int, edge_count: int):
        self._init_status(vertex_count, edge_count)
        self.vertices = []
        self.edges = []
        self.genders = {}
    
    def get_ordering(self) -> Sequence[int]:
        # Return trivial ordering for demo
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
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        level=logging.INFO
    )
    
    # Use the test database
    test_db = os.path.join(os.path.dirname(__file__), 
                          '../tests/util/resources/test.db')
    
    if not os.path.exists(test_db):
        print(f"Test database not found at: {test_db}")
        sys.exit(1)
    
    # Demo 1: Using tqdm progress bars (default)
    print("=" * 70)
    print("Demo 1: Interactive progress bars with tqdm (default)")
    print("=" * 70)
    
    builder1 = DemoGraphBuilder(enable_status=True, use_tqdm=True)
    reader1 = RelationshipDbReader(test_db, 4, builder1)
    result1 = reader1.read()
    
    print(f"\nGraph built successfully!")
    print(f"Result: {result1}")
    
    # Demo 2: Using logging mode with intervals
    print("\n" + "=" * 70)
    print("Demo 2: Logging mode with status_interval=5")
    print("=" * 70)
    
    builder2 = DemoGraphBuilder(enable_status=True, status_interval=5, use_tqdm=False)
    reader2 = RelationshipDbReader(test_db, 4, builder2)
    result2 = reader2.read()
    
    print(f"\nGraph built successfully!")
    print(f"Result: {result2}")
    print("=" * 70)
