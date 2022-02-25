import os
from typing import Sequence

from fscrawler import RelationshipDbReader, AbstractGraphBuilder

class TestGraphBuilder(AbstractGraphBuilder):
    def __init__(self):
        super().__init__()
        self.val = None
        self.row = None
        self.col = None
        self.genders = None
        self.idx = None

    def init_builder(self, vertex_count: int, edge_count: int):
        self.idx = vertex_count + edge_count - 1
        self.val = [0] * (vertex_count + edge_count)
        self.col = [0] * (vertex_count + edge_count)
        self.row = [0] * (vertex_count + edge_count)
        self.genders = [0] * vertex_count

    def get_ordering(self) -> Sequence[int]:
        pass

    def add_vertex(self, vertex_id: int, color: int):
        self.val[self.idx] = color
        self.row[self.idx] = vertex_id
        self.col[self.idx] = vertex_id
        self.idx -= 1

    def add_edge(self, source_id: int, dest_id: int):
        self.val[self.idx] = 3 if self.genders[dest_id] == 1 else 2
        self.row[self.idx] = source_id
        self.col[self.idx] = dest_id
        self.idx -= 1

    def add_gender(self, vertex_id: int, color: int):
        self.genders[vertex_id] = color

    def build(self):
        return self.row, self.col, self.val


def test_rel_db():
    test_db_file = os.path.join(os.path.dirname(__file__), "resources/test.db")
    builder = TestGraphBuilder()
    reader = RelationshipDbReader(test_db_file, 4, builder)
    graph = reader.read()

    # expected graph
    r = -1
    vals = [r, 2, 3, r, 3, 2, 1, 3, 2, r, 2, 3, r, 3, 2, 1, 1, r, 3, 2, 1, r, 2, 3, 1, 1, r, r, 1]
    rows = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 7, 7, 7, 8, 9, 9, 9,10,11,12,13,14]
    cols = [0, 4, 5, 1, 2, 4, 2, 8, 9, 3, 9,10, 4, 6, 7, 5, 6, 7,11,12, 8, 9,13,14,10,11,12,13,14]

    assert graph[0] == rows
    assert graph[1] == cols
    assert graph[2] == vals
