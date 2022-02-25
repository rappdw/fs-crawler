import sqlite3 as sl
from typing import Dict, Sequence, Tuple
from .abstract_graph import AbstractGraphBuilder, VertexInfo

unordered_vertices = "SELECT ROWID, color FROM VERTEX;"
unordered_edge_count = """
    select count(*)
    from VERTEX
    join (
        select VERTEX.ROWID as src_id, destination from VERTEX
        JOIN EDGE on source = VERTEX.ID
        WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
    ) ON VERTEX.id = destination
    order by src_id;
"""
unordered_edges = """
    select src_id, VERTEX.ROWID as dst_id
    from VERTEX
    join (
        select VERTEX.ROWID as src_id, destination from VERTEX
        JOIN EDGE on source = VERTEX.ID
        WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
    ) ON VERTEX.id = destination
    order by src_id;
"""
ordered_vertices = """
    SELECT position, color from VERTEX 
    join ORDERING on ORDERING.id = VERTEX.ROWID 
    ORDER BY position DESC;
"""
ordered_edges = """
    select src_position, position as dst_position
    from VERTEX
    join (
        select position as src_position, destination from VERTEX
        JOIN EDGE on source = VERTEX.ID
        JOIN ORDERING on ORDERING.id = VERTEX.ROWID
        WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
    ) ON VERTEX.id = destination
    JOIN ORDERING on ORDERING.id = VERTEX.ROWID
    order by src_position DESC, dst_position DESC;
"""
vertex_key_query = """
SELECT position, VERTEX.id, given_name, surname from VERTEX
join ORDERING on ORDERING.id = VERTEX.ROWID
ORDER BY position;
"""
create_ordering_table = """
CREATE TABLE IF NOT EXISTS ORDERING (
        id INTEGER NOT NULL PRIMARY KEY,
        position INTEGER
        );
"""
create_ordering_index = """
CREATE INDEX IF NOT EXISTS ORDER_INDEX ON ORDERING(position)
"""


class RelationshipDbReader(VertexInfo):

    # TODO: support hops

    def __init__(self, db_file, hops, graph_builder: AbstractGraphBuilder):
        self.db_file = db_file
        self.hops = hops
        self.graph_builder = graph_builder

    def read(self):
        conn = sl.connect(self.db_file)
        nv = conn.execute("SELECT COUNT(*) FROM VERTEX").fetchone()[0]
        ne = conn.execute(unordered_edge_count).fetchone()[0]
        self.graph_builder.init_builder(nv, ne)
        ordering_table_q = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ORDERING'")
        table_exists = ordering_table_q.fetchone()[0] == 1
        ordering_count = 0
        if table_exists:
            ordering_count = conn.execute("SELECT COUNT(*) FROM ORDERING").fetchone()[0]
        if not table_exists or ordering_count != nv:
            # reading the graph using unordered vertices and edges and then, run transitive closure
            # and then canonical ordering
            self._read_graph(conn, unordered_vertices, unordered_edges)
            ordering = self.graph_builder.get_ordering()
            self.save_ordering(conn, ordering)
        self.graph_builder.init_builder(nv, ne)
        self._read_graph(conn, ordered_vertices, ordered_edges)
        return self.graph_builder.build()

    @staticmethod
    def save_ordering(conn, ordering: Sequence[int]):
        conn.execute(create_ordering_table)
        conn.execute(create_ordering_index)
        # noinspection SqlWithoutWhere
        conn.execute("DELETE FROM ORDERING")
        for idx, vertex_id in enumerate(ordering):
            conn.execute(f"INSERT INTO ORDERING (id, position) values({vertex_id + 1}, {idx + 1})")
        conn.commit()

    def get_vertex_key(self) -> Dict[int, Tuple[str, str]]:
        """
        Get the "vertex key", a dictionary keyed by the vertex id (int) with values
        that are tuples of (external id, string designation)
        """
        conn = sl.connect(self.db_file)
        vertices = conn.execute(vertex_key_query)
        vertex_key = dict()
        for vertex in vertices:
            vertex_id = vertex[0] - 1
            external_id = vertex[1]
            vertex_name = f"'{vertex[3]}', '{vertex[2]}'"
            vertex_key[vertex_id] = (external_id, vertex_name)
        return vertex_key

    def _read_graph(self, conn, vertex_query, edge_query):
        vertices = conn.execute(vertex_query)
        edges = conn.execute(edge_query)

        read_vertex = True
        read_edge = True
        i = color = src = dst = None

        while True:
            if read_vertex:
                try:
                    vertex = next(vertices)
                    i = vertex[0] - 1
                    color = vertex[1]
                    self.graph_builder.add_gender(i, color)
                    read_vertex = False
                except StopIteration:
                    pass
            if read_edge:
                try:
                    edge = next(edges)
                    src = edge[0] - 1
                    dst = edge[1] - 1
                    read_edge = False
                except StopIteration:
                    # we've consumed the last edge, set the src less than zero so that we can process
                    # the last vertex
                    src = -1
            if src < i:
                self.graph_builder.add_vertex(i, color)
                read_vertex = True
                if src < 0:
                    break
            else:
                self.graph_builder.add_edge(src, dst)
                read_edge = True
