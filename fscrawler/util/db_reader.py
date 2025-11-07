import sqlite3 as sl
import logging
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

    def __init__(self, db_file, hops, graph_builder: AbstractGraphBuilder):
        self.db_file = db_file
        self.hops = hops
        self.graph_builder = graph_builder
        self.logger = logging.getLogger(self.__class__.__name__)

    def _check_iteration_data(self, conn) -> bool:
        """
        Check if the database has iteration data available for hop filtering.
        Returns True if iteration field is populated for vertices.
        """
        try:
            result = conn.execute(
                "SELECT COUNT(*) FROM VERTEX WHERE iteration IS NOT NULL"
            ).fetchone()[0]
            return result > 0
        except sl.OperationalError:
            # iteration column doesn't exist
            return False
    
    def _get_filtered_queries(self, hops: int) -> dict:
        """
        Generate SQL queries filtered by iteration (hop count).
        Only includes vertices with iteration < hops.
        """
        iteration_filter = f"iteration < {hops}"
        
        filtered_unordered_vertices = f"SELECT ROWID, color FROM VERTEX WHERE {iteration_filter};"
        
        filtered_unordered_edge_count = f"""
            select count(*)
            from VERTEX
            join (
                select VERTEX.ROWID as src_id, destination from VERTEX
                JOIN EDGE on source = VERTEX.ID
                WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
                  AND VERTEX.{iteration_filter}
            ) ON VERTEX.id = destination
            WHERE VERTEX.{iteration_filter}
            order by src_id;
        """
        
        filtered_unordered_edges = f"""
            select src_id, VERTEX.ROWID as dst_id
            from VERTEX
            join (
                select VERTEX.ROWID as src_id, destination from VERTEX
                JOIN EDGE on source = VERTEX.ID
                WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
                  AND VERTEX.{iteration_filter}
            ) ON VERTEX.id = destination
            WHERE VERTEX.{iteration_filter}
            order by src_id;
        """
        
        filtered_ordered_vertices = f"""
            SELECT position, color from VERTEX 
            join ORDERING on ORDERING.id = VERTEX.ROWID
            WHERE VERTEX.{iteration_filter}
            ORDER BY position DESC;
        """
        
        filtered_ordered_edges = f"""
            select src_position, position as dst_position
            from VERTEX
            join (
                select position as src_position, destination from VERTEX
                JOIN EDGE on source = VERTEX.ID
                JOIN ORDERING on ORDERING.id = VERTEX.ROWID
                WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
                  AND VERTEX.{iteration_filter}
            ) ON VERTEX.id = destination
            JOIN ORDERING on ORDERING.id = VERTEX.ROWID
            WHERE VERTEX.{iteration_filter}
            order by src_position DESC, dst_position DESC;
        """
        
        filtered_vertex_key = f"""
            SELECT position, VERTEX.id, given_name, surname from VERTEX
            join ORDERING on ORDERING.id = VERTEX.ROWID
            WHERE VERTEX.{iteration_filter}
            ORDER BY position;
        """
        
        return {
            'unordered_vertices': filtered_unordered_vertices,
            'unordered_edge_count': filtered_unordered_edge_count,
            'unordered_edges': filtered_unordered_edges,
            'ordered_vertices': filtered_ordered_vertices,
            'ordered_edges': filtered_ordered_edges,
            'vertex_key': filtered_vertex_key,
        }

    def read(self):
        self.logger.info(f"Reading relationship database: {self.db_file}")
        conn = sl.connect(self.db_file)
        
        # Check if hop filtering should be applied
        use_hop_filtering = False
        if self.hops is not None:
            if self._check_iteration_data(conn):
                use_hop_filtering = True
                self.logger.info(f"Applying hop filtering: iteration < {self.hops}")
            else:
                self.logger.warning(f"Hop filtering requested (hops={self.hops}) but iteration data not available. Reading entire graph.")
        
        # Get appropriate queries based on hop filtering
        if use_hop_filtering:
            filtered_queries = self._get_filtered_queries(self.hops)
            nv = conn.execute(f"SELECT COUNT(*) FROM VERTEX WHERE iteration < {self.hops}").fetchone()[0]
            ne = conn.execute(filtered_queries['unordered_edge_count']).fetchone()[0]
            unordered_vert_query = filtered_queries['unordered_vertices']
            unordered_edge_query = filtered_queries['unordered_edges']
            ordered_vert_query = filtered_queries['ordered_vertices']
            ordered_edge_query = filtered_queries['ordered_edges']
        else:
            nv = conn.execute("SELECT COUNT(*) FROM VERTEX").fetchone()[0]
            ne = conn.execute(unordered_edge_count).fetchone()[0]
            unordered_vert_query = unordered_vertices
            unordered_edge_query = unordered_edges
            ordered_vert_query = ordered_vertices
            ordered_edge_query = ordered_edges
        
        self.logger.info(f"Graph size: {nv:,} vertices and {ne:,} edges")
        self.graph_builder.init_builder(nv, ne)
        ordering_table_q = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ORDERING'")
        table_exists = ordering_table_q.fetchone()[0] == 1
        ordering_count = 0
        if table_exists:
            ordering_count = conn.execute("SELECT COUNT(*) FROM ORDERING").fetchone()[0]
        if not table_exists or ordering_count != nv:
            # reading the graph using unordered vertices and edges and then, run transitive closure
            # and then canonical ordering
            self.logger.info("Computing canonical ordering (ordering table not found or outdated)")
            self._read_graph(conn, unordered_vert_query, unordered_edge_query)
            self.logger.info("Computing ordering...")
            ordering = self.graph_builder.get_ordering()
            if not use_hop_filtering:
                # Only save ordering if we're reading the full graph
                self.logger.info("Saving ordering to database...")
                self.save_ordering(conn, ordering)
                self.logger.info("Ordering saved successfully")
            else:
                self.logger.info("Skipping ordering save (hop filtering active)")
        else:
            self.logger.info("Using existing canonical ordering from database")
        self.graph_builder.init_builder(nv, ne)
        self.logger.info("Reading ordered graph...")
        self._read_graph(conn, ordered_vert_query, ordered_edge_query)
        self.logger.info("Graph reading completed")
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
        
        # Use filtered query if hop filtering is active
        if self.hops is not None and self._check_iteration_data(conn):
            filtered_queries = self._get_filtered_queries(self.hops)
            query = filtered_queries['vertex_key']
        else:
            query = vertex_key_query
        
        vertices = conn.execute(query)
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
        edges_exhausted = False

        while True:
            if read_vertex:
                try:
                    vertex = next(vertices)
                    i = vertex[0] - 1
                    color = vertex[1]
                    self.graph_builder.add_gender(i, color)
                    read_vertex = False
                except StopIteration:
                    # No more vertices to read
                    if edges_exhausted:
                        break
                    pass
            if read_edge:
                try:
                    edge = next(edges)
                    src = edge[0] - 1
                    dst = edge[1] - 1
                    read_edge = False
                except StopIteration:
                    # we've consumed the last edge, set the src less than zero so that we can process
                    # remaining vertices
                    src = -1
                    edges_exhausted = True
                    read_edge = False
            if src is not None and i is not None and src < i:
                self.graph_builder.add_vertex(i, color)
                read_vertex = True
            elif src is not None and i is not None:
                self.graph_builder.add_edge(src, dst)
                read_edge = True
