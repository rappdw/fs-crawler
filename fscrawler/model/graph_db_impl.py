import sqlite3 as sl
from typing import Generator, Tuple, Union
from os import rename
from os.path import exists

from . import Graph, Relationship, RelationshipCounts, RelationshipType, Individual


class GraphDbImpl(Graph):
    """
    Graph of individuals in FamilySearch

    This class supports the iterative building of the graph by successively crawling out one link
    from persons in the seed set (self.processing). It does so by adding individuals on the "far"
    end of a relationship link to the frontier (self.frontier) which is used as the seed set for
    the next iteration
    """

    def __init__(self, out_dir, basename: str):
        self.out_dir = out_dir
        self.basename = basename
        self.db_filename = out_dir / f"{basename}.db"
        self.conn = sl.connect(":memory:")
        self.starting_iter = 0
        if exists(self.db_filename):
            self._load_db_from_disk()
        else:
            with self.conn:
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS VERTEX (
                    id VARCHAR(8) NOT NULL PRIMARY KEY,
                    color INTEGER, 
                    surname STRING,
                    given_name STRING,
                    iteration INTEGER,
                    lifespan STRING
                );
                """)
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS EDGE (
                    source VARCHAR(8),
                    destination VARCHAR(8),
                    type STRING,
                    id VARCHAR(8)
                );
                """)
                self.conn.execute("""
                CREATE INDEX IF NOT EXISTS EDGE_SOURCE_IDX ON EDGE(source)
                """)
                self.conn.execute("""
                CREATE INDEX IF NOT EXISTS EDGE_DESTINATION_IDX ON EDGE(destination)
                """)
                self.conn.execute("""
                CREATE INDEX IF NOT EXISTS EDGE_TYPE_IDX ON EDGE(type)
                """)
                self.conn.execute("""
                CREATE INDEX IF NOT EXISTS EDGE_ID_IDX ON EDGE(id)
                """)
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS FRONTIER_VERTEX (
                    id VARCHAR(8) NOT NULL PRIMARY KEY
                );
                """)
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS PROCESSING (
                    id VARCHAR(8) NOT NULL PRIMARY KEY
                );
                """)
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS LOG (
                    iteration INTEGER,
                    duration FLOAT,
                    vertices INTEGER,
                    frontier INTEGER,
                    edges INTEGER,
                    spanning_edges INTEGER,
                    frontier_edges INTEGER
                );
                """)

    def get_processing_count(self):
        with self.conn:
            cursor = self.conn.execute("SELECT COUNT(*) FROM PROCESSING")
            return cursor.fetchone()[0]

    def get_individuals(self) -> Generator[Individual, None, None]:
        with self.conn:
            self.conn.row_factory = sl.Row
            cursor = self.conn.execute("SELECT * FROM VERTEX")
            self.conn.row_factory = None
            for row in cursor:
                yield Individual(**row)

    def _is_individual_in_(self, fs_id: str, table: str) -> bool:
        with self.conn:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE id=?", [fs_id])
            return cursor.fetchone()[0] == 1

    def is_individual_in_graph(self, fs_id: str) -> bool:
        return self._is_individual_in_(fs_id, "VERTEX")

    def get_frontier(self) -> Generator[str, None, None]:
        with self.conn:
            cursor = self.conn.execute("SELECT id FROM FRONTIER_VERTEX")
            for row in cursor:
                yield row[0]

    def get_relationships(self) -> Generator[Tuple[Relationship, str], None, None]:
        with self.conn:
            cursor = self.conn.execute("SELECT * FROM EDGE")
            for row in cursor:
                yield Relationship(row[0], row[1]), row[2]

    def add_visited_individual(self, fs_id: str):
        raise Exception("Shouldn't be called")

    def add_to_frontier(self, fs_id: str):
        if not self.is_individual_in_graph(fs_id) and not self._is_individual_in_(fs_id, "PROCESSING"):
            with self.conn:
                self.conn.execute(f"INSERT OR IGNORE INTO FRONTIER_VERTEX (id) values('{fs_id}')")

    def add_individual(self, person: Individual):
        if not self.is_individual_in_graph(person.fid):
            query = "INSERT INTO VERTEX (id, color, surname, given_name, iteration, lifespan) values(?, ?, ?, ?, ?, ?)"
            try:
                with self.conn:
                    self.conn.execute(query, (person.fid, person.gender.value, person.name.surname, person.name.given,
                                              person.iteration, person.lifespan))
                    self.conn.execute(f"DELETE FROM PROCESSING WHERE id='{person.fid}'")
            except sl.OperationalError as e:
                raise(Exception(f"Error with query: '{query}'", e))

    def add_parent_child_relationship(self, child, parent, rel_id):
        self.add_to_frontier(child)
        self.add_to_frontier(parent)
        with self.conn:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM EDGE WHERE source='{child}' AND destination='{parent}'")
            if cursor.fetchone()[0] == 0:
                self.conn.execute(f"""
                INSERT INTO EDGE (source, destination, type, id) 
                values('{child}', '{parent}', '{RelationshipType.UNTYPED_PARENT.value}', '{rel_id}')
                """)

    def start_iteration(self):
        with self.conn:
            self.conn.execute("INSERT INTO PROCESSING SELECT * FROM FRONTIER_VERTEX")
            # noinspection SqlWithoutWhere
            self.conn.execute("DELETE FROM FRONTIER_VERTEX")

    def end_iteration(self, iteration: int, duration: float):
        rel_counts = self.get_relationship_count()
        vertices = self.get_individual_count()
        frontier = self.get_frontier_count()
        with self.conn:
            self.conn.execute(f"""
            INSERT INTO LOG (iteration, duration, vertices, frontier, edges, spanning_edges, frontier_edges) 
            values('{iteration}', '{duration}', '{vertices}', '{frontier}', '{rel_counts.within}', 
            '{rel_counts.spanning}', '{rel_counts.frontier}')
            """)
            self.conn.commit()
        self.starting_iter = iteration

    def end_relationship_resolution(self, count: int, duration: float):
        with self.conn:
            self.conn.execute(f"""
            INSERT INTO LOG (duration, edges) 
            values('{duration}', '{count}') 
            """)
            self.conn.commit()

    def get_graph_stats(self) -> str:
        rel_counts = self.get_relationship_count()
        return f"{self.get_individual_count():,} vertices, {self.get_frontier_count():,} frontier, " \
               f"{rel_counts.within:,} edges, {rel_counts.spanning:,} spanning edges, " \
               f"{rel_counts.frontier} frontier edges"

    def get_ids_to_process(self) -> Generator[str, None, None]:
        with self.conn:
            cursor = self.conn.execute("SELECT * FROM PROCESSING")
            for row in cursor:
                yield row[0]

    def _get_count(self, table: str):
        with self.conn:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]

    def get_individual_count(self):
        return self._get_count("VERTEX")

    def get_frontier_count(self):
        return self._get_count("FRONTIER_VERTEX")

    def get_relationship_count(self) -> RelationshipCounts:
        with self.conn:
            all_edges = self.conn.execute("SELECT COUNT(*) FROM EDGE").fetchone()[0]
            src_in = self.conn.execute("SELECT COUNT(*) FROM EDGE JOIN VERTEX ON source = VERTEX.id").fetchone()[0]
            dst_in = self.conn.execute("SELECT COUNT(*) FROM EDGE JOIN VERTEX ON destination = VERTEX.id").fetchone()[0]
            rel_count = self.conn.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT destination FROM EDGE JOIN VERTEX ON EDGE.source = VERTEX.id
                ) AS SOURCE_EDGE
                JOIN VERTEX ON SOURCE_EDGE.destination = VERTEX.id
            """).fetchone()[0]
        spanning_rel_count = src_in - rel_count + dst_in - rel_count
        frontier_rel_count = all_edges - rel_count - spanning_rel_count
        return RelationshipCounts(rel_count, spanning_rel_count, frontier_rel_count)

    def _get_untyped_relationships(self) -> Generator[Tuple[str, str, int], None, None]:
        with self.conn:
            cursor = self.conn.execute(f"""
            select source, UNTYPED_EDGE.id as relationship_id, color as dest_color
            from (
                     select source, destination, Edge.id
                     from EDGE
                              join VERTEX on EDGE.source = VERTEX.id
                     where type = '{RelationshipType.UNTYPED_PARENT.value}'
                 ) as UNTYPED_EDGE
                     join VERTEX on UNTYPED_EDGE.destination = VERTEX.id
            order by source
            """)
            for row in cursor:
                yield row

    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        self.determine_resolution(self._get_untyped_relationships())
        with self.conn:
            cursor = self.conn.execute(f"""
            SELECT DISTINCT id FROM EDGE WHERE type = '{RelationshipType.RESOLVE.value}'
            """)
            for row in cursor:
                yield row[0]

    def get_count_of_relationships_to_resolve(self) -> int:
        self.determine_resolution(self._get_untyped_relationships())
        with self.conn:
            cursor = self.conn.execute(f"""
            SELECT COUNT(DISTINCT id) FROM EDGE 
            WHERE type='{RelationshipType.RESOLVE.value}'
            """)
            return cursor.fetchone()[0]

    def update_relationship(self, relationship_id: Union[str, Tuple[str, str]], relationship_type: RelationshipType):
        if isinstance(relationship_id, Tuple):
            query = f"""
            UPDATE EDGE SET type = '{relationship_type.value}'
            WHERE source = '{relationship_id[0]}' AND destination='{relationship_id[1]}'
            """
        else:
            query = f"""
            UPDATE EDGE SET type = '{relationship_type.value}'
            WHERE id = '{relationship_id}'
            """
        with self.conn:
            try:
                self.conn.execute(query)
            except sl.OperationalError as e:
                raise(Exception(f"Error with query: '{query}'", e))

    def close(self):
        file_conn = sl.connect(self.db_filename)
        with file_conn:
            for line in self.conn.iterdump():
                file_conn.execute(line)
        self.conn.close()
        file_conn.close()

    def _load_db_from_disk(self):
        file_conn = sl.connect(self.db_filename)
        with self.conn:
            for line in file_conn.iterdump():
                self.conn.execute(line)
            self.starting_iter = self.conn.execute("select MAX(iteration) FROM LOG").fetchone()[0] + 1
        file_conn.close()
        rename(self.db_filename, f"{self.db_filename}.bak")
