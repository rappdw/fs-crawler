import sqlite3 as sl
from pathlib import Path
from typing import Generator, Tuple, Union

from . import Graph, Relationship, RelationshipCounts, RelationshipType, Individual


CURRENT_SCHEMA_VERSION = 1

class GraphDbImpl(Graph):
    """
    Graph of individuals in FamilySearch

    This class supports the iterative building of the graph by successively crawling out one link
    from persons in the seed set (self.processing). It does so by adding individuals on the "far"
    end of a relationship link to the frontier (self.frontier) which is used as the seed set for
    the next iteration
    """

    def __init__(self, out_dir, basename: str):
        self.out_dir = Path(out_dir)
        self.basename = basename
        self.db_filename = self.out_dir / f"{basename}.db"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sl.connect(str(self.db_filename))
        self._configure_connection()
        self._ensure_schema()
        self.starting_iter = self._load_starting_iter()

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
        self.starting_iter = iteration + 1

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
               f"{rel_counts.frontier:,} frontier edges"

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

    def close(self, gen_sql=False):
        if gen_sql:
            sql_filename = self.out_dir / f"{self.basename}.sql"
            with sql_filename.open("w") as sql_out:
                for line in self.conn.iterdump():
                    sql_out.write(f"{line}\n")
        self.conn.close()

    def _configure_connection(self):
        # enable WAL for better concurrency/durability without blocking readers
        self.conn.execute("PRAGMA journal_mode=WAL").fetchone()
        # prefer durability; FULL is SQLite default, keep explicit for clarity
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")

    def _ensure_schema(self):
        current_version = self._get_user_version()
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS VERTEX (
                    id VARCHAR(8) NOT NULL PRIMARY KEY,
                    color INTEGER,
                    surname STRING,
                    given_name STRING,
                    iteration INTEGER,
                    lifespan STRING
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS EDGE (
                    source VARCHAR(8),
                    destination VARCHAR(8),
                    type STRING,
                    id VARCHAR(8)
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS EDGE_SOURCE_IDX ON EDGE(source)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS EDGE_DESTINATION_IDX ON EDGE(destination)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS EDGE_TYPE_IDX ON EDGE(type)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS EDGE_ID_IDX ON EDGE(id)")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS FRONTIER_VERTEX (
                    id VARCHAR(8) NOT NULL PRIMARY KEY
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS PROCESSING (
                    id VARCHAR(8) NOT NULL PRIMARY KEY
                )
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
                )
            """)
        if current_version == 0:
            self._set_user_version(CURRENT_SCHEMA_VERSION)
        elif current_version > CURRENT_SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported schema version {current_version}")

    def _get_user_version(self) -> int:
        cursor = self.conn.execute("PRAGMA user_version")
        return cursor.fetchone()[0]

    def _set_user_version(self, version: int) -> None:
        self.conn.execute(f"PRAGMA user_version={version}")
        self.conn.commit()

    def _load_starting_iter(self) -> int:
        with self.conn:
            cursor = self.conn.execute("SELECT MAX(iteration) FROM LOG WHERE iteration IS NOT NULL")
            result = cursor.fetchone()[0]
        return (result + 1) if result is not None else 0
