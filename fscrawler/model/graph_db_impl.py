import json
import sqlite3 as sl
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Optional, Tuple, Union

from . import Graph, Relationship, RelationshipCounts, RelationshipType, Individual


CURRENT_SCHEMA_VERSION = 2
FRONTIER_TABLE = "FRONTIER_QUEUE"
PROCESSING_TABLE = "PROCESSING_QUEUE"

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
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {PROCESSING_TABLE}")
            return cursor.fetchone()[0]

    def get_individuals(self) -> Generator[Individual, None, None]:
        with self.conn:
            self.conn.row_factory = sl.Row
            cursor = self.conn.execute("SELECT * FROM VERTEX")
            self.conn.row_factory = None
            for row in cursor:
                yield Individual(**row)

    def _is_individual_in_(self, fs_id: str, table: str, column: str = "id") -> bool:
        with self.conn:
            cursor = self.conn.execute(
                f"SELECT 1 FROM {table} WHERE {column}=? LIMIT 1",
                (fs_id,)
            )
            return cursor.fetchone() is not None

    def is_individual_in_graph(self, fs_id: str) -> bool:
        return self._is_individual_in_(fs_id, "VERTEX")

    def get_frontier(self) -> Generator[str, None, None]:
        with self.conn:
            cursor = self.conn.execute(
                f"SELECT fs_id FROM {FRONTIER_TABLE} ORDER BY seq"
            )
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
        if self.is_individual_in_graph(fs_id):
            return
        if self._is_individual_in_(fs_id, PROCESSING_TABLE, "fs_id"):
            return
        with self.conn:
            self.conn.execute(
                f"INSERT OR IGNORE INTO {FRONTIER_TABLE} (fs_id) VALUES (?)",
                (fs_id,)
            )

    def add_individual(self, person: Individual):
        if not self.is_individual_in_graph(person.fid):
            query = "INSERT INTO VERTEX (id, color, surname, given_name, iteration, lifespan) values(?, ?, ?, ?, ?, ?)"
            try:
                with self.conn:
                    self.conn.execute(query, (person.fid, person.gender.value, person.name.surname, person.name.given,
                                              person.iteration, person.lifespan))
                    self.conn.execute(
                        f"DELETE FROM {PROCESSING_TABLE} WHERE fs_id=?",
                        (person.fid,)
                    )
            except sl.OperationalError as e:
                raise(Exception(f"Error with query: '{query}'", e))

    def add_parent_child_relationship(self, child, parent, rel_id):
        self.add_to_frontier(child)
        self.add_to_frontier(parent)
        with self.conn:
            cursor = self.conn.execute(
                "SELECT COUNT(*) FROM EDGE WHERE source=? AND destination=?",
                (child, parent)
            )
            if cursor.fetchone()[0] == 0:
                self.conn.execute(
                    "INSERT INTO EDGE (source, destination, type, id) VALUES (?, ?, ?, ?)",
                    (
                        child,
                        parent,
                        RelationshipType.UNTYPED_PARENT.value,
                        rel_id,
                    ),
                )

    def start_iteration(self, iteration: int):
        with self.conn:
            self._set_metadata("active_iteration", iteration)
            self.conn.execute(f"DELETE FROM {PROCESSING_TABLE}")
            self.conn.execute(
                f"INSERT INTO {PROCESSING_TABLE} (fs_id) "
                f"SELECT fs_id FROM {FRONTIER_TABLE} ORDER BY seq"
            )
            self.conn.execute(f"DELETE FROM {FRONTIER_TABLE}")
            self._record_checkpoint_metadata(iteration, phase="start")

    def end_iteration(self, iteration: int, duration: float):
        rel_counts = self.get_relationship_count()
        vertices = self.get_individual_count()
        frontier = self.get_frontier_count()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO LOG (iteration, duration, vertices, frontier, edges, spanning_edges, frontier_edges) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    iteration,
                    duration,
                    vertices,
                    frontier,
                    rel_counts.within,
                    rel_counts.spanning,
                    rel_counts.frontier,
                ),
            )
            self.conn.commit()
            self._set_metadata("active_iteration", None)
            self._set_metadata("last_completed_iteration", iteration)
            self._record_checkpoint_metadata(iteration, phase="iteration-complete")
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
            cursor = self.conn.execute(
                f"SELECT fs_id FROM {PROCESSING_TABLE} ORDER BY seq"
            )
            for row in cursor:
                yield row[0]

    def checkpoint(self, iteration: int, reason: str):
        with self.conn:
            self.conn.commit()
            self._record_checkpoint_metadata(iteration, phase=reason)

    def record_run_configuration(self, config: Dict[str, Any]) -> None:
        sanitized = {key: value for key, value in config.items() if key != "password"}
        with self.conn:
            self._set_metadata("run_configuration", sanitized)

    def record_seed_snapshot(self, seeds: Iterable[str]) -> None:
        existing = self._get_metadata("seed_history") or []
        merged = list(dict.fromkeys(list(existing) + [seed for seed in seeds if seed]))
        with self.conn:
            self._set_metadata("seed_history", merged)

    def get_checkpoint_status(self) -> Dict[str, Any]:
        status: Dict[str, Any] = {
            "active_iteration": self._get_metadata("active_iteration"),
            "starting_iteration": self.starting_iter,
            "frontier_count": self.get_frontier_count(),
            "processing_count": self.get_processing_count(),
            "vertex_count": self.get_individual_count(),
            "last_checkpoint": self._get_metadata("last_checkpoint"),
            "run_configuration": self._get_metadata("run_configuration") or {},
            "seed_history": self._get_metadata("seed_history") or [],
            "frontier_preview": self.peek_frontier(5),
        }

        last_completed = self._get_metadata("last_completed_iteration")
        if last_completed is None:
            with self.conn:
                row = self.conn.execute(
                    "SELECT MAX(iteration) FROM LOG WHERE iteration IS NOT NULL"
                ).fetchone()
                last_completed = row[0] if row and row[0] is not None else None
        status["last_completed_iteration"] = last_completed
        return status

    def _get_count(self, table: str):
        with self.conn:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]

    def get_individual_count(self):
        return self._get_count("VERTEX")

    def get_frontier_count(self):
        return self._get_count(FRONTIER_TABLE)

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

    def peek_frontier(self, limit: int = 1) -> Tuple[str, ...]:
        if limit <= 0:
            return tuple()
        with self.conn:
            cursor = self.conn.execute(
                f"SELECT fs_id FROM {FRONTIER_TABLE} ORDER BY seq LIMIT ?",
                (limit,)
            )
            return tuple(row[0] for row in cursor)

    def seed_frontier_if_empty(self, fs_ids: Iterable[str]) -> int:
        inserted = 0
        inserted_ids = []
        with self.conn:
            frontier_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {FRONTIER_TABLE}"
            ).fetchone()[0]
            processing_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {PROCESSING_TABLE}"
            ).fetchone()[0]
            if frontier_count or processing_count:
                return 0
            for fs_id in fs_ids:
                if not fs_id:
                    continue
                in_graph = self.conn.execute(
                    "SELECT 1 FROM VERTEX WHERE id=? LIMIT 1",
                    (fs_id,)
                ).fetchone()
                if in_graph:
                    continue
                cursor = self.conn.execute(
                    f"INSERT OR IGNORE INTO {FRONTIER_TABLE} (fs_id) VALUES (?)",
                    (fs_id,)
                )
                if cursor.rowcount == 1:
                    inserted += 1
                    inserted_ids.append(fs_id)
        if inserted_ids:
            self.record_seed_snapshot(inserted_ids)
        return inserted

    def update_relationship(self, relationship_id: Union[str, Tuple[str, str]], relationship_type: RelationshipType):
        if isinstance(relationship_id, Tuple):
            query = """
                UPDATE EDGE SET type = ?
                WHERE source = ? AND destination = ?
            """
            params = (relationship_type.value, relationship_id[0], relationship_id[1])
        else:
            query = """
                UPDATE EDGE SET type = ?
                WHERE id = ?
            """
            params = (relationship_type.value, relationship_id)
        with self.conn:
            try:
                self.conn.execute(query, params)
            except sl.OperationalError as e:
                raise(Exception(f"Error with query: '{query}'", e))

    def close(self, gen_sql=False):
        if gen_sql:
            sql_filename = self.out_dir / f"{self.basename}.sql"
            with sql_filename.open("w") as sql_out:
                for line in self.conn.iterdump():
                    sql_out.write(f"{line}\n")
        self.conn.close()

    def _record_checkpoint_metadata(self, iteration: Optional[int], phase: str) -> None:
        snapshot = {
            "iteration": iteration,
            "phase": phase,
            "timestamp": self._now_iso(),
            "frontier": self.get_frontier_count(),
            "processing": self.get_processing_count(),
            "vertices": self.get_individual_count(),
            "frontier_preview": self.peek_frontier(5),
        }
        self._set_metadata("last_checkpoint", snapshot)

    def _set_metadata(self, key: str, value: Any) -> None:
        payload = json.dumps(value)
        self.conn.execute(
            """
            INSERT INTO JOB_METADATA (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, payload),
        )

    def _get_metadata(self, key: str) -> Optional[Any]:
        cursor = self.conn.execute(
            "SELECT value FROM JOB_METADATA WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return row[0]

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).replace(microsecond=0).isoformat()

    def _configure_connection(self):
        # enable WAL for better concurrency/durability without blocking readers
        self.conn.execute("PRAGMA journal_mode=WAL").fetchone()
        # prefer durability; FULL is SQLite default, keep explicit for clarity
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")

    def _ensure_schema(self):
        current_version = self._get_user_version()
        if current_version > CURRENT_SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported schema version {current_version}")
        with self.conn:
            self._create_base_tables()
            if current_version == 0:
                self._create_queue_tables()
            elif current_version == 1:
                self._create_queue_tables()
                self._migrate_v1_to_v2()
            else:
                self._create_queue_tables()
        if current_version in {0, 1}:
            self._set_user_version(CURRENT_SCHEMA_VERSION)

    def _get_user_version(self) -> int:
        cursor = self.conn.execute("PRAGMA user_version")
        return cursor.fetchone()[0]

    def _set_user_version(self, version: int) -> None:
        self.conn.execute(f"PRAGMA user_version={version}")
        self.conn.commit()

    def _load_starting_iter(self) -> int:
        active = self._get_metadata("active_iteration")
        if isinstance(active, int):
            return active

        with self.conn:
            cursor = self.conn.execute("SELECT MAX(iteration) FROM LOG WHERE iteration IS NOT NULL")
            result = cursor.fetchone()[0]
        if result is not None:
            return result + 1

        last_completed = self._get_metadata("last_completed_iteration")
        if isinstance(last_completed, int):
            return last_completed + 1
        return 0

    def _create_base_tables(self) -> None:
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
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS JOB_METADATA (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _create_queue_tables(self) -> None:
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {FRONTIER_TABLE} (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                fs_id VARCHAR(8) NOT NULL UNIQUE
            )
        """)
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PROCESSING_TABLE} (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                fs_id VARCHAR(8) NOT NULL UNIQUE
            )
        """)
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {FRONTIER_TABLE}_FS_ID_IDX ON {FRONTIER_TABLE}(fs_id)"
        )
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {PROCESSING_TABLE}_FS_ID_IDX ON {PROCESSING_TABLE}(fs_id)"
        )

    def _migrate_v1_to_v2(self) -> None:
        # Preserve legacy frontier/processing contents while moving to queue-backed tables.
        self.conn.execute(f"""
            INSERT OR IGNORE INTO {FRONTIER_TABLE} (fs_id)
            SELECT id FROM FRONTIER_VERTEX ORDER BY rowid
        """)
        self.conn.execute(f"""
            INSERT OR IGNORE INTO {PROCESSING_TABLE} (fs_id)
            SELECT id FROM PROCESSING ORDER BY rowid
        """)
        self.conn.execute("DROP TABLE IF EXISTS FRONTIER_VERTEX")
        self.conn.execute("DROP TABLE IF EXISTS PROCESSING")
