import copy
import sqlite3

import pytest

from fscrawler.model.graph_db_impl import GraphDbImpl
from fscrawler.model.individual import Individual


@pytest.fixture
def sample_person():
    return {
        "id": "P1",
        "living": False,
        "names": [
            {
                "preferred": True,
                "nameForms": [
                    {
                        "parts": [
                            {"type": "http://gedcomx.org/Given", "value": "Pat"},
                            {"type": "http://gedcomx.org/Surname", "value": "Tester"},
                        ]
                    }
                ],
            }
        ],
        "gender": {"type": "http://gedcomx.org/Male"},
        "display": {"lifespan": "1900-1980"},
    }


def test_graph_db_impl_persists_and_sets_wal(tmp_path, sample_person):
    graph_path = tmp_path / "graph"
    graph = GraphDbImpl(graph_path, "run")

    # Insert an individual and frontier entry, then close to force disk persistence.
    graph.add_to_frontier("P2")
    graph.add_individual(Individual(sample_person, iteration=0))
    graph.close()

    db_file = graph_path / "run.db"
    assert db_file.exists()

    # Re-open the database and ensure data survived.
    graph_reopened = GraphDbImpl(graph_path, "run")
    try:
        assert graph_reopened.is_individual_in_graph("P1")
        assert graph_reopened.get_frontier_count() == 1

        # user_version should reflect the migration we set.
        cursor = graph_reopened.conn.execute("PRAGMA user_version")
        assert cursor.fetchone()[0] == 2

        # journal mode should stay WAL for resumed connections.
        cursor = graph_reopened.conn.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0].lower() == "wal"
    finally:
        graph_reopened.close()


def test_graph_db_impl_resume_iteration(tmp_path, sample_person):
    graph_path = tmp_path / "graph"
    graph = GraphDbImpl(graph_path, "resume")

    graph.add_to_frontier("P1")
    graph.add_to_frontier("P2")
    graph.start_iteration()
    graph.add_individual(Individual(sample_person, iteration=0))
    second = copy.deepcopy(sample_person)
    second["id"] = "P2"
    graph.add_individual(Individual(second, iteration=0))
    graph.end_iteration(0, duration=1.0)
    graph.close()

    reopened = GraphDbImpl(graph_path, "resume")
    try:
        assert reopened.starting_iter == 1
        assert reopened.peek_frontier(1) == tuple()
    finally:
        reopened.close()


def test_frontier_fifo_and_seed_utilities(tmp_path):
    graph = GraphDbImpl(tmp_path, "fifo")
    try:
        inserted = graph.seed_frontier_if_empty(["A", "B", "C"])
        assert inserted == 3
        assert graph.peek_frontier(2) == ("A", "B")

        # Adding more when not empty should not reseed.
        assert graph.seed_frontier_if_empty(["D"]) == 0

        graph.start_iteration()
        ids = list(graph.get_ids_to_process())
        assert ids == ["A", "B", "C"]
        # Process the first id and ensure FIFO removal from processing queue.
        graph.add_individual(Individual({
            "id": "A",
            "living": False,
            "names": [
                {
                    "preferred": True,
                    "nameForms": [
                        {
                            "parts": [
                                {"type": "http://gedcomx.org/Given", "value": "A"},
                                {"type": "http://gedcomx.org/Surname", "value": "Person"},
                            ]
                        }
                    ],
                }
            ],
            "gender": {"type": "http://gedcomx.org/Male"},
            "display": {"lifespan": "1900-1950"},
        }, iteration=0))
        remaining = list(graph.get_ids_to_process())
        assert remaining == ["B", "C"]
    finally:
        graph.close()


def test_migrates_existing_v1_schema(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            PRAGMA user_version=1;
            CREATE TABLE VERTEX (
                id VARCHAR(8) PRIMARY KEY,
                color INTEGER,
                surname STRING,
                given_name STRING,
                iteration INTEGER,
                lifespan STRING
            );
            CREATE TABLE EDGE (
                source VARCHAR(8),
                destination VARCHAR(8),
                type STRING,
                id VARCHAR(8)
            );
            CREATE TABLE FRONTIER_VERTEX (id VARCHAR(8) PRIMARY KEY);
            CREATE TABLE PROCESSING (id VARCHAR(8) PRIMARY KEY);
            CREATE TABLE LOG (
                iteration INTEGER,
                duration FLOAT,
                vertices INTEGER,
                frontier INTEGER,
                edges INTEGER,
                spanning_edges INTEGER,
                frontier_edges INTEGER
            );
            INSERT INTO FRONTIER_VERTEX (id) VALUES ('F1'), ('F2');
            INSERT INTO PROCESSING (id) VALUES ('P1');
            """
        )
    finally:
        conn.close()

    graph = GraphDbImpl(tmp_path, "legacy")
    try:
        assert graph.peek_frontier(5) == ("F1", "F2")
        assert list(graph.get_ids_to_process()) == ["P1"]
        cursor = graph.conn.execute("PRAGMA user_version")
        assert cursor.fetchone()[0] == 2
    finally:
        graph.close()
