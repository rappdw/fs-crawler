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
        assert cursor.fetchone()[0] == 1

        # journal mode should stay WAL for resumed connections.
        cursor = graph_reopened.conn.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0].lower() == "wal"
    finally:
        graph_reopened.close()


def test_graph_db_impl_resume_iteration(tmp_path, sample_person):
    graph_path = tmp_path / "graph"
    graph = GraphDbImpl(graph_path, "resume")

    graph.add_to_frontier("P1")
    graph.start_iteration()
    graph.add_individual(Individual(sample_person, iteration=0))
    graph.end_iteration(0, duration=1.0)
    graph.close()

    reopened = GraphDbImpl(graph_path, "resume")
    try:
        assert reopened.starting_iter == 1
    finally:
        reopened.close()
