"""Tests for the CSV to SQLite database migration script."""

import csv
import sqlite3 as sl
import tempfile
from pathlib import Path

import pytest

# Import the migrator - use relative import
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from migrate_csv_to_db import CSVToDBMigrator


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_csv_files(temp_dir):
    """Create sample CSV files for testing."""
    basename = "test_crawl"
    
    # Create vertices.csv
    vertices_file = temp_dir / f"{basename}.vertices.csv"
    with open(vertices_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#external_id', 'color', 'name', 'iteration', 'lifespan'])
        writer.writerow(['KWQG-123', '1', 'Smith, John', '0', '1800-1850'])
        writer.writerow(['KWQG-456', '2', 'Doe, Jane', '0', '1820-1870'])
        writer.writerow(['KWQG-789', '1', 'Brown, Bob', '1', '1845-1900'])
    
    # Create edges.csv
    edges_file = temp_dir / f"{basename}.edges.csv"
    with open(edges_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
        writer.writerow(['KWQG-789', 'KWQG-123', 'BiologicalParent', 'REL-001'])
        writer.writerow(['KWQG-789', 'KWQG-456', 'BiologicalParent', 'REL-002'])
    
    # Create spanning.edges.csv
    spanning_file = temp_dir / f"{basename}.spanning.edges.csv"
    with open(spanning_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
        writer.writerow(['KWQG-999', 'KWQG-123', 'UntypedParent', 'REL-003'])
    
    # Create frontier.edges.csv
    frontier_edges_file = temp_dir / f"{basename}.frontier.edges.csv"
    with open(frontier_edges_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
        writer.writerow(['KWQG-111', 'KWQG-222', 'UntypedParent', 'REL-004'])
    
    # Create frontier.vertices.csv
    frontier_vertices_file = temp_dir / f"{basename}.frontier.vertices.csv"
    with open(frontier_vertices_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#external_id'])
        writer.writerow(['KWQG-999'])
        writer.writerow(['KWQG-111'])
        writer.writerow(['KWQG-222'])
    
    # Create log.csv
    log_file = temp_dir / f"{basename}.log.csv"
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#iteration', 'duration', 'vertices', 'frontier', 'edges', 'spanning_edges', 'frontier_edges'])
        writer.writerow(['0', '1.5', '2', '3', '2', '1', '1'])
        writer.writerow(['1', '2.3', '3', '3', '2', '1', '1'])
    
    return temp_dir, basename


def test_migration_creates_database(sample_csv_files):
    """Test that migration creates a database file."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    success = migrator.migrate()
    
    assert success
    assert migrator.db_path.exists()
    assert migrator.db_path.is_file()


def test_migration_vertices(sample_csv_files):
    """Test that vertices are migrated correctly."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    migrator.migrate()
    
    conn = sl.connect(migrator.db_path)
    cursor = conn.execute("SELECT * FROM VERTEX ORDER BY id")
    rows = cursor.fetchall()
    conn.close()
    
    assert len(rows) == 3
    
    # Check first vertex
    assert rows[0][0] == 'KWQG-123'  # id
    assert rows[0][1] == 1  # color (male)
    assert rows[0][2] == 'Smith'  # surname
    assert rows[0][3] == 'John'  # given_name
    assert rows[0][4] == 0  # iteration
    assert rows[0][5] == '1800-1850'  # lifespan
    
    # Check second vertex
    assert rows[1][0] == 'KWQG-456'
    assert rows[1][1] == 2  # color (female)
    assert rows[1][2] == 'Doe'
    assert rows[1][3] == 'Jane'
    
    # Check statistics
    assert migrator.stats['vertices'] == 3


def test_migration_edges(sample_csv_files):
    """Test that all edge types are migrated correctly."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    migrator.migrate()
    
    conn = sl.connect(migrator.db_path)
    
    # Check total edges
    cursor = conn.execute("SELECT COUNT(*) FROM EDGE")
    total_edges = cursor.fetchone()[0]
    assert total_edges == 4  # 2 edges + 1 spanning + 1 frontier
    
    # Check regular edges
    cursor = conn.execute("SELECT * FROM EDGE WHERE source = 'KWQG-789' ORDER BY destination")
    edges = cursor.fetchall()
    assert len(edges) == 2
    assert edges[0][1] == 'KWQG-123'  # destination
    assert edges[0][2] == 'BiologicalParent'  # type
    assert edges[0][3] == 'REL-001'  # id
    
    # Check spanning edge
    cursor = conn.execute("SELECT * FROM EDGE WHERE source = 'KWQG-999'")
    spanning = cursor.fetchone()
    assert spanning is not None
    assert spanning[2] == 'UntypedParent'
    
    # Check frontier edge
    cursor = conn.execute("SELECT * FROM EDGE WHERE source = 'KWQG-111'")
    frontier = cursor.fetchone()
    assert frontier is not None
    assert frontier[1] == 'KWQG-222'
    
    conn.close()
    
    # Check statistics
    assert migrator.stats['edges'] == 2
    assert migrator.stats['spanning_edges'] == 1
    assert migrator.stats['frontier_edges'] == 1


def test_migration_frontier_vertices(sample_csv_files):
    """Test that frontier vertices are migrated correctly."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    migrator.migrate()
    
    conn = sl.connect(migrator.db_path)
    cursor = conn.execute("SELECT id FROM FRONTIER_VERTEX ORDER BY id")
    frontier_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    assert len(frontier_ids) == 3
    assert 'KWQG-999' in frontier_ids
    assert 'KWQG-111' in frontier_ids
    assert 'KWQG-222' in frontier_ids
    
    assert migrator.stats['frontier_vertices'] == 3


def test_migration_log(sample_csv_files):
    """Test that log entries are migrated correctly."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    migrator.migrate()
    
    conn = sl.connect(migrator.db_path)
    cursor = conn.execute("SELECT * FROM LOG ORDER BY iteration")
    log_entries = cursor.fetchall()
    conn.close()
    
    assert len(log_entries) == 2
    
    # Check first log entry
    assert log_entries[0][0] == 0  # iteration
    assert log_entries[0][1] == 1.5  # duration
    assert log_entries[0][2] == 2  # vertices
    assert log_entries[0][3] == 3  # frontier
    assert log_entries[0][4] == 2  # edges
    assert log_entries[0][5] == 1  # spanning_edges
    assert log_entries[0][6] == 1  # frontier_edges
    
    # Check second log entry
    assert log_entries[1][0] == 1
    assert log_entries[1][1] == 2.3
    
    assert migrator.stats['log_entries'] == 2


def test_migration_without_log_file(temp_dir):
    """Test that migration works even if log file is missing."""
    basename = "no_log_crawl"
    
    # Create only required files (no log.csv)
    vertices_file = temp_dir / f"{basename}.vertices.csv"
    with open(vertices_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#external_id', 'color', 'name', 'iteration', 'lifespan'])
        writer.writerow(['KWQG-123', '1', 'Smith, John', '0', '1800-1850'])
    
    for suffix in ['edges', 'spanning.edges', 'frontier.edges']:
        file_path = temp_dir / f"{basename}.{suffix}.csv"
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
    
    frontier_file = temp_dir / f"{basename}.frontier.vertices.csv"
    with open(frontier_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['#external_id'])
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    success = migrator.migrate()
    
    assert success
    assert migrator.stats['log_entries'] == 0


def test_parse_name():
    """Test name parsing functionality."""
    migrator = CSVToDBMigrator(Path('.'), 'test')
    
    # Standard format
    surname, given = migrator.parse_name('Smith, John')
    assert surname == 'Smith'
    assert given == 'John'
    
    # With extra spaces
    surname, given = migrator.parse_name('  Smith  ,  John  ')
    assert surname == 'Smith'
    assert given == 'John'
    
    # No comma (surname only)
    surname, given = migrator.parse_name('Smith')
    assert surname == 'Smith'
    assert given == ''
    
    # Empty string
    surname, given = migrator.parse_name('')
    assert surname == ''
    assert given == ''
    
    # Multiple commas (only split on first)
    surname, given = migrator.parse_name('Smith, John, Jr.')
    assert surname == 'Smith'
    assert given == 'John, Jr.'


def test_migration_validates_files(temp_dir):
    """Test that migration validates required files exist."""
    basename = "missing_files"
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    success = migrator.validate_csv_files()
    
    assert not success  # Should fail because files don't exist


def test_database_schema_created(sample_csv_files):
    """Test that all tables and indexes are created."""
    temp_dir, basename = sample_csv_files
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    migrator.migrate()
    
    conn = sl.connect(migrator.db_path)
    
    # Check tables exist
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    expected_tables = ['EDGE', 'FRONTIER_VERTEX', 'LOG', 'PROCESSING', 'VERTEX']
    assert sorted(tables) == sorted(expected_tables)
    
    # Check indexes exist
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
    indexes = [row[0] for row in cursor.fetchall()]
    
    expected_indexes = [
        'EDGE_SOURCE_IDX',
        'EDGE_DESTINATION_IDX',
        'EDGE_TYPE_IDX',
        'EDGE_ID_IDX'
    ]
    
    for expected_index in expected_indexes:
        assert expected_index in indexes
    
    conn.close()


def test_csv_without_hash_prefix(temp_dir):
    """Test that CSV files without # prefix in headers work correctly."""
    basename = "no_hash"
    
    # Create vertices.csv without # prefix
    vertices_file = temp_dir / f"{basename}.vertices.csv"
    with open(vertices_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['external_id', 'color', 'name', 'iteration', 'lifespan'])
        writer.writerow(['KWQG-123', '1', 'Smith, John', '0', '1800-1850'])
    
    # Create edges files without # prefix
    for suffix in ['edges', 'spanning.edges', 'frontier.edges']:
        file_path = temp_dir / f"{basename}.{suffix}.csv"
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
    
    # Create frontier vertices without # prefix
    frontier_file = temp_dir / f"{basename}.frontier.vertices.csv"
    with open(frontier_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['external_id'])
    
    migrator = CSVToDBMigrator(temp_dir, basename)
    success = migrator.migrate()
    
    assert success
    assert migrator.stats['vertices'] == 1
