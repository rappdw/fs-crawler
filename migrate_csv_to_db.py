#!/usr/bin/env python3
"""
Migration script to convert CSV-based crawl data to SQLite database format.

This script reads the legacy CSV files (pre-v0.3.0) and creates a SQLite database
with the new schema. The original CSV files are left intact and unchanged.

Usage:
    python migrate_csv_to_db.py <csv_directory> <basename>

Example:
    python migrate_csv_to_db.py ./output my_crawl
    
This will read:
    - ./output/my_crawl.vertices.csv
    - ./output/my_crawl.edges.csv
    - ./output/my_crawl.spanning.edges.csv
    - ./output/my_crawl.frontier.edges.csv
    - ./output/my_crawl.frontier.vertices.csv
    - ./output/my_crawl.log.csv (optional)

And create:
    - ./output/my_crawl.db
"""

import csv
import sqlite3 as sl
import sys
from pathlib import Path
from typing import Optional


class CSVToDBMigrator:
    """Migrates legacy CSV crawl data to SQLite database format."""
    
    def __init__(self, output_dir: Path, basename: str):
        self.output_dir = output_dir
        self.basename = basename
        
        # CSV file paths
        self.vertices_csv = output_dir / f"{basename}.vertices.csv"
        self.edges_csv = output_dir / f"{basename}.edges.csv"
        self.spanning_edges_csv = output_dir / f"{basename}.spanning.edges.csv"
        self.frontier_edges_csv = output_dir / f"{basename}.frontier.edges.csv"
        self.frontier_vertices_csv = output_dir / f"{basename}.frontier.vertices.csv"
        self.log_csv = output_dir / f"{basename}.log.csv"
        
        # Database path
        self.db_path = output_dir / f"{basename}.db"
        
        # Statistics
        self.stats = {
            'vertices': 0,
            'edges': 0,
            'spanning_edges': 0,
            'frontier_edges': 0,
            'frontier_vertices': 0,
            'log_entries': 0
        }
    
    def validate_csv_files(self) -> bool:
        """Check that required CSV files exist."""
        required_files = [
            self.vertices_csv,
            self.edges_csv,
            self.spanning_edges_csv,
            self.frontier_edges_csv,
            self.frontier_vertices_csv
        ]
        
        missing_files = [f for f in required_files if not f.exists()]
        
        if missing_files:
            print("Error: Missing required CSV files:")
            for f in missing_files:
                print(f"  - {f}")
            return False
        
        if self.db_path.exists():
            print(f"Warning: Database file already exists: {self.db_path}")
            response = input("Overwrite? (yes/no): ").lower()
            if response != 'yes':
                print("Migration aborted.")
                return False
            self.db_path.unlink()
        
        return True
    
    def create_database_schema(self, conn: sl.Connection):
        """Create the SQLite database schema."""
        with conn:
            # Create VERTEX table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS VERTEX (
                id VARCHAR(8) NOT NULL PRIMARY KEY,
                color INTEGER, 
                surname STRING,
                given_name STRING,
                iteration INTEGER,
                lifespan STRING
            );
            """)
            
            # Create EDGE table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS EDGE (
                source VARCHAR(8),
                destination VARCHAR(8),
                type STRING,
                id VARCHAR(8)
            );
            """)
            
            # Create indices for EDGE table
            conn.execute("""
            CREATE INDEX IF NOT EXISTS EDGE_SOURCE_IDX ON EDGE(source)
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS EDGE_DESTINATION_IDX ON EDGE(destination)
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS EDGE_TYPE_IDX ON EDGE(type)
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS EDGE_ID_IDX ON EDGE(id)
            """)
            
            # Create FRONTIER_VERTEX table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS FRONTIER_VERTEX (
                id VARCHAR(8) NOT NULL PRIMARY KEY
            );
            """)
            
            # Create PROCESSING table (empty for migrated data)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS PROCESSING (
                id VARCHAR(8) NOT NULL PRIMARY KEY
            );
            """)
            
            # Create LOG table
            conn.execute("""
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
    
    def parse_name(self, name: str) -> tuple[str, str]:
        """
        Parse 'surname, given_name' format into separate fields.
        
        Args:
            name: Name in format "surname, given_name"
            
        Returns:
            Tuple of (surname, given_name)
        """
        if not name or name.strip() == '':
            return ('', '')
        
        parts = name.split(',', 1)
        if len(parts) == 2:
            return (parts[0].strip(), parts[1].strip())
        else:
            # If no comma, treat entire string as surname
            return (parts[0].strip(), '')
    
    def migrate_vertices(self, conn: sl.Connection):
        """Migrate vertices from CSV to database."""
        print(f"Migrating vertices from {self.vertices_csv}...")
        
        with open(self.vertices_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Handle both with and without # prefix in header
            if '#external_id' in reader.fieldnames:
                id_field = '#external_id'
            else:
                id_field = 'external_id'
            
            vertices = []
            for row in reader:
                external_id = row[id_field]
                color = int(row['color']) if row['color'] else None
                surname, given_name = self.parse_name(row['name'])
                iteration = int(row['iteration']) if row['iteration'] else None
                lifespan = row['lifespan']
                
                vertices.append((external_id, color, surname, given_name, iteration, lifespan))
                self.stats['vertices'] += 1
            
            # Batch insert
            with conn:
                conn.executemany(
                    "INSERT INTO VERTEX (id, color, surname, given_name, iteration, lifespan) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    vertices
                )
        
        print(f"  Migrated {self.stats['vertices']} vertices")
    
    def migrate_edges(self, conn: sl.Connection, csv_path: Path, stat_key: str):
        """
        Migrate edges from a CSV file to database.
        
        Args:
            conn: Database connection
            csv_path: Path to the CSV file
            stat_key: Key to update in stats dictionary
        """
        print(f"Migrating edges from {csv_path}...")
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Handle both with and without # prefix in header
            if '#source_vertex' in reader.fieldnames:
                source_field = '#source_vertex'
            else:
                source_field = 'source_vertex'
            
            edges = []
            for row in reader:
                source = row[source_field]
                destination = row['destination_vertex']
                rel_type = row['relationship_type']
                rel_id = row['relationship_id']
                
                edges.append((source, destination, rel_type, rel_id))
                self.stats[stat_key] += 1
            
            # Batch insert
            if edges:
                with conn:
                    conn.executemany(
                        "INSERT INTO EDGE (source, destination, type, id) VALUES (?, ?, ?, ?)",
                        edges
                    )
        
        print(f"  Migrated {self.stats[stat_key]} edges")
    
    def migrate_frontier_vertices(self, conn: sl.Connection):
        """Migrate frontier vertices from CSV to database."""
        print(f"Migrating frontier vertices from {self.frontier_vertices_csv}...")
        
        with open(self.frontier_vertices_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Handle both with and without # prefix in header
            if '#external_id' in reader.fieldnames:
                id_field = '#external_id'
            else:
                id_field = 'external_id'
            
            frontier_ids = []
            for row in reader:
                external_id = row[id_field]
                frontier_ids.append((external_id,))
                self.stats['frontier_vertices'] += 1
            
            # Batch insert
            if frontier_ids:
                with conn:
                    conn.executemany(
                        "INSERT INTO FRONTIER_VERTEX (id) VALUES (?)",
                        frontier_ids
                    )
        
        print(f"  Migrated {self.stats['frontier_vertices']} frontier vertices")
    
    def migrate_log(self, conn: sl.Connection):
        """Migrate log entries from CSV to database (if log file exists)."""
        if not self.log_csv.exists():
            print(f"No log file found at {self.log_csv}, skipping...")
            return
        
        print(f"Migrating log from {self.log_csv}...")
        
        with open(self.log_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Handle both with and without # prefix in header
            if '#iteration' in reader.fieldnames:
                iteration_field = '#iteration'
            else:
                iteration_field = 'iteration'
            
            log_entries = []
            for row in reader:
                iteration = int(row[iteration_field]) if row[iteration_field] else None
                duration = float(row['duration']) if row['duration'] else None
                vertices = int(row['vertices']) if row['vertices'] else None
                frontier = int(row['frontier']) if row['frontier'] else None
                edges = int(row['edges']) if row['edges'] else None
                spanning_edges = int(row['spanning_edges']) if row['spanning_edges'] else None
                frontier_edges = int(row['frontier_edges']) if row['frontier_edges'] else None
                
                log_entries.append((iteration, duration, vertices, frontier, edges, 
                                   spanning_edges, frontier_edges))
                self.stats['log_entries'] += 1
            
            # Batch insert
            if log_entries:
                with conn:
                    conn.executemany(
                        "INSERT INTO LOG (iteration, duration, vertices, frontier, edges, "
                        "spanning_edges, frontier_edges) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        log_entries
                    )
        
        print(f"  Migrated {self.stats['log_entries']} log entries")
    
    def migrate(self) -> bool:
        """
        Perform the complete migration.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.validate_csv_files():
            return False
        
        print(f"\nStarting migration to {self.db_path}\n")
        
        # Create in-memory database first
        conn = sl.connect(":memory:")
        
        try:
            # Create schema
            self.create_database_schema(conn)
            
            # Migrate all data
            self.migrate_vertices(conn)
            self.migrate_edges(conn, self.edges_csv, 'edges')
            self.migrate_edges(conn, self.spanning_edges_csv, 'spanning_edges')
            self.migrate_edges(conn, self.frontier_edges_csv, 'frontier_edges')
            self.migrate_frontier_vertices(conn)
            self.migrate_log(conn)
            
            # Save to disk
            print(f"\nSaving database to {self.db_path}...")
            disk_conn = sl.connect(self.db_path)
            conn.backup(disk_conn)
            disk_conn.close()
            
            # Print summary
            print("\n" + "="*60)
            print("Migration completed successfully!")
            print("="*60)
            print(f"Vertices:          {self.stats['vertices']:,}")
            print(f"Edges:             {self.stats['edges']:,}")
            print(f"Spanning edges:    {self.stats['spanning_edges']:,}")
            print(f"Frontier edges:    {self.stats['frontier_edges']:,}")
            print(f"Frontier vertices: {self.stats['frontier_vertices']:,}")
            print(f"Log entries:       {self.stats['log_entries']:,}")
            print(f"\nDatabase saved to: {self.db_path}")
            print("\nNote: PROCESSING table is empty (will be auto-populated from")
            print("      FRONTIER_VERTEX when crawl resumes)")
            print("="*60)
            
            return True
            
        except Exception as e:
            print(f"\nError during migration: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            conn.close()


def main():
    """Main entry point for the migration script."""
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    
    output_dir = Path(sys.argv[1])
    basename = sys.argv[2]
    
    if not output_dir.exists():
        print(f"Error: Directory does not exist: {output_dir}")
        sys.exit(1)
    
    if not output_dir.is_dir():
        print(f"Error: Not a directory: {output_dir}")
        sys.exit(1)
    
    migrator = CSVToDBMigrator(output_dir, basename)
    success = migrator.migrate()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
