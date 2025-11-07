# CSV to SQLite Database Migration Guide

This guide explains how to migrate legacy CSV-based crawl data (pre-v0.3.0) to the new SQLite database format.

## Background

In v0.3.0 (commit `e001af5`), the fs-crawler moved from storing crawl data in CSV files to using a SQLite database for better performance and scalability. This migration script helps convert existing CSV-based crawls to the new format.

## CSV Files (Legacy Format)

The old format used these CSV files:

- **`{basename}.vertices.csv`** - Individual records
  - Columns: `#external_id`, `color`, `name`, `iteration`, `lifespan`
  - Name format: `"surname, given_name"`

- **`{basename}.edges.csv`** - Relationships within the graph
  - Columns: `#source_vertex`, `destination_vertex`, `relationship_type`, `relationship_id`

- **`{basename}.spanning.edges.csv`** - Relationships spanning graph boundary
  - Same format as edges.csv

- **`{basename}.frontier.edges.csv`** - Relationships beyond graph boundary
  - Same format as edges.csv

- **`{basename}.frontier.vertices.csv`** - Frontier individuals to crawl next
  - Columns: `#external_id`

- **`{basename}.log.csv`** - Crawl iteration statistics (optional)
  - Columns: `#iteration`, `duration`, `vertices`, `frontier`, `edges`, `spanning_edges`, `frontier_edges`

## SQLite Database (New Format)

The new format uses a single SQLite database with these tables:

- **`VERTEX`** - Individual records
  - Columns: `id`, `color`, `surname`, `given_name`, `iteration`, `lifespan`
  - Note: Name is split into separate surname and given_name fields

- **`EDGE`** - All relationships (edges, spanning, and frontier combined)
  - Columns: `source`, `destination`, `type`, `id`
  - Indexed on: source, destination, type, id

- **`FRONTIER_VERTEX`** - Frontier individuals
  - Columns: `id`

- **`PROCESSING`** - Current processing queue (empty after migration)
  - Columns: `id`

- **`LOG`** - Crawl iteration statistics
  - Columns: `iteration`, `duration`, `vertices`, `frontier`, `edges`, `spanning_edges`, `frontier_edges`

## Migration Instructions

### Prerequisites

- Python 3.10 or later
- Original CSV files intact and readable

### Steps

1. **Locate your CSV files**
   ```bash
   ls -la /path/to/output/directory/
   ```
   
   You should see files like:
   ```
   my_crawl.vertices.csv
   my_crawl.edges.csv
   my_crawl.spanning.edges.csv
   my_crawl.frontier.edges.csv
   my_crawl.frontier.vertices.csv
   my_crawl.log.csv (optional)
   ```

2. **Run the migration script**
   ```bash
   python migrate_csv_to_db.py /path/to/output/directory my_crawl
   ```
   
   Replace:
   - `/path/to/output/directory` with your actual output directory
   - `my_crawl` with your actual basename

3. **Review the output**
   
   The script will display:
   - Validation messages
   - Progress for each file being migrated
   - Summary statistics
   - Final database location

   Example output:
   ```
   Starting migration to /path/to/output/my_crawl.db
   
   Migrating vertices from /path/to/output/my_crawl.vertices.csv...
     Migrated 1,234 vertices
   Migrating edges from /path/to/output/my_crawl.edges.csv...
     Migrated 2,345 edges
   ...
   
   ============================================================
   Migration completed successfully!
   ============================================================
   Vertices:          1,234
   Edges:             2,345
   Spanning edges:    567
   Frontier edges:    890
   Frontier vertices: 456
   Log entries:       5
   
   Database saved to: /path/to/output/my_crawl.db
   ============================================================
   ```

4. **Verify the migration**
   
   You can verify the database was created correctly:
   ```bash
   sqlite3 /path/to/output/my_crawl.db "SELECT COUNT(*) FROM VERTEX;"
   sqlite3 /path/to/output/my_crawl.db "SELECT COUNT(*) FROM EDGE;"
   ```

5. **Use the migrated database**
   
   The new database can now be used with fs-crawler v0.3.0 or later:
   ```bash
   fscrawler --restart /path/to/output my_crawl
   ```

## Important Notes

### Original CSV Files Preserved

The migration script **does not modify or delete** your original CSV files. They remain intact for backup purposes. You can safely delete them after verifying the migration was successful.

### Database Overwrite Protection

If a database file already exists at the target location, the script will ask for confirmation before overwriting it.

### Name Parsing

The script automatically splits the `name` field from the CSV format (`"surname, given_name"`) into separate `surname` and `given_name` fields in the database. If a name doesn't contain a comma, it will be stored as surname only.

### PROCESSING Table

The `PROCESSING` table is created empty during migration. This is correct because:

1. **Transient state**: PROCESSING represents the current working queue during an active iteration
2. **No source data**: The old CSV format never included a processing state
3. **Auto-populated**: When resuming a crawl, `graph.start_iteration()` automatically moves `FRONTIER_VERTEX → PROCESSING`

The migrated database represents a "checkpointed" state with no active processing, which is the proper state for a paused crawl ready to resume.

**State Flow**:
```
Migration:     FRONTIER_VERTEX populated from CSV
               PROCESSING empty ✓

Resume crawl:  start_iteration() called
               FRONTIER_VERTEX → PROCESSING
               
During crawl:  Fetch individual data
               PROCESSING → VERTEX (when complete)
               New relationships add to FRONTIER_VERTEX
```

### Error Handling

If the migration encounters an error:
- An error message and stack trace will be displayed
- The database file will not be created (or will be incomplete)
- Original CSV files remain unchanged
- You can fix the issue and retry the migration

## Troubleshooting

### "Missing required CSV files"

Ensure all required CSV files exist in the specified directory:
- vertices.csv
- edges.csv
- spanning.edges.csv
- frontier.edges.csv
- frontier.vertices.csv

### "Database file already exists"

The script detected an existing database file. Either:
- Type `yes` to overwrite it
- Type `no` to abort and manually handle the existing file

### Encoding Issues

If you encounter encoding errors, ensure your CSV files are UTF-8 encoded.

### Performance

Migration speed depends on the size of your crawl:
- Small crawls (< 10K vertices): < 1 second
- Medium crawls (10K-100K vertices): 1-10 seconds
- Large crawls (> 100K vertices): 10-60 seconds

## Technical Details

### Schema Differences

| CSV Format | Database Format | Notes |
|------------|-----------------|-------|
| name (single field) | surname + given_name | Split on first comma |
| Multiple edge files | Single EDGE table | All edges combined |
| Headers with `#` prefix | Headers without prefix | Both formats supported |

### Indexes

The script creates indexes on the EDGE table for optimal query performance:

**Single-column indices**:
- `EDGE_SOURCE_IDX` on `source`
- `EDGE_DESTINATION_IDX` on `destination`
- `EDGE_TYPE_IDX` on `type`
- `EDGE_ID_IDX` on `id`

**Composite indices** (optimized for `db_reader.py` queries):
- `EDGE_TYPE_SOURCE_IDX` on `(type, source)` - optimizes type-filtered source joins
- `EDGE_TYPE_DEST_IDX` on `(type, destination)` - optimizes type-filtered destination joins

These composite indices dramatically improve performance for graph reading operations that filter by relationship type (e.g., `AssumedBiological`, `BiologicalParent`, `UnspecifiedParentType`).

### In-Memory Processing

The migration uses an in-memory SQLite database during processing and only writes to disk at the end. This provides:
- Better performance
- Atomic operation (all-or-nothing)
- No partial database files on error
