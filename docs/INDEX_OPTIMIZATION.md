# Database Index Optimization

## Overview

The SQLite database includes both single-column and composite indices optimized for different query patterns used throughout the codebase.

## Index Strategy

### Single-Column Indices

These indices support basic lookups and joins:

```sql
CREATE INDEX EDGE_SOURCE_IDX ON EDGE(source);
CREATE INDEX EDGE_DESTINATION_IDX ON EDGE(destination);
CREATE INDEX EDGE_TYPE_IDX ON EDGE(type);
CREATE INDEX EDGE_ID_IDX ON EDGE(id);
```

**Use cases**:
- Basic edge lookups by source or destination
- Filtering by relationship type
- Relationship ID lookups

### Composite Indices

These indices optimize the common query pattern in `db_reader.py`:

```sql
CREATE INDEX EDGE_TYPE_SOURCE_IDX ON EDGE(type, source);
CREATE INDEX EDGE_TYPE_DEST_IDX ON EDGE(type, destination);
```

**Use cases**:
- Queries that filter by relationship type AND join on source/destination
- Graph traversal operations that only follow specific relationship types

## Performance Impact

### Without Composite Indices

SQLite must:
1. Scan `EDGE_TYPE_IDX` for matching types (e.g., `BiologicalParent`)
2. For each match, look up the source/destination in separate index
3. Perform the join operation

For large graphs (100K+ edges), this results in:
- Multiple index seeks per matching edge
- Higher I/O operations
- Slower query execution

### With Composite Indices

SQLite can:
1. Directly scan `(type, source)` or `(type, destination)` pairs
2. Single index lookup gets both the type filter and join key
3. Dramatically reduced index seeks

**Expected speedup**: 5-10x for queries like those in `db_reader.py`

## Query Patterns Optimized

### 1. Unordered Edge Count

```sql
-- From db_reader.py line 6-14
select count(*)
from VERTEX
join (
    select VERTEX.ROWID as src_id, destination from VERTEX
    JOIN EDGE on source = VERTEX.ID
    WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
) ON VERTEX.id = destination
```

**Optimized by**: `EDGE_TYPE_SOURCE_IDX` - filters by type then joins on source

### 2. Unordered Edges

```sql
-- From db_reader.py line 16-24
select src_id, VERTEX.ROWID as dst_id
from VERTEX
join (
    select VERTEX.ROWID as src_id, destination from VERTEX
    JOIN EDGE on source = VERTEX.ID
    WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
) ON VERTEX.id = destination
```

**Optimized by**: `EDGE_TYPE_SOURCE_IDX` for the subquery, `EDGE_TYPE_DEST_IDX` for the outer join

### 3. Ordered Edges

```sql
-- From db_reader.py line 31-41
select src_position, position as dst_position
from VERTEX
join (
    select position as src_position, destination from VERTEX
    JOIN EDGE on source = VERTEX.ID
    JOIN ORDERING on ORDERING.id = VERTEX.ROWID
    WHERE EDGE.type in ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
) ON VERTEX.id = destination
```

**Optimized by**: Both composite indices support the type-filtered joins

## Index Maintenance

### New Databases

Indices are automatically created when a new database is initialized in `GraphDbImpl.__init__()`.

### Migrated Databases

The migration script (`migrate_csv_to_db.py`) creates all indices when converting CSV files to SQLite format.

### Existing Databases

For existing databases without composite indices, you can add them manually:

```bash
sqlite3 your_crawl.db << EOF
CREATE INDEX IF NOT EXISTS EDGE_TYPE_SOURCE_IDX ON EDGE(type, source);
CREATE INDEX IF NOT EXISTS EDGE_TYPE_DEST_IDX ON EDGE(type, destination);
EOF
```

## Index Size Considerations

### Storage Impact

Composite indices require additional storage:
- Each composite index is roughly 1.5x the size of a single-column index
- For a database with 1M edges:
  - Single index: ~20-30 MB
  - Composite index: ~30-45 MB
  
Total additional storage: ~60-90 MB for 1M edges

### Trade-off

The storage cost is minimal compared to the query performance benefits:
- **Storage**: +5-10% database size
- **Performance**: 5-10x faster type-filtered queries

## Verification

### Check Indices Exist

```sql
SELECT name, sql 
FROM sqlite_master 
WHERE type='index' AND name LIKE 'EDGE%'
ORDER BY name;
```

Expected output:
```
EDGE_DESTINATION_IDX | CREATE INDEX EDGE_DESTINATION_IDX ON EDGE(destination)
EDGE_ID_IDX          | CREATE INDEX EDGE_ID_IDX ON EDGE(id)
EDGE_SOURCE_IDX      | CREATE INDEX EDGE_SOURCE_IDX ON EDGE(source)
EDGE_TYPE_DEST_IDX   | CREATE INDEX EDGE_TYPE_DEST_IDX ON EDGE(type, destination)
EDGE_TYPE_IDX        | CREATE INDEX EDGE_TYPE_IDX ON EDGE(type)
EDGE_TYPE_SOURCE_IDX | CREATE INDEX EDGE_TYPE_SOURCE_IDX ON EDGE(type, source)
```

### Verify Index Usage

Use `EXPLAIN QUERY PLAN` to confirm the index is being used:

```sql
EXPLAIN QUERY PLAN
SELECT * FROM EDGE 
WHERE type = 'BiologicalParent' AND source = 'KWQG-123';
```

Should show:
```
SEARCH TABLE EDGE USING INDEX EDGE_TYPE_SOURCE_IDX (type=? AND source=?)
```

## References

- SQLite documentation: https://www.sqlite.org/optoverview.html
- Composite index benefits: https://use-the-index-luke.com/sql/where-clause/the-equals-operator/concatenated-keys
- Implementation: `fscrawler/model/graph_db_impl.py`, `migrate_csv_to_db.py`
