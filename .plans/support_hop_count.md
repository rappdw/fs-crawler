# Hop Count Support Specification for RelationshipDbReader

## Problem Statement

The `RelationshipDbReader` class in `fscrawler/util/db_reader.py` accepts a `hops` parameter but does not implement hop-based filtering. Currently, it reads the entire database regardless of the specified hop count value, leading to memory allocation errors when working with large graphs.

**Current Behavior:**
- Line 62 has a `TODO: support hops` comment
- Line 73 counts ALL vertices: `SELECT COUNT(*) FROM VERTEX`
- Line 74 counts ALL edges without hop filtering
- This causes memory issues when the full graph is millions of vertices but the user only needs a subgraph within N hops

**Example Error:**
```
numpy._core._exceptions._ArrayMemoryError: Unable to allocate 73.7 TiB for an array 
with shape (4502136, 4502136) and data type int32
```
Expected: ~200x200 graph with hopcount=4
Actual: 4,502,136 x 4,502,136 graph (entire database)

## Solution Approach

Implement hop-based filtering using a breadth-first search (BFS) traversal from root node(s) to identify all vertices within N hops, then filter vertices and edges to include only those within the hop limit.

### Key Requirements

1. **Identify Root Nodes**: Determine which vertices are root nodes (vertices with no incoming edges)
2. **BFS Traversal**: Perform BFS from root nodes up to N hops to collect reachable vertices
3. **Filter Queries**: Update SQL queries to only select vertices and edges within the hop-limited subgraph
4. **Maintain Ordering**: Ensure canonical ordering still works with the filtered subgraph
5. **Backwards Compatibility**: Ensure existing behavior is preserved when hops parameter is not specified or is None

### Implementation Strategy

#### Phase 1: Add Hop-Limited Vertex Collection

Create a new method to identify vertices within N hops:

```python
def _get_vertices_within_hops(self, conn, hops: int) -> set:
    """
    Perform BFS from root nodes to find all vertices within N hops.
    
    Returns:
        Set of vertex ROWIDs that are within the hop limit
    """
    # Step 1: Find root nodes (vertices with no incoming edges)
    root_query = """
        SELECT ROWID FROM VERTEX 
        WHERE ROWID NOT IN (
            SELECT DISTINCT VERTEX.ROWID 
            FROM VERTEX
            JOIN EDGE ON VERTEX.ID = EDGE.destination
            WHERE EDGE.type IN ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
        )
    """
    
    # Step 2: BFS traversal
    # For each level (0 to hops):
    #   - Track vertices at current level
    #   - Query for children of current level vertices
    #   - Add children to next level
    #   - Accumulate all vertices seen
    
    # Return set of all vertex ROWIDs within hop limit
```

#### Phase 2: Update Vertex and Edge Counting

Modify the counting queries to respect hop limits:

```python
def read(self):
    conn = sl.connect(self.db_file)
    
    if self.hops is not None:
        # Get filtered vertices
        vertices_in_scope = self._get_vertices_within_hops(conn, self.hops)
        
        # Count only vertices within hop limit
        nv = len(vertices_in_scope)
        
        # Count only edges between vertices within hop limit
        # (both source and destination must be in scope)
        ne = self._count_edges_in_scope(conn, vertices_in_scope)
    else:
        # Original behavior: count all vertices and edges
        nv = conn.execute("SELECT COUNT(*) FROM VERTEX").fetchone()[0]
        ne = conn.execute(unordered_edge_count).fetchone()[0]
    
    # Continue with existing logic...
```

#### Phase 3: Filter SQL Queries

Create filtered versions of the SQL queries that only select vertices/edges within scope:

```python
def _create_filtered_queries(self, vertices_in_scope: set) -> dict:
    """
    Generate SQL queries filtered to only include vertices within scope.
    
    Args:
        vertices_in_scope: Set of vertex ROWIDs to include
    
    Returns:
        Dictionary with filtered query strings
    """
    # Convert set to comma-separated string for SQL IN clause
    vertex_list = ','.join(map(str, vertices_in_scope))
    
    filtered_unordered_vertices = f"""
        SELECT ROWID, color FROM VERTEX 
        WHERE ROWID IN ({vertex_list});
    """
    
    filtered_unordered_edges = f"""
        SELECT src_id, VERTEX.ROWID as dst_id
        FROM VERTEX
        JOIN (
            SELECT VERTEX.ROWID as src_id, destination FROM VERTEX
            JOIN EDGE ON source = VERTEX.ID
            WHERE EDGE.type IN ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
              AND VERTEX.ROWID IN ({vertex_list})
        ) ON VERTEX.id = destination
        WHERE VERTEX.ROWID IN ({vertex_list})
        ORDER BY src_id;
    """
    
    # Similar filtering for ordered_vertices and ordered_edges
    # ...
    
    return {
        'vertices': filtered_unordered_vertices,
        'edges': filtered_unordered_edges,
        # ...
    }
```

#### Phase 4: Update _read_graph Method

Modify `_read_graph` to accept custom query strings:

```python
def _read_graph(self, conn, vertices_query, edges_query):
    """Read graph using provided queries (allows filtering)."""
    # Existing implementation using the provided queries
```

### Detailed Algorithm: BFS for Hop-Limited Traversal

```python
def _get_vertices_within_hops(self, conn, hops: int) -> set:
    """
    Perform BFS from root nodes to find all vertices within N hops.
    
    Algorithm:
    1. Find all root nodes (no incoming edges)
    2. Initialize current_level with root nodes (hop 0)
    3. For hop_count from 0 to hops-1:
       a. Query for children of all vertices in current_level
       b. Add children to next_level
       c. Add next_level to seen_vertices
       d. Set current_level = next_level
    4. Return all seen_vertices
    
    Returns:
        Set of vertex ROWIDs that are within the hop limit
    """
    if hops < 0:
        return set()
    
    # Find root nodes
    root_query = """
        SELECT ROWID FROM VERTEX 
        WHERE ROWID NOT IN (
            SELECT DISTINCT VERTEX.ROWID 
            FROM VERTEX
            JOIN EDGE ON VERTEX.ID = EDGE.destination
            WHERE EDGE.type IN ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
        )
    """
    roots = {row[0] for row in conn.execute(root_query).fetchall()}
    
    if not roots:
        self.logger.warning("No root nodes found in database")
        return set()
    
    seen_vertices = set(roots)
    current_level = roots
    
    # BFS traversal
    for hop in range(hops):
        if not current_level:
            break
        
        # Get children of all vertices in current level
        vertex_list = ','.join(map(str, current_level))
        children_query = f"""
            SELECT DISTINCT VERTEX.ROWID
            FROM VERTEX
            JOIN EDGE ON VERTEX.ID = EDGE.destination
            WHERE EDGE.source IN (
                SELECT ID FROM VERTEX WHERE ROWID IN ({vertex_list})
            )
            AND EDGE.type IN ('AssumedBiological', 'UnspecifiedParentType', 'BiologicalParent')
        """
        
        next_level = {row[0] for row in conn.execute(children_query).fetchall()}
        next_level -= seen_vertices  # Remove already-seen vertices
        
        seen_vertices.update(next_level)
        current_level = next_level
    
    self.logger.info(f"Found {len(seen_vertices)} vertices within {hops} hops from {len(roots)} root nodes")
    return seen_vertices
```

### Edge Cases and Considerations

1. **No Root Nodes**: 
   - If graph has cycles and no clear root, use all vertices as roots
   - Or allow specifying root nodes explicitly

2. **Multiple Disconnected Components**:
   - BFS from all roots ensures all components are traversed
   - Each component will be limited to N hops from its root(s)

3. **Large Hop Counts**:
   - If hops is very large (e.g., > graph diameter), may end up with entire graph anyway
   - Consider logging a warning if filtered result is close to full size

4. **Performance**:
   - BFS with SQL queries could be slow for large databases
   - Consider materializing hop distances in a table if this becomes a bottleneck
   - Cache hop-filtered vertex sets if same database is read multiple times

5. **Backwards Compatibility**:
   - If `hops` is `None`, use original behavior (entire graph)
   - Ensure existing tests still pass

6. **ORDERING Table**:
   - When hop filtering is active, the ORDERING table may contain vertices outside the filtered set
   - Need to either:
     - Filter ORDERING table entries to only include vertices in scope
     - Regenerate ordering for filtered subgraph
     - Skip using cached ORDERING when hops filtering is active

### Testing Strategy

1. **Unit Tests**:
   - Test `_get_vertices_within_hops` with known graph structures
   - Test with hops=0 (only roots), hops=1 (roots + children), etc.
   - Test with disconnected components
   - Test with cyclic graphs

2. **Integration Tests**:
   - Test full read() with various hop counts
   - Verify vertex and edge counts match expected values
   - Verify resulting graph contains only vertices within hop limit
   - Test memory usage doesn't exceed reasonable bounds

3. **Regression Tests**:
   - Ensure existing behavior with hops=None still works
   - Ensure all existing tests pass

### Implementation Checklist

- [ ] Implement `_get_vertices_within_hops()` method
- [ ] Implement `_count_edges_in_scope()` helper method
- [ ] Create `_create_filtered_queries()` method
- [ ] Update `read()` method to use hop filtering when hops is specified
- [ ] Handle ORDERING table with hop-filtered graphs
- [ ] Add logging for hop filtering process
- [ ] Update `_read_graph()` to accept parameterized queries
- [ ] Write unit tests for BFS traversal
- [ ] Write integration tests for full read pipeline
- [ ] Test backwards compatibility (hops=None)
- [ ] Add performance benchmarks
- [ ] Update documentation
- [ ] Remove `TODO: support hops` comment

### Performance Optimization (Future)

For production use with large databases and frequent reads:

1. **Materialize Hop Distances**:
   - Create a `HOP_DISTANCE` table storing distance from each root to each vertex
   - Populate during database creation or as a preprocessing step
   - Makes filtering much faster: `WHERE hop_distance <= N`

2. **Incremental Updates**:
   - When new vertices/edges are added, update hop distances incrementally
   - Avoid full BFS traversal on every read

3. **Indexing**:
   - Create indexes on frequently-filtered columns
   - Index on hop_distance if materialized

### Success Criteria

- [ ] `rbg-graph-builder -c 4` uses ~200 vertices instead of 4.5M vertices
- [ ] Memory usage is proportional to filtered graph size, not full database size
- [ ] No breaking changes to existing API
- [ ] All tests pass
- [ ] Performance is acceptable for typical use cases (< 10 seconds for BFS on 1M vertex graph)
