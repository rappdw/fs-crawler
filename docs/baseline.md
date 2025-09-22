# Baseline Crawl Flow

This document captures the current crawler execution path as implemented in `crawler.py`,
`controller/fsapi.py`, and the `GraphDbImpl` backing store. It focuses on how the frontier,
processing queue, and graph tables interact during a crawl iteration and the subsequent
relationship-resolution phase.

## Crawl Entry Points
- `crawler.crawl` seeds credentials, output paths, and logging, then instantiates `FamilySearchAPI` and `GraphDbImpl` (`fscrawler/crawler.py:16`).
- Startup seeds (`--individuals` or `FamilySearchAPI.get_default_starting_id`) are inserted into the frontier via `GraphDbImpl.add_to_frontier` (`graph_db_impl.py:68`).
- An asyncio event loop is created, and `FamilySearchAPI.iterate` is invoked for each hop (`crawler.py:41`). After the loop completes, `FamilySearchAPI.resolve_relationships` is called to classify parent-child edges.

## Iteration Data Flow
```
frontend seeds
  ↓
FRONTIER_VERTEX (GraphDbImpl)
  ↓ start_iteration()
PROCESSING (GraphDbImpl)
  ↓ get_ids_to_process()
FamilySearchAPI.iterate()
  ├─ partition_requests() splits PROCESSING ids into async batches (`fsapi.py:120`).
  ├─ Session.get_urla() issues batched `/platform/tree/persons` calls.
  └─ process_persons_result():
       • add_individual() inserts into VERTEX and removes id from PROCESSING (`graph_db_impl.py:82`).
       • add_parent_child_relationship() records EDGE rows and pushes unseen ids back onto FRONTIER_VERTEX (`graph_db_impl.py:95`).
```
- Each request batch is awaited with `asyncio.gather`; exceptions bubble out to halt the iteration (`fsapi.py:185`).
- Between batches the crawler throttles with `DELAY_BETWEEN_SUBSEQUENT_REQUESTS` seconds unless partial writes are triggered (`fsapi.py:193`).
- After all batches finish, `graph.end_iteration()` persists iteration metrics (counts, duration) in the `LOG` table and commits the in-memory SQLite state (`graph_db_impl.py:111`). The `starting_iter` pointer is advanced so restarts resume at the next hop.

## Relationship Resolution Sequence
```
GraphDbImpl.determine_resolution()
  ↓
EDGE rows marked RESOLVE when >2 parents per child (`graph.py:156`).
  ↓ resolve_relationships()
partition_requests() over relationship ids (`fsapi.py:153`).
  ↓
Session.get_urla("/platform/tree/child-and-parents-relationships/{id}.json")
  ↓
process_relationship_result() updates EDGE.type (`fsapi.py:71`).
  ↓
GraphDbImpl.update_relationship() writes final type and commits via end_relationship_resolution().
```
- Resolution reuses the same asyncio loop, throttled separately (`DELAY_BETWEEN_SUBSEQUENT_RELATIONSHIP_REQUESTS`).
- `GraphDbImpl.end_relationship_resolution()` logs the count and duration for post-run diagnostics (`graph_db_impl.py:125`).

## Key Observations for Refactor
- Both the frontier and processing queues are transient in SQLite memory; durability depends on the eventual `close()` dump to disk (`graph_db_impl.py:144`).
- Iteration restarts load prior state from the on-disk `.db`, then rename it to `.bak`, meaning a crash mid-run loses current progress (`graph_db_impl.py:167`).
- Rate limiting is hard-coded and global, with no adaptive feedback beyond fixed sleeps (`fsapi.py:186`).

## Offline Baseline Metrics
- `tools/offline_baseline.py` patches the crawler to run against canned FamilySearch responses so we can profile without network access or credentials. The probe performs one iteration starting from seed `P0` and captures timings plus memory via `tracemalloc`.
- Current output (local run): iteration duration ≈ 2.02 s, 1 request, effective rate ≈ 0.49 req/s, peak Python heap ≈ 361 KB, resulting frontier of 2 unprocessed vertices. The 2 s delay is driven by `DELAY_BETWEEN_SUBSEQUENT_REQUESTS`.
- GraphDbImpl's `LOG` table already records iteration duration, vertex/frontier counts, and edge metrics. Requests-per-second can be derived from `FamilySearchAPI.get_counter()` versus wall-clock time; memory usage currently lacks built-in hooks.
- For a real crawl baseline, run the CLI with a low `--hopcount` (e.g., `1`) while sampling RSS via `psutil` or `tracemalloc`, and persist per-iteration counters (HTTP requests, memory delta, queued vertices) to extend the existing LOG table.
