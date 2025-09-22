# FamilySearch Crawler Scaling Plan

## Objectives
- Support long-running crawls that can touch a large fraction of the FamilySearch graph without exhausting local memory.
- Respect FamilySearch rate limits and behave politely while still making steady progress.
- Allow operators to pause, resume, or restart crawls without losing progress.
- Improve overall reliability, observability, and code quality to make the crawler maintainable.

## Current Constraints & Observations
- `GraphDbImpl` keeps the working dataset in an in-memory SQLite database and only flushes to disk at shutdown, which caps crawl size and risks data loss on crash.
- Resume logic depends on a `.db` snapshot that is renamed to `.bak`, so an unexpected termination mid-run cannot restart safely.
- The frontier/visited queues are memory-backed and lack persistence; the crawler can reprocess nodes after restarts.
- Concurrency and throttling are hard-coded constants; there is no adaptive backoff or CLI surface to tune request volume.
- There is no coordinated shutdown or checkpoint hook, so SIGINT/SIGTERM leave the in-memory state unsaved.
- Limited telemetry: logs do not capture request pacing, queue depth, or why work is skipped.

## Roadmap
1. **Establish Baseline**
   - Trace the current crawl loop and data flow (frontier → processing → graph) with sequence diagrams.
   - Capture current runtime characteristics (memory growth, request rate) on a small crawl to define success metrics.

2. **Persistent Storage Refactor**
   - Replace in-memory SQLite usage with an on-disk database opened in WAL mode and incremental commits.
   - Introduce schema migrations/versioning so existing `.db` files upgrade cleanly.
   - Ensure writes are batched to keep I/O manageable while guaranteeing durability on each iteration boundary.

3. **Durable Frontier & Visited Tracking**
   - Move `FRONTIER_VERTEX` and `PROCESSING` sets into disk-backed queues with priority or FIFO semantics.
   - Add indexes and uniqueness guards to prevent duplicate scheduling without relying on Python sets.
   - Expose utility methods to peek queue depth for monitoring and to bootstrap seeds when empty.

4. **Checkpointing & Resume Semantics**
   - Persist crawler metadata (current iteration, active seeds, configuration) inside the database.
   - Implement safe checkpoints at iteration boundaries and after partial batches so an interrupt leaves a coherent state.
   - Add CLI commands (`crawl-fs resume`, `crawl-fs checkpoint --status`) to inspect and resume stored jobs.

5. **Polite Request Management**
   - Introduce a rate limiter that caps requests per second and concurrent connections, informed by config or CLI flags.
   - Implement adaptive backoff on HTTP 429/5xx, including jitter and exponential delay.
   - Centralize throttle settings so both person and relationship fetchers share the same pacing strategy.

6. **Pause/Cancel Handling**
   - Capture OS signals (SIGINT/SIGTERM/SIGUSR1) to trigger a graceful checkpoint instead of abrupt exit.
   - Provide a control file or IPC hook to pause the event loop and persist state on demand.
   - Document operational procedures for pausing and resuming long crawls.

7. **Observability & Diagnostics**
   - Enhance logging (structured logs or log levels) to include queue sizes, request counters, and throttle decisions.
   - Optionally emit metrics (e.g., via Prometheus-compatible exporter or simple CSV) for long-run monitoring.
   - Add sanity checks to detect stalled progress or cycles in the frontier.

8. **Testing & Tooling**
   - Build integration tests around a mocked FamilySearch API to validate resume, throttling, and persistence.
   - Add regression tests for schema migrations and checkpoint recovery paths.
   - Update developer tooling (pre-commit, linting) and write docs for running large crawls safely.

9. **Documentation & Operational Guides**
   - Revise README to cover new CLI options, rate-limiting guidance, and resume workflows.
   - Provide runbooks for handling partial crawls, migrating existing data, and cleaning up checkpoints.

## General Code Quality Targets
- Remove dead or incomplete implementations (e.g., flesh out `GraphMemoryImpl` or consolidate with DB-backed graph).
- Normalize error handling in `Session`/`FamilySearchAPI` to avoid silent retries and to surface actionable messages.
- Refactor shared SQL fragments into parameterized helpers to reduce injection risk and duplication.
- Adopt a consistent configuration system (env vars + config file) instead of scattered constants.
