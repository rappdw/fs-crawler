# Operations Guide

This guide collects day-to-day procedures for running `crawl-fs` in long-lived environments.

## Running and Resuming Crawls

```
crawl-fs run -o /data/crawls -b nightly --hopcount 10 --requests-per-second 8 \
  --max-concurrent-person-requests 32 --metrics-file /data/crawls/nightly.metrics.jsonl
```

- The first invocation seeds the frontier (from `--individuals` or the authenticated account).
- Subsequent invocations can resume using the same `--outdir`/`--basename` pair:
  `crawl-fs resume -o /data/crawls -b nightly --hopcount 20`.
- Resume mode reads the persisted queue state and `hopcount` acts as an upper bound for the resumed run.

## Graceful Pause and Stop

The crawler honours both OS signals and an optional pause control file:

- `SIGUSR2` (or `--pause-file` contents of `pause`) triggers a checkpoint and waits until resumed.
- `SIGINT`/`SIGTERM` (or pause-file `stop`) flushes a final checkpoint and exits cleanly.
- Resume by sending `SIGUSR2` again or writing `resume` to the pause file.

Example using a control file:

```
crawl-fs run -o /data/crawls -b nightly --pause-file /data/crawls/nightly.ctrl
# later…
echo pause > /data/crawls/nightly.ctrl  # crawler will checkpoint and wait
sleep 60
echo resume > /data/crawls/nightly.ctrl
```

## Inspecting Checkpoint State

Use the `checkpoint` subcommand to inspect the last recorded state:

```
crawl-fs checkpoint --status -o /data/crawls -b nightly
```

The JSON output includes frontier/processing queue depths, last checkpoint event, and throttle configuration used for the run.

## Telemetry Stream

When `--metrics-file` is supplied the crawler emits JSON lines describing each batch and iteration:

- `person_batch`: iteration number, batch index, queue sizes, and HTTP request counter.
- `iteration_complete`: total requests and duration for the iteration.
- `relationships_complete`: count and duration for relationship resolution.
- `run_start` / `run_complete`: capture configuration and exit status.

Redirect to `-` to send telemetry to stdout, or to a file for ingestion by log collectors.

## Throttle Tuning

The crawler defaults to the historic throttle values (200 person IDs per request, 40 concurrent person calls, and a 2s inter-batch delay). Override via CLI flags:

- `--requests-per-second` limits aggregate throughput across both person and relationship calls.
- `--max-concurrent-person-requests` / `--max-concurrent-relationship-requests` throttle concurrency.
- Backoff parameters (`--max-retries`, `--backoff-base`, `--backoff-multiplier`, `--backoff-max`) apply exponential delay after HTTP 429/5xx responses.

Settings are persisted in the run metadata (`JOB_METADATA`) and included in checkpoint status output.

## Troubleshooting

- **database is locked**: ensure only one `crawl-fs` process uses a given `--outdir`/`--basename` at a time.
- **pause/resume not responding**: verify the control file path is writable and only contains `pause`, `resume`, or `stop`.
- **flat telemetry**: confirm `--metrics-file` was provided and the destination has write permissions.
