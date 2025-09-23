[![PyPi](https://img.shields.io/pypi/v/fs-crawler.svg)](https://pypi.org/project/fs-crawler/) 
[![PyPi](https://img.shields.io/pypi/wheel/fs-crawler.svg)](https://pypi.org/project/fs-crawler/) 
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/) 
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/) 
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/) 
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/) 
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/) 

# FamilySearch crawler
A python module that crawls FamilySearch. The crawler will extract vertices and edges in the format required for
ingestion into a [RedBlackGraph](https://github.com/rappdw/redblackgraph).

To run:

```shell script
pip install fs-crawler
crawl-fs  --help
```

This implementation was inspired by [getmyancestors](https://github.com/Linekio/getmyancestors).

## Commands

- `crawl-fs run` — start a new crawl, seeding from the authenticated account unless `--individuals` are supplied.
- `crawl-fs resume` — continue a previous crawl, reusing persisted frontier/processing queues.
- `crawl-fs checkpoint --status` — inspect the latest checkpoint metadata (iteration counters, queue depth, throttle settings).

Key options:

| Flag | Description |
|------|-------------|
| `--requests-per-second` | cap outbound HTTP rate (default 6 rps). |
| `--max-concurrent-person-requests` | limit in-flight person batches (default 40). |
| `--pause-file` | path to a control file accepting `pause`, `resume`, or `stop`. |
| `--metrics-file` | write structured JSON telemetry (use `-` for stdout). |

See [docs/operations.md](docs/operations.md) for a full operations guide covering resume workflows, pause control, telemetry ingestion, and recommended throttle tuning.
