# Examples

## Status Updates Demo

The `demo_status_updates.py` script demonstrates the status update features available in `AbstractGraphBuilder`.

### Features

The `AbstractGraphBuilder` now supports two modes for tracking progress:

#### 1. Interactive Progress Bars (tqdm) - Default Mode

```python
builder = DemoGraphBuilder(enable_status=True, use_tqdm=True)
```

This mode displays interactive progress bars for vertices and edges:
```
Vertices: 100%|████████████████| 15/15 [00:00<00:00, 1234.56vertex/s]
Edges: 100%|█████████████████| 14/14 [00:00<00:00, 987.65edge/s]
```

**Benefits:**
- Real-time visual feedback
- Automatic rate calculation (items/second)
- ETA for completion
- Clean, professional output

#### 2. Logging Mode

```python
builder = DemoGraphBuilder(enable_status=True, use_tqdm=False, status_interval=10000)
```

This mode logs status messages at regular intervals:
```
2025-11-07 09:37:56,254 [DemoGraphBuilder] INFO: Added 10,000/100,000 vertices (10.0%)
2025-11-07 09:37:56,455 [DemoGraphBuilder] INFO: Added 20,000/100,000 vertices (20.0%)
```

**Benefits:**
- Works well in non-interactive environments
- Configurable update intervals
- Compatible with log aggregation systems

### Usage in Your Graph Builder

To add status updates to your custom graph builder:

```python
from fscrawler import AbstractGraphBuilder

class MyGraphBuilder(AbstractGraphBuilder):
    def __init__(self, use_tqdm=True):
        super().__init__(use_tqdm=use_tqdm)
        # Your initialization
    
    def init_builder(self, vertex_count, edge_count):
        # Call this to initialize progress tracking
        self._init_status(vertex_count, edge_count)
        # Your initialization
    
    def add_vertex(self, vertex_id, color):
        # Call this to track each vertex
        self._track_vertex()
        # Your vertex addition logic
    
    def add_edge(self, source_id, dest_id):
        # Call this to track each edge
        self._track_edge()
        # Your edge addition logic
    
    def build(self):
        # Call this to close progress bars and log final status
        self._build_status()
        # Your build logic
```

### Running the Demo

```bash
# Run with tqdm (default)
uv run python examples/demo_status_updates.py

# Or directly with python
python examples/demo_status_updates.py
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_status` | bool | `True` | Enable/disable all status updates |
| `use_tqdm` | bool | `True` | Use tqdm progress bars vs. logging |
| `status_interval` | int | `10000` | How often to log (only for logging mode) |

### Disabling Status Updates

For automated scripts or when you don't want progress output:

```python
builder = MyGraphBuilder(enable_status=False)
```
