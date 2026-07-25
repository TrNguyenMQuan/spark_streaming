# Incremental CPG Parser Service & Kafka Producer

This component implements **Tasks 1, 2, and 3** of the Incremental Code Property Graph (CPG) Streaming Pipeline. It scans Python source files from the target repository, parses them into Code Property Graph events with stable identifiers using bounded memory, validates payload schemas, and publishes events into Apache Kafka topics.

---

## 🏗️ Architecture & Component Overview

```mermaid
flowchart LR
    A[Source .py Files] --> B[discover_files.py]
    B -->|Stream file paths| C[cpg_visitor.py AST Engine]
    C --> D[stable_id.py SHA-256 Generator]
    D --> E[schemas/ JSON Envelope Validator]
    E --> F[kafka_producer.py CPGProducer]
    F --> G[Kafka Broker Topics]
```

### Core Components & Files

- **`discover_files.py`**: Enumerates Python source files (`*.py`) in `target-repo`, filtering out test, setup, and generated files (`tests/`, `test_*.py`, `setup.py`, `__init__.py`).
- **`cpg_visitor.py`**: Bounded-memory AST NodeVisitor engine. Parses a single file at a time using Python's built-in `ast` module, extracts AST, CFG, DFG, and CALL node/edge entities, and immediately releases AST memory to prevent RAM exhaustion.
- **`stable_id.py`**: SHA-256 stable identifier generator. Computes deterministic, line-shift resilient `node_id` and `edge_id` hashes based on `(file_path, scope, node_type, sibling_index)` **without** incorporating volatile line numbers.
- **`kafka_producer.py`**: High-performance Kafka Producer client using `kafka-python`. Configured with `linger_ms=50` to batch messages and uses `key = event["file_path"]` as the partition key to guarantee strictly ordered per-file streaming.
- **`parser.py`**: Main CLI execution entry point orchestration script.
- **`schemas/`**: JSON Schema definitions (`nodes.json`, `edges.json`, `metadata.json`, `errors.json`) enforcing `schema_version = "v1"` and ISO UTC `event_timestamp`.

---

## 🚀 Usage & CLI Commands

Run all commands from the project root directory:

```bash
# Preview parsing results for 5 files without sending to Kafka
python parser-service/parser.py --limit 5

# Parse 30 baseline files and publish events to Kafka topics
python parser-service/parser.py --limit 30 --publish

# Reprocess a specific modified file for stream replay
python parser-service/parser.py --file target-repo/src/transformers/models/auto/modeling_auto.py --publish
```

---

## 📊 Kafka Topic Routing & Payload Layout

Events are partitioned into 4 distinct Kafka topics based on payload classification:

| Topic Name | Partitions | Key | Payload Content |
|---|---|---|---|
| `code.events.nodes` | 3 | `file_path` | AST, FunctionDef, ClassDef, Module CPG Node Events |
| `code.events.edges` | 3 | `file_path` | AST, CFG, DFG, CALL CPG Edge Events |
| `code.events.metadata` | 1 | `file_path` | File-level Source Metadata (`loc`, `num_nodes`, `num_edges`, `file_hash`) |
| `code.events.errors` | 1 | `file_path` | Parser Exception & Syntax Error Telemetry |
