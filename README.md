# Lab 04 — Incremental CPG Streaming Pipeline

> Big Data course — VNUHCM University of Science

## Prerequisites

- Docker Desktop (with Docker Compose v2)
- Python 3.10+
- `pip install -r requirements.txt`

---

## Quick Start

### 1. Start Kafka infrastructure

```bash
docker compose up -d
docker compose ps   # Wait until 'kafka' -> healthy
```

### 2. Start Neo4j, Kafka Connect, MongoDB, Spark (Task 4 & 5 services)

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

Wait until all services are healthy:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml ps
```

### 3. Initialize Neo4j constraints and indexes

```bash
python scripts/setup/setup_neo4j_sink.py
```

This script:
- Runs Cypher init scripts from `neo4j/init/` to create constraints and indexes.
- Registers the Kafka Connect Sink connectors from `neo4j/connectors/`.

### 4. Run the Parser Service (Producer)

```bash
# Parse first 30 files and publish to Kafka
python parser-service/parser.py --limit 30 --publish

# Full repository (~2400 files, heavy):
python parser-service/parser.py --publish
```

### 5. Verify the pipeline

```bash
# Run all 5 replay verification testcases + ground-truth audit
python scripts/tests/run_all_tests.py

# Or run individual testcases:
python scripts/tests/test_tc1_add_function.py
python scripts/tests/test_tc2_add_class.py
python scripts/tests/test_tc3_line_shift.py
python scripts/tests/test_tc4_exact_replay.py
python scripts/tests/test_tc5_call_graph.py
python scripts/tests/test_audit_accuracy.py
```

---

## UI Dashboards

| Service | URL | Description |
| :--- | :--- | :--- |
| Kafka UI | http://localhost:8080 | Kafka topics, messages, consumer groups |
| Neo4j Browser | http://localhost:7474 | Graph visualization (`neo4j` / `password123`) |
| Mongo Express | http://localhost:8081 | MongoDB collections browser |
| Kafka Connect REST | http://localhost:8083 | Connector status and configuration |

---

## Project Structure

```
spark_streaming/
├── .github/workflows/         # GitHub Actions: Jupyter Book deploy
├── docs/                      # Lab specification PDF & handoff doc
├── notebooks/                 # Jupyter Book (6 chapters, 01–06)
│   ├── myst.yml               # MyST / Jupyter Book configuration
│   ├── intro.md               # Introduction & Architecture Diagram
│   ├── 01_file_discovery.ipynb
│   ├── 02_cpg_parser.ipynb
│   ├── 03_kafka_producer.ipynb
│   ├── 04_neo4j_ingestion.ipynb
│   ├── 05_mongodb_ingestion.ipynb
│   └── 06_replay_verification.ipynb
├── parser-service/            # Task 1–3: CPG parser, Kafka producer
│   ├── cpg_visitor.py         # AST/CFG/DFG/CALL graph builder
│   ├── stable_id.py           # SHA-256 scope-based stable ID
│   ├── kafka_producer.py      # Kafka producer wrapper
│   ├── discover_files.py      # .py file enumeration
│   ├── parser.py              # Main entry point
│   └── schemas/               # JSON Schema contracts (v1)
├── neo4j/                     # Task 4: Neo4j Sink configuration
│   ├── connectors/            # Kafka Connect Sink JSON configs
│   └── init/                  # Cypher constraints & indexes
├── spark-mongo/               # Task 5: Spark Streaming to MongoDB
│   └── metadata_to_mongodb.py
├── scripts/
│   ├── setup/                 # Infrastructure automation
│   └── tests/                 # Task 6: Modular test suite (TC1–TC5)
├── docker-compose.yml         # Kafka KRaft + Kafka UI + kafka-init
├── docker-compose.override.yml # Neo4j, Kafka Connect, MongoDB, Spark
└── requirements.txt
```

---

## Task Overview

| Task | Description | Key Files |
| :---: | :--- | :--- |
| 1 | Repository Cloning & File Discovery | `parser-service/discover_files.py` |
| 2 | Incremental CPG Parser Service | `parser-service/cpg_visitor.py`, `stable_id.py` |
| 3 | Kafka Topic Design & Producer | `docker-compose.yml`, `parser-service/kafka_producer.py` |
| 4 | Graph Topology Ingestion into Neo4j | `neo4j/connectors/`, `docker-compose.override.yml` |
| 5 | Source Metadata Ingestion into MongoDB | `spark-mongo/metadata_to_mongodb.py` |
| 6 | Idempotent Replay Verification | `scripts/tests/` |

---

## Jupyter Book

The lab report is published as a Jupyter Book. To build locally:

```bash
cd notebooks
pip install mystmd
myst build --html
# Open _build/html/index.html
```

The book is automatically deployed to GitHub Pages on push to `main` or `feature/verification`.
