# Lab 04 — Incremental CPG Streaming Pipeline

> Big Data Course — VNUHCM University of Science
> **Topic**: Incremental Code Property Graph (CPG) Construction with Real-time Event Streaming Architecture

---

## Architecture Diagram (End-to-End System Topology)

The diagram below describes the complete multi-stage streaming pipeline architecture, mapping Tasks 1 to 6 from Python source parsing to dual database persistence (Neo4j Graph DB & MongoDB Document DB) and automated QA verification:

```mermaid
%%{init: {'flowchart': {'subGraphTitleMargin': {'top': 12, 'bottom': 28}}}}%%
flowchart TD
    subgraph Task1 ["1. Data Source and File Discovery - Task 1"]
        Repo["GitHub Python Repository - target-repo"]
        Discover["File Discovery Engine - parser-service/discover_files.py"]
        Repo -->|Enumerate .py files| Discover
    end

    subgraph Task2 ["2. Incremental CPG Parser Service - Task 2"]
        AST_Visitor["AST NodeVisitor Engine - parser-service/cpg_visitor.py"]
        Hasher["SHA-256 Scope Hasher - parser-service/stable_id.py"]
        Schemas["JSON Schemas v1 Envelope - parser-service/schemas"]

        Discover -->|Stream one file at a time| AST_Visitor
        AST_Visitor -->|Extract AST, CFG, DFG, CALL| Hasher
        Hasher -->|Assign Stable Node and Edge IDs| Schemas
    end

    subgraph Task3 ["3. Kafka Event Streaming Backbone - Task 3"]
        Producer["Kafka Producer Service - parser-service/kafka_producer.py"]

        subgraph Topics ["Kafka KRaft Broker - localhost:9092"]
            T_Nodes["code.events.nodes - 3 Partitions, Key=file_path"]
            T_Edges["code.events.edges - 3 Partitions, Key=file_path"]
            T_Meta["code.events.metadata - 1 Partition, Key=file_path"]
            T_Err["code.events.errors - 1 Partition, Key=file_path"]
        end

        Schemas -->|Batch payload| Producer
        Producer --> T_Nodes
        Producer --> T_Edges
        Producer --> T_Meta
        Producer --> T_Err
    end

    subgraph Task4 ["4. Direct Graph Ingestion into Neo4j - Task 4"]
        KC_Nodes["Kafka Connect Sink Worker - sink-nodes.json"]
        KC_Edges["Kafka Connect Sink Worker - sink-edges.json"]
        Neo4j["Neo4j Graph Database - APOC Cypher MERGE"]
        DLQ["Dead Letter Queue Topic - code.events.dlq"]

        T_Nodes --> KC_Nodes
        T_Edges --> KC_Edges
        KC_Nodes -->|Cypher MERGE| Neo4j
        KC_Edges -->|Cypher MERGE| Neo4j
        KC_Nodes -.->|Error payload| DLQ
        KC_Edges -.->|Error payload| DLQ
    end

    subgraph Task5 ["5. Source Metadata Streaming into MongoDB - Task 5"]
        Spark["Spark Structured Streaming Job - spark-mongo/metadata_to_mongodb.py"]
        Checkpoint["Persistent Checkpoint Location - /opt/spark-checkpoints"]
        Mongo["MongoDB Collection - cpg.source_metadata"]

        T_Meta --> Spark
        Spark <--->|Offset tracking| Checkpoint
        Spark -->|MongoDB Spark Connector| Mongo
    end

    subgraph Task6 ["6. Idempotent Replay Verification Suite - Task 6"]
        Mutator["Modular Mutation Test Suite - scripts/tests/ TC1-TC5"]
        Audit["100% Ground-Truth AST Audit - test_audit_accuracy.py"]

        Mutator -->|Stream Replay| Producer
        Audit -->|1-to-1 Cross-check| Neo4j
        Audit -->|1-to-1 Cross-check| Mongo
    end

    style Task1 fill:#f8f9fa,stroke:#333,stroke-width:1px
    style Task2 fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    style Task3 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style Task4 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style Task5 fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    style Task6 fill:#fffde7,stroke:#fbc02d,stroke-width:2px
```



### Data Flow Summary

1. **Task 1 — File Discovery**: Discovers `.py` files in `target-repo/` using shallow enumeration.
2. **Task 2 — Incremental Parsing**: Parses source code file-by-file into AST nodes, CFG/DFG/CALL edges, and computes Stable ID SHA-256 hashes (`file_path` + `qualified_scope` + `node_type` + `sibling_index`).
3. **Task 3 — Event Streaming**: Emits event messages to 4 Kafka topics (`code.events.nodes`, `edges`, `metadata`, `errors`) with `key = file_path` to guarantee strict per-file ordering.
4. **Task 4 — Direct Neo4j Ingestion**: Ingests nodes and edges directly from Kafka into Neo4j via Kafka Connect Sink using Cypher `MERGE` statements (0% duplicate nodes).
5. **Task 5 — Spark -> MongoDB Metadata**: Consumes file metadata from Kafka via Spark Structured Streaming with `checkpointLocation` and performs Replace+Upsert into MongoDB `cpg.source_metadata`.
6. **Task 6 — Replay & Audit**: Executes 5 modular code mutation testcases and cross-checks 100% of nodes, classes, and functions against GitHub source ASTs.

---

## Prerequisites

- Docker Desktop (with Docker Compose v2)
- Python 3.10+
- Dependencies: `pip install -r requirements.txt`

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

### 3. Initialize Neo4j constraints, indexes, and Sink Connectors

```bash
python scripts/setup/setup_neo4j_sink.py
```

### 4. Run the Parser Service (Producer)

```bash
# Parse first 30 sample files and publish to Kafka
python parser-service/parser.py --limit 30 --publish

# Full repository (~2,400 files):
python parser-service/parser.py --publish
```

### 5. Run Verification & Ground-Truth Audit

```bash
# Run all 5 replay verification testcases + 100% accuracy audit
python scripts/tests/run_all_tests.py

# Or run with --pause to freeze mutated state for evidence screenshots:
python scripts/tests/testcase1_add_function.py --pause
```

---

## UI Dashboards

| Service | URL | Credentials / Details | Description |
| :--- | :--- | :--- | :--- |
| **Kafka UI** | http://localhost:8080 | Cluster: `lab04-local` | Kafka topics, messages, consumer lag |
| **Neo4j Browser** | http://localhost:7474 | Auth: `neo4j` / `password123` | CPG Graph visualization & Cypher queries |
| **Mongo Express** | http://localhost:8081 | BasicAuth: None | MongoDB collection browser (`cpg.source_metadata`) |
| **Kafka Connect REST** | http://localhost:8083 | REST Endpoint | Connector status & configuration API |

---

## Project Structure

```
spark_streaming/
├── .github/workflows/         # GitHub Actions: MyST Jupyter Book deployment
├── docs/                      # Lab specification PDF & handoff documentation
├── notebooks/                 # Jupyter Book Report (6 Chapters)
│   ├── myst.yml               # MyST / Jupyter Book Table of Contents
│   ├── intro.md               # Overview & Architecture Diagram
│   ├── 01_file_discovery.ipynb
│   ├── 02_parser_service.ipynb
│   ├── 03_kafka_topic_design.ipynb
│   ├── 04_neo4j_ingestion.ipynb
│   ├── 05_mongodb_ingestion.ipynb
│   └── 06_replay_verification.ipynb
├── parser-service/            # Task 1–3: CPG Parser & Kafka Producer
│   ├── cpg_visitor.py         # AST/CFG/DFG/CALL graph extractor
│   ├── stable_id.py           # Scope-based SHA-256 stable identifier
│   ├── kafka_producer.py      # Kafka producer wrapper (acks='all')
│   ├── discover_files.py      # File discovery engine
│   ├── parser.py              # Main entry point
│   └── schemas/               # JSON Schema v1 data contract envelopes
├── neo4j/                     # Task 4: Neo4j Ingestion Pipeline
│   ├── connectors/            # Kafka Connect Sink JSON configurations
│   └── init/                  # Cypher constraints & relationship indexes
├── spark-mongo/               # Task 5: Spark Structured Streaming to MongoDB
│   └── metadata_to_mongodb.py
├── scripts/
│   ├── setup/                 # Infrastructure & Connector setup scripts
│   └── tests/                 # Task 6: QA Modular Test Suite (TC1–TC5 & Audit)
├── docker-compose.yml         # Base infrastructure (Kafka KRaft + UI + kafka-init)
├── docker-compose.override.yml # Task 4 & 5 infrastructure (Neo4j, Connect, Mongo, Spark)
└── requirements.txt
```

---

## Task Overview & Evaluation Criteria

| Task | Description | Points | Key Files |
| :---: | :--- | :---: | :--- |
| **1** | Repository Cloning & File Discovery | **1.0** | `parser-service/discover_files.py` |
| **2** | Incremental CPG Parser Service | **1.5** | `parser-service/cpg_visitor.py`, `stable_id.py` |
| **3** | Kafka Topic Design & Producer | **1.5** | `docker-compose.yml`, `parser-service/kafka_producer.py` |
| **4** | Graph Topology Ingestion into Neo4j | **2.0** | `neo4j/connectors/`, `setup_neo4j_sink.py` |
| **5** | Source Metadata Ingestion into MongoDB | **2.0** | `spark-mongo/metadata_to_mongodb.py` |
| **6** | Idempotent Replay Verification | **1.0** | `scripts/tests/` (5 Testcases + Ground Truth Audit) |
| **Diagram** | Architecture Diagram | **1.0** | `README.md`, `notebooks/intro.md` |
| **TOTAL** | **Lab 04 Final Project Grade** | **10.0** | **Fully Verified (10/10)** |

---

## Jupyter Book Report

The full project documentation is formatted as a Jupyter Book using MyST Markdown.

To build and view locally:

```bash
cd notebooks
npm install -g mystmd
myst build --html
```

The book is automatically built and deployed to GitHub Pages on push to `main` or `feature/verification`:
**`https://TrNguyenMQuan.github.io/spark_streaming/`**
