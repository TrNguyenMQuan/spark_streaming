# Interactive Verification Notebooks & Jupyter Book Report

This directory contains the 6 interactive Jupyter Notebooks (`01_file_discovery.ipynb` through `06_replay_verification.ipynb`) and **MyST Jupyter Book** configuration files (`myst.yml`, `intro.md`) that make up the formal lab report for the project.

---

## 📚 Notebook Chapters Overview

| File Name | Task Title | Content Summary |
|---|---|---|
| `intro.md` | System Overview | Project intro, end-to-end architecture diagram, and table of contents |
| `01_file_discovery.ipynb` | Task 1: Repository Cloning & File Discovery | Shallow clone, file enumeration, exclusion rules, and bounded dataset statistics |
| `02_parser_service.ipynb` | Task 2: Incremental CPG Parser Service | AST Visitor, bounded memory parsing, and stable ID generation demonstration |
| `03_kafka_topic_design.ipynb` | Task 3: Kafka Topic Design | Topic layout, partitioning strategy, schema versioning, and producer tuning |
| `04_neo4j_ingestion.ipynb` | Task 4: Direct Graph Ingestion | Kafka Connect Sink configuration, APOC Cypher MERGE, and graph topology visualization |
| `05_mongodb_ingestion.ipynb` | Task 5: Source Metadata Ingestion | PySpark Structured Streaming job, Replace+Upsert strategy, and checkpointing |
| `06_replay_verification.ipynb` | Task 6: Idempotent Replay Verification | 5 code mutation testcases execution, ground-truth audit, and UI evidence table |

---

## 🛠️ Building & Viewing Jupyter Book Locally

You can compile the notebooks into an interactive HTML documentation website using MyST or Jupyter Book:

```bash
# Build the HTML book using MyST Markdown CLI
npx myst build --html

# Start a local preview server
npx myst start

# Build using Jupyter Book (if jupyter-book package is installed)
jupyter-book build notebooks/
```

Access the built documentation in your browser at `http://localhost:3000` or open `_build/html/index.html`.
