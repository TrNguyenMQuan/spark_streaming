# Lab 04 — Incremental CPG Streaming Pipeline

Bao cao do an: xay pipeline streaming trich xuat **Code Property Graph (CPG)**
tu repo `huggingface/transformers`, day qua **Apache Kafka**, ingest song song vao
**Neo4j** (Kafka Connect Sink) va **MongoDB** (Spark Structured Streaming).

Moi chuong tuong ung mot task trong de bai.

## Muc luc

- **Task 1** — Repository Cloning & File Discovery
- **Task 2** — Incremental CPG Parser Service
- **Task 3** — Kafka Topic Design & Producer
- **Task 4** — Graph Topology Ingestion into Neo4j
- **Task 5** — Source Metadata Ingestion into MongoDB
- **Task 6** — Idempotent Replay Verification

---

## Architecture Diagram

So do kien truc tong the mo ta luong du lieu end-to-end cua he thong:

```text
+------------------+       +-------------------+       +------------------------------+
|  GitHub Repo     |       |  Parser Service   |       |     Apache Kafka KRaft       |
|  (target-repo/)  +------>+  (cpg_visitor.py)  +------>+  Broker (localhost:9092)      |
|                  | clone |  (stable_id.py)   | publish|                              |
+------------------+       |  (kafka_producer) |       |  Topics:                     |
                           +-------------------+       |  - code.events.nodes    (3P) |
                                                       |  - code.events.edges    (3P) |
                                                       |  - code.events.metadata (1P) |
                                                       |  - code.events.errors   (1P) |
                                                       +-------+----------+-----------+
                                                               |          |
                                            nodes & edges      |          |  metadata
                                                               v          v
                                                  +------------+--+  +----+-----------+
                                                  | Kafka Connect |  | Spark Struct.  |
                                                  | Neo4j Sink    |  | Streaming      |
                                                  | (Cypher MERGE)|  | (checkpoint)   |
                                                  +-------+-------+  +-------+--------+
                                                          |                  |
                                                          v                  v
                                                  +-------+-------+  +------+---------+
                                                  |    Neo4j       |  |   MongoDB      |
                                                  |  Graph DB      |  |  (cpg.source_  |
                                                  | (CPGNode,      |  |   metadata)    |
                                                  |  CPG_EDGE)     |  |                |
                                                  +----------------+  +----------------+
```

### Mo ta luong du lieu

1. **Clone & Discovery**: Clone repository tu GitHub, liet ke tat ca file `.py`.
2. **CPG Parser**: Duyet tung file bang `ast.NodeVisitor`, sinh nodes (AST), edges (AST/CFG/DFG/CALL), metadata va errors. Moi phan tu duoc gan Stable ID SHA-256 theo scope (doc lap so dong code).
3. **Kafka Producer**: Day 4 loai su kien vao 4 Kafka topics. Message key = `file_path` dam bao thu tu xu ly nghiem ngat theo tung file.
4. **Neo4j Sink** (Task 4): Kafka Connect Sink doc topics `nodes` va `edges`, ghi vao Neo4j bang Cypher `MERGE` (idempotent — khong tao trung lap).
5. **Spark Streaming -> MongoDB** (Task 5): Spark Structured Streaming doc topic `metadata`, ghi vao MongoDB bang Replace+Upsert theo `file_path`. Co `checkpointLocation` de resume offset.
6. **Replay Verification** (Task 6): Sua 1 file `.py`, chay lai parser, kiem chung Neo4j 0% duplicate va MongoDB metadata cap nhat dung.
