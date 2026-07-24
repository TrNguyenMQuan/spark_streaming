# Lab 04 — Incremental CPG Streaming Pipeline

Báo cáo đồ án: Xây dựng pipeline streaming trích xuất **Code Property Graph (CPG)** từ kho mã nguồn Python `huggingface/transformers`, phát qua **Apache Kafka**, ingest song song vào **Neo4j** (Kafka Connect Sink) và **MongoDB** (Spark Structured Streaming).

Mỗi chương tương ứng một task trong đề bài.

---

## 🏗️ Sơ đồ Kiến trúc Hệ thống (Architecture Diagram)

Sơ đồ thể hiện luồng dữ liệu end-to-end từ bước trích xuất mã nguồn đến lưu trữ đồ thị và metadata:

```mermaid
flowchart TD
    subgraph Task1 ["1. Data Source & File Discovery (Task 1)"]
        Repo["GitHub Python Repository<br>target-repo/"]
        Discover["File Discovery Engine<br>parser-service/discover_files.py"]
        Repo --> Discover
    end

    subgraph Task2 ["2. Incremental CPG Parser Service (Task 2)"]
        AST_Visitor["AST NodeVisitor Engine<br>parser-service/cpg_visitor.py"]
        Hasher["SHA-256 Scope Hasher<br>parser-service/stable_id.py"]
        Schemas["JSON Schemas v1 Envelope<br>parser-service/schemas/"]
        
        Discover --> AST_Visitor
        AST_Visitor --> Hasher
        Hasher --> Schemas
    end

    subgraph Task3 ["3. Kafka Event Streaming Backbone (Task 3)"]
        Producer["Kafka Producer Service<br>parser-service/kafka_producer.py"]
        
        subgraph Topics ["Kafka KRaft Broker (localhost:9092)"]
            T_Nodes["code.events.nodes (3 Partitions)"]
            T_Edges["code.events.edges (3 Partitions)"]
            T_Meta["code.events.metadata (1 Partition)"]
            T_Err["code.events.errors (1 Partition)"]
        end

        Schemas --> Producer
        Producer --> T_Nodes
        Producer --> T_Edges
        Producer --> T_Meta
        Producer --> T_Err
    end

    subgraph Task4 ["4. Direct Graph Ingestion into Neo4j (Task 4)"]
        KC_Nodes["Kafka Connect Sink Worker<br>sink-nodes.json"]
        KC_Edges["Kafka Connect Sink Worker<br>sink-edges.json"]
        Neo4j[("Neo4j Graph Database<br>:CPGNode, :CPG_EDGE")]
        DLQ["Dead Letter Queue Topic<br>code.events.dlq"]

        T_Nodes --> KC_Nodes
        T_Edges --> KC_Edges
        KC_Nodes -->|Cypher MERGE| Neo4j
        KC_Edges -->|Cypher MERGE| Neo4j
        KC_Nodes -.->|Error payload| DLQ
        KC_Edges -.->|Error payload| DLQ
    end

    subgraph Task5 ["5. Source Metadata Streaming into MongoDB (Task 5)"]
        Spark["Spark Structured Streaming Job<br>spark-mongo/metadata_to_mongodb.py"]
        Checkpoint["Persistent Checkpoint Location<br>/opt/spark-checkpoints"]
        Mongo[("MongoDB Collection<br>cpg.source_metadata")]

        T_Meta --> Spark
        Spark <---> Checkpoint
        Spark --> Mongo
    end

    subgraph Task6 ["6. Idempotent Replay Verification Suite (Task 6)"]
        Mutator["Modular Mutation Test Suite<br>scripts/tests/ (TC1 - TC5)"]
        Audit["100% Ground-Truth AST Audit<br>scripts/tests/test_audit_accuracy.py"]

        Mutator -->|Stream Replay| Producer
        Audit --> Neo4j
        Audit --> Mongo
    end
```

---

## 📚 Mục lục Sách Jupyter Book

- **Chương 1** — Task 1: Repository Cloning & File Discovery
- **Chương 2** — Task 2: Incremental CPG Parser Service
- **Chương 3** — Task 3: Kafka Topic Design & Producer
- **Chương 4** — Task 4: Graph Topology Ingestion into Neo4j
- **Chương 5** — Task 5: Source Metadata Ingestion into MongoDB
- **Chương 6** — Task 6: Idempotent Replay Verification
