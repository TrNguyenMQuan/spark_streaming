# HANDOFF — Lab04 CPG Streaming Pipeline

Tài liệu bàn giao từ **Thành viên A (Data Engineer — producer side)** cho các thành viên
làm **consumer** (Neo4j Sink, Spark→MongoDB), **verification** và **Jupyter Book**.

Nhánh gốc để branch ra: **`feature/config`** (đã push lên `origin`).

---

## 1. Đã xong (producer side)

- **Hạ tầng Kafka** (`docker-compose.yml`): 1 broker KRaft + Kafka UI, tự tạo sẵn 4 topic.
- **Parser Service** (`parser-service/`): đọc từng file `.py` trong `target-repo/`, dùng module
  `ast` sinh **node / edge (AST, CFG, DFG, CALL) / metadata / error**, publish lên Kafka.
- **Hợp đồng dữ liệu** (`parser-service/schemas/`): JSON Schema + README cho 4 topic — **đây là
  thứ các bạn code dựa vào**.

Ranh giới bàn giao = **Kafka**. Các bạn KHÔNG cần đọc code parser; chỉ cần bám vào hợp đồng ở
mục 3 và đọc dữ liệu từ 4 topic.

---

## 2. Quickstart — dựng & seed dữ liệu

```bash
# 1) Dựng Kafka + Kafka UI
docker compose up -d
docker compose ps               # chờ 'kafka' -> healthy

# 2) Cài dependency cho producer (nếu Python 3.14 lỗi import kafka -> đổi sang kafka-python-ng)
pip install -r requirements.txt

# 3) Seed dữ liệu: parse 30 file đầu và publish lên Kafka
python3 parser-service/parser.py --limit 30 --publish
#   --limit N : chỉ N file đầu (demo nhanh). Bỏ --limit = full ~2400 file (~1.6M cạnh, NẶNG).
#   --publish : thực sự đẩy lên Kafka. Bỏ đi = chỉ in summary.

# 4) Kiểm tra
#   - Kafka UI: http://localhost:8080  -> tab Topics (thấy message count > 0)
#   - hoặc console:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic code.events.nodes --from-beginning --max-messages 3
```

### ĐỊA CHỈ KẾT NỐI KAFKA (đọc kỹ — bẫy hay gặp nhất)

Broker bind theo hostname `kafka`, nên tùy chỗ bạn đứng mà dùng địa chỉ khác nhau:

| Bạn chạy ở đâu | bootstrap.servers |
|---|---|
| **Trong container** (Kafka Connect, Spark trong Docker, Kafka UI) | `kafka:19092` |
| **Trên host** (script Python, spark-submit ở máy, console tool) | `localhost:9092` |

Dùng nhầm `localhost:9092` bên trong container sẽ bị **connection refused**.

### Dữ liệu topic
- Có **named volume `kafka-data`** → dữ liệu **giữ nguyên** qua `docker compose stop/start` và cả
  `docker compose down`.
- Chỉ `docker compose down -v` mới **xóa sạch** (dùng khi muốn test lại Task 6 từ đầu). Xóa xong
  chạy lại producer (mục 2.3) để seed lại.

---

## 3. Hợp đồng dữ liệu (data contract)

Chi tiết đầy đủ + ví dụ message ở **`parser-service/schemas/README.md`** và 4 file `*.schema.json`.
Tóm tắt:

| Topic | Nội dung | Partitions | Message key | Consumer |
|---|---|---|---|---|
| `code.events.nodes` | Node CPG (AST node) | 3 | `file_path` | Neo4j Sink (Task 4) |
| `code.events.edges` | Cạnh CPG (AST/CFG/DFG/CALL) | 3 | `file_path` | Neo4j Sink (Task 4) |
| `code.events.metadata` | Thông tin cấp file | 1 | `file_path` | Spark→MongoDB (Task 5) |
| `code.events.errors` | File parse lỗi | 1 | `file_path` | (log/monitor) |

**Envelope chung** (mọi message value): `schema_version` (`"v1"`), `event_timestamp` (ISO 8601 UTC),
`repo_commit`, và `file_path`. Value được serialize **JSON (UTF-8)**; key là chuỗi `file_path` (UTF-8).

### ⚠️ Idempotency — điểm mấu chốt cho Task 4
Chống trùng **KHÔNG** đến từ Kafka key. Nó đến từ việc **`MERGE` theo `node_id` / `edge_id` nằm
trong PAYLOAD** của message:

```cypher
MERGE (n:CPGNode {node_id: event.node_id})  SET n += event
MERGE (a:CPGNode {node_id: event.source_node_id})
MERGE (b:CPGNode {node_id: event.target_node_id})
MERGE (a)-[r:CPG_EDGE {edge_id: event.edge_id}]->(b)  SET r += event
```

`node_id`/`edge_id` là **stable hash theo cấu trúc, độc lập số dòng** → parse lại cùng nội dung ra
cùng id → `MERGE` gộp đúng 1 bản, không tạo trùng (nền tảng cho Task 6).

---

## 4. Việc còn mở (cho các bạn)

### Task 4 — Neo4j Ingestion (Kafka Connect Sink)
- Dựng **Neo4j** + **Kafka Connect** (thêm service vào compose), cài **Neo4j Kafka Connector (Sink)**.
- Subscribe `code.events.nodes` + `code.events.edges`; `bootstrap.servers = kafka:19092`.
- Dùng **Cypher `MERGE`** như mục 3 (idempotent). `tasks.max` có thể tới 3 (khớp 3 partition).

### Task 5 — Spark Structured Streaming → MongoDB
- Dựng **Spark** + **MongoDB**. `spark.readStream.format("kafka")` subscribe `code.events.metadata`.
- Parse JSON theo `metadata_event.schema.json`, ghi vào MongoDB collection (upsert theo `file_path`).
- **Bắt buộc** có `checkpointLocation` (yêu cầu idempotency + để Task 6 skip offset đã xử lý).

### Task 6 — Idempotent Replay Verification
- Sửa **1 file** `.py` trong `target-repo/`, chạy lại producer, kiểm chứng:
  Neo4j **không** tạo node/edge trùng, MongoDB có metadata **cập nhật**, Spark checkpoint **skip**
  các file không đổi.

### Jupyter Book + Architecture diagram
- Mỗi chapter = 1 task. Sơ đồ kiến trúc: Parser → Kafka (4 topic) → {Neo4j Sink, Spark→Mongo}.

---

## 5. Quy tắc phối hợp (quan trọng)

1. **Đóng băng hợp đồng.** KHÔNG đổi tên topic / field / `key` / công thức stable ID. Nếu buộc
   phải đổi → **bump `schema_version` lên `"v2"`**, đừng sửa tại chỗ (sẽ phá consumer đang code).
2. **Tránh xung đột `docker-compose.yml`.** Mỗi nhóm thêm service (Neo4j/Connect/Mongo/Spark) nên
   dùng **`docker-compose.override.yml` riêng** (Compose tự merge), hoặc thống nhất 1 người giữ file
   chính. Đừng cùng sửa 1 file trên nhiều nhánh rồi merge.
3. **Nhánh riêng theo task.** Ví dụ `feature/neo4j-sink`, `feature/spark-mongo`, `feature/notebooks`
   — đều branch từ `feature/config`.

```bash
git fetch origin
git checkout feature/config && git pull
git checkout -b feature/neo4j-sink     # đổi tên tùy task của bạn
```

---

## 6. Cấu trúc thư mục liên quan

```
docker-compose.yml              # Kafka + Kafka UI (các bạn THÊM Neo4j/Connect/Mongo/Spark)
parser-service/
├── schemas/                    # HỢP ĐỒNG DỮ LIỆU — đọc cái này
│   ├── node_event.schema.json
│   ├── edge_event.schema.json
│   ├── metadata_event.schema.json
│   ├── error_event.schema.json
│   └── README.md               # giải thích topic/key/envelope/ví dụ
├── stable_id.py                # hàm hash sinh node_id/edge_id
├── cpg_visitor.py              # duyệt AST -> node + 4 loại cạnh
├── kafka_producer.py           # publish 4 topic (key=file_path)
└── parser.py                   # entrypoint: --limit N --publish
discover_files.py               # liệt kê file .py trong target-repo (lọc test/setup/__init__)
target-repo/                    # repo mẫu: huggingface/transformers-pr-agent (gitignored)
```

Có gì không chạy được, ping mình (Thành viên A). Nhớ: **địa chỉ kết nối** (mục 2) và
**idempotency = MERGE theo id trong payload** (mục 3) là 2 chỗ hay vướng nhất.
