# Task 3 — Thiết kế Kafka Topic & Schema

Tài liệu này mô tả **hợp đồng dữ liệu (data contract)** giữa Parser Service (producer) và
các consumer (Neo4j Sink ở Task 4, Spark → MongoDB ở Task 5). Mọi message đi qua Kafka đều
phải tuân theo schema tương ứng trong thư mục này.

## 1. Danh sách topic

| Topic | Loại event | Message key | Consumer chính |
|---|---|---|---|
| `code.events.nodes` | Node CPG (AST node) | `file_path` | Neo4j Sink (Task 4) |
| `code.events.edges` | Cạnh CPG (AST/CFG/DFG/CALL) | `file_path` | Neo4j Sink (Task 4) |
| `code.events.metadata` | Thông tin cấp file | `file_path` | Spark → MongoDB (Task 5) |
| `code.events.errors` | File parse lỗi | `file_path` | (theo dõi/log) |

> Cấu hình khởi tạo (gợi ý cho môi trường học tập, 1 broker): mỗi topic
> `partitions = 1`, `replication-factor = 1`. Có thể tăng partition khi cần song song.

## 2. Vì sao tách 4 topic thay vì 1 topic chung?

- **Tách theo consumer:** Neo4j chỉ quan tâm node/edge; MongoDB chỉ quan tâm metadata. Tách
  topic để mỗi consumer đọc đúng thứ nó cần, không phải lọc bỏ.
- **Schema rõ ràng:** mỗi topic một cấu trúc cố định, dễ validate và dễ giải thích trong báo cáo.
- **Xử lý lỗi độc lập:** topic `errors` tách riêng để một file hỏng không làm nghẽn luồng node/edge.

## 3. Vì sao chọn key = `file_path`? (và idempotency đến từ đâu)

**Cần tách bạch 2 cơ chế khác nhau, đừng gộp làm một:**

1. **Kafka key** chỉ quyết định **partition** và **thứ tự trong partition**:
   `partition = hash(key) % số_partition` → cùng key luôn vào cùng partition, giữ đúng thứ tự.
   Chọn key = `file_path` cho **cả 4 topic** để **mọi event của cùng một file** (mọi node, mọi
   cạnh, metadata) rơi vào cùng partition và được xử lý **đúng thứ tự thời gian**. Điều này quan
   trọng cho Task 6: khi một file bị sửa và parse lại, các thay đổi của nó không bị đảo thứ tự.

2. **Idempotency (chống trùng)** KHÔNG đến từ Kafka key, mà đến từ **Cypher `MERGE` theo trường
   `node_id`/`edge_id` nằm trong PAYLOAD** của message:

   ```cypher
   MERGE (n:CPGNode {node_id: event.node_id}) SET n += event
   ```

   Parse lại cùng một node → cùng `node_id` trong payload → `MERGE` gộp đúng một bản, **không tạo
   trùng** — độc lập hoàn toàn với việc message nằm ở partition nào. Đây mới là lõi idempotency.

> Vì key không tham gia chống trùng, đổi key sang `file_path` **không** ảnh hưởng tính idempotent.
> Ở môi trường học 1 broker / 1 partition thì mọi message vốn chung 1 partition, nên đổi key gần
> như trung tính về chức năng — nhưng đây là thiết kế **đúng khi scale nhiều partition** và nhất
> quán giữa 4 topic, dễ giải thích trong Jupyter Book.

- `file_path` cho metadata: bản metadata mới của một file **ghi đè** bản cũ (upsert theo file),
  đúng ngữ nghĩa "trạng thái mới nhất của file".

## 4. Các field bắt buộc chung (envelope)

Mọi message value đều có:

- `schema_version` (ví dụ `"v1"`): để consumer đọc đúng khi schema đổi trong tương lai
  (forward compatibility).
- `event_timestamp` (ISO 8601 UTC): **event-time** — thời điểm sự kiện thực sự xảy ra (lúc parse),
  không phụ thuộc lúc Kafka nhận message.
- `repo_commit`: commit hash để truy vết dữ liệu thuộc phiên bản code nào.

## 5. Quy ước Stable ID (ĐỘC LẬP với số dòng — quan trọng cho Task 6)

```
node_id = sha256(f"{file_path}|{qualified_scope}|{node_type}|{sibling_index}")[:24]
edge_id = sha256(f"{edge_type}|{source_node_id}|{target_node_id}|{dfg_variable?}")[:24]
```

Trong đó:
- `qualified_scope`: chuỗi tên các scope bao quanh node, ví dụ `Model.forward` (dùng **tên**, không
  dùng số dòng).
- `sibling_index`: thứ tự của node trong nhóm các node **cùng loại, cùng scope cha** — để phân biệt
  các node vô danh (ví dụ nhiều khối `If`/`For` trong cùng một hàm).

**Nguyên tắc quan trọng:** hash **KHÔNG** chứa `line_start`/`line_end`.

- Task 6 yêu cầu **sửa 1 file rồi parse lại** và không được tạo node trùng. Nếu `node_id` phụ thuộc
  số dòng, chỉ cần thêm 1 dòng ở đầu file là **mọi node phía dưới đổi số dòng → đổi id → Neo4j coi
  là node mới**, còn node cũ thành mồ côi → hỏng idempotent, mất điểm phần replay.
- `line_start`/`line_end` **vẫn được gửi** trong payload như thuộc tính **mô tả**, và được cập nhật
  qua `MERGE ... SET n += event` mỗi lần parse lại — chỉ là **không** tham gia vào hash.
- Node có tên (FunctionDef/ClassDef) → id ổn định tuyệt đối. Node vô danh chỉ đổi id khi bị
  **chèn/đảo vị trí thật sự** trong scope — đó là thay đổi cấu trúc có thật, chấp nhận được.

Dùng **hash nội dung/cấu trúc** (content-addressable), KHÔNG dùng số tăng dần — vì số tăng dần sẽ
sinh id mới mỗi lần parse lại, làm hỏng tính idempotent.

## 6. Ví dụ message (value)

Node:
```json
{
  "schema_version": "v1",
  "event_timestamp": "2026-07-20T10:15:30Z",
  "node_id": "a1b2c3d4e5f60718",
  "node_type": "FunctionDef",
  "name": "load_model",
  "file_path": "src/models/bert.py",
  "line_start": 42,
  "line_end": 58,
  "col_start": 0,
  "col_end": 15,
  "code_snippet": "def load_model(path):",
  "repo": "huggingface/transformers-pr-agent",
  "repo_commit": "abc1234"
}
```

Edge (DFG):
```json
{
  "schema_version": "v1",
  "event_timestamp": "2026-07-20T10:15:30Z",
  "edge_id": "9f8e7d6c5b4a3021",
  "edge_type": "DFG",
  "source_node_id": "a1b2c3d4e5f60718",
  "target_node_id": "b2c3d4e5f6071829",
  "dfg_variable": "path",
  "file_path": "src/models/bert.py",
  "repo_commit": "abc1234"
}
```

Metadata:
```json
{
  "schema_version": "v1",
  "event_timestamp": "2026-07-20T10:15:31Z",
  "file_path": "src/models/bert.py",
  "file_hash": "3b1f...<64 hex>",
  "language": "python",
  "loc": 120,
  "num_nodes": 340,
  "num_edges": 410,
  "repo": "huggingface/transformers-pr-agent",
  "repo_commit": "abc1234"
}
```

Error:
```json
{
  "schema_version": "v1",
  "event_timestamp": "2026-07-20T10:15:32Z",
  "file_path": "src/broken/weird.py",
  "error_type": "SyntaxError",
  "error_message": "invalid syntax (weird.py, line 12)",
  "stack_trace": "Traceback (most recent call last): ...",
  "repo_commit": "abc1234"
}
```

## 7. File schema trong thư mục này

- `node_event.schema.json`
- `edge_event.schema.json`
- `metadata_event.schema.json`
- `error_event.schema.json`

Đây là JSON Schema (draft-07). Ngoài vai trò tài liệu, Parser Service (Task 2) có thể dùng chúng
để validate message trước khi publish.
