# Task 5 — Spark Structured Streaming → MongoDB

`metadata_to_mongodb.py` consumes only `code.events.metadata`, parses each Kafka
value against the v1 metadata contract, and persists the latest document per
`file_path` in MongoDB.

Before writing, it validates every required v1 field, non-negative counts, a
64-character lowercase SHA-256 `file_hash`, `language=python`, a parseable UTC
timestamp, and that the Kafka message key equals `file_path`.

## Run

From the repository root:

```bash
docker compose up -d
docker compose ps
python parser-service/parser.py --limit 30 --publish
docker compose logs -f spark-metadata-to-mongodb
```

The Spark job runs inside Docker, therefore it connects to Kafka using
`kafka:19092` rather than `localhost:9092`. Mongo Express is available at
<http://localhost:8081>; database `cpg`, collection `source_metadata`.

## Verify

```bash
docker compose exec mongodb mongosh cpg --quiet --eval \
  'db.source_metadata.countDocuments({}); db.source_metadata.findOne()'
docker compose exec mongodb mongosh cpg --quiet --eval \
  'db.source_metadata.aggregate([{ $group: { _id: "$file_path", n: { $sum: 1 } } }, { $match: { n: { $gt: 1 } } }]).toArray()'
```

The second command must return `[]`: the MongoDB connector uses
`operationType=replace`, `idFieldList=file_path`, and `upsertDocument=true`.

## Replay behaviour

The ignored local folder `spark-checkpoints/` holds Structured Streaming's
`checkpointLocation`. A restart resumes at the recorded Kafka offsets, so
unchanged messages are skipped. When a file is modified and the producer emits
a new metadata event, MongoDB replaces that file's document instead of adding a
duplicate. Use `docker compose down -v` only when intentionally resetting both
Kafka data and the Spark checkpoint for a fresh demonstration.
