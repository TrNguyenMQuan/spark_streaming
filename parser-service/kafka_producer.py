# publish CPG events to 4 Kafka topics, key = file_path

import json
from kafka import KafkaProducer

TOPIC_NODES     = "code.events.nodes"
TOPIC_EDGES     = "code.events.edges"
TOPIC_METADATA  = "code.events.metadata"
TOPIC_ERRORS    = "code.events.errors"

class CPGProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",     # broker confirms write
            linger_ms=50    # small batching for throughput
        )

    def _send(self, topic: str, event: dict) -> None:
        # key = file_path: same_file -> same_partition -> ordered
        self.producer.send(topic, key=event["file_path"], value=event)

    def publish_nodes(self, nodes: list[dict]) -> None:
        for node in nodes:
            self._send(TOPIC_NODES, node)

    def publish_edges(self, egdes: list[dict]) -> None:
        for edge in egdes:
            self._send(TOPIC_EDGES, edge)

    def publish_metadata(self, meta: dict) -> None:
        self._send(TOPIC_METADATA, meta)

    def publish_errors(self, errors: dict) -> None:
        self._send(TOPIC_ERRORS, errors)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()

        