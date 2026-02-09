"""Kafka producer for publishing detected anomalous queries."""
from datetime import datetime, timezone

import orjson
from quixstreams import Application

from config import KAFKA_BOOTSTRAP_SERVERS


class AnomalousQueryProducer:
    """Kafka producer for publishing anomalous query detections.
    
    Serializes query data to JSON and publishes to a configurable topic.
    """

    def __init__(
        self,
        bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = "anomalous_queries",
        key_col: str | None = "cluster_id",
    ):
        self.bootstrap = bootstrap
        self.topic_name = topic
        self.key_col = key_col

        self.app = Application(broker_address=self.bootstrap)
        self.topic = self.app.topic(self.topic_name)

        self._producer = None

    def start(self):
        """Initialize the Kafka producer connection."""
        if self._producer is None:
            self._producer = self.app.get_producer()
            self._producer.__enter__()


    def normalize_for_json(self, row: dict) -> dict:
        """Convert datetime values to ISO-8601 strings for JSON serialization."""
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                if v.tzinfo is None:
                    v = v.replace(tzinfo=timezone.utc)
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out

    def publish(self, row: dict):
        """Publish a single anomaly record to Kafka."""
        if self._producer is None:
            self.start()

        payload = orjson.dumps(self.normalize_for_json(row))

        key = None
        if self.key_col:
            key_val = row.get(self.key_col)
            if key_val is not None:
                key = str(key_val).encode("utf-8")

        self._producer.produce(
            topic=self.topic.name,
            key=key,
            value=payload,
        )

    def publish_many(self, rows: list[dict]):
        """Publish multiple anomaly records to Kafka."""
        for row in rows:
            self.publish(row)

    def flush(self, timeout: float = 5.0):
        """Ensure all buffered messages are delivered."""
        if self._producer is not None:
            self._producer.flush(timeout=timeout)

    def close(self):
        """Close the producer connection."""
        if self._producer is not None:
            try:
                self._producer.flush()
            finally:
                self._producer.__exit__(None, None, None)
                self._producer = None
