"""
pipeline/kafka_streaming.py
───────────────────────────
Step 17 — Simulate real-time data processing using streaming APIs
Step 18 — Set up Kafka producer-consumer pipeline
Step 19 — Implement partitioning and offset management

Architecture:
  ViolationProducer  →  Kafka Topic: traffic-violations  →  ViolationConsumer
                                  ↓
                        Spark Structured Streaming (Step 20)

Running without a real Kafka cluster:
  USE_REAL_KAFKA=false  → uses an in-process queue to simulate Kafka semantics
  USE_REAL_KAFKA=true   → connects to localhost:9092 (or KAFKA_BOOTSTRAP_SERVERS)

To start real Kafka locally:
  docker-compose up -d   (see docker-compose.yml)
"""

import os
import json
import time
import queue
import random
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
USE_REAL_KAFKA       = os.getenv("USE_REAL_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_VIOLATIONS     = "traffic-violations"
TOPIC_ALERTS         = "traffic-alerts"
NUM_PARTITIONS       = 5       # Step 19 — partition count
CONSUMER_GROUP_ID    = "traffic-pipeline-group"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 17 — Event Schema
# ─────────────────────────────────────────────────────────────────────────────

VIOLATION_TYPES = ["Speeding", "Red-Light", "DUI", "Illegal Park",
                   "Reckless", "Mobile Use", "No Seatbelt"]
ZONES           = ["Downtown", "Highway", "School Zone", "Residential", "Industrial"]

def generate_violation_event() -> dict:
    """
    Generate a single realistic traffic violation event.
    Simulates what a real IoT sensor / camera system would emit.
    """
    vtype  = random.choice(VIOLATION_TYPES)
    zone   = random.choice(ZONES)
    speed  = random.randint(5, 80) if vtype == "Speeding" else random.randint(0, 20)
    fine_map = {
        "DUI": 1000, "Reckless": 800, "Red-Light": 500,
        "Speeding": 200 if speed > 30 else 100,
        "Mobile Use": 250, "Illegal Park": 75, "No Seatbelt": 100,
    }
    return {
        "event_id":       f"EVT-{int(time.time()*1000)}-{random.randint(0,9999):04d}",
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "violation_type": vtype,
        "zone":           zone,
        "speed_kmh":      speed,
        "fine_amount":    fine_map.get(vtype, 150),
        "vehicle_plate":  f"{random.choice('ABCDEFGH')}{random.randint(1000,9999)}",
        "officer_id":     f"OFF-{random.randint(1,50):03d}",
        "latitude":       round(random.uniform(28.40, 28.90), 6),
        "longitude":      round(random.uniform(77.00, 77.40), 6),
        "camera_id":      f"CAM-{zone[:3].upper()}-{random.randint(1,20):02d}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# IN-PROCESS KAFKA SIMULATION (no external dependencies)
# ─────────────────────────────────────────────────────────────────────────────

class SimulatedKafkaBroker:
    """
    Thread-safe in-process message broker simulating Kafka semantics.
    Supports: topics, partitions, consumer groups, offset tracking.
    Single instance shared across producer/consumer via class variable.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        # Step 19 — partitioned topic storage
        self.topics: dict[str, list[queue.Queue]] = {}
        self.offsets: dict[str, dict[str, int]] = {}   # group_id → partition → offset
        self.committed: dict[str, list[dict]] = {}      # topic → list of all messages
        self._lock = threading.Lock()

    def create_topic(self, topic: str, partitions: int = NUM_PARTITIONS):
        with self._lock:
            if topic not in self.topics:
                self.topics[topic]    = [queue.Queue() for _ in range(partitions)]
                self.committed[topic] = []
                logger.info(f"  Kafka: created topic '{topic}' with {partitions} partitions")

    def produce(self, topic: str, key: str, value: dict, partition: int = None) -> int:
        """
        Step 19 — Partition assignment:
        If partition not specified, hash the key to select partition
        (same key always goes to same partition — ordering guarantee).
        """
        with self._lock:
            if topic not in self.topics:
                self.create_topic(topic)
            n = len(self.topics[topic])
            if partition is None:
                partition = hash(key) % n    # consistent hashing by key
            msg = {
                "topic":     topic,
                "partition": partition,
                "offset":    len(self.committed[topic]),
                "key":       key,
                "value":     value,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.topics[topic][partition].put(msg)
            self.committed[topic].append(msg)
            return msg["offset"]

    def consume(self, topic: str, group_id: str,
                partition: int = 0, timeout: float = 0.1) -> Optional[dict]:
        """
        Step 19 — Offset management:
        Each consumer group tracks its own offset per partition.
        Messages are not deleted on consume (Kafka log retention).
        """
        if topic not in self.topics:
            return None
        try:
            msg = self.topics[topic][partition].get(timeout=timeout)
            # Track offset per group
            key = f"{group_id}:{topic}:{partition}"
            self.offsets[key] = msg["offset"] + 1
            return msg
        except queue.Empty:
            return None

    def get_lag(self, topic: str, group_id: str) -> dict:
        """Step 19 — Consumer lag = latest offset - committed offset."""
        lag = {}
        if topic not in self.topics:
            return lag
        for p, q_obj in enumerate(self.topics[topic]):
            key = f"{group_id}:{topic}:{p}"
            committed = self.offsets.get(key, 0)
            latest    = len([m for m in self.committed[topic] if m["partition"] == p])
            lag[p] = latest - committed
        return lag

    def topic_stats(self, topic: str) -> dict:
        if topic not in self.committed:
            return {}
        msgs = self.committed[topic]
        partition_counts = {}
        for m in msgs:
            partition_counts[m["partition"]] = partition_counts.get(m["partition"], 0) + 1
        return {
            "topic":       topic,
            "total_msgs":  len(msgs),
            "partitions":  partition_counts,
        }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 18 — Kafka Producer
# ─────────────────────────────────────────────────────────────────────────────

class ViolationProducer:
    """
    Step 18 — Kafka producer for traffic violation events.

    Partitioning strategy (Step 19):
      Key = zone name → all violations from same zone go to same partition
      → enables zone-ordered processing downstream
    """

    def __init__(self, topic: str = TOPIC_VIOLATIONS):
        self.topic = topic
        self._produced = 0

        if USE_REAL_KAFKA:
            try:
                from kafka import KafkaProducer
                self._producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8"),
                    acks="all",               # wait for all replicas
                    retries=3,
                    compression_type="gzip",
                    batch_size=16384,         # batch small messages
                    linger_ms=5,              # wait 5ms to batch
                )
                logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP}")
            except Exception as e:
                logger.warning(f"Real Kafka unavailable ({e}), using simulation")
                self._producer = None
        else:
            self._producer = None
            self._broker = SimulatedKafkaBroker()
            self._broker.create_topic(topic, partitions=NUM_PARTITIONS)

    def send(self, event: dict) -> int:
        """Send a single violation event. Returns offset."""
        key = event.get("zone", "unknown")

        if self._producer:
            future = self._producer.send(
                self.topic,
                key=key,
                value=event,
            )
            record_metadata = future.get(timeout=10)
            return record_metadata.offset
        else:
            return self._broker.produce(self.topic, key=key, value=event)

    def send_batch(self, events: list) -> list:
        """Send a batch of events efficiently."""
        offsets = []
        for event in events:
            offset = self.send(event)
            offsets.append(offset)
            self._produced += 1
        if self._producer:
            self._producer.flush()
        logger.info(f"  Producer: sent {len(events)} events to '{self.topic}' "
                    f"(total: {self._produced})")
        return offsets

    def produce_continuous(self, rate_per_sec: int = 10,
                           duration_sec: int = 30,
                           on_produce: Callable = None):
        """
        Step 17 — Simulate continuous real-time stream.
        Produces `rate_per_sec` events every second for `duration_sec`.
        """
        logger.info(f"  Streaming: producing {rate_per_sec} events/sec "
                    f"for {duration_sec}s → topic '{self.topic}'")
        total = 0
        interval = 1.0 / rate_per_sec
        end_time = time.time() + duration_sec

        while time.time() < end_time:
            event = generate_violation_event()
            offset = self.send(event)
            total += 1
            if on_produce:
                on_produce(event, offset)
            time.sleep(interval)

        logger.info(f"  Streaming: produced {total} events in {duration_sec}s")
        return total

    def close(self):
        if self._producer:
            self._producer.close()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 18 — Kafka Consumer
# ─────────────────────────────────────────────────────────────────────────────

class ViolationConsumer:
    """
    Step 18 — Kafka consumer with manual offset management.
    Implements at-least-once delivery semantics.
    """

    def __init__(self, topic: str = TOPIC_VIOLATIONS,
                 group_id: str = CONSUMER_GROUP_ID,
                 partitions: list = None):
        self.topic    = topic
        self.group_id = group_id
        self.partitions = partitions or list(range(NUM_PARTITIONS))
        self._consumed = 0
        self._running  = False

        if USE_REAL_KAFKA:
            try:
                from kafka import KafkaConsumer
                self._consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id=group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,   # Step 19 — manual offset commit
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer=lambda k: k.decode("utf-8") if k else None,
                    max_poll_records=500,
                )
                logger.info(f"Kafka consumer connected — group={group_id}")
            except Exception as e:
                logger.warning(f"Real Kafka unavailable ({e}), using simulation")
                self._consumer = None
        else:
            self._consumer = None
            self._broker   = SimulatedKafkaBroker()

    def consume_batch(self, max_messages: int = 100,
                      timeout_ms: int = 1000,
                      processor: Callable = None) -> list:
        """
        Poll for up to `max_messages` records.
        Step 19 — manual commit after successful processing.
        """
        records = []
        if self._consumer:
            raw = self._consumer.poll(timeout_ms=timeout_ms,
                                      max_records=max_messages)
            for tp, msgs in raw.items():
                for msg in msgs:
                    record = {"partition": tp.partition,
                              "offset":   msg.offset,
                              "key":      msg.key,
                              "value":    msg.value}
                    records.append(record)
                    if processor:
                        processor(record)
            if records:
                self._consumer.commit()    # Step 19 — commit after processing
        else:
            for partition in self.partitions:
                for _ in range(max_messages // len(self.partitions)):
                    msg = self._broker.consume(
                        self.topic, self.group_id,
                        partition=partition, timeout=0.01
                    )
                    if msg:
                        records.append(msg)
                        if processor:
                            processor(msg)
                    else:
                        break

        self._consumed += len(records)
        return records

    def consume_stream(self, processor: Callable,
                       max_records: int = 1000,
                       batch_size: int = 50):
        """
        Step 17 — Continuously consume until max_records reached.
        Each batch is processed then committed.
        """
        self._running = True
        total = 0
        logger.info(f"  Consumer: streaming from '{self.topic}'...")

        while self._running and total < max_records:
            batch = self.consume_batch(max_messages=batch_size, processor=processor)
            if not batch:
                time.sleep(0.1)
                continue
            total += len(batch)
            logger.debug(f"  Consumer: processed batch of {len(batch)} "
                         f"(total: {total})")

        logger.info(f"  Consumer: finished — {total} records processed")
        return total

    def get_lag(self) -> dict:
        """Step 19 — Report consumer lag per partition."""
        if self._consumer:
            # Real Kafka lag calculation
            from kafka import TopicPartition
            partitions = [TopicPartition(self.topic, p) for p in self.partitions]
            end_offsets = self._consumer.end_offsets(partitions)
            committed   = {p: (self._consumer.committed(p) or 0) for p in partitions}
            return {p.partition: end_offsets[p] - committed[p] for p in partitions}
        return self._broker.get_lag(self.topic, self.group_id)

    def close(self):
        self._running = False
        if self._consumer:
            self._consumer.close()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 20 — Spark Structured Streaming job spec
# ─────────────────────────────────────────────────────────────────────────────

STRUCTURED_STREAMING_CODE = '''
# pipeline/structured_streaming.py — Step 20
# Run this with: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 structured_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \\
    .appName("TrafficStructuredStreaming") \\
    .config("spark.sql.shuffle.partitions", "8") \\
    .getOrCreate()

# ── Schema ──────────────────────────────────────────────────────────────────
schema = StructType([
    StructField("event_id",       StringType(), True),
    StructField("timestamp",      StringType(), True),
    StructField("violation_type", StringType(), True),
    StructField("zone",           StringType(), True),
    StructField("speed_kmh",      FloatType(),  True),
    StructField("fine_amount",    FloatType(),  True),
    StructField("vehicle_plate",  StringType(), True),
])

# ── Read from Kafka ──────────────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe",               "traffic-violations")
    .option("startingOffsets",         "latest")
    .option("maxOffsetsPerTrigger",    "1000")    # backpressure
    .load()
)

# ── Parse JSON payload ───────────────────────────────────────────────────────
violations_stream = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
)

# ── Stateful aggregation with watermark ─────────────────────────────────────
# Watermark: tolerate up to 2 minutes of late data
windowed = (
    violations_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        F.window("event_time", "5 minutes", "1 minute"),  # 5-min sliding window
        "zone",
        "violation_type",
    )
    .agg(
        F.count("*").alias("violation_count"),
        F.round(F.avg("fine_amount"), 2).alias("avg_fine"),
        F.round(F.sum("fine_amount"), 2).alias("revenue"),
        F.round(F.avg("speed_kmh"),   2).alias("avg_speed"),
    )
)

# ── Alert stream: high-severity events ──────────────────────────────────────
alerts = (
    violations_stream
    .filter(
        (F.col("violation_type").isin("DUI", "Reckless")) |
        (F.col("speed_kmh") > 60)
    )
    .select("event_id", "violation_type", "zone", "speed_kmh",
            "fine_amount", "event_time")
)

# ── Write windowed aggregation to Delta Lake ─────────────────────────────────
query_agg = (
    windowed.writeStream
    .outputMode("update")           # only emit updated windows
    .format("delta")
    .option("path",       "data/spark_output/streaming_agg")
    .option("checkpointLocation", "data/checkpoints/streaming_agg")
    .trigger(processingTime="30 seconds")
    .start()
)

# ── Write alerts to console + Kafka alerts topic ────────────────────────────
query_alerts = (
    alerts.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "traffic-alerts")
    .option("checkpointLocation", "data/checkpoints/alerts")
    .start()
)

query_agg.awaitTermination()
query_alerts.awaitTermination()
'''


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE INTEGRATION — run simulated end-to-end
# ─────────────────────────────────────────────────────────────────────────────

def run_kafka_simulation(n_events: int = 500,
                         rate_per_sec: int = 50) -> dict:
    """
    Run a simulated producer → consumer cycle.
    Returns summary stats. Called from main pipeline orchestrator.
    """
    results_store = []

    def on_consume(msg):
        val = msg.get("value", {})
        results_store.append({
            "zone":      val.get("zone"),
            "type":      val.get("violation_type"),
            "fine":      val.get("fine_amount", 0),
            "partition": msg.get("partition"),
            "offset":    msg.get("offset"),
        })

    producer = ViolationProducer(TOPIC_VIOLATIONS)
    consumer = ViolationConsumer(TOPIC_VIOLATIONS)

    # Generate and send events
    events = [generate_violation_event() for _ in range(n_events)]
    offsets = producer.send_batch(events)

    # Consume all events
    consumer.consume_stream(processor=on_consume, max_records=n_events)

    # Stats
    lag   = consumer.get_lag()
    stats = producer._broker.topic_stats(TOPIC_VIOLATIONS)

    logger.info("  Kafka simulation complete:")
    logger.info(f"    Produced : {n_events} events")
    logger.info(f"    Consumed : {len(results_store)} events")
    logger.info(f"    Partition distribution: {stats.get('partitions', {})}")
    logger.info(f"    Consumer lag: {lag}")

    producer.close()
    consumer.close()

    return {
        "produced":    n_events,
        "consumed":    len(results_store),
        "partition_stats": stats,
        "consumer_lag":    lag,
        "sample_records":  results_store[:5],
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    result = run_kafka_simulation(n_events=200, rate_per_sec=50)
    print(json.dumps(result, indent=2, default=str))

    # Print Structured Streaming code
    print("\n\n" + "="*60)
    print("SPARK STRUCTURED STREAMING CODE (Step 20)")
    print("="*60)
    print(STRUCTURED_STREAMING_CODE)
