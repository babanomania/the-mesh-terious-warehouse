import argparse
import csv
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pika
from pydantic import BaseModel, Field, ValidationError

import sys
from pathlib import Path as _Path

# Ensure parent directory (with base_generator) is on path when executed as a script
sys.path.append(str(_Path(__file__).resolve().parents[1]))

from base_generator import BaseGenerator


REGION = "south"
rng = BaseGenerator(f"restock_event_{REGION}")


class RestockEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_ts: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    event_type: str = "restock_received"
    restock_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    warehouse_id: str
    restock_ts: str
    qty: int


PRODUCT_CATALOG = [f"P{i:05d}" for i in range(1, 100)]
WAREHOUSES = ["WH-S1", "WH-S2", "WH-S3"]


def generate_event() -> RestockEvent:
    product_id = rng.choice(PRODUCT_CATALOG)
    warehouse_id = rng.choice(WAREHOUSES)
    restock_ts = datetime.utcnow().isoformat()
    qty = rng.randint(10, 100)
    return RestockEvent(
        product_id=product_id,
        warehouse_id=warehouse_id,
        restock_ts=restock_ts,
        qty=qty,
    )


def publish_events(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue: str,
    events: Iterable[RestockEvent],
) -> None:
    for event in events:
        try:
            payload = event.json()
            channel.basic_publish(exchange="", routing_key=queue, body=payload)
            print(payload)
        except ValidationError as exc:
            print(f"Validation failed: {exc}")


def live_mode(
    channel: pika.adapters.blocking_connection.BlockingChannel, queue: str, interval: int
) -> None:
    while True:
        publish_events(channel, queue, [generate_event()])
        time.sleep(interval)


def burst_mode(
    channel: pika.adapters.blocking_connection.BlockingChannel, queue: str, count: int
) -> None:
    events = [generate_event() for _ in range(count)]
    publish_events(channel, queue, events)


def replay_mode(
    channel: pika.adapters.blocking_connection.BlockingChannel, queue: str, path: Path
) -> None:
    with path.open() as fh:
        reader = csv.DictReader(fh)
        events = []
        for row in reader:
            evt = RestockEvent(
                product_id=row["product_id"],
                warehouse_id=row["warehouse_id"],
                restock_ts=row["restock_ts"],
                qty=int(row["qty"]),
            )
            events.append(evt)
        publish_events(channel, queue, events)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Produce restock events for the north region"
    )
    parser.add_argument("--live", type=int, help="Emit events every N seconds")
    parser.add_argument("--burst", type=int, help="Emit M events immediately")
    parser.add_argument("--replay", type=Path, help="Replay events from a CSV file")
    args = parser.parse_args()

    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"), os.getenv("RABBITMQ_PASSWORD", "guest")
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        credentials=credentials,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    queue = f"restocks_{REGION}"
    channel.queue_declare(queue=queue, durable=True)

    if args.live:
        live_mode(channel, queue, args.live)
    elif args.burst:
        burst_mode(channel, queue, args.burst)
    elif args.replay:
        replay_mode(channel, queue, args.replay)
    else:
        parser.error("One of --live, --burst, or --replay must be provided")

    connection.close()


if __name__ == "__main__":  # pragma: no cover
    main()
