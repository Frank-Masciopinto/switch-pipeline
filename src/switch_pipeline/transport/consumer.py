"""Kafka consumer side: turns librdkafka records into transport-neutral messages."""

from confluent_kafka import Message

from switch_pipeline.transport.codec import InboundMessage, decode_headers


def to_inbound(record: Message) -> InboundMessage:
    topic, partition, offset = record.topic(), record.partition(), record.offset()
    if topic is None or partition is None or offset is None:
        raise ValueError("consumed record has no topic/partition/offset")
    return InboundMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=record.key(),
        value=record.value(),
        headers=decode_headers(record.headers()),
    )
