"""Transport: everything Kafka.

Client configuration (with the delivery guarantees), the wire format of change
events, topic administration, the producer, consumer streams and group lag.
Depends only on the domain; knows nothing about Snowflake or PostgreSQL.
"""
