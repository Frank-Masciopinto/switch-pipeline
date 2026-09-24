"""Publishes change events to Kafka with confirmed delivery.

``publish`` returns only once the broker has acknowledged every event
(acks=all). Unconfirmed events are re-sent with backoff; if some are still
unconfirmed after the configured attempts it raises BrokerUnavailableError and
the caller must not advance its watermark. Re-sending can duplicate events the
broker did store (at-least-once); consumers drop those via the deterministic
event_id.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import BrokerUnavailableError, FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.retry import Backoff
from switch_pipeline.settings import KafkaSettings
from switch_pipeline.transport.codec import encode
from switch_pipeline.transport.config import producer_config

log = get_logger(__name__)

# Extra time allowed on top of delivery.timeout.ms, by which librdkafka has
# issued a final delivery report for every queued message.
_FLUSH_GRACE_SECONDS = 10.0

# Re-sending cannot fix these. They stop the adapter instead of retrying one
# poison batch forever or, worse, skipping it.
_NON_RETRIABLE = frozenset(
    {
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.INVALID_MSG,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._INVALID_ARG,
    }
)


class FatalPublishError(FatalPipelineError):
    """The producer is unusable or an event can never be delivered."""


@dataclass(frozen=True, slots=True)
class _Failure:
    event: ChangeEvent
    error: KafkaError


class KafkaEventPublisher:
    def __init__(
        self, settings: KafkaSettings, *, client_id: str, sleep: Callable[[float], bool]
    ) -> None:
        self._topic = settings.topic
        self._max_attempts = settings.publish_max_attempts
        self._backoff = Backoff(
            settings.publish_backoff_initial_seconds, settings.publish_backoff_max_seconds
        )
        self._flush_timeout = settings.delivery_timeout_ms / 1000 + _FLUSH_GRACE_SECONDS
        self._sleep = sleep  # returns True when shutdown interrupts the wait
        self._producer = Producer(producer_config(settings, client_id=client_id))

    def publish(self, events: Sequence[ChangeEvent]) -> None:
        pending = list(events)
        attempt = 0
        while True:
            attempt += 1
            failures = self._send(pending)
            if not failures:
                if attempt > 1:
                    log.info("publish_recovered", attempts=attempt, events=len(events))
                return
            error = failures[0].error
            if any(_is_fatal(failure.error) for failure in failures):
                raise FatalPublishError(f"events cannot be delivered: {error.str()}")
            if attempt >= self._max_attempts:
                raise BrokerUnavailableError(
                    f"{len(failures)} of {len(events)} events unconfirmed after "
                    f"{attempt} attempts: {error.str()}"
                )
            delay = self._backoff.delay(attempt)
            log.warning(
                "publish_retry_scheduled",
                unconfirmed=len(failures),
                total=len(events),
                attempt=attempt,
                max_attempts=self._max_attempts,
                retry_in_seconds=round(delay, 2),
                error=error.str(),
            )
            if self._sleep(delay):
                raise BrokerUnavailableError("shutdown requested while retrying delivery")
            pending = [failure.event for failure in failures]

    def close(self) -> None:
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            log.error("producer_closed_with_unconfirmed_messages", count=remaining)

    def _send(self, events: Sequence[ChangeEvent]) -> list[_Failure]:
        failures: list[_Failure] = []
        for event in events:
            self._produce(event, _on_delivery(event, failures))
        still_queued = self._producer.flush(self._flush_timeout)
        if still_queued:
            raise BrokerUnavailableError(
                f"{still_queued} messages still queued after flush timeout"
            )
        return failures

    def _produce(
        self, event: ChangeEvent, callback: Callable[[KafkaError | None, Message], None]
    ) -> None:
        record = encode(event)
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=record.value,
                    key=record.key,
                    headers=record.headers,
                    on_delivery=callback,
                )
            except BufferError:
                # Local queue is full: serve delivery reports to free space, then retry.
                self._producer.poll(0.5)
            except KafkaException as exc:
                error: KafkaError = exc.args[0]
                message = f"producer rejected event {event.event_id}: {error.str()}"
                if _is_fatal(error):
                    raise FatalPublishError(message) from exc
                raise BrokerUnavailableError(message) from exc
            else:
                return


def _on_delivery(
    event: ChangeEvent, failures: list[_Failure]
) -> Callable[[KafkaError | None, Message], None]:
    def callback(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            failures.append(_Failure(event, error))
            return
        log.debug(
            "event_delivered",
            event_id=str(event.event_id),
            entity_key=event.entity_key,
            kafka_partition=message.partition(),
            kafka_offset=message.offset(),
        )

    return callback


def _is_fatal(error: KafkaError) -> bool:
    return error.fatal() or error.code() in _NON_RETRIABLE
