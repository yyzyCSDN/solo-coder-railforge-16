from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from json import dumps
from threading import Condition, RLock
from typing import Iterable, Mapping


class DuplicateEvent(ValueError):
    """Raised when an event id is reused with a different event."""


@dataclass(frozen=True)
class StreamEvent:
    sequence: int
    event_id: str
    occurred_at: datetime
    topic: str
    subject: str
    payload: Mapping[str, object]
    previous_hash: str
    hash: str


@dataclass(frozen=True)
class CursorPage:
    events: tuple[StreamEvent, ...]
    next_cursor: int
    gap: bool
    reset_cursor: int | None = None


class CursorExpired(RuntimeError):
    pass


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(timezone.utc)


class EventStream:
    def __init__(self, retention: int = 10000):
        if retention < 1:
            raise ValueError("retention must be positive")
        self.retention = retention
        self._events: list[StreamEvent] = []
        self._ids: dict[str, StreamEvent] = {}
        self._next_sequence = 1
        self._last_hash = "0" * 64
        self._lock = RLock()
        self._changed = Condition(self._lock)

    @staticmethod
    def _canonical_payload(payload: Mapping[str, object]) -> str:
        return dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    def _hash(self, sequence, event_id, occurred_at, topic, subject, payload, previous_hash):
        raw = "|".join([
            str(sequence), event_id, occurred_at.isoformat(), topic, subject,
            self._canonical_payload(payload), previous_hash,
        ]).encode("utf-8")
        return sha256(raw).hexdigest()

    @staticmethod
    def _same_event(existing: StreamEvent, occurred_at: datetime, topic: str,
                    subject: str, payload: Mapping[str, object]) -> bool:
        return (
            existing.occurred_at == occurred_at
            and existing.topic == topic
            and existing.subject == subject
            and dict(existing.payload) == dict(payload)
        )

    def get(self, event_id: str) -> StreamEvent | None:
        with self._lock:
            return self._ids.get(event_id)

    def publish(self, event_id: str, occurred_at: datetime, topic: str, subject: str,
                payload: Mapping[str, object], *, reject_duplicate: bool = False) -> StreamEvent:
        occurred_at = _utc(occurred_at)
        body = dict(payload)
        with self._lock:
            existing = self._ids.get(event_id)
            if existing is not None:
                if reject_duplicate or not self._same_event(existing, occurred_at, topic, subject, body):
                    raise DuplicateEvent(f"event id already used: {event_id}")
                return existing
            sequence = self._next_sequence
            digest = self._hash(sequence, event_id, occurred_at, topic, subject, body, self._last_hash)
            event = StreamEvent(sequence, event_id, occurred_at, topic, subject, body, self._last_hash, digest)
            self._events.append(event)
            self._ids[event_id] = event
            self._next_sequence += 1
            self._last_hash = digest
            while len(self._events) > self.retention:
                old = self._events.pop(0)
                self._ids.pop(old.event_id, None)
            self._changed.notify_all()
            return event

    def rollback(self, event: StreamEvent) -> None:
        """Remove the tail event; used only by the workflow transaction boundary."""
        with self._lock:
            current = self._events[-1] if self._events else None
            if current is None or current != event:
                raise RuntimeError("event rollback requires the published event to be the tail")
            self._events.pop()
            self._ids.pop(event.event_id, None)
            self._next_sequence = event.sequence
            self._last_hash = event.previous_hash

    @property
    def head(self) -> int:
        with self._lock:
            return self._events[-1].sequence if self._events else 0

    @property
    def floor(self) -> int:
        with self._lock:
            return self._events[0].sequence if self._events else self._next_sequence

    def page(self, cursor: int, limit: int = 100, topics: Iterable[str] | None = None,
             strict: bool = False) -> CursorPage:
        if limit < 1:
            raise ValueError("limit must be positive")
        topic_set = set(topics) if topics is not None else None
        with self._lock:
            floor = self.floor
            gap = bool(self._events and cursor < floor - 1)
            if gap and strict:
                raise CursorExpired(f"cursor {cursor} before retained floor {floor}")
            effective = max(cursor, floor - 1) if gap else cursor
            rows = tuple(e for e in self._events if e.sequence > effective and
                         (topic_set is None or e.topic in topic_set))[:limit]
            next_cursor = rows[-1].sequence if rows else effective
            return CursorPage(rows, next_cursor, gap, floor - 1 if gap else None)

    def wait_page(self, cursor: int, timeout_seconds: float, limit: int = 100,
                  topics: Iterable[str] | None = None) -> CursorPage:
        with self._changed:
            page = self.page(cursor, limit, topics)
            if page.events or page.gap:
                return page
            self._changed.wait(timeout_seconds)
            return self.page(cursor, limit, topics)

    def verify_chain(self) -> bool:
        with self._lock:
            previous = "0" * 64
            for event in self._events:
                expected = self._hash(event.sequence, event.event_id, event.occurred_at,
                                      event.topic, event.subject, event.payload, previous)
                if event.previous_hash != previous or event.hash != expected:
                    return False
                previous = event.hash
            return True

    def snapshot(self) -> tuple[StreamEvent, ...]:
        with self._lock:
            return tuple(self._events)
