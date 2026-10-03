from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
import hashlib
import json

from railforge.core.time import utc


@dataclass(frozen=True)
class Record:
    index: int
    actor: str
    action: str
    subject: str
    at: datetime
    previous_hash: str
    digest: str


class Trail:
    def __init__(self):
        self._rows = []
        self._lock = RLock()

    @staticmethod
    def _digest(index, actor, action, subject, at, previous):
        b = json.dumps({
            "index": index,
            "actor": actor,
            "action": action,
            "subject": subject,
            "at": at.astimezone(timezone.utc).isoformat(),
            "previous": previous,
        }, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(b).hexdigest()

    def append(self, actor, action, subject, at=None):
        at = utc(at or datetime.now(timezone.utc))
        with self._lock:
            prev = self._rows[-1].digest if self._rows else "GENESIS"
            idx = len(self._rows) + 1
            d = self._digest(idx, actor, action, subject, at, prev)
            r = Record(idx, actor, action, subject, at, prev, d)
            self._rows.append(r)
            return r

    def rollback(self, record: Record) -> None:
        """Remove the tail audit record; used only by the workflow transaction boundary."""
        with self._lock:
            current = self._rows[-1] if self._rows else None
            if current is None or current != record:
                raise RuntimeError("audit rollback requires the appended record to be the tail")
            self._rows.pop()

    @property
    def records(self) -> tuple[Record, ...]:
        with self._lock:
            return tuple(self._rows)

    def verify(self):
        with self._lock:
            prev = "GENESIS"
            for r in self._rows:
                if r.previous_hash != prev or r.digest != self._digest(
                    r.index, r.actor, r.action, r.subject, r.at, prev
                ):
                    return False
                prev = r.digest
            return True
