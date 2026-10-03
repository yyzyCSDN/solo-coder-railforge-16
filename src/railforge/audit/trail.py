from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
import hashlib, json

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
        b = json.dumps({'index': index, 'actor': actor, 'action': action, 'subject': subject, 'at': at.astimezone(timezone.utc).isoformat(), 'previous': previous}, sort_keys=True, separators=(',', ':')).encode()
        return hashlib.sha256(b).hexdigest()

    def append(self, actor, action, subject, at=None):
        at = at or datetime.now(timezone.utc)
        with self._lock:
            prev = self._rows[-1].digest if self._rows else 'GENESIS'
            idx = len(self._rows) + 1
            d = self._digest(idx, actor, action, subject, at, prev)
            r = Record(idx, actor, action, subject, at, prev, d)
            self._rows.append(r)
            return r

    def retract(self, record: Record):
        """Remove an audit record previously appended by a failed commit.

        Only the current tail may be retracted so the hash chain stays
        contiguous: rejected decisions leave no audit gap.
        """
        with self._lock:
            if not self._rows or self._rows[-1] is not record:
                raise RuntimeError('only the audit tail may be retracted')
            self._rows.pop()

    def verify(self):
        with self._lock:
            prev = 'GENESIS'
            for r in self._rows:
                if r.previous_hash != prev or r.digest != self._digest(r.index, r.actor, r.action, r.subject, r.at, prev):
                    return False
                prev = r.digest
            return True
