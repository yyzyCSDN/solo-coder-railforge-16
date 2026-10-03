from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Any

from railforge.core.versioning import VersionConflict


@dataclass(frozen=True)
class VersionedValue:
    version: int
    value: Any


class VersionedStore:
    """Small compare-and-swap store used to make operation publication atomic."""

    def __init__(self):
        self._rows: dict[str, VersionedValue] = {}
        self._lock = RLock()

    def write(self, key: str, value: Any, expected_version: int | None = None) -> int:
        with self._lock:
            current = self._rows.get(key)
            actual = current.version if current else 0
            if expected_version is not None and expected_version != actual:
                raise VersionConflict(f"expected={expected_version} actual={actual}")
            version = actual + 1
            self._rows[key] = VersionedValue(version, value)
            return version

    def restore(self, key: str, previous: VersionedValue | None) -> None:
        """Undo a write performed inside a commit that later failed."""
        with self._lock:
            if previous is None:
                self._rows.pop(key, None)
            else:
                self._rows[key] = previous

    def read(self, key: str) -> VersionedValue | None:
        with self._lock:
            return self._rows.get(key)

    def snapshot(self) -> dict[str, VersionedValue]:
        with self._lock:
            return dict(self._rows)
