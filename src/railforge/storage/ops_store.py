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

    def write_with_previous(self, key: str, value: Any,
                            expected_version: int | None = None) -> tuple[int, VersionedValue | None]:
        with self._lock:
            current = self._rows.get(key)
            actual = current.version if current else 0
            if expected_version is not None and expected_version != actual:
                raise VersionConflict(f"expected={expected_version} actual={actual}")
            version = actual + 1
            self._rows[key] = VersionedValue(version, value)
            return version, current

    def write(self, key: str, value: Any, expected_version: int | None = None) -> int:
        return self.write_with_previous(key, value, expected_version)[0]

    def rollback(self, key: str, previous: VersionedValue | None) -> None:
        """Restore a key to its state before this workflow boundary wrote it."""
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
