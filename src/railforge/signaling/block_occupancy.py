from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from railforge.core.time import utc


@dataclass(frozen=True)
class Occupancy:
    train: str
    block: str
    enter: datetime
    exit: datetime
    direction: int


def normalized(row: Occupancy) -> Occupancy:
    """Return the occupancy with both endpoints represented in UTC."""
    if not row.train or not row.block:
        raise ValueError("occupancy train and block are required")
    enter = utc(row.enter)
    exit_at = utc(row.exit)
    if exit_at <= enter:
        raise ValueError("invalid occupancy")
    return replace(row, enter=enter, exit=exit_at, direction=int(row.direction))


def key(row: Occupancy) -> tuple[str, str, str, str, int]:
    row = normalized(row)
    return (
        row.train,
        row.block,
        row.enter.isoformat(),
        row.exit.isoformat(),
        row.direction,
    )


def overlaps(left: Occupancy, right: Occupancy) -> bool:
    left = normalized(left)
    right = normalized(right)
    if left.block != right.block:
        return False
    return max(left.enter, right.enter) < min(left.exit, right.exit)


def conflicts(candidate: Occupancy, existing: list[Occupancy], clearance: timedelta,
              *, include_same_train: bool = False) -> list[str]:
    candidate = normalized(candidate)
    if clearance.total_seconds() < 0:
        raise ValueError("clearance")

    hits = []
    for row in existing:
        row = normalized(row)
        if row.block != candidate.block:
            continue
        if row.train == candidate.train and not include_same_train:
            continue
        protected_enter = row.enter - clearance
        protected_exit = row.exit + clearance
        if max(candidate.enter, protected_enter) < min(candidate.exit, protected_exit):
            hits.append(row.train)
    return sorted(set(hits))


def reserve(candidate: Occupancy, existing: list[Occupancy], clearance: timedelta):
    candidate = normalized(candidate)
    hit = conflicts(candidate, existing, clearance)
    if hit:
        raise ValueError("block conflict: " + ",".join(hit))
    return sorted(
        [normalized(row) for row in existing] + [candidate],
        key=lambda r: (r.block, r.enter, r.train),
    )
