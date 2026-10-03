from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from railforge.core.time import aware

UTC = timezone.utc


@dataclass(frozen=True)
class Occupancy:
    train: str
    block: str
    enter: datetime
    exit: datetime
    direction: int


def normalized(row: Occupancy) -> Occupancy:
    """Return the occupancy with both instants pinned to UTC.

    Naive datetimes are rejected: every dispatcher-facing instant must carry
    an explicit offset so safety windows cannot move across timezone or DST
    boundaries.
    """
    enter = aware(row.enter).astimezone(UTC)
    end = aware(row.exit).astimezone(UTC)
    if end <= enter:
        raise ValueError('invalid occupancy')
    return replace(row, enter=enter, exit=end)


def from_record(row: dict) -> Occupancy:
    """Rebuild an Occupancy from a persisted (JSON-safe) record."""
    return Occupancy(
        train=row['train'],
        block=row['block'],
        enter=datetime.fromisoformat(row['enter']),
        exit=datetime.fromisoformat(row['exit']),
        direction=row['direction'],
    )


def conflicts(candidate: Occupancy, existing: list[Occupancy], clearance: timedelta) -> list[str]:
    """Trains whose protected occupancy window overlaps ``candidate``.

    The clearance margin is applied symmetrically around every *other*
    train's occupancy: touching a released protection window counts as a
    conflict, even when the bare time intervals do not overlap.
    """
    if clearance.total_seconds() < 0:
        raise ValueError('clearance')
    ce = normalized(candidate)
    co = ce.enter
    cx = ce.exit
    hits = []
    for row in existing:
        if row.block != ce.block or row.train == ce.train:
            continue
        n = normalized(row)
        re = n.enter - clearance
        rx = n.exit + clearance
        if max(co, re) < min(cx, rx):
            hits.append(n.train)
    return sorted(set(hits))


def reserve(candidate: Occupancy, existing: list[Occupancy], clearance: timedelta):
    hit = conflicts(candidate, existing, clearance)
    if hit:
        raise ValueError('block conflict: ' + ','.join(hit))
    merged = sorted(existing + [normalized(candidate)], key=lambda r: (r.block, r.enter, r.train))
    return merged
