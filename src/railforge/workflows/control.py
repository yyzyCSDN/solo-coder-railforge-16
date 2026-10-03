from __future__ import annotations

from dataclasses import dataclass, is_dataclass, asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import RLock
from typing import Any, Callable, Mapping, TypeVar

from railforge.audit.trail import Trail
from railforge.braking.brake_percentage import brake_percentage
from railforge.billing.demurrage import charge
from railforge.cargo.segregation import violations
from railforge.consist.integrity import validate
from railforge.core.time import utc
from railforge.core.versioning import VersionConflict
from railforge.customs.holds import active
from railforge.energy.regen import net_energy
from railforge.eta.event_fusion import fused_delay
from railforge.events.cursor import CursorConsumer, FeedEvent
from railforge.events.stream import DuplicateEvent, EventStream
from railforge.intermodal.connections import feasible
from railforge.inventory.wagon_pool import allocate
from railforge.locomotive.tractive_effort import available_effort
from railforge.maintenance.release import releasable
from railforge.possession.windows import conflicts as possession_conflicts
from railforge.routing.clearance import route_ok
from railforge.signaling.block_occupancy import (
    Occupancy,
    conflicts as block_conflicts,
    key as occupancy_key,
    normalized,
)
from railforge.storage.ops_store import VersionedStore, VersionedValue
from railforge.timetable.meets import conflicts as meet_conflicts
from railforge.wagon.axle_load import route_ok as axle_route_ok
from railforge.yard.hump import classify
from railforge.yard.switching import lock_route
from railforge.crew.duty import legal as duty_legal

T = TypeVar("T")
BLOCK_TOPIC = "dispatch.block.reserved"
BLOCK_STATE_PREFIX = "dispatch.block.reservations:"


def _safe(value: Any) -> Any:
    if is_dataclass(value):
        return {k: _safe(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe(v) for v in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def _occupancy_from_safe(value: Mapping[str, Any]) -> Occupancy:
    return Occupancy(
        train=str(value["train"]),
        block=str(value["block"]),
        enter=datetime.fromisoformat(str(value["enter"])),
        exit=datetime.fromisoformat(str(value["exit"])),
        direction=int(value["direction"]),
    )


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    topic: str
    subject: str
    result: Any
    event: Any
    audit_index: int
    version: int


class RailForgeWorkflowControl:
    """Atomic domain -> state -> event -> audit workflow used by every feature surface."""

    def __init__(self, stream: EventStream | None = None, trail: Trail | None = None,
                 store: VersionedStore | None = None):
        self.stream = stream or EventStream()
        self.trail = trail or Trail()
        self.store = store or VersionedStore()
        self._receipts: dict[str, OperationReceipt] = {}
        self._consumers: dict[str, CursorConsumer] = {}
        self._block_requests: dict[str, tuple] = {}
        self._block_occupancies: dict[tuple, str] = {}
        self._commit_lock = RLock()

    @property
    def receipts(self) -> Mapping[str, OperationReceipt]:
        return dict(self._receipts)

    def _chains_valid(self) -> bool:
        return self.trail.verify() and self.stream.verify_chain()

    def _uncommitted_event(self, operation_id: str) -> None:
        if self.stream.get(operation_id) is not None:
            raise DuplicateEvent(f"event id already used: {operation_id}")

    def _rollback_after_publish(self, key: str, previous_value: VersionedValue | None,
                                event, audit, *, written: bool) -> None:
        try:
            if event is not None:
                self.stream.rollback(event)
        finally:
            try:
                if written:
                    self.store.rollback(key, previous_value)
            finally:
                self.trail.rollback(audit)

    def _commit(self, operation_id: str, topic: str, subject: str, at: datetime,
                action: Callable[[], T], evidence: Mapping[str, Any] | None = None) -> OperationReceipt:
        with self._commit_lock:
            previous = self._receipts.get(operation_id)
            if previous is not None:
                if previous.topic != topic or previous.subject != subject:
                    raise DuplicateEvent("operation id reused for a different workflow")
                return previous
            self._uncommitted_event(operation_id)
            if not self._chains_valid():
                raise RuntimeError("workflow chain is not valid")

            result = action()
            audit = self.trail.append("railforge-control", topic, subject, at)
            event = None
            previous_value: VersionedValue | None = None
            written = False
            try:
                version, previous_value = self.store.write_with_previous(subject, _safe(result))
                written = True
                payload = {
                    "operation_id": operation_id,
                    "status": "committed",
                    "control_path": "atomic",
                    "version": version,
                    "result": _safe(result),
                    "audit_index": audit.index,
                    "audit_digest": audit.digest,
                    **dict(evidence or {}),
                }
                event = self.stream.publish(
                    operation_id, at, topic, subject, payload, reject_duplicate=True
                )
                if not self._chains_valid():
                    raise RuntimeError("workflow chain became invalid")
            except BaseException:
                self._rollback_after_publish(
                    subject, previous_value, event, audit, written=written
                )
                raise

            receipt = OperationReceipt(operation_id, topic, subject, result, event, audit.index, version)
            self._receipts[operation_id] = receipt
            return receipt

    def run(self, operation_id: str, topic: str, subject: str, at: datetime,
            action: Callable[[], T], evidence: Mapping[str, Any] | None = None) -> OperationReceipt:
        return self._commit(operation_id, topic, subject, at, action, evidence)

    def reserve_block(self, operation_id: str, candidate: Occupancy,
                      existing: list[Occupancy], clearance, at: datetime,
                      expected_version: int | None = None) -> OperationReceipt:
        """Atomically validate and commit one block reservation.

        ``existing`` is the caller's baseline for a block that has not yet been written
        to this control's versioned store. Once the block is versioned, that stored state
        is authoritative and ``expected_version`` performs optimistic concurrency control.
        """
        candidate = normalized(candidate)
        request_key = occupancy_key(candidate)
        baseline = [normalized(row) for row in existing]
        at = utc(at)
        state_key = BLOCK_STATE_PREFIX + candidate.block

        with self._commit_lock:
            previous = self._receipts.get(operation_id)
            if previous is not None:
                if self._block_requests.get(operation_id) != request_key:
                    raise DuplicateEvent("operation id reused for a different block reservation")
                return previous

            replay_operation = self._block_occupancies.get(request_key)
            if replay_operation is not None:
                replay = self._receipts[replay_operation]
                self._block_requests[operation_id] = request_key
                self._receipts[operation_id] = replay
                return replay

            self._uncommitted_event(operation_id)
            if not self._chains_valid():
                raise RuntimeError("workflow chain is not valid")
            if clearance.total_seconds() < 0:
                raise ValueError("clearance")

            stored = self.store.read(state_key)
            if stored is not None:
                state = [_occupancy_from_safe(row) for row in stored.value]
                actual_version = stored.version
            else:
                state = [row for row in baseline if row.block == candidate.block]
                actual_version = 0
                if any(occupancy_key(row) == request_key for row in state):
                    raise ValueError("duplicate block reservation")

            if expected_version is not None and expected_version != actual_version:
                raise VersionConflict(
                    f"expected={expected_version} actual={actual_version}"
                )

            same_train_overlap = block_conflicts(
                candidate,
                [row for row in state if row.train == candidate.train],
                timedelta(0),
                include_same_train=True,
            )
            if same_train_overlap or any(
                occupancy_key(row) == request_key for row in state
            ):
                raise ValueError("duplicate block reservation")

            blocked_by = block_conflicts(candidate, state, clearance)
            if blocked_by:
                raise ValueError("block conflict: " + ",".join(blocked_by))

            next_state = sorted(state + [candidate], key=lambda r: (r.enter, r.train))
            audit = self.trail.append("railforge-control", BLOCK_TOPIC, candidate.train, at)
            event = None
            previous_value: VersionedValue | None = None
            written = False
            try:
                version, previous_value = self.store.write_with_previous(
                    state_key, _safe(next_state), actual_version
                )
                written = True
                payload = {
                    "operation_id": operation_id,
                    "status": "committed",
                    "control_path": "atomic",
                    "block": candidate.block,
                    "state_key": state_key,
                    "candidate": _safe(candidate),
                    "version": version,
                    "result": _safe(next_state),
                    "audit_index": audit.index,
                    "audit_digest": audit.digest,
                }
                event = self.stream.publish(
                    operation_id, at, BLOCK_TOPIC, candidate.train, payload,
                    reject_duplicate=True,
                )
                if not self._chains_valid():
                    raise RuntimeError("workflow chain became invalid")
            except BaseException:
                self._rollback_after_publish(
                    state_key, previous_value, event, audit, written=written
                )
                raise

            receipt = OperationReceipt(
                operation_id, BLOCK_TOPIC, candidate.train, next_state, event, audit.index, version
            )
            self._receipts[operation_id] = receipt
            self._block_requests[operation_id] = request_key
            self._block_occupancies[request_key] = operation_id
            return receipt

    def validate_consist(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "consist.validated", subject, at, lambda: validate(rows))

    def assess_brakes(self, operation_id, rows, gradient, subject, at):
        return self._commit(operation_id, "brakes.assessed", subject, at,
                            lambda: brake_percentage(rows, gradient))

    def plan_traction(self, operation_id, rows, adhesion_factor, subject, at):
        return self._commit(operation_id, "traction.planned", subject, at,
                            lambda: available_effort(rows, adhesion_factor))

    def assign_duty(self, operation_id, rows, max_duty, max_drive, max_span, subject, at):
        def action():
            result = duty_legal(rows, max_duty, max_drive, max_span)
            if not result:
                raise ValueError("duty is not legal")
            return result
        return self._commit(operation_id, "crew.duty.assigned", subject, at, action)

    def book_meet(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "timetable.meet.booked", subject, at,
                            lambda: meet_conflicts(rows))

    def classify_yard(self, operation_id, cuts, tracks, occupied, subject, at):
        return self._commit(operation_id, "yard.classified", subject, at,
                            lambda: classify(cuts, tracks, occupied))

    def check_axles(self, operation_id, wagon, max_axle, max_gross, subject, at):
        return self._commit(operation_id, "wagon.axle.checked", subject, at,
                            lambda: axle_route_ok(wagon, max_axle, max_gross))

    def book_intermodal(self, operation_id, inbound, outbound, unload, load, cutoff, subject, at):
        return self._commit(operation_id, "intermodal.booked", subject, at,
                            lambda: feasible(inbound, outbound, unload, load, cutoff))

    def protect_possession(self, operation_id, candidate, rows, subject, at):
        return self._commit(operation_id, "possession.protected", subject, at,
                            lambda: possession_conflicts(candidate, rows))

    def release_maintenance(self, operation_id, evidence, policy, check_at, subject, at):
        def action():
            result = releasable(evidence, policy, check_at)
            if not result:
                raise ValueError("maintenance evidence incomplete")
            return result
        return self._commit(operation_id, "maintenance.released", subject, at, action)

    def consume_event(self, operation_id, event: FeedEvent, subject, at):
        consumer = self._consumers.setdefault(event.partition, CursorConsumer())
        return self._commit(operation_id, "events.cursor.consumed", subject, at,
                            lambda: consumer.accept(event))

    def charge_demurrage(self, operation_id, arrival, release, tariff, subject, at):
        return self._commit(operation_id, "billing.demurrage.charged", subject, at,
                            lambda: charge(arrival, release, tariff))

    def check_customs(self, operation_id, rows, scope, check_at, subject, at):
        return self._commit(operation_id, "customs.hold.checked", subject, at,
                            lambda: active(rows, scope, check_at))

    def check_segregation(self, operation_id, rows, rules, subject, at):
        return self._commit(operation_id, "cargo.segregation.checked", subject, at,
                            lambda: violations(rows, rules))

    def calculate_energy(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "energy.calculated", subject, at,
                            lambda: net_energy(rows))

    def lock_switch_route(self, operation_id, route, switches, owner, subject, at):
        return self._commit(operation_id, "yard.route.locked", subject, at,
                            lambda: lock_route(route, switches, owner))

    def check_clearance(self, operation_id, profile, segments, subject, at):
        return self._commit(operation_id, "routing.clearance.checked", subject, at,
                            lambda: route_ok(profile, segments))

    def allocate_wagons(self, operation_id, rows, wagon_type, location, on, count, required_days, forbidden, subject, at):
        return self._commit(operation_id, "inventory.wagons.allocated", subject, at,
                            lambda: allocate(rows, wagon_type, location, on, count, required_days, forbidden))

    def fuse_eta(self, operation_id, rows, max_lateness, subject, at):
        return self._commit(operation_id, "eta.fused", subject, at,
                            lambda: fused_delay(rows, max_lateness))
