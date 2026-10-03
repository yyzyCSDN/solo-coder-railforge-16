import threading
import unittest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from railforge.api import RailForgeService
from railforge.audit.trail import Trail
from railforge.core.versioning import VersionConflict
from railforge.events.stream import EventStream
from railforge.signaling.block_occupancy import Occupancy
from railforge.storage.ops_store import VersionedStore
from railforge.workflows.control import RailForgeWorkflowControl

UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")


class BlockReservationTests(unittest.TestCase):
    def setUp(self):
        self.at = datetime(2026, 1, 1, 7, tzinfo=UTC)
        self.a = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 8, tzinfo=UTC),
            datetime(2026, 1, 1, 9, tzinfo=UTC),
            1,
        )

    def test_adjacent_but_not_overlapping_reservation_respects_release_margin(self):
        service = RailForgeService()
        service.reserve_block("A-1", self.a, [], timedelta(minutes=5), self.at)
        adjacent = Occupancy(
            "B", "B1",
            datetime(2026, 1, 1, 9, tzinfo=UTC),
            datetime(2026, 1, 1, 10, tzinfo=UTC),
            1,
        )

        with self.assertRaisesRegex(ValueError, "block conflict"):
            service.reserve_block("B-1", adjacent, [self.a], timedelta(minutes=5), self.at)

        # Rejection is a no-op: no state version and no broadcast or audit entry.
        control = service.workflow()
        self.assertEqual(control.stream.head, 1)
        self.assertEqual(len(control.trail.records), 1)
        self.assertEqual(
            control.store.read("dispatch.block.reservations:B1").version, 1
        )

        outside_margin = Occupancy(
            "B", "B1",
            datetime(2026, 1, 1, 9, 6, tzinfo=UTC),
            datetime(2026, 1, 1, 10, tzinfo=UTC),
            1,
        )
        receipt = service.reserve_block("B-2", outside_margin, [self.a],
                                        timedelta(minutes=5), self.at)
        self.assertEqual(receipt.version, 2)
        self.assertEqual(receipt.event.payload["status"], "committed")
        self.assertTrue(control.stream.verify_chain())
        self.assertTrue(control.trail.verify())

    def test_same_train_replay_is_idempotent_even_with_new_operation_id(self):
        control = RailForgeWorkflowControl()
        first = control.reserve_block(
            "A-1", self.a, [], timedelta(minutes=5), self.at
        )
        same_operation = control.reserve_block(
            "A-1", self.a, [], timedelta(minutes=5), self.at
        )
        same_request = control.reserve_block(
            "A-retry", self.a, [], timedelta(minutes=5), self.at
        )

        self.assertIs(same_operation, first)
        self.assertIs(same_request, first)
        self.assertEqual(control.stream.head, 1)
        self.assertEqual(len(control.trail.records), 1)
        self.assertEqual(control.store.read("dispatch.block.reservations:B1").version, 1)

        overlap = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 8, 30, tzinfo=UTC),
            datetime(2026, 1, 1, 9, 30, tzinfo=UTC),
            1,
        )
        with self.assertRaisesRegex(ValueError, "duplicate block reservation"):
            control.reserve_block("A-conflict", overlap, [],
                                  timedelta(minutes=5), self.at)
        self.assertEqual(control.stream.head, 1)

    def test_same_operation_id_cannot_change_reservation(self):
        control = RailForgeWorkflowControl()
        control.reserve_block("same-id", self.a, [], timedelta(minutes=5), self.at)
        changed = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 11, tzinfo=UTC),
            datetime(2026, 1, 1, 12, tzinfo=UTC),
            1,
        )
        with self.assertRaises(ValueError):
            control.reserve_block("same-id", changed, [],
                                  timedelta(minutes=5), self.at)
        self.assertEqual(control.stream.head, 1)

    def test_timezone_equivalent_reservation_replays_original_event(self):
        control = RailForgeWorkflowControl()
        first = control.reserve_block(
            "utc", self.a, [], timedelta(minutes=5), self.at
        )
        shanghai_view = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 16, tzinfo=SHANGHAI),
            datetime(2026, 1, 1, 17, tzinfo=SHANGHAI),
            1,
        )
        replay = control.reserve_block(
            "shanghai", shanghai_view, [], timedelta(minutes=5), self.at
        )
        self.assertIs(replay, first)
        self.assertEqual(replay.result[0].enter, self.a.enter)
        self.assertEqual(control.stream.head, 1)

    def test_clearance_is_checked_after_normalizing_timezone_date_boundary(self):
        control = RailForgeWorkflowControl()
        existing = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 23, tzinfo=UTC),
            datetime(2026, 1, 1, 23, 50, tzinfo=UTC),
            1,
        )
        control.reserve_block("A-1", existing, [], timedelta(minutes=5), self.at)
        # 07:55-08:55 on January 2 in Shanghai is 23:55-00:55 UTC and crosses a date.
        across_midnight = Occupancy(
            "B", "B1",
            datetime(2026, 1, 2, 7, 55, tzinfo=SHANGHAI),
            datetime(2026, 1, 2, 8, 55, tzinfo=SHANGHAI),
            1,
        )
        with self.assertRaisesRegex(ValueError, "block conflict: A"):
            control.reserve_block(
                "boundary", across_midnight, [existing],
                timedelta(minutes=10), self.at,
            )
        self.assertEqual(control.stream.head, 1)

    def test_committed_event_is_cryptographically_bound_to_audit_record(self):
        control = RailForgeWorkflowControl()
        receipt = control.reserve_block(
            "A-1", self.a, [], timedelta(minutes=5), self.at
        )
        audit = control.trail.records[-1]
        self.assertEqual(receipt.event.payload["audit_index"], audit.index)
        self.assertEqual(receipt.event.payload["audit_digest"], audit.digest)
        self.assertTrue(control.stream.verify_chain())
        self.assertTrue(control.trail.verify())

    def test_naive_datetime_is_rejected_before_state_or_broadcast(self):
        control = RailForgeWorkflowControl()
        naive = Occupancy(
            "A", "B1",
            datetime(2026, 1, 1, 8),
            datetime(2026, 1, 1, 9),
            1,
        )
        with self.assertRaises(ValueError):
            control.reserve_block("naive", naive, [], timedelta(minutes=5), self.at)
        self.assertEqual(control.stream.head, 0)
        self.assertEqual(control.trail.records, ())
        self.assertIsNone(control.store.read("dispatch.block.reservations:B1"))

    def test_stale_expected_version_is_rejected_without_side_effects(self):
        control = RailForgeWorkflowControl()
        first = control.reserve_block(
            "A-1", self.a, [], timedelta(minutes=5), self.at
        )
        second = Occupancy(
            "B", "B1",
            datetime(2026, 1, 1, 9, 10, tzinfo=UTC),
            datetime(2026, 1, 1, 10, tzinfo=UTC),
            1,
        )
        second_receipt = control.reserve_block(
            "B-1", second, [self.a], timedelta(minutes=5), self.at,
            expected_version=first.version,
        )
        self.assertEqual(second_receipt.version, 2)

        stale = Occupancy(
            "C", "B1",
            datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
            datetime(2026, 1, 1, 11, tzinfo=UTC),
            1,
        )
        with self.assertRaises(VersionConflict):
            control.reserve_block(
                "stale", stale, [], timedelta(minutes=5), self.at, expected_version=1
            )
        self.assertEqual(control.stream.head, 2)
        self.assertEqual(len(control.trail.records), 2)
        self.assertEqual(control.store.read("dispatch.block.reservations:B1").version, 2)

    def test_concurrent_reservations_are_serialized_without_publishing_losers(self):
        control = RailForgeWorkflowControl()
        control.reserve_block("A-0", self.a, [], timedelta(minutes=5), self.at)
        start = threading.Event()
        errors: list[BaseException] = []

        def reserve(operation_id: str, minute: int):
            try:
                start.wait()
                candidate = Occupancy(
                    operation_id, "B1",
                    datetime(2026, 1, 1, 9, minute, tzinfo=UTC),
                    datetime(2026, 1, 1, 10, tzinfo=UTC),
                    1,
                )
                control.reserve_block(
                    operation_id, candidate, [self.a], timedelta(minutes=5), self.at,
                    expected_version=1,
                )
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=reserve, args=("T-1", 10)),
            threading.Thread(target=reserve, args=("T-2", 11)),
            threading.Thread(target=reserve, args=("T-3", 12)),
        ]
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join()

        self.assertTrue(any(isinstance(exc, VersionConflict) for exc in errors))
        self.assertEqual(control.stream.head, 2)
        self.assertEqual(len(control.trail.records), 2)
        self.assertEqual(control.store.read("dispatch.block.reservations:B1").version, 2)
        self.assertTrue(control.stream.verify_chain())
        self.assertTrue(control.trail.verify())

    def test_independent_components_are_committed_as_one_transaction(self):
        stream = FailingOnceStream()
        trail = Trail()
        store = VersionedStore()
        control = RailForgeWorkflowControl(stream=stream, trail=trail, store=store)

        with self.assertRaises(RuntimeError):
            control.reserve_block("A-1", self.a, [], timedelta(minutes=5), self.at)

        self.assertEqual(stream.head, 0)
        self.assertEqual(trail.records, ())
        self.assertIsNone(store.read("dispatch.block.reservations:B1"))


class FailingOnceStream(EventStream):
    def publish(self, *args, **kwargs):
        raise RuntimeError("broker unavailable")


if __name__ == "__main__":
    unittest.main()
