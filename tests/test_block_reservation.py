import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from railforge.api import RailForgeService
from railforge.core.versioning import VersionConflict
from railforge.signaling.block_occupancy import Occupancy, conflicts, reserve
from railforge.workflows.control import BLOCK_TOPIC, RailForgeWorkflowControl

U = timezone.utc
NY = ZoneInfo("America/New_York")
TOKYO = ZoneInfo("Asia/Tokyo")


def occ(train, block, start, minutes=60, direction=1):
    return Occupancy(train, block, start, start + timedelta(minutes=minutes), direction)


class ClearanceWindowTests(unittest.TestCase):
    """The released protection margin must still reject adjacent reservations."""

    def test_adjacent_reservation_piercing_clearance_is_rejected(self):
        existing = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        # B exits 60 min after A but starts only 5 min after A's exit: bare
        # intervals never overlap, yet the 10-minute protection window is hit.
        candidate = occ("B", "B1", datetime(2026, 1, 1, 9, 5, tzinfo=U))
        self.assertEqual(conflicts(candidate, [existing], timedelta(minutes=10)), ["A"])
        with self.assertRaisesRegex(ValueError, "block conflict: A"):
            reserve(candidate, [existing], timedelta(minutes=10))

    def test_outside_clearance_is_allowed(self):
        existing = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        candidate = occ("B", "B1", datetime(2026, 1, 1, 10, tzinfo=U))
        self.assertEqual(conflicts(candidate, [existing], timedelta(minutes=5)), [])

    def test_clearance_tolerance_around_the_boundary(self):
        existing = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U), minutes=60)
        clearance = timedelta(minutes=10)
        # Half-open windows: candidate starting exactly at A.exit + clearance
        # merely touches the boundary and is allowed ...
        touching = occ("B", "B1", datetime(2026, 1, 1, 9, 10, tzinfo=U), minutes=10)
        self.assertEqual(conflicts(touching, [existing], clearance), [])
        # ... starting one second earlier pierces the protected window.
        inside = Occupancy("B", "B1", touching.enter - timedelta(seconds=1),
                           touching.exit - timedelta(seconds=1), 1)
        self.assertEqual(conflicts(inside, [existing], clearance), ["A"])

    def test_other_blocks_are_independent(self):
        existing = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        candidate = occ("B", "B2", datetime(2026, 1, 1, 8, tzinfo=U))
        self.assertEqual(conflicts(candidate, [existing], timedelta(hours=1)), [])

    def test_same_train_is_not_a_conflict(self):
        existing = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        candidate = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        self.assertEqual(conflicts(candidate, [existing], timedelta(minutes=30)), [])

    def test_naive_datetimes_are_rejected(self):
        bad = Occupancy("X", "B1", datetime(2026, 1, 1, 8), datetime(2026, 1, 1, 9), 1)
        with self.assertRaises(ValueError):
            conflicts(bad, [], timedelta(minutes=5))
        good = occ("G", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        with self.assertRaises(ValueError):
            conflicts(good, [bad], timedelta(minutes=5))


class AtomicReservationTests(unittest.TestCase):

    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.at = datetime(2026, 1, 1, 7, 30, tzinfo=U)
        self.clearance = timedelta(minutes=10)

    def test_accepted_reservation_commits_state_event_and_audit(self):
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        receipt = self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        self.assertEqual(receipt.version, 1)
        self.assertEqual(receipt.topic, BLOCK_TOPIC)
        self.assertEqual(receipt.event.payload["status"], "committed")
        self.assertEqual(receipt.event.payload["control_path"], "atomic")
        self.assertEqual(receipt.event.payload["block"], "B1")
        board, version = self.control.block_board("B1")
        self.assertEqual(version, 1)
        self.assertEqual([r.train for r in board], ["A"])
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(receipt.audit_index, 1)
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())

    def test_rejected_reservation_writes_nothing(self):
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        b = occ("B", "B1", datetime(2026, 1, 1, 9, 5, tzinfo=U))
        self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        with self.assertRaisesRegex(ValueError, "block conflict: A"):
            self.control.reserve_block("op-2", b, [], self.clearance, self.at)
        # state, stream and audit all show exactly the single accepted commit
        self.assertEqual(self.control.store.read("dispatch.block:B1").version, 1)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(len(self.control.trail._rows), 1)
        self.assertEqual([r.train for r in self.control.block_board("B1")[0]], ["A"])
        topics = [e.topic for e in self.control.stream.snapshot()]
        self.assertEqual(topics, [BLOCK_TOPIC])
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())

    def test_authoritative_store_state_is_checked_on_followups(self):
        # Caller omits the stored baseline: the control plane must still see
        # the committed board from the versioned store.
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        b = occ("B", "B1", datetime(2026, 1, 1, 9, 5, tzinfo=U))
        self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        with self.assertRaises(ValueError):
            self.control.reserve_block("op-2", b, [], self.clearance, self.at)

    def test_same_train_replay_is_idempotent(self):
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        first = self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        replay = self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        self.assertIs(replay, first)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(len(self.control.trail._rows), 1)
        self.assertEqual(self.control.block_board("B1")[1], 1)

    def test_replay_with_same_id_but_changed_body_returns_canonical_receipt(self):
        # Idempotency key is the operation id; a retried submission must not
        # produce a second event or mutate the committed board.
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        a_again = occ("A", "B1", datetime(2026, 1, 1, 20, tzinfo=U))
        first = self.control.reserve_block("op-1", a, [], self.clearance, self.at)
        replay = self.control.reserve_block("op-1", a_again, [], self.clearance, self.at)
        self.assertIs(replay, first)
        self.assertEqual(replay.result[0].enter, a.enter)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(self.control.block_board("B1")[1], 1)
        # the same train legitimately re-enters later under a fresh operation id
        later = self.control.reserve_block("op-2", a_again, [], self.clearance, self.at)
        self.assertEqual(later.version, 2)
        self.assertEqual(self.control.stream.head, 2)


class TimezoneBoundaryTests(unittest.TestCase):

    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.at = datetime(2026, 11, 1, 4, tzinfo=U)

    def test_fixed_offset_timezone_is_normalized_to_utc(self):
        # 09:00 JST = 00:00 UTC; clearance reaches back to 23:30 UTC
        existing = occ("A", "B1", datetime(2026, 1, 1, 9, tzinfo=TOKYO), minutes=30)
        candidate = occ("B", "B1", datetime(2026, 1, 1, 0, 25, tzinfo=U), minutes=5)
        self.assertEqual(conflicts(candidate, [existing], timedelta(minutes=5)), ["A"])

    def test_dst_fold_disambiguates_identical_wall_times(self):
        # 01:30 on 2026-11-01 occurs twice in New York (05:30Z EDT, 06:30Z EST).
        first = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=0)
        second = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=1)
        end_first = datetime(2026, 11, 1, 1, 50, tzinfo=NY, fold=0)
        end_second = datetime(2026, 11, 1, 1, 50, tzinfo=NY, fold=1)
        self.assertEqual(first.replace(tzinfo=None), second.replace(tzinfo=None))
        c = Occupancy("C", "B3", first, end_first, 1)
        d = Occupancy("D", "B3", second, end_second, 1)
        # 60 actual minutes apart: a 45-minute window bridges them ...
        self.assertEqual(conflicts(d, [c], timedelta(minutes=45)), ["C"])
        # ... a 15-minute window does not.
        self.assertEqual(conflicts(d, [c], timedelta(minutes=15)), [])
        # commits persist UTC-normalized instants
        r = self.control.reserve_block("op-c", c, [], timedelta(0), self.at)
        self.assertEqual(r.result[0].enter, datetime(2026, 11, 1, 5, 30, tzinfo=U))

    def test_occupancy_going_backwards_in_utc_across_fold_is_rejected(self):
        # timedelta arithmetic keeps fold=0 on the result, so 01:30 fold=1
        # plus 20 minutes renders 05:50Z, i.e. before the 06:30Z entry.
        start = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=1)
        bad = Occupancy("D", "B3", start, start + timedelta(minutes=20), 1)
        with self.assertRaises(ValueError):
            conflicts(bad, [], timedelta(minutes=15))

    def test_dst_fold_pair_cannot_both_commit_inside_one_window(self):
        first = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=0)
        second = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=1)
        c = Occupancy("C", "B4", first, datetime(2026, 11, 1, 1, 50, tzinfo=NY, fold=0), 1)
        d = Occupancy("D", "B4", second, datetime(2026, 11, 1, 1, 50, tzinfo=NY, fold=1), 1)
        self.control.reserve_block("op-c", c, [], timedelta(minutes=45), self.at)
        with self.assertRaises(ValueError):
            self.control.reserve_block("op-d", d, [], timedelta(minutes=45), self.at)
        self.assertEqual(self.control.stream.head, 1)


class VersionConflictTests(unittest.TestCase):

    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.at = datetime(2026, 1, 1, 7, tzinfo=U)
        self.clearance = timedelta(minutes=10)

    def test_stale_expected_version_is_rejected_before_any_side_effect(self):
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        b = occ("B", "B1", datetime(2026, 1, 1, 12, tzinfo=U))
        self.control.reserve_block("op-1", a, [], self.clearance, self.at)  # board now v1
        with self.assertRaisesRegex(VersionConflict, "expected=0 actual=1"):
            self.control.reserve_block("op-2", b, [], self.clearance, self.at, expected_version=0)
        self.assertEqual(self.control.block_board("B1")[1], 1)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(len(self.control.trail._rows), 1)

    def test_current_version_commits(self):
        a = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))
        b = occ("B", "B1", datetime(2026, 1, 1, 12, tzinfo=U))
        self.control.reserve_block("op-1", a, [], self.clearance, self.at, expected_version=0)
        _, version = self.control.block_board("B1")
        receipt = self.control.reserve_block("op-2", b, [], self.clearance, self.at,
                                             expected_version=version)
        self.assertEqual(receipt.version, 2)

    def test_only_one_winner_under_concurrent_cas(self):
        # All threads read v0 then attempt the same slot; exactly one CAS wins.
        barrier = threading.Barrier(8)
        results = []

        def submit(i):
            barrier.wait()
            candidate = occ(f"T{i}", "B9", datetime(2026, 1, 1, 8, tzinfo=U))
            try:
                self.control.reserve_block(f"op-{i}", candidate, [], timedelta(0),
                                           self.at, expected_version=0)
                results.append(("ok", i))
            except VersionConflict:
                results.append(("conflict", i))
            except ValueError:
                results.append(("conflict-space", i))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(submit, range(8)))
        winners = [i for kind, i in results if kind == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.control.block_board("B9")[1], 1)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(len(self.control.trail._rows), 1)

    def test_concurrent_adjacent_reservations_never_pierce_the_window(self):
        clearance = timedelta(minutes=30)
        # candidates every 10 minutes; accepted spacing must be >= 40 minutes.
        def submit(i, use_cas):
            candidate = occ(f"T{i}", "B8",
                            datetime(2026, 1, 1, 8, tzinfo=U) + timedelta(minutes=10 * i),
                            minutes=5)
            try:
                if use_cas:
                    _, version = self.control.block_board("B8")
                    self.control.reserve_block(f"op-{i}", candidate, [], clearance,
                                               self.at, expected_version=version)
                else:
                    self.control.reserve_block(f"op-{i}", candidate, [], clearance, self.at)
                return True
            except (ValueError, VersionConflict):
                return False

        for use_cas in (False, True):
            control = RailForgeWorkflowControl()
            self.control = control
            with ThreadPoolExecutor(max_workers=12) as pool:
                accepted = [r for r in pool.map(lambda i: submit(i, use_cas), range(12)) if r]
            board, version = control.block_board("B8")
            self.assertEqual(len(board), 3)
            self.assertEqual(len(accepted), 3)
            self.assertEqual(version, 3)
            for row in board:
                others = [x for x in board if x is not row]
                self.assertEqual(conflicts(row, others, clearance), [])
            self.assertTrue(control.stream.verify_chain())
            self.assertTrue(control.trail.verify())


class CommitRollbackTests(unittest.TestCase):
    """Failures after the decision must compensate state/event/audit in reverse."""

    def setUp(self):
        self.control = RailForgeWorkflowControl()
        self.at = datetime(2026, 1, 1, 7, tzinfo=U)
        self.candidate = occ("A", "B1", datetime(2026, 1, 1, 8, tzinfo=U))

    def _boom(self, *args, **kwargs):
        raise RuntimeError("injected failure")

    def test_publish_failure_rolls_back_state(self):
        self.control.stream.publish = self._boom
        with self.assertRaises(RuntimeError):
            self.control.reserve_block("op-f1", self.candidate, [], timedelta(minutes=5), self.at)
        self.assertIsNone(self.control.store.read("dispatch.block:B1"))
        self.assertEqual(self.control.stream.head, 0)
        self.assertEqual(len(self.control.trail._rows), 0)

    def test_audit_failure_retracts_event_and_state(self):
        self.control.trail.append = self._boom
        with self.assertRaises(RuntimeError):
            self.control.reserve_block("op-f2", self.candidate, [], timedelta(minutes=5), self.at)
        self.assertIsNone(self.control.store.read("dispatch.block:B1"))
        self.assertEqual(self.control.stream.head, 0)
        self.assertEqual(len(self.control.trail._rows), 0)
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())

    def test_rolled_back_operation_can_commit_after_recovery(self):
        self.control.trail.append = self._boom
        with self.assertRaises(RuntimeError):
            self.control.reserve_block("op-f3", self.candidate, [], timedelta(minutes=5), self.at)
        self.control.trail = type(self.control.trail)()
        receipt = self.control.reserve_block("op-f3", self.candidate, [],
                                             timedelta(minutes=5), self.at)
        self.assertEqual(receipt.version, 1)
        self.assertEqual(self.control.stream.head, 1)
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())


class ServiceSurfaceTests(unittest.TestCase):

    def test_service_reserve_and_board_roundtrip(self):
        service = RailForgeService()
        at = datetime(2026, 1, 1, 7, tzinfo=U)
        a = occ("A", "B7", datetime(2026, 1, 1, 8, tzinfo=U))
        receipt = service.reserve_block("svc-1", a, [], timedelta(minutes=10), at)
        self.assertEqual(receipt.version, 1)
        board, version = service.block_board("B7")
        self.assertEqual(version, 1)
        self.assertEqual(board[0].train, "A")
        self.assertEqual(service.health()["workflow"]["audit_valid"], True)


if __name__ == "__main__":
    unittest.main()
