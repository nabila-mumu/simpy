from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
from simulator.metrics import ElectionMetrics
from simulator.network import NetworkConfig


@dataclass
class BidirRingResult:
    avg_time: float
    avg_msg_count: float
    completed_elections: int
    failed_attempts: int


class BidirRingAlgorithm:
    """
    Bidirectional ring leader election on a real Hamiltonian cycle.

    Design for theory-consistent comparison:
    - Two election waves travel simultaneously clockwise and counter-clockwise.
    - Each wave covers approximately half of the cycle.
    - Coordinator announcement also travels simultaneously in two directions.
    - Successful message count remains O(N), usually close to unidirectional ring.
    - Successful election time is roughly half of unidirectional ring because the
      critical path is about N/2 hops instead of N hops.
    """

    def __init__(
        self,
        cycle: list[int],
        network: NetworkConfig,
        failure_schedule: FailureSchedule,
        sim_time: float,
        seed: int | None = None,
        max_attempts: int = 8,
    ):
        if len(cycle) < 3:
            raise ValueError("Bidirectional ring requires a Hamiltonian cycle with at least 3 nodes")
        self.cycle = cycle
        self.n = len(cycle)
        self.index = {node: i for i, node in enumerate(cycle)}
        self.network = network
        self.failure_schedule = failure_schedule
        self.sim_time = sim_time
        self.rng = random.Random(seed)
        self.max_attempts = max_attempts

    def node_at(self, start: int, offset: int) -> int:
        return self.cycle[(self.index[start] + offset) % self.n]

    def run(self) -> BidirRingResult:
        metrics = ElectionMetrics()
        triggers = leader_failure_triggers(self.n, self.failure_schedule, self.sim_time)

        for fail_time in triggers:
            detection_time = fail_time + self.network.election_timeout()
            if detection_time >= self.sim_time:
                continue

            success, finish_time, msg_count, failed_attempts = self._elect_after_trigger(detection_time)
            metrics.failed_attempts += failed_attempts

            if success and finish_time is not None:
                metrics.record_success(finish_time - fail_time, msg_count)
            else:
                metrics.record_failure()

        return BidirRingResult(metrics.avg_time, metrics.avg_msg_count, metrics.completed_elections, metrics.failed_attempts)

    def _send_wave(self, start: int, direction: int, hops: int, start_time: float, alive: set[int], msg_type: str) -> tuple[bool, float, int, set[int]]:
        """Send one directional wave for a fixed number of hops."""
        t = start_time
        current = start
        seen = {start} if start in alive else set()
        messages = 0

        for step in range(1, hops + 1):
            nxt = self.node_at(start, direction * step)
            messages += 1
            if not self.network.delivered(self.rng, hops=1):
                return False, t, messages, seen
            arrival = t + self.network.message_delay(msg_type, self.rng, hops=1)
            if arrival >= self.sim_time:
                return False, t, messages, seen
            if not is_alive(self.failure_schedule, current, t) or not is_alive(self.failure_schedule, nxt, arrival):
                return False, t, messages, seen
            if nxt in alive:
                seen.add(nxt)
            current = nxt
            t = arrival

        return True, t, messages, seen

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        failed_attempts = 0
        timeout = self.network.election_timeout()

        # Forward/backward hops are partitioned so total visited edges ~= N.
        cw_hops = self.n // 2
        ccw_hops = self.n - cw_hops

        for attempt in range(self.max_attempts):
            start_time = detection_time + attempt * timeout
            if start_time >= self.sim_time:
                return False, None, 0, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
            if len(alive) < 2:
                failed_attempts += 1
                continue

            initiator = min(alive)
            if initiator not in self.index:
                failed_attempts += 1
                continue

            ok1, t1, m1, seen1 = self._send_wave(initiator, +1, cw_hops, start_time, alive, "RING_ELECTION")
            ok2, t2, m2, seen2 = self._send_wave(initiator, -1, ccw_hops, start_time, alive, "RING_ELECTION")
            attempt_messages = m1 + m2

            if not (ok1 and ok2):
                failed_attempts += 1
                continue

            candidates = seen1 | seen2
            if not candidates:
                failed_attempts += 1
                continue
            leader = max(candidates)
            wave_start = max(t1, t2)

            ok3, t3, m3, _ = self._send_wave(leader, +1, cw_hops, wave_start, alive, "RING_COORDINATOR")
            ok4, t4, m4, _ = self._send_wave(leader, -1, ccw_hops, wave_start, alive, "RING_COORDINATOR")
            attempt_messages += m3 + m4

            if ok3 and ok4:
                return True, max(t3, t4), attempt_messages, failed_attempts

            failed_attempts += 1

        return False, None, 0, failed_attempts
