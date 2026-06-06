from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
from simulator.metrics import ElectionMetrics
from simulator.network import NetworkConfig


@dataclass
class RingResult:
    avg_time: float
    avg_msg_count: float
    completed_elections: int
    failed_attempts: int


class RingAlgorithm:
    """
    Unidirectional ring leader election on a real Hamiltonian cycle.

    Theory target:
    - Successful message count: about 2N.
    - Successful election time: sequential around the whole ring twice.
    - Message complexity O(N); time grows with N and latency.
    - Message loss/failure can make an attempt fail; retry after timeout.
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
            raise ValueError("Ring algorithm requires a Hamiltonian cycle with at least 3 nodes")
        self.cycle = cycle
        self.n = len(cycle)
        self.index = {node: i for i, node in enumerate(cycle)}
        self.network = network
        self.failure_schedule = failure_schedule
        self.sim_time = sim_time
        self.rng = random.Random(seed)
        self.max_attempts = max_attempts

    def successor(self, node: int) -> int:
        return self.cycle[(self.index[node] + 1) % self.n]

    def run(self) -> RingResult:
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
        return RingResult(metrics.avg_time, metrics.avg_msg_count, metrics.completed_elections, metrics.failed_attempts)

    def _transmit_hop(self, src: int, dst: int, t: float, msg_type: str) -> tuple[bool, float]:
        if not is_alive(self.failure_schedule, src, t):
            return False, t
        if not self.network.delivered(self.rng, hops=1):
            return False, t
        arrival = t + self.network.message_delay(msg_type, self.rng, hops=1)
        if arrival >= self.sim_time:
            return False, t
        if not is_alive(self.failure_schedule, dst, arrival):
            return False, t
        return True, arrival

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        failed_attempts = 0
        timeout = self.network.election_timeout()
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

            t = start_time
            current = initiator
            candidates = {initiator}
            attempt_messages = 0
            ok = True

            for _ in range(self.n):
                nxt = self.successor(current)
                attempt_messages += 1
                ok, t = self._transmit_hop(current, nxt, t, "RING_ELECTION")
                if not ok:
                    break
                if nxt in alive:
                    candidates.add(nxt)
                current = nxt
            if not ok or current != initiator:
                failed_attempts += 1
                continue

            leader = max(candidates)
            current = leader
            for _ in range(self.n):
                nxt = self.successor(current)
                attempt_messages += 1
                ok, t = self._transmit_hop(current, nxt, t, "RING_COORDINATOR")
                if not ok:
                    break
                current = nxt
            if ok and current == leader:
                return True, t, attempt_messages, failed_attempts
            failed_attempts += 1
        return False, None, 0, failed_attempts
