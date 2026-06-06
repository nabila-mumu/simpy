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

    Theory validation expectation:
    - One successful election needs roughly N ELECTION messages and N COORDINATOR
      messages, so message count is O(N).
    - Time is roughly proportional to cycle length N because the token travels
      sequentially around the ring.
    - Message loss causes failed attempts and retries after timeout.
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

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        failed_attempts = 0
        timeout = self.network.election_timeout()

        for attempt in range(self.max_attempts):
            attempt_messages = 0
            start_time = detection_time + attempt * timeout
            if start_time >= self.sim_time:
                return False, None, 0, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
            if len(alive) < 2:
                failed_attempts += 1
                continue

            # Choose lowest alive node as detector/initiator for deterministic comparison.
            initiator = min(alive)
            if initiator not in self.index:
                failed_attempts += 1
                continue

            current = initiator
            t = start_time
            candidates = {initiator}
            attempt_ok = True

            # ELECTION token makes one complete cycle. It may pass through failed nodes;
            # if the next ring neighbor is failed at arrival time, this attempt fails.
            for _ in range(self.n):
                nxt = self.successor(current)
                attempt_messages += 1
                if not self.network.delivered(self.rng, hops=1):
                    attempt_ok = False
                    break
                arrival = t + self.network.message_delay("RING_ELECTION", self.rng, hops=1)
                if arrival >= self.sim_time:
                    attempt_ok = False
                    break
                if not is_alive(self.failure_schedule, current, t) or not is_alive(self.failure_schedule, nxt, arrival):
                    attempt_ok = False
                    break
                if nxt in alive:
                    candidates.add(nxt)
                current = nxt
                t = arrival

            if not attempt_ok or current != initiator:
                failed_attempts += 1
                continue

            leader = max(candidates)
            current = leader

            # COORDINATOR token makes one complete cycle.
            for _ in range(self.n):
                nxt = self.successor(current)
                attempt_messages += 1
                if not self.network.delivered(self.rng, hops=1):
                    attempt_ok = False
                    break
                arrival = t + self.network.message_delay("RING_COORDINATOR", self.rng, hops=1)
                if arrival >= self.sim_time:
                    attempt_ok = False
                    break
                if not is_alive(self.failure_schedule, current, t) or not is_alive(self.failure_schedule, nxt, arrival):
                    attempt_ok = False
                    break
                current = nxt
                t = arrival

            if attempt_ok and current == leader:
                return True, t, attempt_messages, failed_attempts

            failed_attempts += 1

        return False, None, 0, failed_attempts
