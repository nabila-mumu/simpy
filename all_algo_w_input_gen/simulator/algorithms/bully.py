from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
from simulator.graph import Graph, shortest_path_length
from simulator.metrics import ElectionMetrics
from simulator.network import NetworkConfig


@dataclass
class BullyResult:
    avg_time: float
    avg_msg_count: float
    completed_elections: int
    failed_attempts: int


class BullyAlgorithm:
    """
    Standard Bully leader election.

    Theory target:
    - Successful worst-case message complexity O(N^2).
    - Lowest alive initiator exposes worst-case behavior.
    - Uses graph shortest paths so latency/bandwidth/loss/path length matter.
    """

    def __init__(self, graph: Graph, network: NetworkConfig, failure_schedule: FailureSchedule, sim_time: float, seed: int | None = None, max_attempts: int = 8):
        self.graph = graph
        self.n = len(graph)
        self.network = network
        self.failure_schedule = failure_schedule
        self.sim_time = sim_time
        self.rng = random.Random(seed)
        self.max_attempts = max_attempts

    def run(self) -> BullyResult:
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
        return BullyResult(metrics.avg_time, metrics.avg_msg_count, metrics.completed_elections, metrics.failed_attempts)

    def _send(self, src: int, dst: int, t: float, msg_type: str) -> tuple[bool, float, int]:
        path_len = shortest_path_length(self.graph, src, dst)
        if path_len is None:
            return False, t, 0
        if not is_alive(self.failure_schedule, src, t):
            return False, t, 1
        if not self.network.delivered(self.rng, hops=path_len):
            return False, t, 1
        arrival = t + self.network.message_delay(msg_type, self.rng, hops=path_len)
        if arrival >= self.sim_time:
            return False, t, 1
        if not is_alive(self.failure_schedule, dst, arrival):
            return False, t, 1
        return True, arrival, 1

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        failed_attempts = 0
        timeout = self.network.election_timeout()
        for attempt in range(self.max_attempts):
            attempt_messages = 0
            start_time = detection_time + attempt * timeout
            if start_time >= self.sim_time:
                return False, None, 0, failed_attempts
            alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
            if not alive:
                failed_attempts += 1
                continue

            initiator = min(alive)
            active_nodes = {initiator}
            queue = [initiator]
            current_time = start_time
            ok = True

            while queue and ok:
                u = queue.pop(0)
                if not is_alive(self.failure_schedule, u, current_time):
                    continue
                higher_nodes = [v for v in range(u + 1, self.n) if v in alive]
                got_ok = False
                latest_reply_time = current_time
                for v in higher_nodes:
                    delivered, arrival_time, m = self._send(u, v, current_time, "BULLY_ELECTION")
                    attempt_messages += m
                    if not delivered:
                        continue
                    delivered, reply_time, m = self._send(v, u, arrival_time, "BULLY_OK")
                    attempt_messages += m
                    if not delivered:
                        continue
                    got_ok = True
                    latest_reply_time = max(latest_reply_time, reply_time)
                    if v not in active_nodes:
                        active_nodes.add(v)
                        queue.append(v)
                current_time = latest_reply_time

            alive_active = [u for u in active_nodes if is_alive(self.failure_schedule, u, current_time)]
            if not alive_active:
                failed_attempts += 1
                continue
            leader = max(alive_active)
            finish_time = current_time
            for v in alive:
                if v == leader:
                    continue
                delivered, arrival_time, m = self._send(leader, v, current_time, "BULLY_COORDINATOR")
                attempt_messages += m
                if not delivered:
                    ok = False
                    break
                finish_time = max(finish_time, arrival_time)
            if ok:
                return True, finish_time, attempt_messages, failed_attempts
            failed_attempts += 1
        return False, None, 0, failed_attempts
