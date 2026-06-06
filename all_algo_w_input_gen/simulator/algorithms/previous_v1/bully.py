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

    Rule:
    - A node starts election by sending ELECTION to all higher-ID nodes.
    - Any alive higher-ID node replies OK.
    - Higher-ID nodes then start/continue their own election.
    - Highest alive reachable node becomes coordinator.
    - Coordinator announces COORDINATOR to all lower-ID alive nodes.

    Validation expectation:
    - Worst-case message complexity is O(N^2), especially when low-ID nodes initiate.
    - Election time increases with latency and path length.
    - Message loss increases failed attempts and can increase average message count/time.
    """

    def __init__(
        self,
        graph: Graph,
        network: NetworkConfig,
        failure_schedule: FailureSchedule,
        sim_time: float,
        seed: int | None = None,
        max_attempts: int = 8,
    ):
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
                # Election time measured from actual leader failure to new leader announcement.
                metrics.record_success(finish_time - fail_time, msg_count)
            else:
                metrics.record_failure()

        return BullyResult(
            avg_time=metrics.avg_time,
            avg_msg_count=metrics.avg_msg_count,
            completed_elections=metrics.completed_elections,
            failed_attempts=metrics.failed_attempts,
        )

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        total_messages = 0
        failed_attempts = 0
        timeout = self.network.election_timeout()

        for attempt in range(self.max_attempts):
            start_time = detection_time + attempt * timeout
            if start_time >= self.sim_time:
                return False, None, total_messages, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
            if not alive:
                failed_attempts += 1
                continue

            # Detection node: choose lowest alive node to make Bully's O(N^2)
            # behavior visible and theoretically valid in worst-case situations.
            initiator = min(alive)

            active_nodes = {initiator}
            queue = [initiator]
            current_time = start_time
            highest_candidate = initiator
            attempt_ok = True

            while queue and attempt_ok:
                u = queue.pop(0)
                if not is_alive(self.failure_schedule, u, current_time):
                    continue

                higher_nodes = [v for v in range(u + 1, self.n) if v in alive]
                got_ok = False
                latest_reply_time = current_time

                for v in higher_nodes:
                    path_len = shortest_path_length(self.graph, u, v)
                    if path_len is None:
                        continue

                    # u -> v : ELECTION
                    total_messages += 1
                    if not self.network.delivered(self.rng, hops=path_len):
                        continue
                    arrival_time = current_time + self.network.message_delay("BULLY_ELECTION", self.rng, hops=path_len)
                    if arrival_time >= self.sim_time:
                        attempt_ok = False
                        break
                    if not is_alive(self.failure_schedule, u, arrival_time) or not is_alive(self.failure_schedule, v, arrival_time):
                        continue

                    # v -> u : OK
                    total_messages += 1
                    if not self.network.delivered(self.rng, hops=path_len):
                        continue
                    reply_time = arrival_time + self.network.message_delay("BULLY_OK", self.rng, hops=path_len)
                    if reply_time >= self.sim_time:
                        attempt_ok = False
                        break
                    if not is_alive(self.failure_schedule, u, reply_time) or not is_alive(self.failure_schedule, v, reply_time):
                        continue

                    got_ok = True
                    latest_reply_time = max(latest_reply_time, reply_time)
                    highest_candidate = max(highest_candidate, v)
                    if v not in active_nodes:
                        active_nodes.add(v)
                        queue.append(v)

                current_time = latest_reply_time

                # If u gets no OK, it may be coordinator, but final coordinator
                # is the highest such active node.
                if not got_ok:
                    highest_candidate = max(highest_candidate, u)

            if not attempt_ok:
                failed_attempts += 1
                continue

            leader = max(node for node in active_nodes if is_alive(self.failure_schedule, node, current_time))
            finish_time = current_time

            # Coordinator announcement from leader to all lower-ID alive nodes.
            for v in alive:
                if v == leader:
                    continue
                path_len = shortest_path_length(self.graph, leader, v)
                if path_len is None:
                    continue

                total_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    attempt_ok = False
                    break
                arrival_time = current_time + self.network.message_delay("BULLY_COORDINATOR", self.rng, hops=path_len)
                if arrival_time >= self.sim_time:
                    attempt_ok = False
                    break
                if not is_alive(self.failure_schedule, leader, arrival_time) or not is_alive(self.failure_schedule, v, arrival_time):
                    attempt_ok = False
                    break
                finish_time = max(finish_time, arrival_time)

            if attempt_ok:
                return True, finish_time, total_messages, failed_attempts

            failed_attempts += 1

        return False, None, total_messages, failed_attempts
