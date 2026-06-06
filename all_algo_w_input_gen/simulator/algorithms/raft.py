from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
from simulator.graph import Graph, shortest_path_length
from simulator.metrics import ElectionMetrics
from simulator.network import NetworkConfig


@dataclass
class RaftResult:
    avg_time: float
    avg_msg_count: float
    completed_elections: int
    failed_attempts: int


class RaftElectionAlgorithm:
    """
    Raft leader election only.

    Theory target:
    - Successful message complexity O(N) with graph/path effects.
    - RequestVote + VoteResponse + one initial leader-confirmation round.
    - Continuous heartbeat traffic is not counted.
    - Split vote is modeled moderately, not pessimistically.
    """

    def __init__(self, graph: Graph, network: NetworkConfig, failure_schedule: FailureSchedule, sim_time: float, seed: int | None = None, max_attempts: int = 8):
        self.graph = graph
        self.n = len(graph)
        self.network = network
        self.failure_schedule = failure_schedule
        self.sim_time = sim_time
        self.rng = random.Random(seed)
        self.max_attempts = max_attempts

    def run(self) -> RaftResult:
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
        return RaftResult(metrics.avg_time, metrics.avg_msg_count, metrics.completed_elections, metrics.failed_attempts)

    def _one_way(self, src: int, dst: int, t: float, msg_type: str) -> tuple[bool, float, int]:
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
        majority = self.n // 2 + 1
        for attempt in range(self.max_attempts):
            attempt_messages = 0
            term_start = detection_time + attempt * timeout
            if term_start >= self.sim_time:
                return False, None, 0, failed_attempts
            alive = set(alive_nodes(self.failure_schedule, self.n, term_start))
            if len(alive) < majority:
                failed_attempts += 1
                continue

            # Wider randomized timeout window: realistic Raft avoids frequent split votes.
            candidate_start_times = {node: term_start + self.rng.uniform(timeout, 3.0 * timeout) for node in alive}
            ordered = sorted(candidate_start_times.items(), key=lambda x: x[1])
            candidate, start_time = ordered[0]
            if start_time >= self.sim_time or not is_alive(self.failure_schedule, candidate, start_time):
                failed_attempts += 1
                continue

            # Moderate split vote, only if second candidate starts within one local RPC delay.
            if len(ordered) > 1:
                _, second_start = ordered[1]
                collision_window = self.network.message_delay("RAFT_REQUEST_VOTE", self.rng, hops=1)
                if second_start - start_time < collision_window and self.rng.random() < 0.5:
                    failed_attempts += 1
                    continue

            vote_arrivals: list[float] = []
            for peer in range(self.n):
                if peer == candidate:
                    continue
                ok, req_arrival, m = self._one_way(candidate, peer, start_time, "RAFT_REQUEST_VOTE")
                attempt_messages += m
                if not ok:
                    continue
                ok, vote_arrival, m = self._one_way(peer, candidate, req_arrival, "RAFT_VOTE_RESPONSE")
                attempt_messages += m
                if ok:
                    vote_arrivals.append(vote_arrival)

            needed = majority - 1
            vote_arrivals.sort()
            if len(vote_arrivals) < needed:
                failed_attempts += 1
                continue
            majority_time = vote_arrivals[needed - 1]

            alive_at_majority = set(alive_nodes(self.failure_schedule, self.n, majority_time))
            if candidate not in alive_at_majority:
                failed_attempts += 1
                continue
            confirm_finish = majority_time
            confirm_ok = True
            for peer in alive_at_majority:
                if peer == candidate:
                    continue
                ok, confirm_arrival, m = self._one_way(candidate, peer, majority_time, "RAFT_HEARTBEAT")
                attempt_messages += m
                if not ok:
                    confirm_ok = False
                    break
                confirm_finish = max(confirm_finish, confirm_arrival)
            if not confirm_ok:
                failed_attempts += 1
                continue
            return True, confirm_finish, attempt_messages, failed_attempts
        return False, None, 0, failed_attempts
