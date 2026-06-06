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

    Fixed version for fair comparison.

    Main corrections:
    1. avg_msg_count records only the successful election attempt.
       Failed attempts are counted separately in failed_attempts.
    2. Successful Raft election includes one initial leader-confirmation phase
       using RAFT_HEARTBEAT / AppendEntries-style authority messages.
       This makes it comparable with Ring's COORDINATOR phase.
    3. Split-vote / election-collision behavior is modeled.
       If another follower times out too close to the first candidate, the term
       fails and retries after timeout.
    4. Majority is based on configured cluster size N:
           majority = floor(N/2) + 1
    5. Continuous heartbeat traffic is NOT counted.
       Counted messages:
           RequestVote, VoteResponse, one initial leader-confirmation round.
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

        return RaftResult(
            avg_time=metrics.avg_time,
            avg_msg_count=metrics.avg_msg_count,
            completed_elections=metrics.completed_elections,
            failed_attempts=metrics.failed_attempts,
        )

    def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
        failed_attempts = 0
        timeout = self.network.election_timeout()
        majority = self.n // 2 + 1

        for attempt in range(self.max_attempts):
            term_start = detection_time + attempt * timeout
            if term_start >= self.sim_time:
                return False, None, 0, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, term_start))
            if len(alive) < majority:
                failed_attempts += 1
                continue

            # Randomized Raft election timeout for every alive follower.
            candidate_start_times = {
                node: term_start + self.rng.uniform(timeout, 2.0 * timeout)
                for node in alive
            }
            ordered = sorted(candidate_start_times.items(), key=lambda x: x[1])
            candidate, start_time = ordered[0]

            if start_time >= self.sim_time or not is_alive(self.failure_schedule, candidate, start_time):
                failed_attempts += 1
                continue

            # Split-vote / election collision.
            # If another node becomes candidate before the first candidate can
            # reasonably reach voters, the term is treated as a split vote.
            if len(ordered) > 1:
                _, second_start = ordered[1]
                collision_window = max(
                    self.network.message_delay("RAFT_REQUEST_VOTE", self.rng, hops=1),
                    0.25 * timeout,
                )
                if second_start - start_time < collision_window:
                    failed_attempts += 1
                    continue

            attempt_messages = 0
            vote_arrival_times: list[float] = []
            candidate_crashed = False

            # Candidate sends RequestVote RPCs in parallel.
            for peer in range(self.n):
                if peer == candidate:
                    continue

                path_len = shortest_path_length(self.graph, candidate, peer)
                if path_len is None:
                    continue

                # RequestVote: candidate -> peer
                attempt_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    continue

                request_arrival = start_time + self.network.message_delay(
                    "RAFT_REQUEST_VOTE", self.rng, hops=path_len
                )
                if request_arrival >= self.sim_time:
                    continue

                if not is_alive(self.failure_schedule, candidate, request_arrival):
                    candidate_crashed = True
                    break

                if not is_alive(self.failure_schedule, peer, request_arrival):
                    continue

                # VoteResponse: peer -> candidate
                attempt_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    continue

                vote_arrival = request_arrival + self.network.message_delay(
                    "RAFT_VOTE_RESPONSE", self.rng, hops=path_len
                )
                if vote_arrival >= self.sim_time:
                    continue

                if not is_alive(self.failure_schedule, peer, vote_arrival):
                    continue

                if not is_alive(self.failure_schedule, candidate, vote_arrival):
                    candidate_crashed = True
                    break

                vote_arrival_times.append(vote_arrival)

            if candidate_crashed:
                failed_attempts += 1
                continue

            needed_responses = majority - 1
            vote_arrival_times.sort()

            if len(vote_arrival_times) < needed_responses:
                failed_attempts += 1
                continue

            majority_time = vote_arrival_times[needed_responses - 1]

            # Initial leader confirmation round.
            # This is election-related and comparable to coordinator announcement.
            confirm_finish = majority_time
            confirm_ok = True
            alive_at_majority = set(alive_nodes(self.failure_schedule, self.n, majority_time))

            if candidate not in alive_at_majority:
                failed_attempts += 1
                continue

            for peer in alive_at_majority:
                if peer == candidate:
                    continue

                path_len = shortest_path_length(self.graph, candidate, peer)
                if path_len is None:
                    continue

                attempt_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    confirm_ok = False
                    break

                confirm_arrival = majority_time + self.network.message_delay(
                    "RAFT_HEARTBEAT", self.rng, hops=path_len
                )
                if confirm_arrival >= self.sim_time:
                    confirm_ok = False
                    break

                if not is_alive(self.failure_schedule, candidate, confirm_arrival):
                    confirm_ok = False
                    break

                if not is_alive(self.failure_schedule, peer, confirm_arrival):
                    confirm_ok = False
                    break

                confirm_finish = max(confirm_finish, confirm_arrival)

            if not confirm_ok:
                failed_attempts += 1
                continue

            return True, confirm_finish, attempt_messages, failed_attempts

        return False, None, 0, failed_attempts
