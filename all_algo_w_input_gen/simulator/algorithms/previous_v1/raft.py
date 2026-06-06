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

    This is intentionally NOT full Raft. It does not simulate log replication,
    client commands, or continuous heartbeat traffic. It only simulates the
    election phase after a leader failure.

    Theory-valid election rules implemented:
    - A new election term starts after leader failure detection timeout.
    - Alive followers have randomized election timeouts.
    - The first timed-out follower becomes candidate.
    - Candidate votes for itself.
    - Candidate sends RequestVote RPCs to other nodes.
    - Each reachable alive node grants at most one vote in that term.
    - Candidate becomes leader when it receives a majority of votes from the
      configured cluster, i.e. floor(N/2)+1 votes.
    - If no majority is reached because of crashes or message loss, the attempt
      fails and a later timeout starts a new term.

    Message counting:
    - Count RequestVote messages.
    - Count VoteResponse messages only if the RequestVote was delivered to an
      alive receiver and the receiver attempts a reply.
    - Do NOT count continuous heartbeats, because the comparison is election-only.

    Validation expectation:
    - Successful election message complexity is O(N) in the common case.
    - Election time is roughly one randomized timeout plus one round-trip time
      to enough voters.
    - Message loss and crashes increase failed_attempts and may increase average
      time/message count due to retries.
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
        total_messages = 0
        failed_attempts = 0
        timeout = self.network.election_timeout()
        majority = self.n // 2 + 1

        for attempt in range(self.max_attempts):
            # New Raft term after each retry timeout.
            term_start = detection_time + attempt * timeout
            if term_start >= self.sim_time:
                return False, None, total_messages, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, term_start))
            if len(alive) < majority:
                failed_attempts += 1
                continue

            # Randomized election timeout. Earliest alive node becomes candidate.
            # This captures Raft's collision-reduction behavior without simulating
            # long-running heartbeat state.
            candidate_start_times = {
                node: term_start + self.rng.uniform(timeout, 2.0 * timeout)
                for node in alive
            }
            candidate = min(candidate_start_times, key=candidate_start_times.get)
            start_time = candidate_start_times[candidate]

            if start_time >= self.sim_time or not is_alive(self.failure_schedule, candidate, start_time):
                failed_attempts += 1
                continue

            votes = 1  # candidate votes for itself
            latest_vote_time = start_time
            attempt_ok = True

            # Candidate sends RequestVote to all other configured servers.
            # Network routing follows the generated graph using shortest path.
            peers = [v for v in range(self.n) if v != candidate]

            # To model parallel RPCs fairly, calculate each peer's response time
            # independently from start_time. Finish time is when majority vote arrives.
            response_times: list[float] = []

            for peer in peers:
                path_len = shortest_path_length(self.graph, candidate, peer)
                if path_len is None:
                    continue

                # RequestVote candidate -> peer
                total_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    continue

                request_arrival = start_time + self.network.message_delay("RAFT_REQUEST_VOTE", self.rng, hops=path_len)
                if request_arrival >= self.sim_time:
                    attempt_ok = False
                    break

                if not is_alive(self.failure_schedule, candidate, request_arrival):
                    attempt_ok = False
                    break
                if not is_alive(self.failure_schedule, peer, request_arrival):
                    continue

                # VoteResponse peer -> candidate. In this simplified election-only
                # model the peer grants the vote because it has not voted yet in this term.
                total_messages += 1
                if not self.network.delivered(self.rng, hops=path_len):
                    continue

                vote_arrival = request_arrival + self.network.message_delay("RAFT_VOTE_RESPONSE", self.rng, hops=path_len)
                if vote_arrival >= self.sim_time:
                    attempt_ok = False
                    break

                if not is_alive(self.failure_schedule, peer, vote_arrival):
                    continue
                if not is_alive(self.failure_schedule, candidate, vote_arrival):
                    attempt_ok = False
                    break

                response_times.append(vote_arrival)

            if not attempt_ok:
                failed_attempts += 1
                continue

            response_times.sort()
            needed_responses = majority - 1
            if len(response_times) >= needed_responses:
                if needed_responses > 0:
                    latest_vote_time = response_times[needed_responses - 1]
                else:
                    latest_vote_time = start_time

                # At this point Raft has elected a leader. We intentionally do not
                # add continuous heartbeats to election metrics.
                return True, latest_vote_time, total_messages, failed_attempts

            failed_attempts += 1

        return False, None, total_messages, failed_attempts
