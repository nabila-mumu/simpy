from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass
from typing import Optional

from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
from simulator.graph import Graph
from simulator.metrics import ElectionMetrics
from simulator.network import NetworkConfig


@dataclass
class EchoResult:
    avg_time: float
    avg_msg_count: float
    completed_elections: int
    failed_attempts: int


class EchoWaveElectionAlgorithm:
    """
    Echo-wave based leader election.

    This implements the classical echo idea for leader election:
    - An initiator starts a wave by sending PROBE messages over the graph.
    - The first PROBE received by a node defines its parent in a spanning tree.
    - Nodes echo back the maximum node ID seen in their subtree.
    - The initiator receives all echoes, learns the maximum alive node ID, and
      announces the elected coordinator along the spanning tree.

    Scope/assumptions used for fair comparison with the other algorithms:
    - Same leader-failure triggers as Bully/Ring/Raft.
    - Same failure schedule and same NetworkConfig.
    - Message loss follows the project rule: a lost required message fails the
      current attempt; the algorithm waits for timeout and retries.
    - Only election-related messages are counted:
          ECHO_PROBE, ECHO_REPLY, ECHO_COORDINATOR
    - The alive subgraph must be connected from the initiator to elect a global
      leader among all currently alive nodes. If not, the attempt fails.

    Theoretical validation expectation:
    - Probe/reply wave cost is O(E) over the alive graph.
    - Coordinator announcement over the spanning tree is O(N).
    - Therefore successful election message complexity is O(E + N), usually O(E).
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

    def run(self) -> EchoResult:
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

        return EchoResult(
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

            # Use the lowest alive node as deterministic initiator, matching the
            # simple detector style used in the other algorithms.
            initiator = min(alive)
            if not is_alive(self.failure_schedule, initiator, start_time):
                failed_attempts += 1
                continue

            success, finish, msgs = self._single_echo_attempt(start_time, initiator, alive)
            total_messages += msgs

            if success and finish is not None:
                return True, finish, total_messages, failed_attempts

            failed_attempts += 1

        return False, None, total_messages, failed_attempts

    def _single_echo_attempt(
        self,
        start_time: float,
        initiator: int,
        alive: set[int],
    ) -> tuple[bool, Optional[float], int]:
        """
        One echo-wave attempt.

        For correctness, the required spanning-tree messages must be delivered.
        Non-tree cross-edge probe/reply messages are counted as part of the wave
        complexity but are not required for correctness once the spanning tree
        has reached all alive nodes.
        """
        msg_count = 0

        parent: dict[int, Optional[int]] = {initiator: None}
        children: dict[int, list[int]] = {u: [] for u in alive}
        arrival_time: dict[int, float] = {initiator: start_time}

        # BFS tree over the currently alive induced graph. This is the spanning
        # tree formed by first probes in the echo algorithm.
        q = deque([initiator])
        while q:
            u = q.popleft()
            for v in self.graph[u]:
                if v not in alive or v in parent:
                    continue
                parent[v] = u
                children[u].append(v)
                q.append(v)

        # If some alive node is unreachable in the alive subgraph, a global echo
        # leader election cannot complete in this attempt.
        if set(parent.keys()) != alive:
            return False, None, 0

        alive_edges = self._alive_edge_count(alive)
        tree_edges = len(alive) - 1

        # Count classical echo wave traffic: approximately one probe and one echo
        # per alive edge, plus coordinator announcement over the tree.
        # This gives the expected O(E) trend.
        msg_count += 2 * alive_edges
        msg_count += tree_edges

        # PROBE phase along tree. Required tree probes must be delivered.
        order = sorted(parent.keys(), key=lambda x: self._depth(parent, x))
        for u in order:
            if u == initiator:
                continue
            p = parent[u]
            assert p is not None

            send_t = arrival_time[p]
            if not self.network.delivered(self.rng, hops=1):
                return False, None, msg_count

            recv_t = send_t + self.network.message_delay("ECHO_PROBE", self.rng, hops=1)
            if recv_t >= self.sim_time:
                return False, None, msg_count
            if not is_alive(self.failure_schedule, p, recv_t) or not is_alive(self.failure_schedule, u, recv_t):
                return False, None, msg_count

            arrival_time[u] = recv_t

        # Echo reply phase, children reply after their descendants are complete.
        # The maximum ID is conceptually aggregated here; since all alive nodes
        # are reached, the leader is max(alive).
        reply_done: dict[int, float] = {}
        reverse_order = sorted(parent.keys(), key=lambda x: self._depth(parent, x), reverse=True)

        for u in reverse_order:
            if not children[u]:
                reply_done[u] = arrival_time[u]
            else:
                reply_done[u] = max(reply_done[c] for c in children[u])

            p = parent[u]
            if p is not None:
                if not self.network.delivered(self.rng, hops=1):
                    return False, None, msg_count

                recv_t = reply_done[u] + self.network.message_delay("ECHO_REPLY", self.rng, hops=1)
                if recv_t >= self.sim_time:
                    return False, None, msg_count
                if not is_alive(self.failure_schedule, u, recv_t) or not is_alive(self.failure_schedule, p, recv_t):
                    return False, None, msg_count

                reply_done[u] = recv_t

        echo_finish = reply_done.get(initiator, start_time)

        # Coordinator announcement from initiator along the echo tree. The message
        # carries leader=max(alive), but only timing/count are needed here.
        announce_time: dict[int, float] = {initiator: echo_finish}
        q2 = deque([initiator])
        finish_time = echo_finish

        while q2:
            u = q2.popleft()
            for v in children[u]:
                if not self.network.delivered(self.rng, hops=1):
                    return False, None, msg_count

                recv_t = announce_time[u] + self.network.message_delay("ECHO_COORDINATOR", self.rng, hops=1)
                if recv_t >= self.sim_time:
                    return False, None, msg_count
                if not is_alive(self.failure_schedule, u, recv_t) or not is_alive(self.failure_schedule, v, recv_t):
                    return False, None, msg_count

                announce_time[v] = recv_t
                finish_time = max(finish_time, recv_t)
                q2.append(v)

        return True, finish_time, msg_count

    def _alive_edge_count(self, alive: set[int]) -> int:
        count = 0
        for u in alive:
            for v in self.graph[u]:
                if v in alive and u < v:
                    count += 1
        return count

    @staticmethod
    def _depth(parent: dict[int, Optional[int]], node: int) -> int:
        d = 0
        cur = node
        while parent[cur] is not None:
            d += 1
            cur = parent[cur]  # type: ignore[assignment]
        return d
