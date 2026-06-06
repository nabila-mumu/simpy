# from __future__ import annotations

# import random
# from collections import deque
# from dataclasses import dataclass
# from typing import Optional

# from simulator.failures import FailureSchedule, alive_nodes, is_alive, leader_failure_triggers
# from simulator.graph import Graph
# from simulator.metrics import ElectionMetrics
# from simulator.network import NetworkConfig


# @dataclass
# class EchoResult:
#     avg_time: float
#     avg_msg_count: float
#     completed_elections: int
#     failed_attempts: int


# class EchoWaveElectionAlgorithm:
#     """
#     Echo-wave based leader election.

#     Theory target:
#     - Messages: O(E) for probe/reply wave + O(N) coordinator announcement.
#     - Time: depends on tree depth/diameter under parallel propagation, with a
#       contention/processing penalty so dense graphs are not unrealistically fastest.
#     """

#     def __init__(self, graph: Graph, network: NetworkConfig, failure_schedule: FailureSchedule, sim_time: float, seed: int | None = None, max_attempts: int = 8):
#         self.graph = graph
#         self.n = len(graph)
#         self.network = network
#         self.failure_schedule = failure_schedule
#         self.sim_time = sim_time
#         self.rng = random.Random(seed)
#         self.max_attempts = max_attempts

#     def run(self) -> EchoResult:
#         metrics = ElectionMetrics()
#         triggers = leader_failure_triggers(self.n, self.failure_schedule, self.sim_time)
#         for fail_time in triggers:
#             detection_time = fail_time + self.network.election_timeout()
#             if detection_time >= self.sim_time:
#                 continue
#             success, finish_time, msg_count, failed_attempts = self._elect_after_trigger(detection_time)
#             metrics.failed_attempts += failed_attempts
#             if success and finish_time is not None:
#                 metrics.record_success(finish_time - fail_time, msg_count)
#             else:
#                 metrics.record_failure()
#         return EchoResult(metrics.avg_time, metrics.avg_msg_count, metrics.completed_elections, metrics.failed_attempts)

#     def _edge_send(self, src: int, dst: int, t: float, msg_type: str) -> tuple[bool, float]:
#         if not is_alive(self.failure_schedule, src, t):
#             return False, t
#         if not self.network.delivered(self.rng, hops=1):
#             return False, t
#         arrival = t + self.network.message_delay(msg_type, self.rng, hops=1)
#         if arrival >= self.sim_time:
#             return False, t
#         if not is_alive(self.failure_schedule, dst, arrival):
#             return False, t
#         return True, arrival

#     def _elect_after_trigger(self, detection_time: float) -> tuple[bool, Optional[float], int, int]:
#         failed_attempts = 0
#         timeout = self.network.election_timeout()
#         for attempt in range(self.max_attempts):
#             start_time = detection_time + attempt * timeout
#             if start_time >= self.sim_time:
#                 return False, None, 0, failed_attempts
#             alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
#             if not alive:
#                 failed_attempts += 1
#                 continue
#             initiator = min(alive)
#             success, finish, messages = self._single_echo_attempt(start_time, initiator, alive)
#             if success and finish is not None:
#                 return True, finish, messages, failed_attempts
#             failed_attempts += 1
#         return False, None, 0, failed_attempts

#     def _single_echo_attempt(self, start_time: float, initiator: int, alive: set[int]) -> tuple[bool, Optional[float], int]:
#         parent: dict[int, Optional[int]] = {initiator: None}
#         children: dict[int, list[int]] = {u: [] for u in alive}
#         arrival_time: dict[int, float] = {initiator: start_time}

#         q = deque([initiator])
#         while q:
#             u = q.popleft()
#             for v in self.graph[u]:
#                 if v not in alive or v in parent:
#                     continue
#                 parent[v] = u
#                 children[u].append(v)
#                 q.append(v)
#         if set(parent.keys()) != alive:
#             return False, None, 0

#         alive_edges = self._alive_edge_count(alive)
#         tree_edges = len(alive) - 1
#         msg_count = 2 * alive_edges + tree_edges

#         avg_alive_degree = 2.0 * alive_edges / max(len(alive), 1)
#         contention_round = 0.10 * avg_alive_degree * self.network.message_delay("ECHO_PROBE", self.rng, hops=1)

#         order = sorted(parent.keys(), key=lambda x: self._depth(parent, x))
#         for u in order:
#             if u == initiator:
#                 continue
#             p = parent[u]
#             assert p is not None
#             send_t = arrival_time[p] + contention_round
#             ok, recv_t = self._edge_send(p, u, send_t, "ECHO_PROBE")
#             if not ok:
#                 return False, None, msg_count
#             arrival_time[u] = recv_t

#         reply_done: dict[int, float] = {}
#         reverse_order = sorted(parent.keys(), key=lambda x: self._depth(parent, x), reverse=True)
#         for u in reverse_order:
#             ready_t = max((reply_done[c] for c in children[u]), default=arrival_time[u])
#             p = parent[u]
#             if p is None:
#                 reply_done[u] = ready_t
#             else:
#                 send_t = ready_t + contention_round
#                 ok, recv_t = self._edge_send(u, p, send_t, "ECHO_REPLY")
#                 if not ok:
#                     return False, None, msg_count
#                 reply_done[u] = recv_t

#         echo_finish = reply_done.get(initiator, start_time)
#         announce_time: dict[int, float] = {initiator: echo_finish}
#         q2 = deque([initiator])
#         finish_time = echo_finish
#         while q2:
#             u = q2.popleft()
#             for v in children[u]:
#                 send_t = announce_time[u] + contention_round
#                 ok, recv_t = self._edge_send(u, v, send_t, "ECHO_COORDINATOR")
#                 if not ok:
#                     return False, None, msg_count
#                 announce_time[v] = recv_t
#                 finish_time = max(finish_time, recv_t)
#                 q2.append(v)
#         return True, finish_time, msg_count

#     def _alive_edge_count(self, alive: set[int]) -> int:
#         count = 0
#         for u in alive:
#             for v in self.graph[u]:
#                 if v in alive and u < v:
#                     count += 1
#         return count

#     @staticmethod
#     def _depth(parent: dict[int, Optional[int]], node: int) -> int:
#         d = 0
#         cur = node
#         while parent[cur] is not None:
#             d += 1
#             cur = parent[cur]  # type: ignore[assignment]
#         return d


from __future__ import annotations

import random
from collections import deque, defaultdict
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

    Corrected timing model.

    Theory target:
    - Message complexity: O(E + N), normally dominated by O(E).
      We count:
          2 * alive_edges      for probe + echo over alive edges
          + (alive_nodes - 1)  for coordinator announcement over the echo tree
    - Time complexity:
      Echo is parallel, so it can be faster than unidirectional Ring on some graphs,
      especially dense/low-diameter graphs.
      But it should NOT be unrealistically fastest almost always.
      This version adds per-level congestion/processing delay proportional to
      the number of alive edges examined in that wave level.

    Comparison behavior intended:
    - Sparse graph: Echo time can be comparable to Ring/BidirRing.
    - Dense graph: Echo message count grows strongly with E.
    - Dense graph time does not become almost zero just because diameter is small.
    - Higher latency increases time.
    - Lower bandwidth increases message delay.
    - Higher loss increases failed_attempts.
    - Higher failure_rate increases failed_attempts and may reduce completed elections.
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
        failed_attempts = 0
        timeout = self.network.election_timeout()

        for attempt in range(self.max_attempts):
            start_time = detection_time + attempt * timeout
            if start_time >= self.sim_time:
                return False, None, 0, failed_attempts

            alive = set(alive_nodes(self.failure_schedule, self.n, start_time))
            if not alive:
                failed_attempts += 1
                continue

            initiator = min(alive)
            if not is_alive(self.failure_schedule, initiator, start_time):
                failed_attempts += 1
                continue

            success, finish_time, msg_count = self._single_echo_attempt(start_time, initiator, alive)
            if success and finish_time is not None:
                return True, finish_time, msg_count, failed_attempts

            failed_attempts += 1

        return False, None, 0, failed_attempts

    def _single_echo_attempt(
        self,
        start_time: float,
        initiator: int,
        alive: set[int],
    ) -> tuple[bool, Optional[float], int]:
        parent: dict[int, Optional[int]] = {initiator: None}
        children: dict[int, list[int]] = {u: [] for u in alive}
        depth: dict[int, int] = {initiator: 0}

        q = deque([initiator])
        while q:
            u = q.popleft()
            for v in self.graph[u]:
                if v not in alive or v in parent:
                    continue
                parent[v] = u
                children[u].append(v)
                depth[v] = depth[u] + 1
                q.append(v)

        # Echo needs connected alive subgraph for a global leader among alive nodes.
        if set(parent.keys()) != alive:
            return False, None, 0

        alive_edges = self._alive_edge_count(alive)
        tree_edges = len(alive) - 1

        # Message count follows O(E + N).
        msg_count = 2 * alive_edges + tree_edges

        # Group nodes by BFS depth.
        levels: dict[int, list[int]] = defaultdict(list)
        for u, d in depth.items():
            levels[d].append(u)
        max_depth = max(levels.keys()) if levels else 0

        # Base per-hop delays.
        probe_delay = self.network.message_delay("ECHO_PROBE", self.rng, hops=1)
        reply_delay = self.network.message_delay("ECHO_REPLY", self.rng, hops=1)
        coord_delay = self.network.message_delay("ECHO_COORDINATOR", self.rng, hops=1)

        # Congestion/processing model:
        # In echo wave, many edges may be probed/replied around the same time.
        # Even if the tree depth is small, nodes must process/ignore many
        # non-tree probes. This makes time sensitive to E without making it
        # sequential over all E.
        avg_alive_degree = (2.0 * alive_edges) / max(len(alive), 1)

        # This factor is intentionally moderate:
        # - not as slow as sequential O(E) time
        # - not as optimistic as pure diameter-only time
        congestion_unit = 0.12 * avg_alive_degree * probe_delay

        arrival_time: dict[int, float] = {initiator: start_time}

        # -------------------------
        # Probe phase
        # -------------------------
        for d in range(1, max_depth + 1):
            level_nodes = levels[d]

            # Number of alive edges incident to this frontier approximates
            # the probes/duplicates processed at this wave level.
            frontier_edge_load = sum(
                1
                for u in level_nodes
                for v in self.graph[u]
                if v in alive
            )
            level_congestion = congestion_unit * max(1.0, frontier_edge_load / max(len(level_nodes), 1))

            for u in level_nodes:
                p = parent[u]
                assert p is not None

                send_time = arrival_time[p] + level_congestion

                if not self.network.delivered(self.rng, hops=1):
                    return False, None, msg_count

                recv_time = send_time + probe_delay
                if recv_time >= self.sim_time:
                    return False, None, msg_count

                if not is_alive(self.failure_schedule, p, send_time):
                    return False, None, msg_count
                if not is_alive(self.failure_schedule, u, recv_time):
                    return False, None, msg_count

                arrival_time[u] = recv_time

        # -------------------------
        # Echo reply phase
        # -------------------------
        reply_done: dict[int, float] = {}

        for d in range(max_depth, -1, -1):
            level_nodes = levels[d]
            frontier_edge_load = sum(
                1
                for u in level_nodes
                for v in self.graph[u]
                if v in alive
            )
            level_congestion = congestion_unit * max(1.0, frontier_edge_load / max(len(level_nodes), 1))

            for u in level_nodes:
                if children[u]:
                    ready_time = max(reply_done[c] for c in children[u])
                else:
                    ready_time = arrival_time[u]

                p = parent[u]
                if p is None:
                    reply_done[u] = ready_time
                    continue

                send_time = ready_time + level_congestion

                if not self.network.delivered(self.rng, hops=1):
                    return False, None, msg_count

                recv_time = send_time + reply_delay
                if recv_time >= self.sim_time:
                    return False, None, msg_count

                if not is_alive(self.failure_schedule, u, send_time):
                    return False, None, msg_count
                if not is_alive(self.failure_schedule, p, recv_time):
                    return False, None, msg_count

                reply_done[u] = recv_time

        echo_finish = reply_done.get(initiator, start_time)

        # -------------------------
        # Coordinator phase
        # -------------------------
        announce_time: dict[int, float] = {initiator: echo_finish}
        finish_time = echo_finish

        for d in range(1, max_depth + 1):
            level_nodes = levels[d]
            # Coordinator uses tree only, so congestion is lighter than probe/reply.
            level_congestion = 0.35 * congestion_unit

            for u in level_nodes:
                p = parent[u]
                assert p is not None

                send_time = announce_time[p] + level_congestion

                if not self.network.delivered(self.rng, hops=1):
                    return False, None, msg_count

                recv_time = send_time + coord_delay
                if recv_time >= self.sim_time:
                    return False, None, msg_count

                if not is_alive(self.failure_schedule, p, send_time):
                    return False, None, msg_count
                if not is_alive(self.failure_schedule, u, recv_time):
                    return False, None, msg_count

                announce_time[u] = recv_time
                finish_time = max(finish_time, recv_time)

        return True, finish_time, msg_count

    def _alive_edge_count(self, alive: set[int]) -> int:
        count = 0
        for u in alive:
            for v in self.graph[u]:
                if v in alive and u < v:
                    count += 1
        return count
