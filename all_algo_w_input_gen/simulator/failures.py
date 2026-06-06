from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Tuple


RECOVERY_FACTOR = 0.20


@dataclass
class FailureSchedule:
    alive_intervals: Dict[int, List[Tuple[float, float]]]
    crash_events: List[Tuple[float, int]]


def generate_failure_schedule(num_nodes: int, sim_time: float, failure_rate: float, seed: int | None = None) -> FailureSchedule:
    """
    Failure model kept as your previous choice:
        mean_crash_time = 1 / failure_rate
        mean_recovery_time = mean_crash_time * RECOVERY_FACTOR

    Nodes start alive at t=0.
    """
    rng = random.Random(seed)

    if failure_rate <= 0:
        return FailureSchedule(
            alive_intervals={i: [(0.0, sim_time)] for i in range(num_nodes)},
            crash_events=[],
        )

    mean_crash_time = 1.0 / failure_rate
    mean_recovery_time = mean_crash_time * RECOVERY_FACTOR

    alive_intervals: Dict[int, List[Tuple[float, float]]] = {}
    crash_events: List[Tuple[float, int]] = []

    for node in range(num_nodes):
        t = 0.0
        alive = True
        alive_start = 0.0
        intervals: List[Tuple[float, float]] = []

        while t < sim_time:
            if alive:
                crash_t = t + rng.expovariate(1.0 / mean_crash_time)
                if crash_t >= sim_time:
                    intervals.append((alive_start, sim_time))
                    break
                intervals.append((alive_start, crash_t))
                crash_events.append((crash_t, node))
                t = crash_t
                alive = False
            else:
                recover_t = t + rng.expovariate(1.0 / mean_recovery_time)
                if recover_t >= sim_time:
                    break
                t = recover_t
                alive_start = recover_t
                alive = True

        alive_intervals[node] = intervals

    crash_events.sort()
    return FailureSchedule(alive_intervals=alive_intervals, crash_events=crash_events)


def is_alive(schedule: FailureSchedule, node: int, t: float) -> bool:
    return any(start <= t < end for start, end in schedule.alive_intervals[node])


def alive_nodes(schedule: FailureSchedule, num_nodes: int, t: float) -> list[int]:
    return [node for node in range(num_nodes) if is_alive(schedule, node, t)]


def leader_failure_triggers(num_nodes: int, schedule: FailureSchedule, sim_time: float) -> list[float]:
    """
    Election trigger rule:
    - Initial election at t=0.
    - Later election whenever the current highest-alive leader crashes.
    This is deterministic from the same failure schedule, so all algorithms can
    later use the same trigger list.
    """
    triggers = [0.0]
    current_alive = alive_nodes(schedule, num_nodes, 0.0)
    current_leader = max(current_alive) if current_alive else None

    for t, node in schedule.crash_events:
        if t >= sim_time:
            break
        if node == current_leader:
            triggers.append(t)
            after = alive_nodes(schedule, num_nodes, min(t + 1e-9, sim_time))
            current_leader = max(after) if after else None
        elif current_leader is None:
            after = alive_nodes(schedule, num_nodes, min(t + 1e-9, sim_time))
            current_leader = max(after) if after else None

    return sorted(set(round(x, 9) for x in triggers if x < sim_time))
