from __future__ import annotations

from dataclasses import dataclass, asdict

from simulator.algorithms.bidir_ring import BidirRingAlgorithm
from simulator.algorithms.bully import BullyAlgorithm
from simulator.algorithms.ring import RingAlgorithm
from simulator.algorithms.raft import RaftElectionAlgorithm
from simulator.algorithms.echo import EchoWaveElectionAlgorithm
from simulator.failures import generate_failure_schedule
from simulator.graph import (
    avg_degree,
    connected_components,
    find_hamiltonian_cycle,
    graph_density,
    random_connected_graph,
)
from simulator.network import NetworkConfig


@dataclass
class ScenarioInput:
    num_nodes: int
    num_edges: int
    bandwidth: float
    latency: float
    failure_rate: float
    loss_rate: float


@dataclass
class ScenarioOutput:
    num_nodes: int
    num_edges: int
    graph_density: float
    avg_degree: float
    num_connected_component: int
    bandwidth: float
    latency: float
    failure_rate: float
    loss_rate: float
    hamiltonian_cycle_found: int

    bully_time: float
    bully_msg_count: float
    bully_elections: int
    bully_failed_attempts: int

    ring_time: float
    ring_msg_count: float
    ring_elections: int
    ring_failed_attempts: int

    bidir_ring_time: float
    bidir_ring_msg_count: float
    bidir_ring_elections: int
    bidir_ring_failed_attempts: int

    # Placeholders for later steps. Keep columns now so the final CSV shape is stable.
    raft_time: float
    raft_msg_count: float
    raft_elections: int
    raft_failed_attempts: int

    echo_time: float
    echo_msg_count: float
    echo_elections: int
    echo_failed_attempts: int

    def to_dict(self):
        return asdict(self)


def _scenario_seeds(inp: ScenarioInput, seed: int) -> dict[str, int]:
    base = (
        seed
        + inp.num_nodes * 1009
        + inp.num_edges * 917
        + int(inp.bandwidth * 101)
        + int(inp.latency * 103)
        + int(inp.failure_rate * 1_000_000) * 107
        + int(inp.loss_rate * 1_000_000) * 109
    )
    return {
        "graph": base + 11,
        "cycle": base + 13,
        "failure": base + 17,
        "bully": base + 19,
        "ring": base + 23,
        "bidir": base + 29,
        "raft": base + 31,
        "echo": base + 37,
    }


def run_bully_scenario(inp: ScenarioInput, sim_time: float = 1000.0, seed: int = 42) -> ScenarioOutput:
    """Backward-compatible wrapper: runs all current algorithms but you can read Bully columns only."""
    return run_current_scenario(inp, sim_time=sim_time, seed=seed)


def run_current_scenario(inp: ScenarioInput, sim_time: float = 1000.0, seed: int = 42) -> ScenarioOutput:
    """
    Generate one graph from input, one shared failure schedule, and run:
        - Bully
        - Ring, only if a Hamiltonian cycle is found
        - Bidirectional Ring, only if the same Hamiltonian cycle is found
        - Raft leader election only
        - Echo-wave based leader election

    This makes Bully/Ring/Bidir/Raft/Echo comparable for the same scenario.
    """
    seeds = _scenario_seeds(inp, seed)

    g = random_connected_graph(inp.num_nodes, inp.num_edges, seed=seeds["graph"])
    cycle = find_hamiltonian_cycle(g, seed=seeds["cycle"])
    schedule = generate_failure_schedule(inp.num_nodes, sim_time, inp.failure_rate, seed=seeds["failure"])
    network = NetworkConfig(
        bandwidth_mbps=inp.bandwidth,
        latency_ms=inp.latency,
        loss_rate=inp.loss_rate,
    )

    bully = BullyAlgorithm(
        graph=g,
        network=network,
        failure_schedule=schedule,
        sim_time=sim_time,
        seed=seeds["bully"],
    ).run()

    if cycle is not None:
        ring = RingAlgorithm(
            cycle=cycle,
            network=network,
            failure_schedule=schedule,
            sim_time=sim_time,
            seed=seeds["ring"],
        ).run()

        bidir = BidirRingAlgorithm(
            cycle=cycle,
            network=network,
            failure_schedule=schedule,
            sim_time=sim_time,
            seed=seeds["bidir"],
        ).run()

        hamiltonian_cycle_found = 1
    else:
        # Required by your research choice: no logical ring from node ID.
        # Therefore if no Hamiltonian cycle exists, Ring/Bidir are not run.
        ring = None
        bidir = None
        hamiltonian_cycle_found = 0

    raft = RaftElectionAlgorithm(
        graph=g,
        network=network,
        failure_schedule=schedule,
        sim_time=sim_time,
        seed=seeds["raft"],
    ).run()

    echo = EchoWaveElectionAlgorithm(
        graph=g,
        network=network,
        failure_schedule=schedule,
        sim_time=sim_time,
        seed=seeds["echo"],
    ).run()

    return ScenarioOutput(
        num_nodes=inp.num_nodes,
        num_edges=inp.num_edges,
        graph_density=graph_density(inp.num_nodes, inp.num_edges),
        avg_degree=avg_degree(inp.num_nodes, inp.num_edges),
        num_connected_component=connected_components(g),
        bandwidth=inp.bandwidth,
        latency=inp.latency,
        failure_rate=inp.failure_rate,
        loss_rate=inp.loss_rate,
        hamiltonian_cycle_found=hamiltonian_cycle_found,

        bully_time=bully.avg_time,
        bully_msg_count=bully.avg_msg_count,
        bully_elections=bully.completed_elections,
        bully_failed_attempts=bully.failed_attempts,

        ring_time=ring.avg_time if ring else 0.0,
        ring_msg_count=ring.avg_msg_count if ring else 0.0,
        ring_elections=ring.completed_elections if ring else 0,
        ring_failed_attempts=ring.failed_attempts if ring else 0,

        bidir_ring_time=bidir.avg_time if bidir else 0.0,
        bidir_ring_msg_count=bidir.avg_msg_count if bidir else 0.0,
        bidir_ring_elections=bidir.completed_elections if bidir else 0,
        bidir_ring_failed_attempts=bidir.failed_attempts if bidir else 0,

        raft_time=raft.avg_time,
        raft_msg_count=raft.avg_msg_count,
        raft_elections=raft.completed_elections,
        raft_failed_attempts=raft.failed_attempts,

        echo_time=echo.avg_time,
        echo_msg_count=echo.avg_msg_count,
        echo_elections=echo.completed_elections,
        echo_failed_attempts=echo.failed_attempts,
    )
