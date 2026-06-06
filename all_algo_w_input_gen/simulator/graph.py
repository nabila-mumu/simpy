from __future__ import annotations

import random
from collections import deque
from typing import Dict, List, Optional

Graph = Dict[int, List[int]]


def add_edge(g: Graph, u: int, v: int) -> None:
    if v not in g[u]:
        g[u].append(v)
    if u not in g[v]:
        g[v].append(u)


def has_edge(g: Graph, u: int, v: int) -> bool:
    return v in g[u]


def random_connected_graph(num_nodes: int, num_edges: int, seed: int | None = None) -> Graph:
    """Generate a connected undirected simple graph with num_nodes and num_edges."""
    if num_nodes < 2:
        raise ValueError("num_nodes must be at least 2")

    max_edges = num_nodes * (num_nodes - 1) // 2
    min_edges = num_nodes - 1
    if num_edges < min_edges or num_edges > max_edges:
        raise ValueError(f"num_edges must be between {min_edges} and {max_edges} for n={num_nodes}")

    rng = random.Random(seed)
    g: Graph = {i: [] for i in range(num_nodes)}

    # Random spanning tree first, ensuring connectivity.
    nodes = list(range(num_nodes))
    rng.shuffle(nodes)
    for i in range(1, num_nodes):
        u = nodes[i]
        v = nodes[rng.randrange(0, i)]
        add_edge(g, u, v)

    current_edges = min_edges
    possible = [(i, j) for i in range(num_nodes) for j in range(i + 1, num_nodes) if not has_edge(g, i, j)]
    rng.shuffle(possible)

    while current_edges < num_edges:
        u, v = possible.pop()
        add_edge(g, u, v)
        current_edges += 1

    for u in g:
        g[u].sort()
    return g


def graph_density(num_nodes: int, num_edges: int) -> float:
    if num_nodes <= 1:
        return 0.0
    return (2.0 * num_edges) / (num_nodes * (num_nodes - 1))


def avg_degree(num_nodes: int, num_edges: int) -> float:
    return (2.0 * num_edges) / num_nodes if num_nodes else 0.0


def connected_components(g: Graph) -> int:
    seen = set()
    comp = 0
    for s in g:
        if s in seen:
            continue
        comp += 1
        q = deque([s])
        seen.add(s)
        while q:
            u = q.popleft()
            for v in g[u]:
                if v not in seen:
                    seen.add(v)
                    q.append(v)
    return comp


def shortest_path_length(g: Graph, src: int, dst: int) -> int | None:
    if src == dst:
        return 0
    q = deque([(src, 0)])
    seen = {src}
    while q:
        u, d = q.popleft()
        for v in g[u]:
            if v == dst:
                return d + 1
            if v not in seen:
                seen.add(v)
                q.append((v, d + 1))
    return None


def find_hamiltonian_cycle(g: Graph, seed: int | None = None, max_expansions: int = 100_000) -> Optional[List[int]]:
    """
    Bounded backtracking Hamiltonian cycle finder.

    Returns cycle [v0, v1, ..., v(n-1)] if consecutive nodes and last-first are edges.
    Returns None if no cycle is found within max_expansions.

    This is intentionally exact/bounded instead of using a fake logical ring.
    Sparse graphs may be skipped if no Hamiltonian cycle is found.
    """
    rng = random.Random(seed)
    n = len(g)
    if n < 3:
        return None
    if any(len(g[u]) < 2 for u in g):
        return None

    nodes = list(g.keys())
    starts = nodes[:]
    rng.shuffle(starts)
    expansions = 0

    for start in starts:
        path = [start]
        used = {start}
        neighbor_order = {u: list(g[u]) for u in nodes}
        for u in nodes:
            rng.shuffle(neighbor_order[u])

        def dfs(u: int) -> Optional[List[int]]:
            nonlocal expansions
            expansions += 1
            if expansions > max_expansions:
                return None
            if len(path) == n:
                return path.copy() if start in g[u] else None

            # Low-degree-first helps close cycles in sparse graphs.
            candidates = [v for v in neighbor_order[u] if v not in used]
            candidates.sort(key=lambda x: len(g[x]))
            for v in candidates:
                used.add(v)
                path.append(v)
                ans = dfs(v)
                if ans is not None:
                    return ans
                path.pop()
                used.remove(v)
            return None

        ans = dfs(start)
        if ans is not None:
            return ans
        if expansions > max_expansions:
            return None

    return None
