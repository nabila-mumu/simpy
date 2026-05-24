def create_ring_topology(nodes):

    n = len(nodes)

    for i in range(n):

        next_index = (i + 1) % n

        nodes[i].neighbors.append(
            nodes[next_index]
        )