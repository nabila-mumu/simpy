# import simpy
# import random
# import os

# from logger import EventLogger
# from network import Network
# from node import Node
# from topology import create_ring_topology
# from metrics import ElectionMetrics


# # =========================================================
# # Failure Manager
# # =========================================================
# def failure_manager(
#     env,
#     nodes,
#     logger,
#     failure_probability,
#     recovery_probability,
#     check_interval
# ):

#     while True:

#         yield env.timeout(check_interval)

#         for node in nodes:

#             if node.is_alive:

#                 if random.random() < failure_probability:

#                     was_leader = node.is_leader

#                     node.fail()

#                     # Trigger election
#                     if was_leader:

#                         alive_nodes = [
#                             n for n in nodes
#                             if n.is_alive
#                         ]

#                         if alive_nodes:

#                             starter = random.choice(
#                                 alive_nodes
#                             )

#                             starter.start_election()

#             else:

#                 if random.random() < recovery_probability:

#                     node.recover()


# # =========================================================
# # Main
# # =========================================================
# def main():

#     random.seed(42)

#     # Create logs directory
#     os.makedirs("logs", exist_ok=True)

#     SIMULATION_TIME = 200

#     NUM_NODES = 6

#     MIN_LATENCY = 1
#     MAX_LATENCY = 4

#     FAILURE_PROBABILITY = 0.15

#     RECOVERY_PROBABILITY = 0.30

#     FAILURE_CHECK_INTERVAL = 5

#     env = simpy.Environment()

#     logger = EventLogger(
#         "logs/event_log.txt"
#     )

#     metrics = ElectionMetrics(
#         "logs/election_metrics.txt"
#     )

#     network = Network(
#         env,
#         logger,
#         MIN_LATENCY,
#         MAX_LATENCY
#     )

#     # Create nodes
#     nodes = []

#     for i in range(NUM_NODES):

#         node = Node(
#             env,
#             i,
#             network,
#             logger,
#             metrics
#         )

#         nodes.append(node)

#     # Create ring
#     create_ring_topology(nodes)

#     # Initial leader election
#     nodes[0].start_election()

#     # Start failure manager
#     env.process(
#         failure_manager(
#             env,
#             nodes,
#             logger,
#             FAILURE_PROBABILITY,
#             RECOVERY_PROBABILITY,
#             FAILURE_CHECK_INTERVAL
#         )
#     )

#     logger.log("\nStarting Simulation...\n")

#     env.run(until=SIMULATION_TIME)

#     metrics.write_summary()

#     logger.log(
#         "\nSimulation Finished."
#     )


# if __name__ == "__main__":
#     main()

import simpy
import random
import os

from logger import EventLogger
from network import Network
from node import Node
from topology import create_ring_topology
from metrics import ElectionMetrics
from controller import SimulationController


def choose_alive_node(nodes):
    alive_nodes = [
        node for node in nodes
        if node.is_alive
    ]

    if not alive_nodes:
        return None

    return random.choice(alive_nodes)


def failure_manager(
    env,
    nodes,
    logger,
    controller,
    failure_probability,
    recovery_probability,
    check_interval
):

    while True:
        yield env.timeout(check_interval)

        leader_failed = False
        recovered_higher_node = None

        for node in nodes:

            if node.is_alive:

                if random.random() < failure_probability:

                    node.fail()

                    if controller.current_leader == node.node_id:

                        leader_failed = True

                        logger.log(
                            f"[{env.now}] CURRENT LEADER Node {node.node_id} FAILED"
                        )

                        controller.current_leader = None

                        # Important fix:
                        # clear stuck election/coordinator state
                        controller.reset_election()

            else:

                if random.random() < recovery_probability:

                    node.recover()

                    if controller.current_leader is not None:

                        if node.node_id > controller.current_leader:
                            recovered_higher_node = node

        # Fix 2: timeout reset for stuck election
        if (
            controller.election_in_progress
            and controller.active_election is not None
        ):

            election_age = (
                env.now
                - controller.active_election["start_time"]
            )

            if election_age > controller.election_timeout:

                logger.log(
                    f"[{env.now}] "
                    f"Election timed out "
                    f"-> resetting election state"
                )

                controller.reset_election()

                starter = choose_alive_node(nodes)

                if starter is not None:

                    logger.log(
                        f"[{env.now}] "
                        f"Timeout recovery -> "
                        f"Node {starter.node_id} STARTS NEW ELECTION"
                    )

                    starter.start_election()

                continue

        if leader_failed:

            starter = choose_alive_node(nodes)

            if starter is not None:

                logger.log(
                    f"[{env.now}] LEADER FAILED -> "
                    f"Node {starter.node_id} STARTS NEW ELECTION"
                )

                starter.start_election()

        elif recovered_higher_node is not None:

            if not controller.election_in_progress:

                logger.log(
                    f"[{env.now}] RECOVERED Node {recovered_higher_node.node_id} "
                    f"HAS HIGHER ID THAN CURRENT LEADER "
                    f"-> STARTING REELECTION"
                )

                recovered_higher_node.start_election()


def main():

    random.seed(42)

    os.makedirs("logs", exist_ok=True)

    SIMULATION_TIME = 200
    NUM_NODES = 6

    MIN_LATENCY = 1
    MAX_LATENCY = 4

    FAILURE_PROBABILITY = 0.15
    RECOVERY_PROBABILITY = 0.30
    FAILURE_CHECK_INTERVAL = 5

    env = simpy.Environment()

    logger = EventLogger(
        "logs/event_log.txt"
    )

    metrics = ElectionMetrics(
        "logs/election_metrics.txt"
    )

    controller = SimulationController()

    network = Network(
        env,
        logger,
        MIN_LATENCY,
        MAX_LATENCY
    )

    nodes = []

    for i in range(NUM_NODES):

        node = Node(
            env,
            i,
            network,
            logger,
            metrics,
            controller
        )

        nodes.append(node)

    create_ring_topology(nodes)

    nodes[0].start_election()

    env.process(
        failure_manager(
            env,
            nodes,
            logger,
            controller,
            FAILURE_PROBABILITY,
            RECOVERY_PROBABILITY,
            FAILURE_CHECK_INTERVAL
        )
    )

    logger.log("\nStarting Simulation...\n")

    env.run(until=SIMULATION_TIME)

    metrics.write_summary()

    logger.log("\nSimulation Finished.")


if __name__ == "__main__":
    main()