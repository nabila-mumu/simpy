# import simpy


# class Node:

#     def __init__(
#         self,
#         env,
#         node_id,
#         network,
#         logger,
#         metrics
#     ):

#         self.env = env

#         self.node_id = node_id

#         self.network = network

#         self.logger = logger

#         self.metrics = metrics

#         self.inbox = simpy.Store(env)

#         self.neighbors = []

#         self.is_alive = True

#         self.is_leader = False

#         self.current_leader = None

#         self.election_running = False

#         # Track elections globally
#         self.active_elections = {}

#         self.env.process(self.run())

#     # =====================================================
#     # Failure / Recovery
#     # =====================================================
#     def fail(self):

#         self.is_alive = False

#         self.is_leader = False

#         self.logger.log(
#             f"[{self.env.now}] "
#             f"NODE {self.node_id} FAILED"
#         )

#     def recover(self):

#         self.is_alive = True

#         self.logger.log(
#             f"[{self.env.now}] "
#             f"NODE {self.node_id} RECOVERED"
#         )

#     # =====================================================
#     # Ring Neighbor
#     # =====================================================
#     def get_next_alive_neighbor(self):

#         visited = set()

#         current = self

#         while True:

#             neighbor = current.neighbors[0]

#             if neighbor.node_id in visited:
#                 return None

#             visited.add(neighbor.node_id)

#             if neighbor.is_alive:
#                 return neighbor

#             current = neighbor

#     # =====================================================
#     # Start Election
#     # =====================================================
#     def start_election(self):

#         if not self.is_alive:
#             return

#         if self.election_running:
#             return

#         self.election_running = True

#         election_id = (
#             int(self.env.now * 1000)
#             + self.node_id
#         )

#         # Store metrics for this election
#         self.active_elections[election_id] = {
#             "start_time": self.env.now,
#             "message_count": 0,
#             "initiator": self.node_id
#         }

#         self.logger.log(
#             f"[{self.env.now}] "
#             f"Node {self.node_id} "
#             f"STARTED ELECTION #{election_id}"
#         )

#         msg = {
#             "type": "ELECTION",
#             "candidate": self.node_id,
#             "initiator": self.node_id,
#             "election_id": election_id
#         }

#         next_node = self.get_next_alive_neighbor()

#         if next_node:

#             self.active_elections[election_id][
#                 "message_count"
#             ] += 1

#             self.network.send(
#                 self,
#                 next_node,
#                 msg
#             )

#     # =====================================================
#     # Main Process
#     # =====================================================
#     def run(self):

#         while True:

#             msg = yield self.inbox.get()

#             if not self.is_alive:
#                 continue

#             if msg["type"] == "ELECTION":

#                 # yield self.env.process(
#                 self.handle_election(msg)
#                 # )

#             elif msg["type"] == "COORDINATOR":

#                 # yield self.env.process(
#                 self.handle_coordinator(msg)
#                 # )

#     # =====================================================
#     # Handle Election
#     # =====================================================
#     def handle_election(self, msg):

#         candidate = msg["candidate"]

#         initiator = msg["initiator"]

#         election_id = msg["election_id"]

#         highest = max(
#             candidate,
#             self.node_id
#         )

#         # Election completed
#         if initiator == self.node_id:

#             self.is_leader = True

#             self.current_leader = highest

#             election_data = self.active_elections[
#                 election_id
#             ]

#             election_time = (
#                 self.env.now
#                 - election_data["start_time"]
#             )

#             message_count = election_data[
#                 "message_count"
#             ]

#             self.logger.log(
#                 f"[{self.env.now}] "
#                 f"LEADER ELECTED: "
#                 f"Node {highest}"
#             )

#             self.metrics.record_election(
#                 election_id=election_id,
#                 initiator=self.node_id,
#                 leader=highest,
#                 message_count=message_count,
#                 election_time=election_time
#             )

#             # Send coordinator message
#             coord_msg = {
#                 "type": "COORDINATOR",
#                 "leader": highest,
#                 "initiator": self.node_id,
#                 "election_id": election_id
#             }

#             next_node = self.get_next_alive_neighbor()

#             if next_node:

#                 self.network.send(
#                     self,
#                     next_node,
#                     coord_msg
#                 )

#             return

#         # Forward election message
#         forward_msg = {
#             "type": "ELECTION",
#             "candidate": highest,
#             "initiator": initiator,
#             "election_id": election_id
#         }

#         next_node = self.get_next_alive_neighbor()

#         if next_node:

#             # Count message ONLY at initiator
#             initiator_node = self.find_node(
#                 initiator
#             )

#             if (
#                 initiator_node
#                 and election_id
#                 in initiator_node.active_elections
#             ):

#                 initiator_node.active_elections[
#                     election_id
#                 ]["message_count"] += 1

#             self.network.send(
#                 self,
#                 next_node,
#                 forward_msg
#             )

#     # =====================================================
#     # Handle Coordinator
#     # =====================================================
#     def handle_coordinator(self, msg):

#         leader = msg["leader"]

#         initiator = msg["initiator"]

#         election_id = msg["election_id"]

#         self.current_leader = leader

#         self.is_leader = (
#             self.node_id == leader
#         )

#         self.election_running = False

#         self.logger.log(
#             f"[{self.env.now}] "
#             f"Node {self.node_id} "
#             f"ACKNOWLEDGES LEADER "
#             f"{leader}"
#         )

#         # Coordinator completed full ring
#         if initiator == self.node_id:

#             if (
#                 election_id
#                 in self.active_elections
#             ):

#                 del self.active_elections[
#                     election_id
#                 ]

#             return

#         next_node = self.get_next_alive_neighbor()

#         if next_node:

#             self.network.send(
#                 self,
#                 next_node,
#                 msg
#             )

#     # =====================================================
#     # Utility
#     # =====================================================
#     def find_node(self, node_id):

#         visited = set()

#         current = self

#         while True:

#             if current.node_id == node_id:
#                 return current

#             visited.add(current.node_id)

#             next_node = current.neighbors[0]

#             if next_node.node_id in visited:
#                 return None

#             current = next_node

import simpy


class Node:

    def __init__(
        self,
        env,
        node_id,
        network,
        logger,
        metrics,
        controller
    ):
        self.env = env
        self.node_id = node_id
        self.network = network
        self.logger = logger
        self.metrics = metrics
        self.controller = controller

        self.inbox = simpy.Store(env)
        self.neighbors = []

        self.is_alive = True
        self.current_leader = None

        self.env.process(self.run())

    def fail(self):
        self.is_alive = False

        self.logger.log(
            f"[{self.env.now}] NODE {self.node_id} FAILED"
        )

    def recover(self):
        self.is_alive = True

        self.logger.log(
            f"[{self.env.now}] NODE {self.node_id} RECOVERED"
        )

    def get_next_alive_neighbor(self):
        visited = set()
        current = self

        while True:
            if not current.neighbors:
                return None

            neighbor = current.neighbors[0]

            if neighbor.node_id in visited:
                return None

            visited.add(neighbor.node_id)

            if neighbor.is_alive:
                return neighbor

            current = neighbor

    def find_node(self, node_id):

        visited = set()
        current = self

        while True:

            if current.node_id == node_id:
                return current

            visited.add(current.node_id)

            if not current.neighbors:
                return None

            next_node = current.neighbors[0]

            if next_node.node_id in visited:
                return None

            current = next_node    

    def start_election(self):
        if not self.is_alive:
            return False

        if self.controller.election_in_progress:
            self.logger.log(
                f"[{self.env.now}] Election request ignored "
                f"because another election is running"
            )
            return False

        election_id = self.controller.start_new_election(
            self.env,
            self.node_id
        )

        self.logger.log(
            f"[{self.env.now}] Node {self.node_id} "
            f"STARTED ELECTION #{election_id}"
        )

        msg = {
            "type": "ELECTION",
            "candidate": self.node_id,
            "initiator": self.node_id,
            "election_id": election_id
        }

        next_node = self.get_next_alive_neighbor()

        if next_node is None:
            self.controller.current_leader = self.node_id
            self.controller.reset_election()
            return True

        self.controller.count_message()

        self.network.send(
            self,
            next_node,
            msg
        )

        return True

    def run(self):
        while True:
            msg = yield self.inbox.get()

            if not self.is_alive:
                continue

            if msg["type"] == "ELECTION":
                self.handle_election(msg)

            elif msg["type"] == "COORDINATOR":
                self.handle_coordinator(msg)

    def handle_election(self, msg):
        candidate = msg["candidate"]
        election_id = msg["election_id"]

        if (
            self.controller.active_election is None
            or election_id != self.controller.active_election["election_id"]
        ):
            return
        
        candidate_node = self.find_node(candidate)
        if candidate_node is None or not candidate_node.is_alive:
            candidate = self.node_id

        if candidate == self.node_id:
            self.controller.current_leader = self.node_id
            self.current_leader = self.node_id

            election_data = self.controller.active_election

            election_time = (
                self.env.now - election_data["start_time"]
            )

            message_count = election_data["message_count"]

            self.logger.log(
                f"[{self.env.now}] LEADER ELECTED: Node {self.node_id}"
            )

            self.metrics.record_election(
                election_id=election_id,
                initiator=election_data["initiator"],
                leader=self.node_id,
                message_count=message_count,
                election_time=election_time
            )

            coord_msg = {
                "type": "COORDINATOR",
                "leader": self.node_id,
                "initiator": self.node_id,
                "election_id": election_id
            }

            next_node = self.get_next_alive_neighbor()

            if next_node:
                self.controller.count_message()

                self.network.send(
                    self,
                    next_node,
                    coord_msg
                )
            else:
                self.controller.reset_election()

            return

        if candidate < self.node_id:
            candidate = self.node_id

        forward_msg = {
            "type": "ELECTION",
            "candidate": candidate,
            "initiator": msg["initiator"],
            "election_id": election_id
        }

        next_node = self.get_next_alive_neighbor()

        if next_node:
            self.controller.count_message()

            self.network.send(
                self,
                next_node,
                forward_msg
            )

    def handle_coordinator(self, msg):
        leader = msg["leader"]
        initiator = msg["initiator"]
        election_id = msg["election_id"]

        if (
            self.controller.active_election is None
            or election_id != self.controller.active_election["election_id"]
        ):
            return

        self.current_leader = leader

        self.logger.log(
            f"[{self.env.now}] Node {self.node_id} "
            f"ACKNOWLEDGES LEADER {leader}"
        )

        if self.node_id == initiator:
            self.controller.reset_election()
            return

        next_node = self.get_next_alive_neighbor()

        if next_node:
            self.controller.count_message()

            self.network.send(
                self,
                next_node,
                msg
            )