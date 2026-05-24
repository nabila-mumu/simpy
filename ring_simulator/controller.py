class SimulationController:

    def __init__(self):
        self.current_leader = None
        self.election_in_progress = False
        self.election_counter = 0
        self.active_election = None
        self.election_start_time = None
        self.election_timeout = 30

    def start_new_election(self, env, initiator_id):
        self.election_in_progress = True
        self.election_counter += 1

        self.active_election = {
            "election_id": self.election_counter,
            "initiator": initiator_id,
            "start_time": env.now,
            "message_count": 0
        }

        return self.election_counter

    def count_message(self):
        if self.active_election is not None:
            self.active_election["message_count"] += 1

    def reset_election(self):
        self.election_in_progress = False
        self.active_election = None
