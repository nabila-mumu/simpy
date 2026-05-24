class ElectionMetrics:

    def __init__(self, filename):

        self.filename = filename

        self.elections = []

        with open(self.filename, "w") as f:

            f.write("===================================\n")
            f.write("ELECTION METRICS\n")
            f.write("===================================\n\n")

    def record_election( 
        self,
        election_id,
        initiator,
        leader,
        message_count,
        election_time
    ):

        self.elections.append({
            "election_id": election_id,
            "initiator": initiator,
            "leader": leader,
            "message_count": message_count,
            "election_time": election_time
        })

        with open(self.filename, "a") as f:

            f.write(
                f"Election #{election_id}\n"
            )

            f.write(
                f"Initiator: Node {initiator}\n"
            )

            f.write(
                f"Elected Leader: Node {leader}\n"
            )

            f.write(
                f"Messages Used: {message_count}\n"
            )

            f.write(
                f"Election Time: {election_time}\n"
            )

            f.write(
                "-----------------------------\n"
            )

    def write_summary(self):

        if len(self.elections) == 0:
            return

        avg_msg = (
            sum(e["message_count"] for e in self.elections)
            / len(self.elections)
        )

        avg_time = (
            sum(e["election_time"] for e in self.elections)
            / len(self.elections)
        )

        with open(self.filename, "a") as f:

            f.write("\n===================================\n")
            f.write("FINAL SUMMARY\n")
            f.write("===================================\n")

            f.write(
                f"Total Elections: "
                f"{len(self.elections)}\n"
            )

            f.write(
                f"Average Message Count: "
                f"{avg_msg:.2f}\n"
            )

            f.write(
                f"Average Election Time: "
                f"{avg_time:.2f}\n"
            )