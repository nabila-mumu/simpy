from datetime import datetime


class EventLogger:

    def __init__(self, filename):

        self.filename = filename

        with open(self.filename, "w") as f:

            f.write("===================================\n")
            f.write("SIMULATION LOG\n")
            f.write("===================================\n")
            f.write(f"Started: {datetime.now()}\n\n")

    def log(self, message):

        print(message)

        with open(self.filename, "a") as f:
            f.write(message + "\n")