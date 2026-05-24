import random


class Network:

    def __init__(
        self,
        env,
        logger,
        min_latency,
        max_latency
    ):

        self.env = env
        self.logger = logger

        self.min_latency = min_latency
        self.max_latency = max_latency

    def send(
        self,
        sender,
        receiver,
        message
    ):

        latency = random.randint(
            self.min_latency,
            self.max_latency
        )

        self.logger.log(
            f"[{self.env.now}] "
            f"Node {sender.node_id} -> "
            f"Node {receiver.node_id} "
            f"{message}"
        )

        return self.env.process(
            self.deliver(
                receiver,
                message,
                latency
            )
        )

    def deliver(
        self,
        receiver,
        message,
        latency
    ):

        yield self.env.timeout(latency)

        if receiver.is_alive:

            yield receiver.inbox.put(message)