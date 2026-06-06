from __future__ import annotations

import random
from dataclasses import dataclass

MESSAGE_SIZE_BYTES = {
    "BULLY_ELECTION": 128,
    "BULLY_OK": 64,
    "BULLY_COORDINATOR": 128,
    "RING_ELECTION": 128,
    "RING_COORDINATOR": 128,
    "RAFT_REQUEST_VOTE": 160,
    "RAFT_VOTE_RESPONSE": 96,
    "ECHO_PROBE": 128,
    "ECHO_REPLY": 128,
    "ECHO_COORDINATOR": 128,
}


@dataclass(frozen=True)
class NetworkConfig:
    bandwidth_mbps: float
    latency_ms: float
    loss_rate: float

    def message_delay(self, msg_type: str, rng: random.Random, hops: int = 1) -> float:
        """
        End-to-end message delay across a path.
        Real-life impact:
        - higher latency increases propagation delay
        - lower bandwidth increases serialization delay
        - more hops increase total delay
        """
        size_bytes = MESSAGE_SIZE_BYTES.get(msg_type, 128)
        latency_s = self.latency_ms / 1000.0
        bandwidth_bps = max(self.bandwidth_mbps * 1_000_000.0, 1.0)
        serialization_s = (size_bytes * 8) / bandwidth_bps
        jitter_s = rng.uniform(0.0, 0.15 * latency_s)
        return hops * (latency_s + serialization_s + jitter_s)

    def delivered(self, rng: random.Random, hops: int = 1) -> bool:
        """
        If message crosses multiple hops, each hop may lose the message.
        Effective delivery probability = (1-loss_rate)^hops.
        """
        if self.loss_rate <= 0:
            return True
        return rng.random() < ((1.0 - self.loss_rate) ** max(hops, 1))

    def election_timeout(self) -> float:
        """
        Timeout grows with latency and with poorer bandwidth.
        This is used for failure detection and retry spacing.
        """
        return 0.20 + 4.0 * (self.latency_ms / 1000.0) + 0.02 / max(self.bandwidth_mbps, 1.0)
