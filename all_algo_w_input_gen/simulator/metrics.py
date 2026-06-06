from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ElectionMetrics:
    election_times: list[float] = field(default_factory=list)
    message_counts: list[int] = field(default_factory=list)
    failed_attempts: int = 0

    def record_success(self, election_time: float, msg_count: int) -> None:
        self.election_times.append(election_time)
        self.message_counts.append(msg_count)

    def record_failure(self) -> None:
        self.failed_attempts += 1

    @property
    def completed_elections(self) -> int:
        return len(self.election_times)

    @property
    def avg_time(self) -> float:
        return sum(self.election_times) / len(self.election_times) if self.election_times else 0.0

    @property
    def avg_msg_count(self) -> float:
        return sum(self.message_counts) / len(self.message_counts) if self.message_counts else 0.0
