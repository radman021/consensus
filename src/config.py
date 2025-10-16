from dataclasses import dataclass
import math


@dataclass
class NBFTConfig:
    """
    Configuration object defining NBFT network parameters and constants.

    Attributes:
        n: Total number of nodes in the network.
        m: Number of nodes in each consensus group.
        view_number: Current view number used for leader rotation and hashing.
        previous_hash: Hash of the last confirmed block.
        master_ip: IP address of the last round's primary node.
        round_timeout_sec: Global timeout for a consensus round.
        inprep2_deadline_sec: Timeout for the in-prepare2 aggregation phase.
    Derived attributes:
        E: Maximum Byzantine nodes tolerated per group.
        R: Number of groups in the network.
        omega: Maximum number of abnormal groups tolerated in the network.
        redis_url: Redis connection string.
    """

    n: int
    m: int
    view_number: int = 0
    previous_hash: str = "genesis"
    master_ip: str = "10.0.0.1"
    round_timeout_sec: float = 2.0
    inprep2_deadline_sec: float = 1.0

    @property
    def E(self) -> int:
        return (self.m - 1) // 3

    @property
    def R(self) -> int:
        return math.ceil(self.n / self.m)

    @property
    def omega(self) -> int:
        return max(0, (self.R - 1) // 3)

    @property
    def redis_url(self) -> str:
        return "redis://localhost:6379/0"
