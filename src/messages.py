from dataclasses import dataclass, asdict
import time


@dataclass
class PrePrepare1:
    """Initial proposal message broadcast by the primary node."""

    rid: int
    proposer: str
    value: str
    ts: float = time.time()

    def to_fields(self):
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass
class InPrepare:
    """Local prepare message sent by each node within its group."""

    rid: int
    group_id: int
    node_id: str
    value: str
    sig: str
    ts: float = time.time()

    def to_fields(self):
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass
class RepAggregate:
    """Aggregated message of valid signatures produced by a group representative."""

    rid: int
    group_id: int
    rep_id: str
    value: str
    valid_sigs: int
    sigs_json: str
    ts: float = time.time()

    def to_fields(self):
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass
class Alert:
    """
    Alert message used in the Node Decision Broadcast Model.
    Broadcasts evidence of faulty or inconsistent representative behavior.
    """

    rid: int
    group_id: int
    node_id: str
    reason: str
    evidence: str
    ts: float = time.time()

    def to_fields(self):
        return {k: str(v) for k, v in asdict(self).items()}
