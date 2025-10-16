import asyncio
import json
import time
import redis.asyncio as redis
from messages import InPrepare, RepAggregate, Alert
from logger import Logger


class Node:
    """
    Represents an NBFT consensus node participating in intra-group consensus.

    Attributes:
        id: Node identifier.
        cfg: Global NBFT configuration.
        group_id: Group to which the node belongs.
        rep_id: Representative primary node of the group.
        honest: Whether the node behaves honestly or exhibits Byzantine behavior.
    """

    def __init__(self, node_id, cfg, group_id, rep_id, honest=True):
        self.id = node_id
        self.cfg = cfg
        self.group_id = group_id
        self.rep_id = rep_id
        self.honest = honest
        self.r = redis.from_url(cfg.redis_url)
        self.logger = Logger.get_logger(str(node_id))

    async def sign(self, payload: dict) -> str:
        """Simulates message signing by returning a deterministic string signature."""
        sig = f"sig:{self.id}:{payload.get('rid', '?')}"
        self.logger.info(f"[NODE {self.id}] Signed payload {payload} -> {sig}")
        return sig

    async def in_prepare1(self, rid, value):
        """Sends an InPrepare message with its vote to the group stream."""
        sig = await self.sign({"rid": rid, "val": value})
        msg = InPrepare(rid, self.group_id, self.id, value, sig)
        await self.r.xadd(f"nbft:inprep1:{self.group_id}", msg.to_fields())
        self.logger.info(
            f"[NODE {self.id}] Broadcasted InPrepare (rid={rid}, value={value}) "
            f"to stream nbft:inprep1:{self.group_id}"
        )

    async def in_prepare2_collect(self, rid, deadline_sec):
        """
        Executes the in-prepare2 aggregation phase for the group's representative node.

        Implements Algorithm 1 (Node Decision Broadcast Model).
        If timeout, inconsistency, or insufficient signatures occur,
        the representative broadcasts alerts to all other groups.
        """
        if self.id != self.rep_id:
            self.logger.debug(
                f"[NODE {self.id}] Skipping aggregation (not representative for group {self.group_id})"
            )
            return None

        self.logger.info(
            f"[REP {self.id}] Starting in_prepare2_collect for round {rid} (group={self.group_id})"
        )

        start = time.time()
        seen = {}

        while time.time() - start < deadline_sec and len(seen) < self.cfg.m:
            resp = await self.r.xread(
                {f"nbft:inprep1:{self.group_id}": "0-0"}, count=50, block=200
            )
            if not resp:
                continue

            for _, msgs in resp:
                for _, fields in msgs:
                    gid = int(fields[b"group_id"])
                    nid = fields[b"node_id"].decode()
                    val = fields[b"value"].decode()

                    if gid != self.group_id:
                        continue
                    if nid in seen:
                        continue

                    seen[nid] = fields
                    self.logger.info(
                        f"[REP {self.id}] Received InPrepare from node {nid}: value={val}"
                    )

        values = {fields[b"value"].decode() for fields in seen.values()}
        consistent = len(values) == 1
        value = list(values)[0] if values else "⊥"
        valid_sigs = len(seen) if consistent else 0

        self.logger.info(
            f"[REP {self.id}] Aggregation complete: {len(seen)} msgs, "
            f"consistent={consistent}, value={value}, valid_sigs={valid_sigs}"
        )

        agg = RepAggregate(
            rid,
            self.group_id,
            self.id,
            value,
            valid_sigs,
            json.dumps(list(seen.keys())),
        )
        await self.r.xadd(f"nbft:inprep2:{self.group_id}", agg.to_fields())
        self.logger.info(
            f"[REP {self.id}] Published RepAggregate -> nbft:inprep2:{self.group_id}"
        )

        reasons = []
        if time.time() - start >= deadline_sec:
            reasons.append("timeout")
        if not consistent:
            reasons.append("mismatch")
        if valid_sigs <= (2 * self.cfg.E + 1):
            reasons.append("weak_sig")

        if reasons:
            self.logger.warning(
                f"[REP {self.id}] Detected anomaly -> {reasons}, valid_sigs={valid_sigs}"
            )

        for reason in reasons:
            alert = Alert(
                rid, self.group_id, self.id, reason, f"valid_sigs={valid_sigs}"
            )
            await self.r.xadd(f"nbft:alerts:{rid}", alert.to_fields())
            self.logger.warning(
                f"[REP {self.id}] Broadcasted ALERT ({reason}) to nbft:alerts:{rid}"
            )

        self.logger.info(
            f"[REP {self.id}] Finished in_prepare2_collect (group={self.group_id})"
        )
        return agg
