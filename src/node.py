import asyncio, json, random, time
import redis.asyncio as redis
from messages import InPrepare, RepAggregate, Alert
from logger import Logger


class Node:
    def __init__(self, node_id, cfg, group_id, rep_id, honest=True):
        self.id = node_id
        self.cfg = cfg
        self.group_id = group_id
        self.rep_id = rep_id
        self.honest = honest
        self.r = redis.from_url(cfg.redis_url)
        self.logger = Logger.get_logger(str(node_id))

    async def sign(self, payload: dict) -> str:
        sig = f"sig:{self.id}:{payload.get('rid', '?')}"
        self.logger.info(
            f"[SIGN | GROUP {self.group_id}] Signed payload {payload} -> {sig}"
        )
        return sig

    async def in_prepare1(self, rid, value):
        stage = "IN_PREPARE1"
        if not self.honest:
            # symulate malicioius attempt
            value = f"BLOCK_FAKE_{self.id}"
        sig = await self.sign({"rid": rid, "val": value})
        msg = InPrepare(rid, self.group_id, self.id, value, sig)
        await self.r.xadd(f"nbft:inprep1:{self.group_id}", msg.to_fields())
        self.logger.info(
            f"[{stage} | GROUP {self.group_id}] Sent InPrepare (value={value})"
        )

    async def in_prepare2_collect(self, rid, deadline_sec):
        stage = "IN_PREPARE2"
        if self.id != self.rep_id:
            return None

        self.logger.info(
            f"[{stage} | GROUP {self.group_id} | REPRESENTATIVE] Aggregating messages..."
        )
        start = time.time()
        seen = {}
        last_id = "0-0"

        while time.time() - start < deadline_sec and len(seen) < self.cfg.m:
            resp = await self.r.xread(
                {f"nbft:inprep1:{self.group_id}": last_id}, count=50, block=200
            )
            if not resp:
                continue
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    last_id = msg_id
                    gid = int(fields[b"group_id"])
                    nid = fields[b"node_id"].decode()
                    val = fields[b"value"].decode()
                    if gid != self.group_id or nid in seen:
                        continue
                    seen[nid] = fields
                    self.logger.info(
                        f"[{stage} | GROUP {self.group_id} | REPRESENTATIVE] Received from {nid}: {val}"
                    )

        E = self.cfg.E
        twoEplus1 = 2 * E + 1

        counts = {}
        for f in seen.values():
            v = f[b"value"].decode()
            counts[v] = counts.get(v, 0) + 1

        if counts:
            majority_value, top_count = max(counts.items(), key=lambda kv: kv[1])
        else:
            majority_value, top_count = "⊥", 0

        has_quorum = top_count >= twoEplus1
        value = majority_value if has_quorum else "⊥"
        valid_sigs = top_count if has_quorum else 0

        if not self.honest and random.random() < 0.0:
            pass

        agg = RepAggregate(
            rid,
            self.group_id,
            self.id,
            value,
            valid_sigs,
            json.dumps(list(seen.keys())),
        )
        await self.r.xadd(f"nbft:inprep2:{self.group_id}", agg.to_fields())

        reasons = []
        if time.time() - start >= deadline_sec:
            reasons.append("timeout")
        if not has_quorum:
            if len(counts) > 1:
                reasons.append("mismatch")
            reasons.append("weak_sig")

        for reason in reasons:
            alert = Alert(
                rid,
                self.group_id,
                self.id,
                reason,
                f"valid_sigs={valid_sigs}, rep={self.rep_id}",
            )
            await self.r.xadd(f"nbft:alerts:{rid}:{self.group_id}", alert.to_fields())
            self.logger.warning(
                f"[{stage} | GROUP {self.group_id}] ALERT broadcasted ({reason})."
            )

        return agg
