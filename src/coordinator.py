import asyncio
import time
import math
from collections import defaultdict
import redis.asyncio as redis
from messages import PrePrepare1


class Coordinator:
    """
    Coordinates inter-group consensus for NBFT rounds.

    - Applies Node Decision Broadcast Model (exclude only groups with alerts)
    - Applies Threshold Vote-Counting Model with 'lost-vote' behavior:
      weight = m only if valid_sigs == m; otherwise weight = valid_sigs.
    """

    def __init__(self, cfg, groups, reps, logger):
        self.cfg = cfg
        self.groups = groups
        self.reps = reps
        self.r = redis.from_url(cfg.redis_url)
        self.logger = logger

    async def store_round_config(self, rid, node_ids):
        await self._clean_redis(rid)

        await self.r.hset(
            f"nbft:round:{rid}:config",
            mapping={
                "n": self.cfg.n,
                "m": self.cfg.m,
                "R": self.cfg.R,
                "E": self.cfg.E,
                "omega": self.cfg.omega,
                "view": self.cfg.view_number,
                "prev": self.cfg.previous_hash,
            },
        )
        await self.r.hset(
            f"nbft:groups:{rid}",
            mapping={nid: str(gid) for gid, g in enumerate(self.groups) for nid in g},
        )
        await self.r.hset(
            f"nbft:rep:{rid}", mapping={str(gid): nid for gid, nid in self.reps.items()}
        )

    def group_weight(self, valid_sigs, is_rep=True, msg_valid=True):
        """
        Threshold Vote-Counting Model (NBFT interpretation with clear logging):
        - If a group's valid signatures >= ceil(m - E), it contributes full m votes.
        - Otherwise, it contributes exactly valid_sigs.
        - If msg_valid is False, contribute minimal weight (1).
        """
        m, E = self.cfg.m, self.cfg.E
        if not msg_valid:
            return 1
        threshold = math.ceil(m - E)
        if is_rep:
            if valid_sigs >= threshold:
                self.logger.info(
                    f"[COORD] Retuning group with full weight (valid_sigs={valid_sigs} ≥ threshold={threshold}) -> {m} votes"
                )
                return m
            else:
                self.logger.info(
                    f"[COORD] Group below threshold (valid_sigs={valid_sigs} < threshold={threshold}) -> {valid_sigs} votes"
                )
                return valid_sigs
        else:
            return valid_sigs

    async def _clean_redis(self, rid):

        for gid in range(len(self.groups)):
            await self.r.delete(f"nbft:alerts:{rid}:{gid}")
            await self.r.delete(f"nbft:inprep1:{gid}")
            await self.r.delete(f"nbft:inprep2:{gid}")

        await self.r.delete(f"nbft:rep_votes:{rid}")
        await self.r.delete(f"nbft:decisions:{rid}")
        await self.r.delete("nbft:commit")
        await self.r.delete("nbft:outprepare")
        await self.r.delete("nbft:preprepare1")
        await self.r.delete("nbft:preprepare2")

    async def run_round(self, rid: int, value: str):

        primary = list(self.reps.values())[0]
        pre = PrePrepare1(rid, primary, value)
        await self.r.xadd("nbft:preprepare1", pre.to_fields())

        deadline = time.time() + self.cfg.inprep2_deadline_sec + 0.7
        aggregates = {}
        self.logger.info("[COORD] Waiting for group aggregates...")

        while time.time() < deadline and len(aggregates) < len(self.groups):
            for gid in range(len(self.groups)):
                if gid in aggregates:
                    continue
                resp = await self.r.xrevrange(f"nbft:inprep2:{gid}", count=1)
                if resp:
                    _, fields = resp[0]
                    if int(fields[b"rid"]) == rid:
                        aggregates[gid] = {
                            "rep": fields[b"rep_id"].decode(),
                            "value": fields[b"value"].decode(),
                            "valid_sigs": int(fields[b"valid_sigs"]),
                        }
                        self.logger.info(
                            f"[COORD] Received aggregate from group {gid}: "
                            f"rep={aggregates[gid]['rep']}, value={aggregates[gid]['value']}, "
                            f"valid_sigs={aggregates[gid]['valid_sigs']}"
                        )
            await asyncio.sleep(0.05)

        if not aggregates:
            self.logger.warning("[COORD] No aggregates received before timeout!")

        self.logger.info("\n")

        exclude = set()
        for gid in range(len(self.groups)):
            alerts = await self.r.xrange(f"nbft:alerts:{rid}:{gid}", "-", "+")
            relevant = [
                a
                for a in alerts
                if b"group_id" in a[1] and int(a[1][b"group_id"]) == gid
            ]
            if relevant:
                exclude.add(gid)
                self.logger.warning(
                    f"[COORD] Excluding group {gid} due to {len(relevant)} relevant alerts"
                )

        agreed_groups = []
        group_results = {}

        for gid, agg in aggregates.items():
            if gid in exclude:
                continue
            group_results[gid] = agg["value"]
            if agg["valid_sigs"] >= (2 * self.cfg.E + 1):
                agreed_groups.append(agg["value"])

        counts = defaultdict(int)
        for val in agreed_groups:
            counts[val] += 1

        total_groups = len(self.groups)
        valid_groups = len(agreed_groups)
        threshold_groups = total_groups - self.cfg.omega

        winner, group_votes = max(
            counts.items(), key=lambda kv: kv[1], default=("⊥", 0)
        )
        consensus_reached = group_votes >= threshold_groups

        self.logger.info("\n")
        self.logger.info(f"[COORD] Group results: {group_results}")
        self.logger.info(
            f"{valid_groups}/{total_groups} groups " f"reached internal consensus."
        )
        self.logger.info(
            f"[COORD] Tally: {dict(counts)}, threshold={threshold_groups}, "
            f"winner='{winner}', group_votes={group_votes}"
        )

        if consensus_reached:
            self.logger.info(
                f"[COORD] ✅ Global consensus reached: value='{winner}' "
                f"({group_votes}/{total_groups} groups agreed)."
            )
        else:
            self.logger.warning(
                f"[COORD] ❌ Consensus not reached "
                f"({group_votes}/{total_groups} groups agreed, threshold={threshold_groups})."
            )
