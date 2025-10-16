import asyncio
import time
from collections import defaultdict
import redis.asyncio as redis
from messages import PrePrepare1


class Coordinator:
    """
    Coordinates inter-group consensus for NBFT rounds.

    Responsibilities:
        - Broadcasts primary proposals.
        - Collects aggregated signatures from group representatives.
        - Executes Algorithm 2 (Threshold Vote-Counting Model).
        - Determines network-level consensus threshold and finality.
    """

    def __init__(self, cfg, groups, reps, logger):
        self.cfg = cfg
        self.groups = groups
        self.reps = reps
        self.r = redis.from_url(cfg.redis_url)
        self.logger = logger

    async def store_round_config(self, rid, node_ids):
        """Stores current round configuration and group mappings in Redis."""
        self.logger.info(f"[COORD] Storing round config for round {rid}...")
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
        self.logger.info(
            f"[COORD] Round {rid} config stored: groups={len(self.groups)}, reps={self.reps}"
        )

    def group_weight(self, valid_sigs, is_rep=True, msg_valid=True):
        """
        Implements Algorithm 2 (Threshold Vote-Counting Model) for vote weighting.
        Returns the number of effective votes contributed by a group.
        """
        m, E = self.cfg.m, self.cfg.E
        if not msg_valid:
            return 1
        if is_rep:
            return m if valid_sigs >= (m - E) else valid_sigs
        else:
            return valid_sigs

    async def run_round(self, rid: int, value: str):
        """
        Runs a full NBFT round consisting of:
            1. PrePrepare1 broadcast.
            2. Collection of group aggregates.
            3. Alert filtering.
            4. Weighted vote counting.
            5. Final threshold check and commit broadcast.
        """
        self.logger.info(f"[COORD] Starting consensus round {rid} for value '{value}'")

        primary = list(self.reps.values())[0]
        pre = PrePrepare1(rid, primary, value)
        await self.r.xadd("nbft:preprepare1", pre.to_fields())
        self.logger.info(
            f"[COORD] Broadcasted PrePrepare1 from primary node '{primary}'"
        )

        deadline = time.time() + self.cfg.inprep2_deadline_sec + 0.7
        aggregates = {}
        self.logger.info("[COORD] Waiting for group aggregates (RepAggregates)...")

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
                            f"[COORD] Received aggregate from group {gid}: rep={aggregates[gid]['rep']}, "
                            f"value={aggregates[gid]['value']}, valid_sigs={aggregates[gid]['valid_sigs']}"
                        )
            await asyncio.sleep(0.05)

        if not aggregates:
            self.logger.warning("[COORD] No aggregates received before timeout!")

        exclude = set()
        for gid in range(len(self.groups)):
            alerts = await self.r.xrange(f"nbft:alerts:{rid}:{gid}", "-", "+")
            if alerts:
                exclude.add(gid)

        if exclude:
            self.logger.warning(f"[COORD] Excluding groups with alerts: {exclude}")

        tally = defaultdict(int)
        for gid, agg in aggregates.items():
            if gid in exclude:
                continue
            w = self.group_weight(agg["valid_sigs"], is_rep=True)
            tally[agg["value"]] += w
            self.logger.info(
                f"[COORD] Counting group {gid}: value={agg['value']}, weight={w}, valid_sigs={agg['valid_sigs']}"
            )

        threshold = (self.cfg.R - self.cfg.omega) * self.cfg.m
        total_votes = sum(tally.values())
        winner, votes = max(tally.items(), key=lambda kv: kv[1], default=("⊥", 0))
        consensus_reached = total_votes >= threshold

        self.logger.info(
            f"[COORD] Tally result: {dict(tally)}, total_votes={total_votes}, threshold={threshold}, "
            f"winner='{winner}', votes={votes}"
        )

        if tally:
            await self.r.hset(
                f"nbft:rep_votes:{rid}", mapping={k: str(v) for k, v in tally.items()}
            )

        await self.r.xadd(
            "nbft:outprepare",
            {
                "rid": str(rid),
                "winner": winner,
                "votes": str(votes),
                "total": str(total_votes),
                "threshold": str(threshold),
                "consensus": str(consensus_reached),
            },
        )
        self.logger.info(
            f"[COORD] OutPrepare message written to Redis (consensus={consensus_reached})"
        )

        if consensus_reached:
            await self.r.xadd(
                "nbft:commit", {"rid": str(rid), "value": winner, "votes": str(votes)}
            )
            await self.r.hset(
                f"nbft:decisions:{rid}", mapping={"value": winner, "votes": str(votes)}
            )
            await self.r.xadd("nbft:preprepare2", {"rid": str(rid), "value": winner})
            self.logger.info(
                f"[COORD] ✅ Consensus reached: value='{winner}' (votes={votes}/{total_votes}) committed."
            )
        else:
            self.logger.warning(
                f"[COORD] ❌ Consensus not reached (votes={total_votes}, threshold={threshold})."
            )
