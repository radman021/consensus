import asyncio, sys, io, random
from config import NBFTConfig
from grouping import assign_groups, pick_representative
from node import Node
from coordinator import Coordinator
from logger import Logger

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


async def main():
    logger = Logger.get_logger("simulation")
    logger.info("[SYSTEM] Starting simulation...")

    try:
        # ekstrem koji uspe kad su bar 3 maliciozna u jednoj grupi a svi ostali u razlicitim
        # cfg = NBFTConfig(n=16, m=4, mal_nodes=6)

        # konsenzus moze da propadne tek kada bude bar 4 maliciozna (po 2 u 2 grupe)

        cfg = NBFTConfig(n=16, m=4, mal_nodes=4)
        node_ids = [f"node-{i}" for i in range(cfg.n)]

        groups = assign_groups(node_ids, cfg, logger)
        reps = {gid: pick_representative(g, cfg, gid) for gid, g in enumerate(groups)}
        logger.info(
            f"[SYSTEM] Initialized {len(groups)} groups with representatives: {reps}"
        )

        malicious_ids = random.sample(node_ids, cfg.mal_nodes)
        logger.info(f"[SYSTEM] Malicious nodes assigned: {malicious_ids}")

        nodes = []
        for gid, g in enumerate(groups):
            rep = reps[gid]
            for nid in g:
                is_mal = nid in malicious_ids
                if is_mal:
                    logger.info(f"[SYSTEM] Created MALICIOUS node={nid} in group={gid}")
                nodes.append(Node(nid, cfg, gid, rep, honest=not is_mal))

        rid = 1
        coord = Coordinator(cfg, groups, reps, logger)
        await coord.store_round_config(rid, node_ids)

        value = "BLOCK_HASH_ABC"
        logger.info("[SYSTEM] Starting NBFT consensus round...")

        await asyncio.gather(*[n.in_prepare1(rid, value) for n in nodes])
        await asyncio.gather(
            *[n.in_prepare2_collect(rid, cfg.inprep2_deadline_sec) for n in nodes]
        )
        await coord.run_round(rid, value)

        logger.info(" [SYSTEM] Consensus round completed successfully.")
    except Exception as e:
        logger.error(f"[SYSTEM] Consensus simulation failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
