import asyncio
import sys
import io

from config import NBFTConfig
from grouping import assign_groups, pick_representative
from node import Node
from coordinator import Coordinator
from logger import Logger

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


async def main():
    """
    Entry point for simulating one NBFT consensus round.
    Initializes groups, representatives, and nodes; runs intra-group and inter-group consensus.
    """
    logger = Logger.get_logger("simulation")
    logger.info(f"Starting simulation...")

    try:
        cfg = NBFTConfig(n=17, m=4, view_number=0)
        node_ids = [f"node-{i}" for i in range(cfg.n)]

        groups = assign_groups(node_ids, cfg, logger)
        reps = {gid: pick_representative(g, cfg, gid) for gid, g in enumerate(groups)}

        logger.info(f"Initialized {len(groups)} groups with representatives: {reps}")

        nodes = []
        for gid, g in enumerate(groups):
            rep = reps[gid]
            for nid in g:
                nodes.append(Node(nid, cfg, gid, rep, honest=True))

        rid = 1
        coord = Coordinator(cfg, groups, reps, logger)
        await coord.store_round_config(rid, node_ids)

        value = "BLOCK_HASH_ABC"
        logger.info("Starting NBFT consensus round...")

        await asyncio.gather(*[n.in_prepare1(rid, value) for n in nodes])
        await asyncio.gather(
            *[n.in_prepare2_collect(rid, cfg.inprep2_deadline_sec) for n in nodes]
        )
        await coord.run_round(rid, value)

        logger.info("Consensus round completed successfully.")

    except Exception as e:
        logger.error(f"Consensus simulation failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
