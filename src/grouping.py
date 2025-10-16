from hash_ring import HashRing


def assign_groups(node_ids, cfg, logger):
    """
    Partitions all nodes into consensus groups using consistent hashing.

    Returns:
        A list of node groups where each group contains m nodes.
    """
    logger.info(f"Starting group generation...")

    ring = HashRing(node_ids)
    start_key = f"{cfg.view_number}_{len(node_ids)}"
    walk = ring.clockwise_walk(start_key)

    groups = [[] for _ in range(cfg.R)]
    assigned = set()

    for i in range(cfg.R):
        for _ in range(cfg.m):
            try:
                nid = next(walk)
                while nid in assigned:
                    nid = next(walk)
                groups[i].append(nid)
                assigned.add(nid)
                if len(assigned) >= len(node_ids):
                    break
            except StopIteration:
                break

    logger.info(f"Created {len(groups)} groups.")
    return [g for g in groups if g]


def pick_representative(group, cfg, group_number):
    """
    Selects a representative primary node for the given group based on consistent hashing.

    Returns:
        Node ID chosen as representative primary node for the group.
    """
    ring = HashRing(group)
    key = f"{cfg.master_ip}|{cfg.view_number}|{group_number}"
    return ring.next(key)
