import zlib
from bisect import bisect_right


def h32(value: str) -> int:
    """Computes a 32-bit hash using CRC32 for consistent hashing."""
    return zlib.crc32(value.encode()) & 0xFFFFFFFF


class HashRing:
    """
    Implements consistent hashing for NBFT node and representative selection.
    Nodes are positioned on a 32-bit hash ring; traversal is clockwise.
    """

    def __init__(self, node_ids):
        self.nodes = sorted((h32(n), n) for n in node_ids)
        self.hashes = [h for h, _ in self.nodes]

    def next(self, key: str) -> str:
        """Returns the node clockwise to the given hash key."""
        hv = h32(key)
        i = bisect_right(self.hashes, hv)
        if i == len(self.nodes):
            i = 0
        return self.nodes[i][1]

    def clockwise_walk(self, start_key: str):
        """Yields nodes in clockwise order starting from a key on the hash ring."""
        hv = h32(start_key)
        i = bisect_right(self.hashes, hv)
        while True:
            if i == len(self.nodes):
                i = 0
            yield self.nodes[i][1]
            i += 1
