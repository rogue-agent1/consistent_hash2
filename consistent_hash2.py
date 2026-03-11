#!/usr/bin/env python3
"""Consistent hashing with virtual nodes and rebalancing metrics.

One file. Zero deps. Does one thing well.

Used in distributed systems (DynamoDB, Cassandra, CDNs) for load balancing
with minimal key redistribution when nodes join/leave.
"""
import hashlib, bisect, sys

class ConsistentHash:
    def __init__(self, nodes=None, vnodes=150):
        self.vnodes = vnodes
        self.ring = []       # sorted hash positions
        self.ring_map = {}   # hash -> node
        self.nodes = set()
        if nodes:
            for n in nodes:
                self.add_node(n)

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.vnodes):
            h = self._hash(f"{node}:{i}")
            self.ring_map[h] = node
            bisect.insort(self.ring, h)

    def remove_node(self, node):
        self.nodes.discard(node)
        to_remove = []
        for i in range(self.vnodes):
            h = self._hash(f"{node}:{i}")
            to_remove.append(h)
            del self.ring_map[h]
        self.ring = [h for h in self.ring if h not in set(to_remove)]

    def get_node(self, key):
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        return self.ring_map[self.ring[idx]]

    def get_nodes(self, key, n=3):
        """Get n distinct nodes for replication."""
        if not self.ring:
            return []
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h)
        seen, result = set(), []
        for i in range(len(self.ring)):
            node = self.ring_map[self.ring[(idx + i) % len(self.ring)]]
            if node not in seen:
                seen.add(node)
                result.append(node)
                if len(result) >= n:
                    break
        return result

    def distribution(self, keys):
        """Show key distribution across nodes."""
        counts = {}
        for k in keys:
            node = self.get_node(k)
            counts[node] = counts.get(node, 0) + 1
        return counts


def main():
    nodes = ["node-A", "node-B", "node-C"]
    ch = ConsistentHash(nodes, vnodes=150)
    keys = [f"key-{i}" for i in range(10000)]
    dist = ch.distribution(keys)
    print("Distribution (3 nodes, 10K keys):")
    for n in sorted(dist):
        pct = dist[n] / len(keys) * 100
        bar = "█" * int(pct / 2)
        print(f"  {n}: {dist[n]:5d} ({pct:5.1f}%) {bar}")
    # Add a node
    ch.add_node("node-D")
    dist2 = ch.distribution(keys)
    moved = sum(1 for k in keys if ch.get_node(k) != (ConsistentHash(nodes, 150).get_node(k)))
    print(f"\nAfter adding node-D: ~{moved} keys moved ({moved/len(keys)*100:.1f}%)")
    for n in sorted(dist2):
        print(f"  {n}: {dist2[n]:5d} ({dist2[n]/len(keys)*100:5.1f}%)")
    # Replication
    print(f"\nReplica nodes for 'user:42': {ch.get_nodes('user:42', 3)}")

if __name__ == "__main__":
    main()
