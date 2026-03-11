#!/usr/bin/env python3
"""Consistent Hashing — ring with virtual nodes + jump hash."""
import hashlib, bisect
class ConsistentHash:
    def __init__(self, vnodes=150):
        self.vnodes = vnodes; self.ring = []; self.nodes = {}
    def add_node(self, node):
        for i in range(self.vnodes):
            h = int(hashlib.md5(f"{node}:{i}".encode()).hexdigest(), 16)
            bisect.insort(self.ring, h); self.nodes[h] = node
    def remove_node(self, node):
        remove = [h for h, n in self.nodes.items() if n == node]
        for h in remove: self.ring.remove(h); del self.nodes[h]
    def get_node(self, key):
        if not self.ring: return None
        h = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        idx = bisect.bisect(self.ring, h) % len(self.ring)
        return self.nodes[self.ring[idx]]

def jump_hash(key, num_buckets):
    b, j = -1, 0; k = key
    while j < num_buckets:
        b = j; k = ((k * 2862933555777941757) + 1) & ((1 << 64) - 1)
        j = int((b + 1) * (1 << 31) / ((k >> 33) + 1))
    return b

if __name__ == "__main__":
    ch = ConsistentHash(100)
    for n in ["server-1", "server-2", "server-3"]: ch.add_node(n)
    dist = {}
    for i in range(1000):
        n = ch.get_node(f"key-{i}"); dist[n] = dist.get(n, 0) + 1
    print("Ring distribution:", dist)
    print(f"Jump hash: {[jump_hash(i, 3) for i in range(10)]}")
