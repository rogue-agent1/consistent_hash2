#!/usr/bin/env python3
"""consistent_hash2 - Consistent hashing ring implementation."""
import sys, hashlib, bisect
class ConsistentHash:
    def __init__(self, replicas=150):
        self.replicas=replicas; self.ring={}; self.sorted_keys=[]
    def _hash(self, key): return int(hashlib.md5(key.encode()).hexdigest(), 16)
    def add_node(self, node):
        for i in range(self.replicas):
            h=self._hash(f"{node}:{i}"); self.ring[h]=node
        self.sorted_keys=sorted(self.ring.keys())
    def remove_node(self, node):
        for i in range(self.replicas):
            h=self._hash(f"{node}:{i}"); del self.ring[h]
        self.sorted_keys=sorted(self.ring.keys())
    def get_node(self, key):
        if not self.ring: return None
        h=self._hash(key); idx=bisect.bisect_right(self.sorted_keys, h)
        if idx==len(self.sorted_keys): idx=0
        return self.ring[self.sorted_keys[idx]]
if __name__=="__main__":
    ch=ConsistentHash(replicas=50)
    for n in ["server-1","server-2","server-3"]: ch.add_node(n)
    dist={}
    for i in range(1000):
        node=ch.get_node(f"key_{i}"); dist[node]=dist.get(node,0)+1
    print("Distribution (1000 keys):")
    for n, c in sorted(dist.items()): print(f"  {n}: {c} ({c/10:.1f}%)")
