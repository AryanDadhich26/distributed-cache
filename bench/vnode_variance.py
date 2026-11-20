import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from distcache.hashring import HashRing

NODES = ["node-a", "node-b", "node-c", "node-d", "node-e"]
NUM_KEYS = 100_000


def measure(vnodes):
    ring = HashRing(NODES, vnodes=vnodes)
    counts = {node: 0 for node in NODES}
    for i in range(NUM_KEYS):
        counts[ring.get_node(f"key-{i}")] += 1
    values = list(counts.values())
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    return counts, mean, stdev, stdev / mean * 100


def report(label, vnodes):
    counts, mean, stdev, cv = measure(vnodes)
    print(f"{label} (vnodes={vnodes})")
    print(f"  counts: {counts}")
    print(f"  mean={mean:.1f}  stdev={stdev:.1f}  coefficient_of_variation={cv:.2f}%")
    print()
    return cv


if __name__ == "__main__":
    print(f"5 physical nodes, {NUM_KEYS} keys, coefficient of variation = stdev/mean\n")
    cv_1 = report("no virtual nodes", 1)
    cv_150 = report("with virtual nodes", 150)
    print(f"virtual nodes cut key-distribution variance by {cv_1 / cv_150:.1f}x")
