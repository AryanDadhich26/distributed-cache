import bisect
import hashlib


def ring_hash(data: str) -> int:
    # md5 for spread, not security; same choice ketama makes
    digest = hashlib.md5(data.encode()).digest()
    return int.from_bytes(digest[:4], "big")


class HashRing:
    def __init__(self, nodes=None, vnodes=150):
        self.vnodes = vnodes
        self._ring = {}          # point hash -> physical node id
        self._sorted_points = []  # sorted point hashes, kept in sync with _ring
        self._node_points = {}   # physical node id -> its point hashes

        for node in nodes or []:
            self.add_node(node)

    def add_node(self, node_id):
        if node_id in self._node_points:
            return
        points = []
        for i in range(self.vnodes):
            point = ring_hash(f"{node_id}#{i}")
            self._ring[point] = node_id
            bisect.insort(self._sorted_points, point)
            points.append(point)
        self._node_points[node_id] = points

    def remove_node(self, node_id):
        points = self._node_points.pop(node_id, None)
        if points is None:
            return
        for point in points:
            del self._ring[point]
            idx = bisect.bisect_left(self._sorted_points, point)
            del self._sorted_points[idx]

    def get_node(self, key):
        if not self._sorted_points:
            return None
        point = ring_hash(key)
        idx = bisect.bisect(self._sorted_points, point) % len(self._sorted_points)
        return self._ring[self._sorted_points[idx]]

    def get_preference_list(self, key, n):
        if not self._sorted_points:
            return []
        n = min(n, len(self._node_points))
        point = ring_hash(key)
        start = bisect.bisect(self._sorted_points, point) % len(self._sorted_points)

        result = []
        seen = set()
        i = start
        while len(result) < n:
            node = self._ring[self._sorted_points[i]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            i = (i + 1) % len(self._sorted_points)
        return result

    @property
    def nodes(self):
        return frozenset(self._node_points)
