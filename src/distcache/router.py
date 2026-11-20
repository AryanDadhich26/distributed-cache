import asyncio
import time

from .client import NodeConnection
from .hashring import HashRing


class QuorumError(Exception):
    pass


class Router:
    def __init__(self, replication_factor: int = 3, read_quorum: int = 2,
                 write_quorum: int = 2, vnodes: int = 150):
        if read_quorum + write_quorum <= replication_factor:
            raise ValueError("read_quorum + write_quorum must exceed replication_factor")
        self.ring = HashRing(vnodes=vnodes)
        self.n = replication_factor
        self.r = read_quorum
        self.w = write_quorum
        self._connections: dict[str, NodeConnection] = {}
        self._down: set[str] = set()
        self._heartbeat_task: asyncio.Task | None = None

    async def add_node(self, node_id: str, host: str, port: int):
        conn = await NodeConnection.connect(host, port)
        self._connections[node_id] = conn
        self.ring.add_node(node_id)
        self._down.discard(node_id)

    async def remove_node(self, node_id: str):
        self.ring.remove_node(node_id)
        self._down.discard(node_id)
        conn = self._connections.pop(node_id, None)
        if conn is not None:
            await conn.close()

    def mark_down(self, node_id: str):
        if node_id in self._down:
            return
        self._down.add(node_id)
        if node_id in self.ring.nodes:
            self.ring.remove_node(node_id)

    def preference_list(self, key: bytes) -> list[str]:
        return self.ring.get_preference_list(key.decode(), self.n)

    def owner(self, key: bytes) -> str | None:
        prefs = self.preference_list(key)
        return prefs[0] if prefs else None

    async def put(self, key: bytes, value: bytes, timestamp: int | None = None) -> int:
        if timestamp is None:
            timestamp = time.time_ns() // 1000
        targets = self.preference_list(key)
        if len(targets) < self.w:
            raise QuorumError(f"only {len(targets)} live replicas, need write quorum {self.w}")

        async def write_one(node_id):
            await self._connections[node_id].put(key, value, timestamp)

        results = await asyncio.gather(*(write_one(n) for n in targets), return_exceptions=True)
        acked = sum(1 for r in results if not isinstance(r, Exception))
        if acked < self.w:
            raise QuorumError(f"only {acked}/{len(targets)} replicas acked, need write quorum {self.w}")
        return timestamp

    async def get(self, key: bytes):
        targets = self.preference_list(key)
        if len(targets) < self.r:
            raise QuorumError(f"only {len(targets)} live replicas, need read quorum {self.r}")

        async def read_one(node_id):
            return await self._connections[node_id].get(key)

        results = await asyncio.gather(*(read_one(n) for n in targets), return_exceptions=True)
        successes = [r for r in results if not isinstance(r, Exception)]
        if len(successes) < self.r:
            raise QuorumError(
                f"only {len(successes)}/{len(targets)} replicas responded, need read quorum {self.r}"
            )

        present = [entry for entry in successes if entry is not None]
        if not present:
            return None
        # last-write-wins across whatever quorum of replicas responded
        return max(present, key=lambda entry: entry[1])

    async def delete(self, key: bytes) -> int:
        targets = self.preference_list(key)
        if len(targets) < self.w:
            raise QuorumError(f"only {len(targets)} live replicas, need write quorum {self.w}")

        async def delete_one(node_id):
            await self._connections[node_id].delete(key)

        results = await asyncio.gather(*(delete_one(n) for n in targets), return_exceptions=True)
        acked = sum(1 for r in results if not isinstance(r, Exception))
        if acked < self.w:
            raise QuorumError(f"only {acked}/{len(targets)} replicas acked, need write quorum {self.w}")
        return acked

    def start_heartbeat(self, interval: float = 1.0, failure_threshold: int = 3):
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval, failure_threshold))

    async def stop_heartbeat(self):
        if self._heartbeat_task is None:
            return
        self._heartbeat_task.cancel()
        try:
            await self._heartbeat_task
        except asyncio.CancelledError:
            pass
        self._heartbeat_task = None

    async def _heartbeat_loop(self, interval: float, failure_threshold: int):
        misses: dict[str, int] = {}
        while True:
            await asyncio.sleep(interval)
            for node_id, conn in list(self._connections.items()):
                if node_id in self._down:
                    continue
                try:
                    ok = await asyncio.wait_for(conn.ping(), timeout=interval)
                except Exception:
                    ok = False
                if ok:
                    misses[node_id] = 0
                else:
                    misses[node_id] = misses.get(node_id, 0) + 1
                    if misses[node_id] >= failure_threshold:
                        self.mark_down(node_id)

    async def close(self):
        await self.stop_heartbeat()
        for conn in self._connections.values():
            await conn.close()
