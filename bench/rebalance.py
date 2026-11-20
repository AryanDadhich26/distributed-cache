import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from distcache.node import CacheNode
from distcache.router import Router

NODE_IDS = [f"node-{i}" for i in range(10)]
NUM_KEYS = 20_000


async def start_node():
    node = CacheNode()
    server = await node.serve("127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return node, server, port


async def measure_join_and_leave(n: int):
    servers, ports = [], []
    for _ in range(n + 1):  # one spare, started but not joined to the ring yet
        _, server, port = await start_node()
        servers.append(server)
        ports.append(port)

    router = Router(replication_factor=min(3, n), read_quorum=min(2, n), write_quorum=min(2, n))
    for node_id, port in zip(NODE_IDS[:n], ports[:n]):
        await router.add_node(node_id, "127.0.0.1", port)

    keys = [f"key{i}".encode() for i in range(NUM_KEYS)]
    owners_initial = {key: router.owner(key) for key in keys}

    await router.add_node(NODE_IDS[n], "127.0.0.1", ports[n])
    owners_after_join = {key: router.owner(key) for key in keys}
    moved_on_join = sum(1 for k in keys if owners_after_join[k] != owners_initial[k])

    await router.remove_node(NODE_IDS[0])
    owners_after_leave = {key: router.owner(key) for key in keys}
    moved_on_leave = sum(1 for k in keys if owners_after_leave[k] != owners_after_join[k])

    ideal_pct = 100 / (n + 1)
    print(f"\n--- {n} -> {n + 1} nodes ---")
    print(f"join:  {moved_on_join}/{NUM_KEYS} keys moved "
          f"({moved_on_join / NUM_KEYS * 100:.2f}%, ideal ~{ideal_pct:.2f}%)")
    print(f"leave: {moved_on_leave}/{NUM_KEYS} keys moved "
          f"({moved_on_leave / NUM_KEYS * 100:.2f}%, ideal ~{ideal_pct:.2f}%)")

    await router.close()
    for server in servers:
        server.close()
        await server.wait_closed()


async def main():
    for n in (3, 5, 9):
        await measure_join_and_leave(n)


if __name__ == "__main__":
    asyncio.run(main())
