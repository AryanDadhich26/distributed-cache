import asyncio

from distcache.client import NodeConnection
from distcache.node import CacheNode
from distcache.router import Router

NODE_IDS = ["node-a", "node-b", "node-c", "node-d", "node-e"]


async def _start_nodes(n=5):
    nodes, servers, ports = [], [], []
    for _ in range(n):
        node = CacheNode(capacity=10_000)
        server = await node.serve("127.0.0.1", 0)
        nodes.append(node)
        servers.append(server)
        ports.append(server.sockets[0].getsockname()[1])
    return nodes, servers, ports


async def _stop_nodes(servers):
    for server in servers:
        server.close()
        await server.wait_closed()


async def _build_router(ports, **kwargs):
    router = Router(**kwargs)
    for node_id, port in zip(NODE_IDS, ports):
        await router.add_node(node_id, "127.0.0.1", port)
    return router


async def _put_lands_on_every_replica_and_nowhere_else():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            keys = [f"key{i}".encode() for i in range(300)]
            for i, key in enumerate(keys):
                await router.put(key, f"value{i}".encode())

            direct_conns = {}
            for node_id, port in zip(NODE_IDS, ports):
                direct_conns[node_id] = await NodeConnection.connect("127.0.0.1", port)
            try:
                for i, key in enumerate(keys):
                    replicas = set(router.preference_list(key))
                    for node_id, conn in direct_conns.items():
                        entry = await conn.get(key)
                        if node_id in replicas:
                            assert entry is not None and entry[0] == f"value{i}".encode()
                        else:
                            assert entry is None
            finally:
                for conn in direct_conns.values():
                    await conn.close()
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_put_lands_on_every_replica_and_nowhere_else():
    asyncio.run(_put_lands_on_every_replica_and_nowhere_else())


async def _removing_a_node_only_rebalances_its_own_keys():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            keys = [f"key{i}".encode() for i in range(2000)]
            owners_before = {key: router.owner(key) for key in keys}

            await router.remove_node("node-c")

            owners_after = {key: router.owner(key) for key in keys}

            moved = 0
            for key, before in owners_before.items():
                after = owners_after[key]
                assert after != "node-c"
                if after != before:
                    moved += 1
                    assert before == "node-c"

            assert moved == sum(1 for o in owners_before.values() if o == "node-c")
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_removing_a_node_only_rebalances_its_own_keys():
    asyncio.run(_removing_a_node_only_rebalances_its_own_keys())
