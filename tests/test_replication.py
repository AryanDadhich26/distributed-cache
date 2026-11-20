import asyncio

import pytest

from distcache.client import NodeConnection
from distcache.node import CacheNode
from distcache.router import QuorumError, Router

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


def test_write_quorum_plus_read_quorum_must_exceed_replication_factor():
    with pytest.raises(ValueError):
        Router(replication_factor=3, read_quorum=1, write_quorum=1)


async def _read_after_write_sees_the_value():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            await router.put(b"key", b"value", timestamp=100)
            value, ts = await router.get(b"key")
            assert value == b"value"
            assert ts == 100
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_read_after_write_sees_the_value():
    asyncio.run(_read_after_write_sees_the_value())


async def _cluster_survives_one_node_dying_mid_operation():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            keys = [f"key{i}".encode() for i in range(300)]
            for i, key in enumerate(keys):
                await router.put(key, f"value{i}".encode())

            # no heartbeat running - quorum alone must tolerate one dead replica
            from collections import Counter
            primary_counts = Counter(router.owner(k) for k in keys)
            victim = primary_counts.most_common(1)[0][0]
            victim_index = NODE_IDS.index(victim)
            await nodes[victim_index].kill()

            failures = 0
            for i, key in enumerate(keys):
                try:
                    value, _ = await router.get(key)
                    assert value == f"value{i}".encode()
                except QuorumError:
                    failures += 1
            assert failures == 0
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_cluster_survives_one_node_dying_mid_operation():
    asyncio.run(_cluster_survives_one_node_dying_mid_operation())


async def _heartbeat_detects_a_dead_node_and_reroutes():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            keys = [f"key{i}".encode() for i in range(200)]
            for i, key in enumerate(keys):
                await router.put(key, f"value{i}".encode())

            from collections import Counter
            primary_counts = Counter(router.owner(k) for k in keys)
            victim = primary_counts.most_common(1)[0][0]
            victim_index = NODE_IDS.index(victim)

            router.start_heartbeat(interval=0.05, failure_threshold=2)
            await nodes[victim_index].kill()

            # give the heartbeat loop a few intervals to notice and act
            for _ in range(40):
                await asyncio.sleep(0.05)
                if victim in router._down:
                    break

            assert victim in router._down
            assert victim not in router.ring.nodes

            for i, key in enumerate(keys):
                value, _ = await router.get(key)
                assert value == f"value{i}".encode()
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_heartbeat_detects_a_dead_node_and_reroutes():
    asyncio.run(_heartbeat_detects_a_dead_node_and_reroutes())


async def _last_write_wins_on_conflicting_replicas():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=3, write_quorum=3)
        try:
            await router.put(b"key", b"old", timestamp=100)
            replicas = router.preference_list(b"key")
            port_by_node = dict(zip(NODE_IDS, ports))

            # one replica gets a newer value directly, bypassing the router
            direct = await NodeConnection.connect("127.0.0.1", port_by_node[replicas[0]])
            try:
                await direct.put(b"key", b"new", timestamp=200)
            finally:
                await direct.close()

            value, ts = await router.get(b"key")
            assert value == b"new"
            assert ts == 200
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_last_write_wins_on_conflicting_replicas():
    asyncio.run(_last_write_wins_on_conflicting_replicas())


async def _quorum_error_when_too_few_replicas_are_live():
    nodes, servers, ports = await _start_nodes()
    try:
        router = await _build_router(ports, replication_factor=3, read_quorum=2, write_quorum=2)
        try:
            # simulate a mass outage: drop every node but one
            for node_id in NODE_IDS[1:]:
                router.mark_down(node_id)

            with pytest.raises(QuorumError):
                await router.put(b"key", b"value")
            with pytest.raises(QuorumError):
                await router.get(b"key")
        finally:
            await router.close()
    finally:
        await _stop_nodes(servers)


def test_quorum_error_when_too_few_replicas_are_live():
    asyncio.run(_quorum_error_when_too_few_replicas_are_live())
