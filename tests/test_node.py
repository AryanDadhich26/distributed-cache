import asyncio

from distcache.client import NodeConnection
from distcache.node import CacheNode


async def _run_server():
    node = CacheNode(capacity=10_000)
    server = await node.serve("127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return node, server, port


async def _get_put_delete_roundtrip():
    node, server, port = await _run_server()
    try:
        conn = await NodeConnection.connect("127.0.0.1", port)
        try:
            assert await conn.get(b"missing") is None

            await conn.put(b"key1", b"value1", 100)
            value, ts = await conn.get(b"key1")
            assert value == b"value1"
            assert ts == 100

            await conn.put(b"key1", b"value2", 200)
            value, ts = await conn.get(b"key1")
            assert value == b"value2"
            assert ts == 200

            await conn.delete(b"key1")
            assert await conn.get(b"key1") is None

            assert await conn.ping() is True
        finally:
            await conn.close()
    finally:
        server.close()
        await server.wait_closed()


def test_get_put_delete_roundtrip():
    asyncio.run(_get_put_delete_roundtrip())


async def _multiple_clients_share_one_node():
    node, server, port = await _run_server()
    try:
        writer_conn = await NodeConnection.connect("127.0.0.1", port)
        reader_conn = await NodeConnection.connect("127.0.0.1", port)
        try:
            await writer_conn.put(b"shared", b"seen-by-both", 1)
            value, _ = await reader_conn.get(b"shared")
            assert value == b"seen-by-both"
        finally:
            await writer_conn.close()
            await reader_conn.close()
    finally:
        server.close()
        await server.wait_closed()


def test_multiple_clients_share_one_node():
    asyncio.run(_multiple_clients_share_one_node())


async def _eviction_under_real_connection():
    node = CacheNode(capacity=2)
    server = await node.serve("127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        conn = await NodeConnection.connect("127.0.0.1", port)
        try:
            await conn.put(b"a", b"1", 1)
            await conn.put(b"b", b"2", 2)
            await conn.put(b"c", b"3", 3)  # evicts "a"
            assert await conn.get(b"a") is None
            assert (await conn.get(b"b"))[0] == b"2"
            assert (await conn.get(b"c"))[0] == b"3"
        finally:
            await conn.close()
    finally:
        server.close()
        await server.wait_closed()


def test_eviction_under_real_connection():
    asyncio.run(_eviction_under_real_connection())
