import asyncio

from . import protocol
from .lru import LRUCache


class CacheNode:
    def __init__(self, capacity: int = 10_000):
        self.store = LRUCache(capacity)
        self._server = None
        self._writers: set = set()

    def _dispatch(self, opcode, fields):
        if opcode == protocol.GET:
            (key,) = fields
            entry = self.store.get(key)
            if entry is None:
                return protocol.encode(protocol.NOT_FOUND)
            value, ts = entry
            return protocol.encode(protocol.VALUE, value, protocol.pack_timestamp(ts))

        if opcode == protocol.PUT:
            key, value, ts_bytes = fields
            self.store.put(key, (value, protocol.unpack_timestamp(ts_bytes)))
            return protocol.encode(protocol.OK)

        if opcode == protocol.DELETE:
            (key,) = fields
            self.store.delete(key)
            return protocol.encode(protocol.OK)

        if opcode == protocol.PING:
            return protocol.encode(protocol.PONG)

        return protocol.encode(protocol.ERROR, b"unknown opcode")

    async def _handle_connection(self, reader, writer):
        self._writers.add(writer)
        try:
            while True:
                try:
                    opcode, fields = await protocol.read_frame(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break
                writer.write(self._dispatch(opcode, fields))
                await writer.drain()
        finally:
            self._writers.discard(writer)
            writer.close()

    async def serve(self, host: str = "127.0.0.1", port: int = 0):
        self._server = await asyncio.start_server(self._handle_connection, host, port)
        return self._server

    async def kill(self):
        # aborts transports instead of a graceful close - simulates kill -9, not shutdown
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
