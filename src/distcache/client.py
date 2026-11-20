import asyncio

from . import protocol


class ProtocolError(Exception):
    def __init__(self, opcode, fields):
        super().__init__(f"unexpected opcode {opcode}: {fields}")


class NodeConnection:
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._lock = asyncio.Lock()  # one request at a time per connection

    @classmethod
    async def connect(cls, host: str, port: int):
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def _request(self, frame: bytes):
        async with self._lock:
            self._writer.write(frame)
            await self._writer.drain()
            return await protocol.read_frame(self._reader)

    async def get(self, key: bytes):
        opcode, fields = await self._request(protocol.encode(protocol.GET, key))
        if opcode == protocol.NOT_FOUND:
            return None
        if opcode == protocol.VALUE:
            value, ts_bytes = fields
            return value, protocol.unpack_timestamp(ts_bytes)
        raise ProtocolError(opcode, fields)

    async def put(self, key: bytes, value: bytes, timestamp: int):
        frame = protocol.encode(protocol.PUT, key, value, protocol.pack_timestamp(timestamp))
        opcode, fields = await self._request(frame)
        if opcode != protocol.OK:
            raise ProtocolError(opcode, fields)

    async def delete(self, key: bytes):
        opcode, fields = await self._request(protocol.encode(protocol.DELETE, key))
        if opcode != protocol.OK:
            raise ProtocolError(opcode, fields)

    async def ping(self) -> bool:
        opcode, _ = await self._request(protocol.encode(protocol.PING))
        return opcode == protocol.PONG

    async def close(self):
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except OSError:
            pass
