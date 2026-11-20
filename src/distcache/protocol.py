import struct

GET = 1
PUT = 2
DELETE = 3
PING = 4

OK = 0x80
VALUE = 0x81
NOT_FOUND = 0x82
ERROR = 0x83
PONG = 0x84

_LEN = struct.Struct(">I")
_TS = struct.Struct(">Q")


def pack_timestamp(ts: int) -> bytes:
    return _TS.pack(ts)


def unpack_timestamp(data: bytes) -> int:
    return _TS.unpack(data)[0]


def encode(opcode: int, *fields: bytes) -> bytes:
    payload = bytes([opcode]) + b"".join(_LEN.pack(len(f)) + f for f in fields)
    return _LEN.pack(len(payload)) + payload


def decode(payload: bytes):
    opcode = payload[0]
    fields = []
    offset = 1
    while offset < len(payload):
        (flen,) = _LEN.unpack_from(payload, offset)
        offset += 4
        fields.append(payload[offset:offset + flen])
        offset += flen
    return opcode, fields


async def read_frame(reader):
    (length,) = _LEN.unpack(await reader.readexactly(4))
    payload = await reader.readexactly(length)
    return decode(payload)
