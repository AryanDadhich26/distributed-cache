from distcache import protocol


def test_encode_decode_roundtrip_no_fields():
    frame = protocol.encode(protocol.PING)
    opcode, fields = protocol.decode(frame[4:])
    assert opcode == protocol.PING
    assert fields == []


def test_encode_decode_roundtrip_multiple_fields():
    frame = protocol.encode(protocol.PUT, b"key", b"value", protocol.pack_timestamp(123))
    opcode, fields = protocol.decode(frame[4:])
    assert opcode == protocol.PUT
    assert fields == [b"key", b"value", protocol.pack_timestamp(123)]


def test_length_prefix_matches_payload_size():
    frame = protocol.encode(protocol.GET, b"somekey")
    (declared_len,) = protocol._LEN.unpack(frame[:4])
    assert declared_len == len(frame) - 4


def test_fields_with_embedded_length_like_bytes_survive():
    tricky = b"\x00\x00\x00\xff" + b"rest"
    frame = protocol.encode(protocol.PUT, b"key", tricky, protocol.pack_timestamp(1))
    _, fields = protocol.decode(frame[4:])
    assert fields[1] == tricky


def test_timestamp_roundtrip():
    packed = protocol.pack_timestamp(1234567890123)
    assert protocol.unpack_timestamp(packed) == 1234567890123
