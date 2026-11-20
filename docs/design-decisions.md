# Design decisions

## md5 for ring points

`ring_hash` takes the first 4 bytes of an md5 digest as a big-endian uint32, same as ketama. Not
for security, just spread across a 32-bit space.

## Virtual nodes

100k keys onto 5 nodes: one point per node gives 731 keys on one and 45,351 on another against a
20,000 average, 91% coefficient of variation. 150 vnodes/node brings it to 6.79%.

## vnode hash collisions aren't handled

Two nodes' points could hash to the same value. `_ring[point] = node_id` lets the second overwrite
the first, and `remove_node` deletes from `_sorted_points` by value, so a collision removes the
wrong one. A few thousand points in a 2^32 space makes it negligible; namespacing points per node
would fix it and isn't worth the bookkeeping here.

## Preference list starts where get_node points

The list begins at `get_node(key)` and walks clockwise, skipping vnodes of a node already in it.
## Length-prefixed frames, not a text protocol

`[4-byte payload length][1-byte opcode][fields, each 4-byte-length-prefixed]`. Values hold
arbitrary bytes, newlines included, so a line protocol would need escaping.
`test_fields_with_embedded_length_like_bytes_survive` stores a value that looks like a length
prefix.

## Timestamp on the wire from the start

A single node ignores it, but last-write-wins needs it on every stored value and adding a field
later breaks the format. It's a wall clock, not a logical clock, so skew across machines can pick
the wrong winner. Vector clocks are Dynamo's answer and aren't implemented.

## LRU via OrderedDict

`OrderedDict` is already a linked list plus hash map with O(1) `move_to_end` and `popitem`.

## One request at a time per connection

No pipelining: a client can't send a second request before the first response lands on that
connection. Many connections still run concurrently, and the router spreads requests across the
cluster, which is where the concurrency matters.

## Write to N, need W acks; read from N, need R responses

`put` sends to the whole preference list in parallel and returns once `W` ack. `get` takes the
highest-timestamp value among the `R` that responded. The constructor rejects `R + W <= N`.

Strict quorum: a write that can't reach `W` live replicas raises `QuorumError` instead of parking
on a stand-in node for later replay. Hinted handoff isn't worth the complexity here.

R+W>N assumes the preference list is the same for the write and the read. A membership change
between the two shifts the list and the guarantee doesn't carry across it.

## Shared connection corrupted by concurrent callers

`get()` intermittently returned `only 1/3 replicas responded` with all replicas alive. One socket
per node, and the heartbeat's `ping()` raced the router's `get()`/`put()` on it, so a caller could
read back the wrong response. Fix: an `asyncio.Lock` around each request/response pair in
`NodeConnection._request`.

## Closing a broken connection raised

Closing a `StreamWriter` whose transport was already reset raises `ConnectionAbortedError` out of
`wait_closed()`. `close()` treats `OSError` there as expected.

## Benchmarks run real processes

Subprocesses over loopback TCP rather than `CacheNode` as tasks in one event loop, which would
hide socket and scheduling cost.

1 node does ~14.9k puts/sec at 1.4ms p50, 3 and 5 nodes ~3k/sec at 6-7ms. N=3 means three parallel
round trips and a wait on the quorum, so latency tracks the slowest. 5 nodes doesn't beat 3 since
the replication factor is capped at 3 either way.

## Docker setup

Docker Engine inside WSL2 rather than Docker Desktop, keeping the image store on the same drive as
everything else. 5 node containers, each port-mapped to the host.

## Readiness check needs a real request

First chaos run failed every write, 14260/14260, and repeated on a fresh cluster with no kill.
`wait_for_port` called a node ready on a bare TCP connect, but a container accepts the handshake
before `asyncio.start_server` is stable behind it, and the first write then gets
`ConnectionResetError`, killing that connection permanently. `wait_ready` does a connect/PING/close
round trip and retries; it clears in 1-2 tries.

