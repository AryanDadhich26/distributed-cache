# distributed-cache

A distributed in-memory key-value cache: consistent hash ring with virtual nodes, a wire protocol
over raw TCP sockets, N-way replication with quorum reads and writes, heartbeat failure detection.
Python, standard library only.

Status: 34 tests, throughput and rebalancing benchmarks, and a Docker Compose chaos test that
kills a node under load. `render.yaml` is set up but not deployed.

## Hash ring

`src/distcache/hashring.py`, no external library. `bench/vnode_variance.py`:

```
5 physical nodes, 100000 keys, coefficient of variation = stdev/mean

no virtual nodes (vnodes=1)
  counts: {'node-a': 731, 'node-b': 38503, 'node-c': 45351, 'node-d': 5966, 'node-e': 9449}
  mean=20000.0  stdev=18246.1  coefficient_of_variation=91.23%

with virtual nodes (vnodes=150)
  counts: {'node-a': 18702, 'node-b': 21916, 'node-c': 19613, 'node-d': 18527, 'node-e': 21242}
  mean=20000.0  stdev=1357.8  coefficient_of_variation=6.79%

virtual nodes cut key-distribution variance by 13.4x
```

`get_preference_list(key, n)` returns the primary plus the next `n-1` distinct nodes clockwise.

## Wire protocol and cache node

`src/distcache/protocol.py` frames as `[4-byte length][1-byte opcode][length-prefixed fields]`.
`src/distcache/node.py` is an asyncio TCP server over an LRU store, `src/distcache/client.py` the
client side.

```python
node = CacheNode(capacity=10_000)
server = await node.serve("127.0.0.1", 5000)

conn = await NodeConnection.connect("127.0.0.1", 5000)
await conn.put(b"key1", b"hello", timestamp=1)
value, ts = await conn.get(b"key1")   # (b"hello", 1)
```

## Replication, quorum and failure detection

`src/distcache/router.py` keeps the ring client-side, like groupcache and Dynamo.
`Router(replication_factor=3, read_quorum=2, write_quorum=2)` refuses to construct if
`read_quorum + write_quorum <= replication_factor`.

```python
router = Router(replication_factor=3, read_quorum=2, write_quorum=2)
await router.add_node("node-a", "127.0.0.1", 5001)
await router.add_node("node-b", "127.0.0.1", 5002)
await router.add_node("node-c", "127.0.0.1", 5003)

await router.put(b"key", b"value")           # all 3 replicas, needs 2 acks
value, ts = await router.get(b"key")         # all 3, needs 2 responses, freshest wins

router.start_heartbeat(interval=1.0, failure_threshold=3)
```

A node that misses three heartbeats leaves the ring and its keys fail over to the next replica.

## Benchmarks

`bench/throughput.py`, nodes as subprocesses, 20 concurrent clients:

```
--- 1 nodes (N=1 R=1 W=1), 20 concurrent clients x 100 ops ---
PUT: 14893 ops/sec  p50=1.40ms  p99=2.14ms  max=2.75ms
GET: 13760 ops/sec  p50=1.40ms  p99=3.05ms  max=3.34ms

--- 3 nodes (N=3 R=2 W=2), 20 concurrent clients x 100 ops ---
PUT: 2923 ops/sec  p50=7.22ms  p99=14.92ms  max=18.76ms
GET: 3284 ops/sec  p50=6.39ms  p99=10.97ms  max=11.65ms

--- 5 nodes (N=3 R=2 W=2), 20 concurrent clients x 100 ops ---
PUT: 3399 ops/sec  p50=6.59ms  p99=8.39ms  max=8.64ms
GET: 3272 ops/sec  p50=6.69ms  p99=11.86ms  max=12.68ms
```

Latency rises from 1 to 3 nodes: N=3 waits on a quorum of three parallel round trips, so it
tracks the slowest of them.

`bench/rebalance.py`, keyspace moved on a membership change:

```
--- 3 -> 4 nodes ---   join: 25.17% moved (ideal 25.00%)   leave: 22.86% moved
--- 5 -> 6 nodes ---   join: 17.16% moved (ideal 16.67%)   leave: 15.46% moved
--- 9 -> 10 nodes ---  join: 9.86% moved (ideal 10.00%)    leave: 10.46% moved
```

## Chaos test

`scripts/chaos_test.py` starts the 5-node Compose cluster, drives concurrent load, `docker kill`s
a container 5 seconds in, and checks every write that got a quorum ack is still readable.

```
docker compose -f docker/docker-compose.yml up -d --build
python scripts/chaos_test.py
```

```
=== chaos test report ===
duration: 15s, 10 concurrent workers, distcache-node-2 killed at t=5s
writes attempted: 12911
writes acknowledged (quorum met): 12911
writes failed (quorum not met): 0
ops/sec (acknowledged writes): 860.7
p50=1.29ms  p99=3.05ms
acknowledged writes lost after node kill: 0/12911
RESULT: PASS - zero data loss for every quorum-acknowledged write
```

## Build

```
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt
.venv/Scripts/pytest
```

## Structure

```
distributed-cache/
├── src/distcache/
│   ├── hashring.py        consistent hash ring with virtual nodes
│   ├── protocol.py        binary wire framing
│   ├── lru.py             LRU store
│   ├── node.py            asyncio TCP cache server
│   ├── client.py          client side of the protocol
│   └── router.py          ring, replication, quorum, failure detection
├── tests/
├── bench/
│   ├── vnode_variance.py  virtual-node key distribution
│   ├── throughput.py      ops/sec and latency percentiles
│   └── rebalance.py       keys moved on node join/leave
├── docker/                Dockerfile and 5-node Compose setup
├── scripts/
│   ├── run_node.py        node entrypoint
│   └── chaos_test.py      kills a node under load, checks for data loss
├── render.yaml
└── docs/design-decisions.md
```
