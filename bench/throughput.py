import asyncio
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from distcache.client import NodeConnection
from distcache.router import Router

NODE_IDS = [f"node-{i}" for i in range(8)]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_node(port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(ROOT / "scripts" / "run_node.py"), "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def wait_ready(port: int, timeout: float = 5.0):
    # a bare connect isn't enough, the node has to answer
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = await NodeConnection.connect("127.0.0.1", port)
            ok = await conn.ping()
            await conn.close()
            if ok:
                return
        except OSError:
            pass
        await asyncio.sleep(0.05)
    raise TimeoutError(f"node on port {port} never became ready")


def percentiles(latencies_ms):
    ordered = sorted(latencies_ms)
    p50 = ordered[len(ordered) // 2]
    p99 = ordered[int(len(ordered) * 0.99)]
    return p50, p99, ordered[-1]


async def put_phase(router: Router, ops: int, key_prefix: str):
    latencies = []
    for i in range(ops):
        key = f"{key_prefix}-{i}".encode()
        value = f"value-{key_prefix}-{i}".encode()
        t0 = time.perf_counter()
        await router.put(key, value)
        latencies.append((time.perf_counter() - t0) * 1000)
    return latencies


async def get_phase(router: Router, ops: int, key_prefix: str):
    latencies = []
    for i in range(ops):
        key = f"{key_prefix}-{i}".encode()
        t0 = time.perf_counter()
        await router.get(key)
        latencies.append((time.perf_counter() - t0) * 1000)
    return latencies


async def run_benchmark(num_nodes: int, num_clients: int = 20, ops_per_client: int = 100):
    ports = [free_port() for _ in range(num_nodes)]
    procs = [start_node(p) for p in ports]
    try:
        for p in ports:
            await wait_ready(p)

        n = min(3, num_nodes)
        r = min(2, n)
        w = min(2, n)
        if r + w <= n:
            r = n

        routers = []
        for _ in range(num_clients):
            router = Router(replication_factor=n, read_quorum=r, write_quorum=w)
            for node_id, port in zip(NODE_IDS, ports):
                await router.add_node(node_id, "127.0.0.1", port)
            routers.append(router)

        total_ops = num_clients * ops_per_client

        put_start = time.perf_counter()
        put_results = await asyncio.gather(*(
            put_phase(routers[i], ops_per_client, f"client{i}") for i in range(num_clients)
        ))
        put_elapsed = time.perf_counter() - put_start

        get_start = time.perf_counter()
        get_results = await asyncio.gather(*(
            get_phase(routers[i], ops_per_client, f"client{i}") for i in range(num_clients)
        ))
        get_elapsed = time.perf_counter() - get_start

        for router in routers:
            await router.close()

        put_p50, put_p99, put_max = percentiles([lat for r in put_results for lat in r])
        get_p50, get_p99, get_max = percentiles([lat for r in get_results for lat in r])

        print(f"\n--- {num_nodes} nodes (N={n} R={r} W={w}), "
              f"{num_clients} concurrent clients x {ops_per_client} ops ---")
        print(f"PUT: {total_ops / put_elapsed:.0f} ops/sec  "
              f"p50={put_p50:.2f}ms  p99={put_p99:.2f}ms  max={put_max:.2f}ms")
        print(f"GET: {total_ops / get_elapsed:.0f} ops/sec  "
              f"p50={get_p50:.2f}ms  p99={get_p99:.2f}ms  max={get_max:.2f}ms")
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait(timeout=5)


async def main():
    for n in (1, 3, 5):
        await run_benchmark(n)


if __name__ == "__main__":
    asyncio.run(main())
