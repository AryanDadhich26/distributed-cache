import asyncio
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from distcache.client import NodeConnection
from distcache.router import Router

COMPOSE_FILE = ROOT / "docker" / "docker-compose.yml"
NODES = [
    ("node-0", 6000),
    ("node-1", 6001),
    ("node-2", 6002),
    ("node-3", 6003),
    ("node-4", 6004),
]
KILL_TARGET = "distcache-node-2"
LOAD_SECONDS = 15
KILL_AFTER_SECONDS = 5
NUM_WORKERS = 10


def compose(*args):
    subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), *args], check=True)


async def wait_ready(host: str, port: int, timeout: float = 30.0):
    # a bare connect isn't enough, the node has to answer
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = await NodeConnection.connect(host, port)
            ok = await conn.ping()
            await conn.close()
            if ok:
                return
        except OSError:
            pass
        await asyncio.sleep(0.2)
    raise TimeoutError(f"{host}:{port} never became ready")


class LoadState:
    def __init__(self):
        self.attempted = 0
        self.succeeded = 0
        self.failed = 0
        self.latencies_ms = []
        self.acknowledged = {}
        self.lock = asyncio.Lock()


async def worker(router: Router, worker_id: int, state: LoadState, stop_event: asyncio.Event):
    i = 0
    while not stop_event.is_set():
        key = f"chaos-{worker_id}-{i}".encode()
        value = f"value-{worker_id}-{i}".encode()
        t0 = time.perf_counter()
        async with state.lock:
            state.attempted += 1
        try:
            await router.put(key, value)
            async with state.lock:
                state.succeeded += 1
                state.acknowledged[key] = value
                state.latencies_ms.append((time.perf_counter() - t0) * 1000)
        except Exception:
            async with state.lock:
                state.failed += 1
        i += 1
        await asyncio.sleep(0.01)


async def main():
    print("building and starting the 5-node cluster...")
    compose("up", "-d", "--build")
    try:
        for _, port in NODES:
            await wait_ready("127.0.0.1", port)
        print("all 5 nodes up")

        router = Router(replication_factor=3, read_quorum=2, write_quorum=2)
        for node_id, port in NODES:
            await router.add_node(node_id, "127.0.0.1", port)
        router.start_heartbeat(interval=0.5, failure_threshold=3)

        state = LoadState()
        stop_event = asyncio.Event()
        workers = [
            asyncio.create_task(worker(router, w, state, stop_event))
            for w in range(NUM_WORKERS)
        ]

        print(f"load running, killing {KILL_TARGET} after {KILL_AFTER_SECONDS}s...")
        await asyncio.sleep(KILL_AFTER_SECONDS)
        subprocess.run(["docker", "kill", KILL_TARGET], check=True)
        print(f"killed {KILL_TARGET}")

        await asyncio.sleep(LOAD_SECONDS - KILL_AFTER_SECONDS)
        stop_event.set()
        await asyncio.gather(*workers)

        print("verifying every acknowledged write is still readable...")
        lost = 0
        for key, expected_value in state.acknowledged.items():
            entry = await router.get(key)
            if entry is None or entry[0] != expected_value:
                lost += 1

        await router.close()

        latencies = sorted(state.latencies_ms)
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        print("\n=== chaos test report ===")
        print(f"duration: {LOAD_SECONDS}s, {NUM_WORKERS} concurrent workers, "
              f"{KILL_TARGET} killed at t={KILL_AFTER_SECONDS}s")
        print(f"writes attempted: {state.attempted}")
        print(f"writes acknowledged (quorum met): {state.succeeded}")
        print(f"writes failed (quorum not met): {state.failed}")
        print(f"ops/sec (acknowledged writes): {state.succeeded / LOAD_SECONDS:.1f}")
        print(f"p50={p50:.2f}ms  p99={p99:.2f}ms")
        print(f"acknowledged writes lost after node kill: {lost}/{len(state.acknowledged)}")
        print("RESULT: PASS - zero data loss for every quorum-acknowledged write"
              if lost == 0 else "RESULT: FAIL - data loss detected")
    finally:
        print("tearing down cluster...")
        compose("down")


if __name__ == "__main__":
    asyncio.run(main())
