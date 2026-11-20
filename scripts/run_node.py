import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from distcache.node import CacheNode


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--capacity", type=int, default=100_000)
    args = parser.parse_args()

    node = CacheNode(capacity=args.capacity)
    server = await node.serve(args.host, args.port)
    print(f"ready on {args.host}:{args.port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
