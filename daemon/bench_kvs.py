#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import random
import string
import time
from dataclasses import dataclass
from statistics import mean

# -----------------------------
# utils
# -----------------------------
def percentile(sorted_vals, p: float):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

def rand_bytes(n: int) -> bytes:
    return b"x" * n

def make_keys(n: int, key_len: int):
    alphabet = string.ascii_letters + string.digits
    keys = []
    r = random.Random(42)
    for _ in range(n):
        keys.append(("k" + "".join(r.choice(alphabet) for _ in range(key_len - 1))).encode())
    return keys


# -----------------------------
# Hinotetsu client (README protocol)
#   set <key> <value>\n -> STORED
#   get <key>\n -> <value>\n or NOT_FOUND\n (assumption)
# -----------------------------
class HinotetsuClient:
    def __init__(self, host: str, port: int, timeout_s: float = 5.0):
        self.host = host
        self.port = port
        self.timeout_s = timeout_s
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout_s
        )

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

    async def set(self, key: bytes, value: bytes) -> None:
        # Hinotetsu CLI example: "set name Alice"
        # Use \n (most servers accept \r\n too; \n matches typical simple parsers)
        cmd = b"set " + key + b" " + value + b"\n"
        self.writer.write(cmd)
        await self.writer.drain()

        line = await self.reader.readline()
        if not line:
            raise RuntimeError("no response")
        if line.strip() != b"STORED":
            raise RuntimeError(f"set failed: {line!r}")

    async def get(self, key: bytes) -> bytes | None:
        self.writer.write(b"get " + key + b"\n")
        await self.writer.drain()

        line = await self.reader.readline()
        if not line:
            raise RuntimeError("no response")
        v = line.rstrip(b"\r\n")
        if v in (b"NOT_FOUND", b"END", b""):
            return None
        return v


# -----------------------------
# Memcached text protocol client
# -----------------------------
class MemcachedTextClient:
    """
    set <key> <flags> <exptime> <bytes>\r\n<data>\r\n -> STORED\r\n
    get <key>\r\n -> VALUE ... \r\n<data>\r\nEND\r\n   or END\r\n
    """
    def __init__(self, host: str, port: int, timeout_s: float = 5.0):
        self.host = host
        self.port = port
        self.timeout_s = timeout_s
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout_s
        )

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

    async def set(self, key: bytes, value: bytes, exptime: int = 0) -> None:
        header = (
            b"set " + key + b" 0 " + str(exptime).encode() + b" " + str(len(value)).encode() + b"\r\n"
        )
        self.writer.write(header)
        self.writer.write(value + b"\r\n")
        await self.writer.drain()

        line = await self.reader.readline()
        if not line:
            raise RuntimeError("no response")
        if line.strip() != b"STORED":
            raise RuntimeError(f"set failed: {line!r}")

    async def get(self, key: bytes) -> bytes | None:
        self.writer.write(b"get " + key + b"\r\n")
        await self.writer.drain()

        first = await self.reader.readline()
        if not first:
            raise RuntimeError("no response")
        if first.strip() == b"END":
            return None
        if not first.startswith(b"VALUE "):
            raise RuntimeError(f"bad response: {first!r}")

        parts = first.split()
        nbytes = int(parts[3])
        data = await self.reader.readexactly(nbytes)
        _ = await self.reader.readexactly(2)  # \r\n
        end = await self.reader.readline()
        if end.strip() != b"END":
            raise RuntimeError(f"expected END, got: {end!r}")
        return data


# -----------------------------
# Redis client (async via redis-py)
# -----------------------------
async def redis_make_client(host: str, port: int):
    try:
        import redis.asyncio as redis
    except Exception as e:
        raise RuntimeError("redis-py not installed. pip install redis") from e
    r = redis.Redis(host=host, port=port, decode_responses=False)
    await r.ping()
    return r


# -----------------------------
# Benchmark core
# -----------------------------
@dataclass
class BenchResult:
    name: str
    ops: int
    seconds: float
    lat_ms: list[float]

    def summary(self):
        s = sorted(self.lat_ms)
        return {
            "name": self.name,
            "ops": self.ops,
            "seconds": self.seconds,
            "ops_per_sec": self.ops / self.seconds if self.seconds > 0 else 0,
            "avg_ms": mean(self.lat_ms) if self.lat_ms else None,
            "p50_ms": percentile(s, 0.50),
            "p95_ms": percentile(s, 0.95),
            "p99_ms": percentile(s, 0.99),
        }


async def bench_hinotetsu(host: str, port: int, keys: list[bytes], value_size: int,
                         concurrency: int, total_ops: int, mode: str):
    lat = []
    payload = rand_bytes(value_size)

    clients = [HinotetsuClient(host, port) for _ in range(concurrency)]
    for c in clients:
        await c.connect()

    async def worker(idx: int, count: int):
        c = clients[idx]
        r = random.Random(1000 + idx)
        for i in range(count):
            key = keys[r.randrange(len(keys))]
            t0 = time.perf_counter()
            if mode == "set":
                await c.set(key, payload)
            elif mode == "get":
                _ = await c.get(key)
            elif mode == "mixed":
                if (i & 1) == 0:
                    await c.set(key, payload)
                else:
                    _ = await c.get(key)
            else:
                raise ValueError(mode)
            t1 = time.perf_counter()
            lat.append((t1 - t0) * 1000.0)

    per = total_ops // concurrency
    rem = total_ops % concurrency
    counts = [per + (1 if i < rem else 0) for i in range(concurrency)]

    t0 = time.perf_counter()
    await asyncio.gather(*[worker(i, counts[i]) for i in range(concurrency)])
    t1 = time.perf_counter()

    for c in clients:
        await c.close()

    return BenchResult(name=f"hinotetsu:{mode}", ops=total_ops, seconds=t1 - t0, lat_ms=lat)


async def bench_memcached(host: str, port: int, keys: list[bytes], value_size: int,
                         concurrency: int, total_ops: int, mode: str, exptime: int):
    lat = []
    payload = rand_bytes(value_size)

    clients = [MemcachedTextClient(host, port) for _ in range(concurrency)]
    for c in clients:
        await c.connect()

    async def worker(idx: int, count: int):
        c = clients[idx]
        r = random.Random(2000 + idx)
        for i in range(count):
            key = keys[r.randrange(len(keys))]
            t0 = time.perf_counter()
            if mode == "set":
                await c.set(key, payload, exptime=exptime)
            elif mode == "get":
                _ = await c.get(key)
            elif mode == "mixed":
                if (i & 1) == 0:
                    await c.set(key, payload, exptime=exptime)
                else:
                    _ = await c.get(key)
            else:
                raise ValueError(mode)
            t1 = time.perf_counter()
            lat.append((t1 - t0) * 1000.0)

    per = total_ops // concurrency
    rem = total_ops % concurrency
    counts = [per + (1 if i < rem else 0) for i in range(concurrency)]

    t0 = time.perf_counter()
    await asyncio.gather(*[worker(i, counts[i]) for i in range(concurrency)])
    t1 = time.perf_counter()

    for c in clients:
        await c.close()

    return BenchResult(name=f"memcached:{mode}", ops=total_ops, seconds=t1 - t0, lat_ms=lat)


async def bench_redis(host: str, port: int, keys: list[bytes], value_size: int,
                     concurrency: int, total_ops: int, mode: str, exptime: int):
    r = await redis_make_client(host, port)
    lat = []
    payload = rand_bytes(value_size)

    async def worker(idx: int, count: int):
        rr = random.Random(3000 + idx)
        for i in range(count):
            key = keys[rr.randrange(len(keys))]
            t0 = time.perf_counter()
            if mode == "set":
                if exptime > 0:
                    await r.set(key, payload, ex=exptime)
                else:
                    await r.set(key, payload)
            elif mode == "get":
                _ = await r.get(key)
            elif mode == "mixed":
                if (i & 1) == 0:
                    if exptime > 0:
                        await r.set(key, payload, ex=exptime)
                    else:
                        await r.set(key, payload)
                else:
                    _ = await r.get(key)
            else:
                raise ValueError(mode)
            t1 = time.perf_counter()
            lat.append((t1 - t0) * 1000.0)

    per = total_ops // concurrency
    rem = total_ops % concurrency
    counts = [per + (1 if i < rem else 0) for i in range(concurrency)]

    t0 = time.perf_counter()
    await asyncio.gather(*[worker(i, counts[i]) for i in range(concurrency)])
    t1 = time.perf_counter()

    await r.aclose()
    return BenchResult(name=f"redis:{mode}", ops=total_ops, seconds=t1 - t0, lat_ms=lat)


def print_table(results: list[BenchResult]):
    rows = [r.summary() for r in results]
    headers = ["name", "ops", "seconds", "ops_per_sec", "avg_ms", "p50_ms", "p95_ms", "p99_ms"]
    print("\n=== results ===")
    print("\t".join(headers))
    for row in rows:
        print("\t".join([
            str(row["name"]),
            str(row["ops"]),
            f'{row["seconds"]:.3f}',
            f'{row["ops_per_sec"]:.1f}',
            f'{row["avg_ms"]:.3f}' if row["avg_ms"] is not None else "NA",
            f'{row["p50_ms"]:.3f}' if row["p50_ms"] is not None else "NA",
            f'{row["p95_ms"]:.3f}' if row["p95_ms"] is not None else "NA",
            f'{row["p99_ms"]:.3f}' if row["p99_ms"] is not None else "NA",
        ]))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hinotetsu-host", default="127.0.0.1")
    ap.add_argument("--hinotetsu-port", type=int, default=11211)
    ap.add_argument("--memcached-host", default="127.0.0.1")
    ap.add_argument("--memcached-port", type=int, default=11212)
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", type=int, default=6379)

    ap.add_argument("--keyspace", type=int, default=10000)
    ap.add_argument("--key-len", type=int, default=16)
    ap.add_argument("--value-size", type=int, default=256)
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--ops", type=int, default=200000)
    ap.add_argument("--mode", choices=["set", "get", "mixed"], default="mixed")
    ap.add_argument("--ttl", type=int, default=0, help="memcached/redis only (seconds)")
    ap.add_argument("--targets", default="hinotetsu,memcached,redis",
                    help="comma-separated: hinotetsu,memcached,redis")
    args = ap.parse_args()

    keys = make_keys(args.keyspace, args.key_len)
    preload_ops = min(args.keyspace, 20000)

    targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    results = []

    if "hinotetsu" in targets:
        if args.mode in ("get", "mixed"):
            await bench_hinotetsu(args.hinotetsu_host, args.hinotetsu_port, keys,
                                  args.value_size, min(32, args.concurrency), preload_ops, "set")
        results.append(await bench_hinotetsu(args.hinotetsu_host, args.hinotetsu_port, keys,
                                             args.value_size, args.concurrency, args.ops, args.mode))

    if "memcached" in targets:
        if args.mode in ("get", "mixed"):
            await bench_memcached(args.memcached_host, args.memcached_port, keys,
                                  args.value_size, min(32, args.concurrency), preload_ops, "set", args.ttl)
        results.append(await bench_memcached(args.memcached_host, args.memcached_port, keys,
                                             args.value_size, args.concurrency, args.ops, args.mode, args.ttl))

    if "redis" in targets:
        if args.mode in ("get", "mixed"):
            await bench_redis(args.redis_host, args.redis_port, keys,
                              args.value_size, min(32, args.concurrency), preload_ops, "set", args.ttl)
        results.append(await bench_redis(args.redis_host, args.redis_port, keys,
                                         args.value_size, args.concurrency, args.ops, args.mode, args.ttl))

    print_table(results)


if __name__ == "__main__":
    asyncio.run(main())
