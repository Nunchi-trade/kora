#!/usr/bin/env python3
"""Monitor Kora devnet block production during chaos tests."""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

RPC_PORTS = [8545, 8546, 8547, 8548]
NODE_NAMES = ["node0", "node1", "node2", "node3"]


@dataclass
class Sample:
    ts: float
    heights: list[int | None]
    views: list[int | None]
    nullified: list[int | None]


def rpc(port: int, method: str, params: list[Any] | None = None) -> dict[str, Any]:
    body = json.dumps({"jsonrpc": "2.0", "method": method, "params": params or [], "id": 1}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, ConnectionResetError, json.JSONDecodeError) as err:
        raise RuntimeError(str(err)) from err


def sample() -> Sample:
    heights: list[int | None] = []
    views: list[int | None] = []
    nullified: list[int | None] = []
    for port in RPC_PORTS:
        try:
            height = int(rpc(port, "eth_blockNumber")["result"], 16)
            status = rpc(port, "kora_nodeStatus")["result"]
            heights.append(height)
            views.append(int(status.get("currentView", 0)))
            nullified.append(int(status.get("nullifiedCount", 0)))
        except (RuntimeError, KeyError, ValueError):
            heights.append(None)
            views.append(None)
            nullified.append(None)
    return Sample(time.time(), heights, views, nullified)


def fmt_sample(label: str, s: Sample, prev: Sample | None) -> str:
    parts = [f"[{label}] t={s.ts:.0f}"]
    for i, name in enumerate(NODE_NAMES):
        h = s.heights[i]
        v = s.views[i]
        n = s.nullified[i]
        delta = ""
        if prev and h is not None and prev.heights[i] is not None:
            dh = h - prev.heights[i]
            if dh:
                delta = f" (+{dh})"
        parts.append(f"{name}: h={h}{delta} view={v} null={n}")
    if prev and prev.heights[0] is not None and s.heights[0] is not None:
        dt = s.ts - prev.ts
        dh = s.heights[0] - prev.heights[0]
        if dt > 0 and dh >= 0:
            parts.append(f"net_rate={dh/dt:.3f} blk/s (~{dt/max(dh,1):.3f}s/blk)")
    return " | ".join(parts)


def monitor(duration_secs: int, interval: float, label_prefix: str) -> list[str]:
    lines: list[str] = []
    end = time.time() + duration_secs
    prev: Sample | None = None
    while time.time() < end:
        s = sample()
        line = fmt_sample(label_prefix, s, prev)
        print(line, flush=True)
        lines.append(line)
        prev = s
        time.sleep(interval)
    return lines


def main() -> int:
    if len(sys.argv) != 4:
        print(f"usage: {sys.argv[0]} <duration_secs> <interval_secs> <label>", file=sys.stderr)
        return 2
    duration = int(sys.argv[1])
    interval = float(sys.argv[2])
    label = sys.argv[3]
    monitor(duration, interval, label)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
