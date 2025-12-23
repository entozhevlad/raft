from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.timers import _request_vote_rpc


class DummyResp:
    def __init__(self, term: int, granted: bool = True, status_code: int = 200):
        self.status_code = status_code
        self._json = {"term": term, "vote_granted": granted}

    def json(self):
        return self._json


@pytest.mark.parametrize("n_peers, delay_s", [(3, 0.2)])
def test_request_vote_parallel_timing(monkeypatch, n_peers: int, delay_s: float):
    """
    Проверяем параллельность на уровне helper-а:
    - у каждого peer задержка delay_s
    - если бы было последовательно: ~ n_peers*delay_s
    - параллельно: ~ delay_s (с небольшим оверхедом)
    """

    async def fake_post(self, url, json):
        await asyncio.sleep(delay_s)
        return DummyResp(term=int(json["term"]), granted=True)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post, raising=True)

    node = RaftNode(node_id="nodeX", peers=[f"n{i}" for i in range(n_peers)])
    node.role = RaftRole.CANDIDATE
    node.current_term = 1

    async def run():
        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks = [
                asyncio.create_task(
                    _request_vote_rpc(
                        client=client,
                        node=node,
                        peer_id=f"n{i}",
                        base_url=f"http://n{i}:8000",
                        current_term=1,
                        last_index=0,
                        last_term=0,
                    )
                )
                for i in range(n_peers)
            ]
            t0 = time.monotonic()
            await asyncio.gather(*tasks)
            t1 = time.monotonic()
            return t1 - t0

    elapsed = asyncio.run(run())

    # параллельно должно быть близко к delay_s, а не n_peers*delay_s
    assert elapsed < (delay_s * n_peers) * 0.7, f"too slow, looks sequential: elapsed={elapsed:.3f}s"
    assert elapsed < delay_s * 2.5, f"unexpectedly slow: elapsed={elapsed:.3f}s"