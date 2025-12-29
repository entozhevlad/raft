from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Dict

import httpx
from fastapi import FastAPI

from src.app.raft.models import RequestVoteRequest
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.persistence import save_full_state, save_metadata
from src.app.raft.replication import advance_commit_index, replicate_to_peer

logger = logging.getLogger("raft")


ELECTION_TIMEOUT_RANGE = (1.5, 3.0)
HEARTBEAT_INTERVAL = 0.5


def start_background_tasks(app: FastAPI, node: RaftNode) -> list[asyncio.Task]:
    """Запуск election_loop и heartbead_loop в фоне."""
    logger.info("[%s] start election timer/heartbeat tasks", node.node_id)
    tasks = [
        asyncio.create_task(election_loop(app, node), name=f"election_loop-{node.node_id}"),
        asyncio.create_task(heartbeat_loop(app, node), name=f"heartbeat_loop-{node.node_id}"),
    ]
    return tasks


async def stop_background_tasks(tasks: list[asyncio.Task]) -> None:
    """Останавливает фоновые задачи."""
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _request_vote_rpc(
    *,
    client: httpx.AsyncClient,
    node: RaftNode,
    peer_id: str,
    base_url: str,
    current_term: int,
    last_index: int,
    last_term: int,
) -> tuple[str, int, bool]:
    """Запросить голос у ноды."""

    req = RequestVoteRequest(
        term=current_term,
        candidate_id=node.node_id,
        last_log_index=last_index,
        last_log_term=last_term,
    )

    resp = await client.post(f"{base_url}/raft/request_vote", json=req.model_dump())
    if resp.status_code != 200:
        return peer_id, current_term, False

    data = resp.json()
    resp_term = int(data.get("term", current_term))
    vote_granted = bool(data.get("vote_granted", False))
    return peer_id, resp_term, vote_granted


async def election_loop(app: FastAPI, node: RaftNode) -> None:
    """Функция цикла выборов."""
    peer_addresses: Dict[str, str] = app.state.peer_addresses
    node_data_dir: str = app.state.data_dir

    while True:
        timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)
        await asyncio.sleep(timeout)

        if node.role == RaftRole.LEADER:
            continue

        now = time.monotonic()
        if now - node.last_heartbeat_ts < timeout:
            continue

        node.become_candidate()
        save_metadata(node, node_data_dir)

        current_term = node.current_term
        last_index, last_term = node.get_last_log_index_term()

        votes_granted_by = {node.node_id}

        logger.info(
            "[%s] Start election term=%s (cluster=%s)",
            node.node_id,
            current_term,
            sorted(list(node.get_cluster_nodes())),
        )

        targets: list[tuple[str, str]] = []
        for peer_id, base_url in peer_addresses.items():
            if peer_id == node.node_id:
                continue
            targets.append((peer_id, base_url))

        if not targets:
            if node.role == RaftRole.CANDIDATE and node.current_term == current_term:
                node.become_leader()
            continue

        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks = [
                asyncio.create_task(
                    _request_vote_rpc(
                        client=client,
                        node=node,
                        peer_id=peer_id,
                        base_url=base_url,
                        current_term=current_term,
                        last_index=last_index,
                        last_term=last_term,
                    ),
                    name=f"request_vote-{node.node_id}-to-{peer_id}-t{current_term}",
                )
                for peer_id, base_url in targets
            ]

            try:
                for fut in asyncio.as_completed(tasks):
                    if node.role != RaftRole.CANDIDATE or node.current_term != current_term:
                        break

                    try:
                        peer_id, resp_term, vote_granted = await fut
                    except Exception as exc:
                        logger.warning(
                            "[%s] RequestVote task failed: %r",
                            node.node_id,
                            exc,
                        )
                        continue

                    if resp_term > node.current_term:
                        node.become_follower(resp_term)
                        save_metadata(node, node_data_dir)
                        break

                    if vote_granted:
                        votes_granted_by.add(peer_id)
                        logger.info(
                            "[%s] vote from %s (votes_by=%s)",
                            node.node_id,
                            peer_id,
                            sorted(list(votes_granted_by)),
                        )
                        if len(votes_granted_by) >= node.majority():
                            node.become_leader()
                            break
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

        if (
            node.role == RaftRole.CANDIDATE
            and node.current_term == current_term
            and len(votes_granted_by) >= node.majority()
        ):
            node.become_leader()


async def heartbeat_loop(app: FastAPI, node: RaftNode) -> None:
    """Цикл heartbeat'ов лидера."""

    peer_addresses: Dict[str, str] = app.state.peer_addresses
    node_data_dir: str = app.state.data_dir

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)

        if node.role != RaftRole.LEADER:
            continue

        cluster_size = 1 + len(peer_addresses)

        async with httpx.AsyncClient(timeout=1.0) as client:
            leader_commit = node.commit_index

            for peer_id, base_url in peer_addresses.items():
                if peer_id == node.node_id:
                    continue

                ok = await replicate_to_peer(
                    client=client,
                    node=node,
                    peer_id=peer_id,
                    base_url=base_url,
                    leader_commit=leader_commit,
                )

                if node.role != RaftRole.LEADER:
                    save_metadata(node, node_data_dir)
                    break

                logger.debug(
                    "[%s] heartbeat/replicate peer=%s ok=%s nextIndex=%s matchIndex=%s",
                    node.node_id,
                    peer_id,
                    ok,
                    node.next_index.get(peer_id),
                    node.match_index.get(peer_id),
                )

            if node.role != RaftRole.LEADER:
                continue

            advanced = advance_commit_index(node, cluster_size=cluster_size)
            if advanced is not None:
                logger.info(
                    "[%s] leader advanced commit_index -> %s",
                    node.node_id,
                    node.commit_index,
                )
                node.apply_committed_entries()
                save_full_state(node, node_data_dir)

            save_metadata(node, node_data_dir)