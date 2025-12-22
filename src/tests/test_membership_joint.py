import pytest

from src.app.raft.log import LogEntry
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.replication import advance_commit_index


def _seed_log(node: RaftNode, *, last_index: int, term: int) -> None:
    node.log.entries = []
    for i in range(1, last_index + 1):
        node.log.append([LogEntry(term=term, index=i, command={"op": "noop"})])


def test_joint_consensus_commit_requires_both_majorities():
    # old: n1 n2 n3   (maj=2)
    # new: n1 n2 n3 n4 (maj=3)
    node = RaftNode(node_id="n1", peers=["n2", "n3", "n4"])
    node.role = RaftRole.LEADER
    node.current_term = 7

    node.joint_old = {"n1", "n2", "n3"}
    node.joint_new = {"n1", "n2", "n3", "n4"}
    node.members = set(node.joint_old) | set(node.joint_new)
    node.peers = sorted(list(node.members - {"n1"}))

    _seed_log(node, last_index=5, term=7)
    node.commit_index = 4

    # Репликация: есть только self и n2 -> для old кворум есть (2/3), для new нет (2/4)
    node.match_index = {"n2": 5, "n3": 0, "n4": 0}
    advanced = advance_commit_index(node)
    assert advanced is None
    assert node.commit_index == 4

    # Добавляем n3 -> для new теперь 3/4, для old 3/3
    node.match_index["n3"] = 5
    advanced2 = advance_commit_index(node)
    assert advanced2 == 5
    assert node.commit_index == 5


def test_apply_cluster_config_final_clears_joint_and_updates_peers():
    node = RaftNode(node_id="n1", peers=["n2", "n3"])
    node.members = {"n1", "n2", "n3"}
    node.peer_addresses = {"n2": "http://n2:8000", "n3": "http://n3:8000"}

    # Входим в joint
    node.apply_cluster_config(
        {
            "op": "config",
            "phase": "joint",
            "old": ["n1", "n2", "n3"],
            "new": ["n1", "n2"],
            "peer_addresses": {"n2": "http://n2:8000", "n3": "http://n3:8000"},
        }
    )
    assert node.is_joint()

    # Финализируем new конфиг
    node.apply_cluster_config(
        {
            "op": "config",
            "phase": "final",
            "members": ["n1", "n2"],
            "peer_addresses": {"n2": "http://n2:8000"},
        }
    )

    assert not node.is_joint()
    assert node.members == {"n1", "n2"}
    assert node.peers == ["n2"]
    assert "n3" not in node.peer_addresses