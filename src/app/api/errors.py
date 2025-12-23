from __future__ import annotations

from typing import Any, Dict, Optional


def not_leader(*, node_id: str, role: str, leader_id: Optional[str], leader_address: Optional[str] = None) -> Dict[str, Any]:
    detail: Dict[str, Any] = {
        "error": "not_leader",
        "node_id": node_id,
        "role": role,
        "leader_id": leader_id,
    }
    if leader_address:
        detail["leader_address"] = leader_address
    return detail


def key_not_found(key: str) -> Dict[str, Any]:
    return {"error": "key_not_found", "key": key}


def bad_request(reason: str) -> Dict[str, Any]:
    return {"error": "bad_request", "reason": reason}

