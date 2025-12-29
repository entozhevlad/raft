from fastapi import FastAPI

from src.app.api.healthz import router as health_router
from src.app.api.kv_routes import router as kv_router
from src.app.api.raft_routes import router as raft_router
from src.app.config import load_node_config
from src.app.raft.node import RaftNode
from src.app.raft.persistence import ensure_node_data_dir, load_node_state
from src.app.raft.runtime import setup_runtime
from src.app.utils.logging import setup_logging


def create_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="Raft KV Store",
        version="0.1.0",
        description="Распределённое key-value хранилище на основе RAFT",
    )

    config = load_node_config()

    raft_node = RaftNode(node_id=config.node_id, peers=config.peers)
    raft_node.peer_addresses = dict(config.peer_addresses)

    node_data_dir = ensure_node_data_dir(config.data_dir, config.node_id)

    load_node_state(raft_node, node_data_dir)
    raft_node.apply_committed_entries()

    app.state.raft_node = raft_node
    app.state.peer_addresses = raft_node.peer_addresses
    app.state.data_dir = node_data_dir

    setup_runtime(app)

    app.include_router(health_router)
    app.include_router(raft_router, prefix="/raft")
    app.include_router(kv_router, prefix="/kv")

    return app


app = create_app()
