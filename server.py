#!/usr/bin/env python3
"""
骰界 Relay Server
WebSocket relay for cross-platform multiplayer room codes.

Usage: python relay/server.py --port 8765
Protocol version: "2"

Peer IDs are room-relative (host always gets 1, clients get 2, 3, …).
This mirrors ENet behaviour so Godot net.gd needs no special casing.

v2 (断线续连):
  - 每个 peer 进房时拿到一个 token。连接意外断开后房间席位保留 RESUME_GRACE 秒，
    期间其余人收到 peer_away；用 {"type":"resume", room, peer_id, token} 可原位归来
    （peer_id 不变），其余人收到 peer_back。
  - 宽限期过后才真正移除并广播 peer_left；若离开的是 host，房间随即关闭（room_closed）。
  - 主动离开发 {"type":"leave"}，立即生效，不走宽限期。
  - relay 只转发消息，不掷骰、不改状态、不保存权威局面。
"""

import argparse
import asyncio
import http
import json
import logging
import os
import random
import secrets
import string
import time
from typing import Any, Dict, Optional

try:
    import websockets
except ImportError:
    raise SystemExit("Missing dependency: pip install 'websockets>=13'")

# ─── Config ────────────────────────────────────────────────────────────────────
RELAY_VERSION = "2"
ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # 去掉易混淆的 I/O/0/1
ROOM_CODE_LENGTH = 6
MAX_ROOM_PEERS = 4        # 房间人数上限（与 Godot Net.MAX_PEERS / ENet 一致）
HEARTBEAT_INTERVAL = 15   # seconds between server pings
HEARTBEAT_TIMEOUT = 45    # seconds without activity before the socket is dropped
RESUME_GRACE = 150        # seconds a dropped peer's seat is kept for resume
SWEEP_INTERVAL = 2        # seconds between away-expiry sweeps
MAX_MESSAGE_BYTES = 256 * 1024

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("relay")


# ─── Data model ────────────────────────────────────────────────────────────────
class Peer:
    """One seat in a room (or a not-yet-seated connection)."""
    _global_counter = 0

    def __init__(self, ws: Any) -> None:
        Peer._global_counter += 1
        self.global_id: int = Peer._global_counter   # unique server-lifetime ID
        self.room_peer_id: int = 0                   # room-relative ID (host=1)
        self.ws: Any = ws                            # None while away
        self.room_code: Optional[str] = None
        self.is_host: bool = False
        self.token: str = ""
        self.last_activity: float = time.time()
        self.away_since: float = 0.0

    @property
    def away(self) -> bool:
        return self.ws is None

    async def send(self, msg: dict) -> None:
        if self.ws is None:
            return
        try:
            await self.ws.send(json.dumps(msg, ensure_ascii=False))
        except Exception:
            pass  # connection may already be closing


class Room:
    """One relay room. `peers` is keyed by room-relative peer IDs."""

    def __init__(self, code: str) -> None:
        self.code = code
        self.host: Optional[Peer] = None
        self.peers: Dict[int, Peer] = {}   # room_peer_id → Peer
        self._next_room_peer_id: int = 1

    def add(self, peer: Peer) -> int:
        rid = self._next_room_peer_id
        self._next_room_peer_id += 1
        peer.room_peer_id = rid
        peer.room_code = self.code
        peer.token = secrets.token_urlsafe(18)
        self.peers[rid] = peer
        return rid

    def remove(self, peer: Peer) -> None:
        if peer.room_peer_id:
            self.peers.pop(peer.room_peer_id, None)

    def present_ids(self) -> list:
        return sorted(rid for rid, p in self.peers.items() if not p.away)

    async def broadcast(self, msg: dict, exclude_rid: Optional[int] = None) -> None:
        for rid, peer in list(self.peers.items()):
            if exclude_rid is not None and rid == exclude_rid:
                continue
            await peer.send(msg)


# ─── Global state ──────────────────────────────────────────────────────────────
rooms: Dict[str, Room] = {}
all_peers: Dict[int, Peer] = {}   # global_id → Peer with a live socket (for heartbeat)


def _gen_code() -> str:
    for _ in range(200):
        code = "".join(random.choices(ROOM_CODE_CHARS, k=ROOM_CODE_LENGTH))
        if code not in rooms:
            return code
    raise RuntimeError("Room code space exhausted")


async def _close_room(room: Room, reason: str) -> None:
    """Host is gone for good: tell everyone and drop the room."""
    rooms.pop(room.code, None)
    for peer in list(room.peers.values()):
        await peer.send({"type": "room_closed", "reason": reason})
        peer.room_code = None
        if peer.ws is not None:
            try:
                await peer.ws.close(1000, "room closed")
            except Exception:
                pass
    room.peers.clear()
    log.info("Room %s closed (%s)", room.code, reason)


async def _remove_peer(room: Room, peer: Peer, reason: str) -> None:
    """Final removal of a seat (explicit leave or grace expiry)."""
    rid = peer.room_peer_id
    room.remove(peer)
    peer.room_code = None
    if peer.is_host:
        await _close_room(room, "host_left")
        return
    await room.broadcast({"type": "peer_left", "peer_id": rid})
    log.info("Room %s: peer #%d removed (%s)", room.code, rid, reason)
    if not room.peers:
        rooms.pop(room.code, None)
        log.info("Room %s cleaned up (empty)", room.code)


# ─── Message handlers ──────────────────────────────────────────────────────────
async def _handle_create(peer: Peer) -> None:
    if peer.room_code:
        await peer.send({"type": "error", "code": "ALREADY_IN_ROOM"})
        return
    code = _gen_code()
    room = Room(code)
    rid = room.add(peer)   # host always gets room_peer_id = 1
    room.host = peer
    peer.is_host = True
    rooms[code] = room
    await peer.send({"type": "room_created", "room": code, "peer_id": rid, "token": peer.token})
    log.info("[g%d] Created room %s as peer #%d", peer.global_id, code, rid)


async def _handle_join(peer: Peer, msg: dict) -> None:
    if peer.room_code:
        await peer.send({"type": "error", "code": "ALREADY_IN_ROOM"})
        return
    code = str(msg.get("room", "")).upper().strip()
    room = rooms.get(code)
    if room is None:
        await peer.send({"type": "error", "code": "ROOM_NOT_FOUND"})
        return
    if len(room.peers) >= MAX_ROOM_PEERS:
        await peer.send({"type": "error", "code": "ROOM_FULL"})
        log.info("[g%d] Join rejected, room %s full (%d/%d)",
                 peer.global_id, code, len(room.peers), MAX_ROOM_PEERS)
        return
    if room.host is None or room.host.away:
        await peer.send({"type": "error", "code": "HOST_AWAY"})
        return
    rid = room.add(peer)
    await peer.send({
        "type": "room_joined",
        "room": code,
        "peer_id": rid,
        "host_peer_id": room.host.room_peer_id,
        "token": peer.token,
    })
    await room.host.send({"type": "peer_joined", "peer_id": rid})
    log.info("[g%d] Joined room %s as peer #%d", peer.global_id, code, rid)


async def _handle_resume(conn: Peer, msg: dict) -> Optional[Peer]:
    """Re-attach a fresh connection to an existing seat. Returns the seat on success."""
    code = str(msg.get("room", "")).upper().strip()
    room = rooms.get(code)
    try:
        rid = int(msg.get("peer_id", 0))
    except (ValueError, TypeError):
        rid = 0
    seat = room.peers.get(rid) if room else None
    token = str(msg.get("token", ""))
    if seat is None or not token or not secrets.compare_digest(seat.token, token):
        await conn.send({"type": "error", "code": "RESUME_FAILED"})
        return None
    # 旧连接可能尚未被判定断开（半开连接）：顶掉它
    old_ws = seat.ws
    if old_ws is not None and old_ws is not conn.ws:
        all_peers.pop(seat.global_id, None)
        try:
            await old_ws.close(1000, "superseded")
        except Exception:
            pass
    was_away = seat.away or old_ws is not None
    all_peers.pop(conn.global_id, None)
    seat.ws = conn.ws
    seat.away_since = 0.0
    seat.last_activity = time.time()
    all_peers[seat.global_id] = seat
    await seat.send({
        "type": "resumed",
        "room": code,
        "peer_id": rid,
        "host_peer_id": room.host.room_peer_id if room.host else 1,
        "present": room.present_ids(),
    })
    if was_away:
        await room.broadcast({"type": "peer_back", "peer_id": rid}, exclude_rid=rid)
    log.info("[g%d] Resumed room %s as peer #%d", seat.global_id, code, rid)
    return seat


async def _handle_relay(peer: Peer, msg: dict) -> None:
    room = rooms.get(peer.room_code or "")
    if room is None:
        await peer.send({"type": "error", "code": "NOT_IN_ROOM"})
        return
    to = msg.get("to", "all")
    envelope = {
        "type": "relay",
        "from": peer.room_peer_id,
        "to": to,
        "data": msg.get("data", {}),
    }
    if to == "all":
        await room.broadcast(envelope, exclude_rid=peer.room_peer_id)
        return
    try:
        target_rid = int(to)
    except (ValueError, TypeError):
        await peer.send({"type": "error", "code": "INVALID_TARGET"})
        return
    target = room.peers.get(target_rid)
    if target:
        await target.send(envelope)


async def _on_socket_closed(peer: Peer, ws: Any) -> None:
    """The socket died. Keep the seat for RESUME_GRACE unless it was superseded."""
    if peer.ws is not ws:
        return  # this seat already resumed on a newer connection
    all_peers.pop(peer.global_id, None)
    peer.ws = None
    room = rooms.get(peer.room_code or "")
    if room is None or room.peers.get(peer.room_peer_id) is not peer:
        return
    peer.away_since = time.time()
    await room.broadcast({"type": "peer_away", "peer_id": peer.room_peer_id}, exclude_rid=peer.room_peer_id)
    log.info("Room %s: peer #%d away (seat kept %ds)", room.code, peer.room_peer_id, RESUME_GRACE)


# ─── Connection handler ────────────────────────────────────────────────────────
async def handle_connection(ws: Any) -> None:
    peer = Peer(ws)
    all_peers[peer.global_id] = peer
    log.info("[g%d] Connected from %s", peer.global_id, getattr(ws, "remote_address", "?"))

    try:
        # Version handshake: first message must be {"type":"hello","version":"2"}
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
        except asyncio.TimeoutError:
            await peer.send({"type": "error", "code": "HANDSHAKE_TIMEOUT"})
            return
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            await peer.send({"type": "error", "code": "INVALID_JSON"})
            return
        if not isinstance(msg, dict) or msg.get("type") != "hello":
            await peer.send({"type": "error", "code": "EXPECTED_HELLO"})
            return
        if str(msg.get("version", "")) != RELAY_VERSION:
            log.warning("[g%d] Version mismatch: client=%r expected=%r",
                        peer.global_id, msg.get("version"), RELAY_VERSION)
            await peer.send({"type": "error", "code": "VERSION_MISMATCH"})
            return

        await peer.send({"type": "hello_ok", "version": RELAY_VERSION})
        peer.last_activity = time.time()

        async for raw in ws:
            peer.last_activity = time.time()
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                await peer.send({"type": "error", "code": "INVALID_JSON"})
                continue
            if not isinstance(msg, dict):
                await peer.send({"type": "error", "code": "INVALID_JSON"})
                continue

            mtype = msg.get("type", "")
            if mtype == "relay":
                await _handle_relay(peer, msg)
            elif mtype == "pong":
                pass  # last_activity already updated
            elif mtype == "create":
                await _handle_create(peer)
            elif mtype == "join":
                await _handle_join(peer, msg)
            elif mtype == "resume":
                if peer.room_code:
                    await peer.send({"type": "error", "code": "ALREADY_IN_ROOM"})
                    continue
                seat = await _handle_resume(peer, msg)
                if seat is not None:
                    peer = seat   # this connection now speaks for the resumed seat
            elif mtype == "leave":
                room = rooms.get(peer.room_code or "")
                if room is not None:
                    all_peers.pop(peer.global_id, None)
                    peer.ws = None
                    await _remove_peer(room, peer, "leave")
                return
            else:
                await peer.send({"type": "error", "code": "UNKNOWN_TYPE"})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        log.info("[g%d] Socket closed (room=%s peer#%d)",
                 peer.global_id, peer.room_code or "-", peer.room_peer_id)
        await _on_socket_closed(peer, ws)


# ─── Background loops ──────────────────────────────────────────────────────────
async def heartbeat_loop() -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        for peer in list(all_peers.values()):
            if peer.ws is None:
                continue
            if now - peer.last_activity > HEARTBEAT_TIMEOUT:
                log.info("[g%d] Heartbeat timeout (%.0fs), closing", peer.global_id, now - peer.last_activity)
                try:
                    await peer.ws.close(1001, "heartbeat timeout")
                except Exception:
                    pass
            else:
                await peer.send({"type": "ping"})


async def sweep_loop() -> None:
    """Expire seats whose owner did not resume within the grace period."""
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        now = time.time()
        for room in list(rooms.values()):
            for peer in list(room.peers.values()):
                if peer.away and peer.away_since and now - peer.away_since > RESUME_GRACE:
                    await _remove_peer(room, peer, "grace expired")
                    if room.code not in rooms:
                        break


def _health_check(connection: Any, request: Any) -> Any:
    """Plain HTTP GET /health → 200，方便云平台健康检查；其余请求走 WebSocket 握手。"""
    if getattr(request, "path", "") == "/health":
        return connection.respond(http.HTTPStatus.OK, "OK rooms=%d\n" % len(rooms))
    return None


# ─── Entry point ───────────────────────────────────────────────────────────────
async def main(host: str, port: int) -> None:
    log.info("骰界 Relay Server v%s  %s:%d", RELAY_VERSION, host, port)
    log.info("Heartbeat every %ds (timeout %ds); resume grace %ds",
             HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, RESUME_GRACE)

    tasks = [asyncio.create_task(heartbeat_loop()), asyncio.create_task(sweep_loop())]
    try:
        async with websockets.serve(
            handle_connection,
            host,
            port,
            ping_interval=None,  # application-level pings only
            ping_timeout=None,
            max_size=MAX_MESSAGE_BYTES,
            process_request=_health_check,
        ):
            log.info("Listening on ws://%s:%d", host, port)
            await asyncio.Future()  # run forever
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="骰界 Relay Server")
    parser.add_argument("--host", default=os.environ.get("RELAY_HOST", "0.0.0.0"), help="Bind address")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")),
                        help="WebSocket port (default: $PORT or 8765)")
    parser.add_argument("--grace", type=int, default=RESUME_GRACE, help="Seconds to keep a dropped peer's seat")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    RESUME_GRACE = max(1, args.grace)
    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        pass
