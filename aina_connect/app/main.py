from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib

import httpx
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [aina-connect] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

AINA_SERVER: str = os.environ["AINA_SERVER"].rstrip("/")
PAIRING_CODE: str = os.environ.get("PAIRING_CODE", "")

# In HA OS add-on context SUPERVISOR_TOKEN is injected automatically.
# In standalone Docker mode, HA_TOKEN (a Long-Lived Access Token) is used instead.
_SUPERVISOR_TOKEN: str = os.environ.get("SUPERVISOR_TOKEN", "")
_HA_TOKEN: str = os.environ.get("HA_TOKEN", "")
HA_TOKEN: str = _SUPERVISOR_TOKEN or _HA_TOKEN

_HA_URL = os.environ.get("HA_URL", "").rstrip("/")
HA_BASE_URL = _HA_URL or "http://supervisor/core"
HA_WS_URL = (
    (
        _HA_URL.replace("https://", "wss://").replace("http://", "ws://")
        + "/api/websocket"
    )
    if _HA_URL
    else "ws://supervisor/core/websocket"
)
RELAY_TOKEN_PATH = pathlib.Path("/data/relay_token")
HEARTBEAT_INTERVAL = 30
RECONNECT_DELAY = 5


# ---------------------------------------------------------------------------
# Relay token persistence
# ---------------------------------------------------------------------------


def _load_relay_token() -> str | None:
    if RELAY_TOKEN_PATH.exists():
        token = RELAY_TOKEN_PATH.read_text().strip()
        return token if token else None
    return None


def _save_relay_token(token: str) -> None:
    RELAY_TOKEN_PATH.write_text(token)


# ---------------------------------------------------------------------------
# HA local HTTP proxy
# ---------------------------------------------------------------------------


async def _proxy_request(method: str, path: str, body: object) -> tuple[int, object]:
    url = f"{HA_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.request(
                method,
                url,
                headers=headers,
                json=body if body is not None else None,
            )
        try:
            return resp.status_code, resp.json()
        except Exception:
            # HA occasionally returns plain text (e.g. "401 Unauthorized")
            logger.warning(
                f"HA returned non-JSON: status={resp.status_code} body={resp.text[:100]!r}"
            )
            return resp.status_code, {"_text": resp.text}
    except Exception as exc:
        logger.error(f"HA HTTP proxy error: {exc}")
        return 502, {"error": str(exc)}


# ---------------------------------------------------------------------------
# HA local WebSocket — fetch_registries
# ---------------------------------------------------------------------------


async def _fetch_registries() -> dict:
    """
    Open a local HA WebSocket session, authenticate, and pull the three
    config registries in a single connection.  Returns before the socket is
    closed so the caller can forward the result to Aina.
    """
    results: dict[int, object] = {}
    requests = {
        1: "config/area_registry/list",
        2: "config/device_registry/list",
        3: "config/entity_registry/list",
    }

    async with websockets.connect(HA_WS_URL) as ha_ws:
        # Auth handshake
        auth_req = json.loads(await ha_ws.recv())
        if auth_req.get("type") != "auth_required":
            raise RuntimeError(f"Unexpected HA WS message: {auth_req}")

        await ha_ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
        auth_result = json.loads(await ha_ws.recv())
        if auth_result.get("type") != "auth_ok":
            raise RuntimeError("HA WebSocket auth failed")

        # Send all three registry requests
        for msg_id, msg_type in requests.items():
            await ha_ws.send(json.dumps({"id": msg_id, "type": msg_type}))

        # Collect results until all three answered
        while len(results) < 3:
            raw = await asyncio.wait_for(ha_ws.recv(), timeout=15)
            msg = json.loads(raw)
            if msg.get("id") in requests and msg.get("success"):
                results[msg["id"]] = msg["result"]

    return {
        "areas": results.get(1, []),
        "devices": results.get(2, []),
        "entities": results.get(3, []),
    }


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


async def _heartbeat(ws: websockets.WebSocketClientProtocol) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await ws.send(json.dumps({"type": "ping"}))


# ---------------------------------------------------------------------------
# Main relay connection
# ---------------------------------------------------------------------------


async def _relay_session(ws_url: str) -> str | None:
    """
    Open one relay session.  Returns the relay_token on successful handshake,
    or None if authentication failed (caller should not retry with same URL).
    Raises on network/protocol errors so the caller can reconnect.
    """
    logger.info(f"Connecting to Aina relay: {ws_url}")
    async with websockets.connect(ws_url, ping_interval=None) as ws:
        # Handshake — receive connected message
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        msg = json.loads(raw)

        if msg.get("type") == "error":
            logger.error(f"Relay rejected connection: {msg.get('message')}")
            return None

        if msg.get("type") != "connected":
            raise RuntimeError(f"Unexpected relay message: {msg}")

        relay_token: str = msg["relay_token"]
        connection_id: str = msg["connection_id"]
        logger.info(f"Relay handshake OK — connection_id={connection_id}")

        # Acknowledge and persist token
        await ws.send(json.dumps({"type": "ready"}))
        _save_relay_token(relay_token)

        # Start heartbeat as a background task
        heartbeat_task = asyncio.create_task(_heartbeat(ws))

        try:
            async for raw_msg in ws:
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type")

                if msg_type == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

                elif msg_type == "pong":
                    pass

                elif msg_type == "request":
                    asyncio.create_task(_handle_request(ws, msg))

                elif msg_type == "command":
                    asyncio.create_task(_handle_command(ws, msg))

        finally:
            heartbeat_task.cancel()

    return relay_token


async def _handle_request(ws: websockets.WebSocketClientProtocol, msg: dict) -> None:
    request_id = msg["id"]
    status, body = await _proxy_request(
        msg.get("method", "GET"),
        msg.get("path", "/"),
        msg.get("body"),
    )
    await ws.send(
        json.dumps(
            {"type": "response", "id": request_id, "status": status, "body": body}
        )
    )


async def _handle_command(ws: websockets.WebSocketClientProtocol, msg: dict) -> None:
    request_id = msg["id"]
    command = msg.get("command")

    if command == "fetch_registries":
        try:
            body = await _fetch_registries()
            status = 200
        except Exception as exc:
            logger.error(f"fetch_registries failed: {exc}")
            body = {"error": str(exc)}
            status = 500
    else:
        body = {"error": f"Unknown command: {command}"}
        status = 400

    await ws.send(
        json.dumps(
            {"type": "response", "id": request_id, "status": status, "body": body}
        )
    )


# ---------------------------------------------------------------------------
# Entry point with auto-reconnect
# ---------------------------------------------------------------------------


async def main() -> None:
    relay_token = _load_relay_token()

    while True:
        try:
            if relay_token:
                ws_url = f"{AINA_SERVER}/api/iot_v2/home_assistant/relay/reconnect/{relay_token}"
                ws_url = ws_url.replace("https://", "wss://").replace(
                    "http://", "ws://"
                )
                result = await _relay_session(ws_url)
                if result is None:
                    # Token rejected — fall back to pairing code on next loop
                    logger.warning("Relay token rejected, falling back to pairing code")
                    relay_token = None
                    RELAY_TOKEN_PATH.unlink(missing_ok=True)
                    continue
            else:
                if not PAIRING_CODE:
                    logger.error(
                        "No relay token on disk and PAIRING_CODE is empty. "
                        "Enter your pairing code in the add-on configuration."
                    )
                    await asyncio.sleep(RECONNECT_DELAY)
                    continue
                ws_url = (
                    f"{AINA_SERVER}/api/iot_v2/home_assistant/relay/ws/{PAIRING_CODE}"
                )
                ws_url = ws_url.replace("https://", "wss://").replace(
                    "http://", "ws://"
                )
                result = await _relay_session(ws_url)
                if result is None:
                    # Pairing code was invalid
                    logger.error(
                        "Pairing code was rejected. Update it in the add-on configuration."
                    )
                    await asyncio.sleep(RECONNECT_DELAY)
                    continue
                relay_token = result

        except Exception as exc:
            logger.error(
                f"Relay connection error: {exc}. Retrying in {RECONNECT_DELAY}s…"
            )
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    asyncio.run(main())
