"""Flask-SocketIO backend for MQTT Dashboard."""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
from typing import Any

from flask import Flask, render_template, request
from flask_socketio import SocketIO

from mqtt_client import MQTTManager

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "mqtt-dashboard-dev-secret")
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins="*",
)

# One MQTTManager per Socket.IO session. Access is protected because Flask-SocketIO
# may serve events from multiple threads.
managers: dict[str, MQTTManager] = {}
managers_lock = threading.Lock()


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------
@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    """Small health endpoint useful for local checks and deployment probes."""
    with managers_lock:
        active_sessions = len(managers)
    return {"status": "ok", "active_sessions": active_sessions}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_manager() -> MQTTManager:
    """Return this Socket.IO session's manager, creating it if necessary."""
    sid = request.sid
    with managers_lock:
        manager = managers.get(sid)
    if manager is not None:
        return manager

    manager = MQTTManager(socketio, sid)
    with managers_lock:
        return managers.setdefault(sid, manager)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _error_response(manager: MQTTManager, message: str) -> dict[str, Any]:
    manager.emit_status("error", message)
    return {"ok": False, "error": message}


# ---------------------------------------------------------------------------
# Socket.IO lifecycle
# ---------------------------------------------------------------------------
@socketio.on("connect")
def handle_socket_connect(auth: Any = None) -> None:
    sid = request.sid
    manager = MQTTManager(socketio, sid)
    with managers_lock:
        previous = managers.pop(sid, None)
        managers[sid] = manager

    # Defensive cleanup; a Socket.IO sid is normally unique.
    if previous is not None:
        previous.disconnect()
    logger.info("Socket.IO client connected: %s", sid)


@socketio.on("disconnect")
def handle_socket_disconnect(reason: Any = None) -> None:
    sid = request.sid
    with managers_lock:
        manager = managers.pop(sid, None)

    # This is the key cleanup point: disconnect and stop paho's thread.
    if manager is not None:
        manager.disconnect()
    logger.info("Socket.IO client disconnected: %s (reason=%s)", sid, reason)


# ---------------------------------------------------------------------------
# MQTT events from the browser
# ---------------------------------------------------------------------------
@socketio.on("connect_broker")
def handle_connect_broker(data: Any = None) -> dict[str, Any]:
    manager = _get_manager()
    data = data if isinstance(data, dict) else {}

    host = str(data.get("host", "")).strip()
    if not host:
        return _error_response(manager, "Vui lòng nhập địa chỉ broker.")

    use_tls = _as_bool(data.get("use_tls", False))
    raw_port = data.get("port")
    if raw_port in (None, ""):
        port = 8883 if use_tls else 1883
    else:
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            return _error_response(manager, "Port phải là số nguyên.")

    username = data.get("username")
    if isinstance(username, str):
        username = username.strip() or None

    password = data.get("password")
    if password is not None:
        password = str(password)
    if password is not None and username is None:
        return _error_response(manager, "Password chỉ được dùng khi có username.")

    try:
        manager.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls,
        )
    except Exception as exc:
        logger.exception("Could not configure MQTT client for session %s", request.sid)
        return _error_response(manager, f"Không thể kết nối: {exc}")

    return {
        "ok": True,
        "message": f"Đang kết nối tới {host}:{port}...",
        "host": host,
        "port": port,
        "use_tls": use_tls,
    }


@socketio.on("disconnect_broker")
def handle_disconnect_broker(data: Any = None) -> dict[str, Any]:
    manager = _get_manager()
    manager.disconnect()
    return {"ok": True, "message": "Đã ngắt kết nối broker."}


@socketio.on("subscribe")
def handle_subscribe(data: Any = None) -> dict[str, Any]:
    manager = _get_manager()
    data = data if isinstance(data, dict) else {}
    topic = data.get("topic")

    if not isinstance(topic, str) or not topic:
        return _error_response(manager, "Vui lòng nhập topic cần subscribe.")

    try:
        mid = manager.subscribe(topic)
    except Exception as exc:
        logger.warning("Subscribe failed for session %s: %s", request.sid, exc)
        return _error_response(manager, f"Subscribe thất bại: {exc}")

    return {"ok": True, "topic": topic, "mid": mid}


@socketio.on("unsubscribe")
def handle_unsubscribe(data: Any = None) -> dict[str, Any]:
    manager = _get_manager()
    data = data if isinstance(data, dict) else {}
    topic = data.get("topic")

    if not isinstance(topic, str) or not topic:
        return _error_response(manager, "Vui lòng nhập topic cần unsubscribe.")

    try:
        mid = manager.unsubscribe(topic)
    except Exception as exc:
        logger.warning("Unsubscribe failed for session %s: %s", request.sid, exc)
        return _error_response(manager, f"Unsubscribe thất bại: {exc}")

    return {"ok": True, "topic": topic, "mid": mid}


@socketio.on("publish")
def handle_publish(data: Any = None) -> dict[str, Any]:
    manager = _get_manager()
    data = data if isinstance(data, dict) else {}
    topic = data.get("topic")

    if not isinstance(topic, str) or not topic:
        return _error_response(manager, "Vui lòng nhập topic cần publish.")

    payload = data.get("payload", "")
    if isinstance(payload, (dict, list)):
        payload = json.dumps(payload, ensure_ascii=False)
    elif payload is None:
        payload = ""
    elif not isinstance(payload, (str, bytes)):
        payload = str(payload)

    try:
        qos = int(data.get("qos", 0))
    except (TypeError, ValueError):
        return _error_response(manager, "QoS phải là số nguyên (0, 1 hoặc 2).")
    retain = _as_bool(data.get("retain", False))

    try:
        mid = manager.publish(topic=topic, payload=payload, qos=qos, retain=retain)
    except Exception as exc:
        logger.warning("Publish failed for session %s: %s", request.sid, exc)
        return _error_response(manager, f"Publish thất bại: {exc}")

    return {
        "ok": True,
        "topic": topic,
        "qos": qos,
        "retain": retain,
        "mid": mid,
    }


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------
def _shutdown_all_managers() -> None:
    with managers_lock:
        active = list(managers.values())
        managers.clear()
    for manager in active:
        try:
            manager.disconnect()
        except Exception:
            logger.exception("Could not close MQTT client during shutdown")


atexit.register(_shutdown_all_managers)


if __name__ == "__main__":
    # Werkzeug is adequate for a classroom demo. Use a production WSGI server
    # with a Socket.IO-compatible worker for a real deployment.
    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
        allow_unsafe_werkzeug=True,
    )
