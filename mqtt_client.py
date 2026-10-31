"""Paho MQTT wrapper for a single Socket.IO session.

Each browser session owns one MQTTManager. The manager forwards MQTT events
only to that session's Socket.IO room (the room name is the Socket.IO sid).
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt


class MQTTManager:
    """Manage one paho MQTT client and bridge it to one Socket.IO client."""

    def __init__(self, socketio: Any, sid: str) -> None:
        self.socketio = socketio
        self.sid = sid
        self.logger = logging.getLogger(f"{__name__}.{sid[:8]}")

        self._client: mqtt.Client | None = None
        self._connected = False
        self._subscriptions: dict[str, int] = {}
        self._host = ""
        self._port = 1883
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def connect(
        self,
        host: str,
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = False,
    ) -> None:
        """Start an asynchronous connection to an MQTT broker.

        paho's network loop performs the initial connection and keeps retrying
        after connection loss. Subscriptions are restored after every reconnect.
        """
        host = (host or "").strip()
        if not host:
            raise ValueError("Broker host không được để trống.")

        try:
            port = int(port)
        except (TypeError, ValueError) as exc:
            raise ValueError("Port phải là số nguyên.") from exc
        if not 1 <= port <= 65535:
            raise ValueError("Port phải nằm trong khoảng 1-65535.")

        if password is not None and username is None:
            raise ValueError("Password chỉ được dùng khi có username.")

        # Replace an existing connection of this session, if any.
        with self._lock:
            old_client = self._client
            self._client = None
            self._connected = False
        if old_client is not None:
            self._shutdown_client(old_client)

        client_id = f"dash-{self.sid[:8]}-{uuid.uuid4().hex[:6]}"
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = self._on_connect
        client.on_connect_fail = self._on_connect_fail
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.enable_logger(self.logger)

        if username is not None:
            client.username_pw_set(username, password)
        if use_tls:
            # paho's defaults: system CA bundle, certificate + hostname checks.
            client.tls_set()

        with self._lock:
            self._client = client
            self._host = host
            self._port = port

        self.logger.info(
            "Connecting to %s:%s (tls=%s, username=%s)",
            host,
            port,
            use_tls,
            "yes" if username else "no",
        )
        self.emit_status(
            "connecting",
            f"Đang kết nối tới {host}:{port}...",
            host=host,
            port=port,
        )

        try:
            client.connect_async(host, port, keepalive=60)
            client.loop_start()
        except Exception:
            self.logger.exception("Could not start MQTT client")
            with self._lock:
                if self._client is client:
                    self._client = None
                    self._connected = False
            self._shutdown_client(client)
            raise

    def disconnect(self) -> None:
        """Stop the MQTT loop and close the broker connection."""
        with self._lock:
            client = self._client
            self._client = None
            self._connected = False

        if client is None:
            return

        self._shutdown_client(client)
        self.logger.info("MQTT client disconnected")
        self.emit_status("disconnected", "Đã ngắt kết nối MQTT broker.")

    def subscribe(self, topic: str) -> int:
        """Subscribe to a topic filter and remember it for reconnects."""
        self._validate_topic_filter(topic)
        client = self._require_connected_client()

        result, mid = client.subscribe(topic, qos=0)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Subscribe thất bại (mã lỗi {result}).")

        with self._lock:
            self._subscriptions[topic] = 0
        self.logger.info("Subscribed to %s", topic)
        return mid

    def unsubscribe(self, topic: str) -> int | None:
        """Remove a topic filter, also removing it from reconnect tracking."""
        self._validate_topic_filter(topic)

        with self._lock:
            self._subscriptions.pop(topic, None)
            client = self._client
            connected = self._connected

        if client is None or not connected:
            # The local subscription list is still updated while reconnecting.
            return None

        result, mid = client.unsubscribe(topic)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Unsubscribe thất bại (mã lỗi {result}).")

        self.logger.info("Unsubscribed from %s", topic)
        return mid

    def publish(
        self,
        topic: str,
        payload: str | bytes,
        qos: int = 0,
        retain: bool = False,
    ) -> int:
        """Publish a message and return paho's message id."""
        self._validate_topic_name(topic)
        if qos not in (0, 1, 2):
            raise ValueError("QoS chỉ có thể là 0, 1 hoặc 2.")

        client = self._require_connected_client()
        info = client.publish(topic, payload=payload, qos=qos, retain=bool(retain))
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Publish thất bại (mã lỗi {info.rc}).")

        self.logger.info("Published to %s (qos=%s, retain=%s)", topic, qos, retain)
        return info.mid

    def emit_status(self, state: str, message: str, **extra: Any) -> None:
        """Send a status event to this manager's Socket.IO room."""
        data = {"state": state, "message": message, **extra}
        try:
            self.socketio.emit("status", data, room=self.sid)
        except Exception:
            self.logger.exception("Could not emit Socket.IO status event")

    # ------------------------------------------------------------------
    # paho callbacks (run in paho's network thread)
    # ------------------------------------------------------------------
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if client is not self._client:
            return

        if reason_code.is_failure:
            with self._lock:
                self._connected = False
            self.logger.error("Broker refused connection: %s", reason_code)
            self.emit_status("error", f"Broker từ chối kết nối: {reason_code}")
            return

        with self._lock:
            self._connected = True
            subscriptions = list(self._subscriptions.items())

        self.logger.info("Connected to %s:%s", self._host, self._port)
        self.emit_status(
            "connected",
            f"Đã kết nối tới {self._host}:{self._port}.",
            host=self._host,
            port=self._port,
        )

        # clean_session=True means the broker forgets subscriptions after a
        # reconnect, so restore the topics owned by this session.
        for topic, qos in subscriptions:
            try:
                result, _ = client.subscribe(topic, qos=qos)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    self.logger.warning(
                        "Could not restore subscription %s (error %s)", topic, result
                    )
            except Exception:
                self.logger.exception("Could not restore subscription %s", topic)

    def _on_connect_fail(self, client: mqtt.Client, userdata: Any) -> None:
        if client is not self._client:
            return

        with self._lock:
            self._connected = False
        self.logger.warning("Connection attempt failed, paho will retry")
        self.emit_status(
            "error",
            f"Không thể kết nối tới {self._host}:{self._port}. Đang thử lại...",
            reconnecting=True,
        )

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if client is not self._client:
            return

        with self._lock:
            self._connected = False

        self.logger.warning("MQTT connection lost: %s", reason_code)
        self.emit_status(
            "disconnected",
            "Mất kết nối MQTT. Đang tự động kết nối lại...",
            reconnecting=True,
            reason=str(reason_code),
        )

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage,
    ) -> None:
        if client is not self._client:
            return

        try:
            try:
                payload = msg.payload.decode("utf-8")
            except UnicodeDecodeError:
                # Binary payloads are still useful to the dashboard as hex.
                payload = msg.payload.hex()

            data = {
                "topic": msg.topic,
                "payload": payload,
                "qos": msg.qos,
                "retain": bool(msg.retain),
                "timestamp": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            }
            self.socketio.emit("mqtt_message", data, room=self.sid)
        except Exception:
            # A bad message must never terminate paho's network loop.
            self.logger.exception("Could not forward MQTT message")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _require_connected_client(self) -> mqtt.Client:
        with self._lock:
            client = self._client
            connected = self._connected

        if client is None or not connected:
            raise RuntimeError("Chưa kết nối tới MQTT broker.")
        return client

    def _shutdown_client(self, client: mqtt.Client) -> None:
        try:
            client.disconnect()
        except Exception:
            self.logger.debug("Error while disconnecting MQTT client", exc_info=True)
        try:
            client.loop_stop()
        except Exception:
            self.logger.exception("Error while stopping MQTT network loop")

    @staticmethod
    def _validate_topic_filter(topic: str) -> None:
        if not isinstance(topic, str) or not topic:
            raise ValueError("Topic không được để trống.")
        if "\x00" in topic:
            raise ValueError("Topic không được chứa ký tự NULL.")

        levels = topic.split("/")
        for index, level in enumerate(levels):
            if "#" in level and (level != "#" or index != len(levels) - 1):
                raise ValueError("Wildcard '#' phải đứng riêng ở cuối topic.")
            if "+" in level and level != "+":
                raise ValueError("Wildcard '+' phải đứng riêng trong một level.")

    @staticmethod
    def _validate_topic_name(topic: str) -> None:
        if not isinstance(topic, str) or not topic:
            raise ValueError("Topic không được để trống.")
        if "\x00" in topic:
            raise ValueError("Topic không được chứa ký tự NULL.")
        if "+" in topic or "#" in topic:
            raise ValueError("Topic publish không được chứa wildcard '+' hoặc '#'.")
