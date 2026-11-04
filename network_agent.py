#!/usr/bin/env python3
"""Agent giám sát thiết bị LAN và gửi kết quả qua MQTT."""

from __future__ import annotations

import argparse
import concurrent.futures
import ipaddress
import json
import logging
import math
import os
import platform
import random
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Set, Tuple


VERSION = 1
MAX_TARGETS = 64
DEVICE_ID_RE = re.compile(r"[^A-Za-z0-9._-]")
AGENT_ID_RE = re.compile(r"[^A-Za-z0-9_-]")
PING_LATENCY_RE = re.compile(r"[=<]\s*(\d+(?:[.,]\d+)?)\s*ms", re.IGNORECASE)
IPV4_RE = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
MAC_RE = re.compile(r"(?<![0-9A-Fa-f])(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}(?![0-9A-Fa-f])")
SEVERITY_BY_EVENT = {
    "agent_started": "info",
    "device_down": "critical",
    "device_up": "info",
    "new_device": "warning",
    "port_opened": "warning",
    "port_closed": "info",
}


@dataclass(frozen=True)
class Target:
    """Thông tin một thiết bị cần theo dõi."""

    host: str
    name: str
    ports: Tuple[int, ...] = ()
    known: bool = True
    mac: Optional[str] = None


@dataclass
class ProbeResult:
    """Kết quả thô của một lần kiểm tra thiết bị."""

    online: bool
    latency_ms: Optional[float]
    ports: Dict[str, bool]


@dataclass
class DeviceState:
    """Trạng thái debounce và lịch sử mất gói của một thiết bị."""

    history: Deque[bool]
    online: Optional[bool] = None
    failures: int = 0
    down_since: Optional[float] = None
    previous_raw_online: Optional[bool] = None
    previous_ports: Dict[str, bool] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.previous_ports is None:
            self.previous_ports = {}


def utc_timestamp() -> str:
    """Trả về thời điểm UTC theo định dạng ISO có mili giây."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sanitize_id(value: str) -> str:
    """Thay ký tự không hợp lệ trong mã thiết bị bằng dấu gạch dưới."""
    return DEVICE_ID_RE.sub("_", str(value))


def sanitize_agent_id(value: str) -> str:
    """Làm sạch mã agent theo quy tắc topic MQTT."""
    cleaned = AGENT_ID_RE.sub("_", str(value))
    return cleaned or "agent"


def parse_ping_latency(text: str) -> Optional[float]:
    """Đọc độ trễ ping từ output Windows hoặc Unix."""
    match = PING_LATENCY_RE.search(text or "")
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def parse_arp(text: str) -> Dict[str, str]:
    """Đọc các cặp IPv4/MAC từ output arp của Windows hoặc Unix."""
    found: Dict[str, str] = {}
    for line in (text or "").splitlines():
        mac_match = MAC_RE.search(line)
        if not mac_match:
            continue
        mac = mac_match.group(0).replace("-", ":").lower()
        for ip_match in IPV4_RE.finditer(line):
            candidate = ip_match.group(0)
            try:
                ipaddress.IPv4Address(candidate)
            except ipaddress.AddressValueError:
                continue
            found[candidate] = mac
            break
    return found


def build_device_payload(
    agent: str,
    target: Target,
    online: bool,
    latency_ms: Optional[float],
    loss_pct: float,
    ports: Mapping[str, bool],
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    """Tạo bản ghi thiết bị đúng hợp đồng dashboard."""
    return {
        "agent": agent,
        "id": sanitize_id(target.host),
        "host": target.host,
        "name": target.name,
        "online": bool(online),
        "latency_ms": round(latency_ms, 1) if latency_ms is not None else None,
        "loss_pct": round(float(loss_pct), 1),
        "ports": {str(port): bool(is_open) for port, is_open in sorted(ports.items())},
        "mac": target.mac.lower() if target.mac else None,
        "known": bool(target.known),
        "ts": ts or utc_timestamp(),
    }


def build_event_payload(
    agent: str,
    event_type: str,
    host: str,
    name: str,
    message: str,
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    """Tạo payload sự kiện với mức độ nghiêm trọng cố định."""
    return {
        "agent": agent,
        "type": event_type,
        "severity": SEVERITY_BY_EVENT.get(event_type, "info"),
        "host": host,
        "name": name,
        "message": message,
        "ts": ts or utc_timestamp(),
    }


def _parse_ports(values: Sequence[Any], label: str) -> Tuple[int, ...]:
    ports: List[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
            raise ValueError("{}: cổng {!r} phải là số nguyên từ 1 đến 65535".format(label, value))
        if value not in ports:
            ports.append(value)
    return tuple(ports)


def _parse_ports_csv(value: str, label: str) -> Tuple[int, ...]:
    if not value.strip():
        return ()
    items: List[int] = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            raise ValueError("{}: danh sách cổng có phần tử rỗng".format(label))
        try:
            port = int(raw)
        except ValueError:
            raise ValueError("{}: cổng {!r} không phải số nguyên".format(label, raw))
        items.append(port)
    return _parse_ports(items, label)


def _load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as exc:
        raise ValueError("Không đọc được file cấu hình {!r}: {}".format(path, exc))
    except json.JSONDecodeError as exc:
        raise ValueError("JSON cấu hình không hợp lệ tại dòng {}: {}".format(exc.lineno, exc.msg))
    if not isinstance(data, dict):
        raise ValueError("File cấu hình phải chứa một đối tượng JSON")
    return data


def _config_targets(config: Mapping[str, Any]) -> List[Target]:
    raw_targets = config.get("targets", [])
    if not isinstance(raw_targets, list):
        raise ValueError("config.targets phải là danh sách")
    targets: List[Target] = []
    for index, item in enumerate(raw_targets, 1):
        label = "Thiết bị cấu hình #{}".format(index)
        if not isinstance(item, dict):
            raise ValueError("{} phải là đối tượng JSON".format(label))
        host = item.get("host")
        if not isinstance(host, str) or not host.strip():
            raise ValueError("{}: host không được để trống".format(label))
        host = host.strip()
        name = item.get("name", host)
        if not isinstance(name, str):
            raise ValueError("{} ({}): name phải là chuỗi".format(label, host))
        raw_ports = item.get("ports", [])
        if not isinstance(raw_ports, list):
            raise ValueError("{} ({}): ports phải là danh sách số nguyên".format(label, host))
        ports = _parse_ports(raw_ports, "{} ({})".format(label, host))
        targets.append(Target(host=host, name=name.strip() or host, ports=ports))
    return _validate_targets(targets)


def _targets_from_csv(value: str, ports: Tuple[int, ...]) -> List[Target]:
    targets = []
    for raw_host in value.split(","):
        host = raw_host.strip()
        if not host:
            raise ValueError("--targets có host rỗng; hãy dùng danh sách phân cách bằng dấu phẩy")
        targets.append(Target(host=host, name=host, ports=ports))
    return _validate_targets(targets)


def _validate_targets(targets: List[Target]) -> List[Target]:
    if len(targets) > MAX_TARGETS:
        raise ValueError("Tối đa {} thiết bị; hiện có {}".format(MAX_TARGETS, len(targets)))
    seen: Set[str] = set()
    for target in targets:
        if not target.host.strip():
            raise ValueError("Host thiết bị không được để trống")
        if target.host in seen:
            raise ValueError("Host {!r} bị khai báo lặp".format(target.host))
        seen.add(target.host)
    return targets


def _value(args: argparse.Namespace, config: Mapping[str, Any], cli_name: str, *config_names: str, default: Any = None) -> Any:
    cli_value = getattr(args, cli_name)
    if cli_value is not None:
        return cli_value
    for name in config_names:
        if name in config:
            return config[name]
    return default


def _positive_number(value: Any, label: str, minimum: float) -> float:
    if isinstance(value, bool):
        raise ValueError("{} phải là số".format(label))
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError("{} phải là số".format(label))
    if not math.isfinite(result) or result < minimum:
        raise ValueError("{} phải lớn hơn hoặc bằng {}".format(label, minimum))
    return result


def resolve_settings(args: argparse.Namespace) -> Dict[str, Any]:
    """Đọc cấu hình, áp dụng ưu tiên CLI và kiểm tra dữ liệu đầu vào."""
    config = _load_config(args.config)
    if args.targets is not None:
        default_ports = _parse_ports_csv(args.ports or "", "--ports")
        targets = _targets_from_csv(args.targets, default_ports)
    else:
        targets = _config_targets(config)
        if args.ports is not None:
            shared_ports = _parse_ports_csv(args.ports, "--ports")
            targets = [replace(target, ports=shared_ports) for target in targets]

    default_host = "broker.hivemq.com"
    tls_value = _value(args, config, "tls", "tls", default=False)
    if not isinstance(tls_value, bool):
        raise ValueError("config.tls phải là true hoặc false")
    tls = tls_value
    port = _value(args, config, "port", "port", default=None)
    if port is None:
        port = 8883 if tls else 1883
    if isinstance(port, bool):
        raise ValueError("--port phải là số nguyên từ 1 đến 65535")
    if isinstance(port, float) and not port.is_integer():
        raise ValueError("--port phải là số nguyên từ 1 đến 65535")
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("--port phải là số nguyên từ 1 đến 65535")
    if not 1 <= port <= 65535:
        raise ValueError("--port phải là số nguyên từ 1 đến 65535")

    interval = _positive_number(_value(args, config, "interval", "interval", default=5), "--interval", 2)
    fail_threshold_raw = _value(args, config, "fail_threshold", "fail_threshold", default=3)
    if isinstance(fail_threshold_raw, bool):
        raise ValueError("--fail-threshold phải là số nguyên dương")
    if isinstance(fail_threshold_raw, float) and not fail_threshold_raw.is_integer():
        raise ValueError("--fail-threshold phải là số nguyên dương")
    try:
        fail_threshold = int(fail_threshold_raw)
    except (TypeError, ValueError):
        raise ValueError("--fail-threshold phải là số nguyên dương")
    if fail_threshold < 1:
        raise ValueError("--fail-threshold phải là số nguyên dương")
    ping_timeout = _positive_number(_value(args, config, "ping_timeout", "ping_timeout", default=1.0), "--ping-timeout", 0.01)
    port_timeout = _positive_number(_value(args, config, "port_timeout", "port_timeout", default=0.5), "--port-timeout", 0.01)
    discover_every = _positive_number(_value(args, config, "discover_every", "discover_every", default=60), "--discover-every", 1)
    count = args.count
    if count < 0:
        raise ValueError("--count phải lớn hơn hoặc bằng 0")

    agent_default = socket.gethostname()
    agent_id = sanitize_agent_id(str(_value(args, config, "agent_id", "agent_id", "agent", default=agent_default)))
    prefix = str(_value(args, config, "prefix", "prefix", default="lanmon")).strip("/")
    if not prefix or any(not level for level in prefix.split("/")) or "+" in prefix or "#" in prefix:
        raise ValueError("--prefix phải có giá trị và không chứa ký tự MQTT + hoặc #")
    host = str(_value(args, config, "host", "host", default=default_host))
    if not host.strip():
        raise ValueError("--host không được để trống")
    username = _value(args, config, "username", "username", default=None)
    password = _value(args, config, "password", "password", default=None)
    discover = _value(args, config, "discover", "discover", default=None)
    baseline_file = str(_value(args, config, "baseline_file", "baseline_file", default="baseline.json"))
    config_learn = config.get("learn", False)
    if not isinstance(config_learn, bool):
        raise ValueError("config.learn phải là true hoặc false")
    learn = args.learn or config_learn
    if not targets and discover is None and not args.simulate:
        raise ValueError(
            "Cần --config, --targets, --discover hoặc --simulate. Ví dụ: "
            "python network_agent.py --targets 192.168.1.1,8.8.8.8 --count 3"
        )
    if not targets and args.simulate:
        targets = [
            Target("192.168.1.1", "Router", (80, 443)),
            Target("192.168.1.10", "PC-Giaovien", (22,)),
            Target("192.168.1.20", "Camera", (80,)),
            Target("192.168.1.30", "Printer", (80,)),
            Target("192.168.1.40", "Laptop-Khach", (22,)),
        ]
    if not args.simulate:
        _validate_targets(targets)
    discovery = _parse_discovery(str(discover)) if discover is not None else None
    return {
        "host": host.strip(), "port": port, "tls": tls, "username": username,
        "password": password, "prefix": prefix, "agent_id": agent_id,
        "targets": targets, "interval": interval, "fail_threshold": fail_threshold,
        "ping_timeout": ping_timeout, "port_timeout": port_timeout,
        "discover": discovery, "discover_every": discover_every,
        "baseline_file": baseline_file, "learn": learn,
        "simulate": args.simulate, "count": count, "verbose": args.verbose,
    }


def _parse_discovery(value: str) -> Tuple[ipaddress.IPv4Network, Optional[str]]:
    if value.lower() == "auto":
        try:
            local_ip = _guess_lan_ip()
            network = ipaddress.ip_network("{}/24".format(local_ip), strict=False)
        except OSError as exc:
            raise ValueError("Không đoán được IP LAN cho --discover auto: {}".format(exc))
    else:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError as exc:
            raise ValueError("CIDR --discover không hợp lệ: {}".format(exc))
        try:
            local_ip = _guess_lan_ip()
        except OSError:
            local_ip = None
    if not isinstance(network, ipaddress.IPv4Network):
        raise ValueError("--discover chỉ hỗ trợ CIDR IPv4")
    if network.prefixlen < 24 or network.num_addresses > 256:
        raise ValueError("Mạng --discover phải có prefix /24 trở lên (tối đa 256 địa chỉ)")
    private_networks = (
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
        ipaddress.ip_network("192.168.0.0/16"),
    )
    if not any(network.subnet_of(private) for private in private_networks):
        raise ValueError("--discover chỉ chấp nhận mạng private RFC 1918")
    return network, local_ip


def _guess_lan_ip() -> str:
    """Chọn IP LAN bằng UDP connect; thao tác này không gửi gói UDP."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 53))
        return str(probe.getsockname()[0])
    finally:
        probe.close()


def _ping_command(host: str, timeout: float) -> List[str]:
    if platform.system().lower() == "windows":
        return ["ping", "-n", "1", "-w", str(max(1, int(math.ceil(timeout * 1000)))), host]
    return ["ping", "-c", "1", "-W", str(max(1, int(math.ceil(timeout)))), host]


def _run_ping(host: str, timeout: float) -> Tuple[bool, Optional[float]]:
    started = time.perf_counter()
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if platform.system().lower() == "windows" else 0
    try:
        result = subprocess.run(
            _ping_command(host, timeout), capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=timeout + 1.0,
            creationflags=creationflags, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False, None
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    output = (result.stdout or "") + "\n" + (result.stderr or "")
    ok = result.returncode == 0 and "ttl=" in output.lower()
    if not ok:
        return False, None
    return True, parse_ping_latency(output) if parse_ping_latency(output) is not None else elapsed_ms


def _probe_target(target: Target, ping_timeout: float, port_timeout: float) -> ProbeResult:
    ping_ok, latency = _run_ping(target.host, ping_timeout)
    port_results: Dict[str, bool] = {}
    first_tcp_latency: Optional[float] = None
    for port in target.ports:
        started = time.perf_counter()
        opened = False
        try:
            connection = socket.create_connection((target.host, port), timeout=port_timeout)
            try:
                opened = True
                if first_tcp_latency is None:
                    first_tcp_latency = (time.perf_counter() - started) * 1000.0
            finally:
                connection.close()
        except (OSError, TimeoutError):
            opened = False
        port_results[str(port)] = opened
    online = ping_ok or any(port_results.values())
    if not ping_ok and first_tcp_latency is not None:
        latency = first_tcp_latency
    return ProbeResult(online, round(latency, 1) if online and latency is not None else None, port_results)


def _is_usable_mac(mac: str) -> bool:
    normalized = mac.lower().replace("-", ":")
    if normalized == "ff:ff:ff:ff:ff:ff" or normalized.startswith("01:00:5e"):
        return False
    try:
        first_octet = int(normalized.split(":", 1)[0], 16)
    except ValueError:
        return False
    return (first_octet & 1) == 0


def _run_arp() -> Optional[Dict[str, str]]:
    command = ["arp", "-a"] if platform.system().lower() == "windows" else ["arp", "-an"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=5, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        logging.warning("Không chạy được arp: %s", exc)
        return None
    if result.returncode != 0:
        logging.warning("Lệnh arp trả mã %s", result.returncode)
        return None
    return parse_arp(result.stdout)


def _atomic_write_baseline(path: str, devices: Mapping[str, Mapping[str, str]]) -> None:
    destination = Path(path)
    temp_path = destination.with_name(destination.name + ".tmp." + uuid.uuid4().hex[:8])
    content = {"created": utc_timestamp(), "devices": dict(devices)}
    try:
        with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(content, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(str(temp_path), str(destination))
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _load_baseline(path: str) -> Optional[Dict[str, Dict[str, str]]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Không đọc được baseline {!r}: {}".format(path, exc))
    if not isinstance(data, dict) or not isinstance(data.get("devices"), dict):
        raise ValueError("Baseline phải có trường devices dạng đối tượng")
    result: Dict[str, Dict[str, str]] = {}
    for mac, item in data["devices"].items():
        normalized = str(mac).lower().replace("-", ":")
        if not isinstance(item, dict) or not isinstance(item.get("ip"), str):
            raise ValueError("Baseline có mục thiết bị không hợp lệ cho MAC {}".format(mac))
        result[normalized] = {"ip": item["ip"], "first_seen": str(item.get("first_seen", data.get("created", utc_timestamp())))}
    return result


class MQTTTransport:
    """Kết nối MQTT, phát hiện mất kết nối và không xếp hàng khi offline."""

    def __init__(self, settings: Mapping[str, Any], targets: Callable[[], int]):
        try:
            import paho.mqtt.client as mqtt  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("Thiếu paho-mqtt 2.x; cài requirements.txt trong môi trường hiện tại") from exc
        self.mqtt = mqtt
        self.settings = settings
        self.target_count = targets
        client_id = "{}.{}.{}".format(settings["prefix"], settings["agent_id"], uuid.uuid4().hex[:4])
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        if settings.get("username") is not None or settings.get("password") is not None:
            self.client.username_pw_set(settings.get("username"), settings.get("password"))
        if settings["tls"]:
            self.client.tls_set()
        self.agent = str(settings["agent_id"])
        self.prefix = str(settings["prefix"])
        self.connected = threading.Event()
        self.first_connected = False
        self.warning_lock = threading.Lock()
        self.last_offline_warning = 0.0
        self.client.will_set(
            self._topic("status"), self._json({"agent": self.agent, "state": "offline"}), qos=1, retain=True
        )
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _topic(self, suffix: str) -> str:
        return "{}/{}/{}".format(self.prefix, self.agent, suffix)

    @staticmethod
    def _json(payload: Mapping[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def _status_payload(self, state: str) -> Dict[str, Any]:
        return {
            "agent": self.agent, "state": state, "interval_s": self.settings["interval"],
            "targets": self.target_count(), "version": VERSION, "ts": utc_timestamp(),
        }

    def _on_connect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        try:
            success = int(reason_code) == 0
        except (TypeError, ValueError):
            success = str(reason_code) == "Success"
        if not success:
            self.connected.clear()
            logging.warning("Broker từ chối kết nối MQTT: %s", reason_code)
            return
        self.connected.set()
        client.publish(self._topic("status"), self._json(self._status_payload("online")), qos=1, retain=True)
        if not self.first_connected:
            self.first_connected = True
            event = build_event_payload(self.agent, "agent_started", "", self.agent, "Agent giám sát LAN đã khởi động")
            client.publish(self._topic("event"), self._json(event), qos=1, retain=False)
            logging.info("Đã kết nối MQTT broker")

    def _on_disconnect(self, client: Any, userdata: Any, disconnect_flags: Any, reason_code: Any, properties: Any) -> None:
        self.connected.clear()

    def start(self) -> None:
        try:
            self.client.connect_async(self.settings["host"], self.settings["port"], keepalive=60)
            self.client.loop_start()
        except Exception as exc:
            logging.warning("Không khởi tạo được kết nối MQTT: %s", exc)

    def _warn_if_offline(self) -> None:
        now = time.monotonic()
        with self.warning_lock:
            if now - self.last_offline_warning >= 30:
                logging.warning("Mất kết nối MQTT; vẫn kiểm tra thiết bị, tạm bỏ qua publish")
                self.last_offline_warning = now

    def publish(self, topic: str, payload: Mapping[str, Any], qos: int, retain: bool) -> None:
        if not self.connected.is_set():
            self._warn_if_offline()
            return
        try:
            info = self.client.publish(topic, self._json(payload), qos=qos, retain=retain)
            rc = getattr(info, "rc", 0)
            if rc != 0:
                self.connected.clear()
                self._warn_if_offline()
        except Exception as exc:
            self.connected.clear()
            self._warn_if_offline()
            logging.debug("Publish MQTT lỗi: %s", exc)

    def publish_status(self, state: str) -> None:
        self.publish(self._topic("status"), self._status_payload(state), qos=1, retain=True)

    def publish_device(self, payload: Mapping[str, Any]) -> None:
        self.publish(self._topic("device/{}".format(payload["id"])), payload, qos=0, retain=True)

    def publish_event(self, payload: Mapping[str, Any]) -> None:
        self.publish(self._topic("event"), payload, qos=1, retain=False)

    def stop(self) -> None:
        info = None
        if self.connected.is_set():
            try:
                info = self.client.publish(self._topic("status"), self._json(self._status_payload("offline")), qos=1, retain=True)
                if hasattr(info, "wait_for_publish"):
                    info.wait_for_publish(timeout=2.0)
            except Exception as exc:
                logging.debug("Không phát được trạng thái offline: %s", exc)
        try:
            self.client.disconnect()
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass


class DiscoveryWorker(threading.Thread):
    """Quét ARP nền và thêm thiết bị mới vào danh sách theo dõi."""

    def __init__(self, settings: Mapping[str, Any], registry: Dict[str, Target], registry_lock: threading.Lock,
                 transport: MQTTTransport, stop_event: threading.Event):
        super().__init__(name="lan-discovery", daemon=True)
        self.settings = settings
        self.registry = registry
        self.registry_lock = registry_lock
        self.transport = transport
        self.stop_event = stop_event
        self.network, self.local_ip = settings["discover"]
        self.baseline: Optional[Dict[str, Dict[str, str]]] = _load_baseline(settings["baseline_file"])
        self.learning = bool(settings["learn"] or self.baseline is None)
        if settings["learn"]:
            self.baseline = None
        self.alerted_macs: Set[str] = set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._scan_once()
            except Exception as exc:
                logging.warning("Lỗi quét mạng: %s", exc)
            if self.stop_event.wait(float(self.settings["discover_every"])):
                return

    def _scan_once(self) -> None:
        hosts = [str(ip) for ip in self.network.hosts() if str(ip) != self.local_ip]
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            list(pool.map(lambda host: _run_ping(host, 0.5), hosts))
        arp_map = _run_arp()
        if arp_map is None:
            return
        usable = {
            ip: mac for ip, mac in arp_map.items()
            if ipaddress.ip_address(ip) in self.network and _is_usable_mac(mac)
        }
        if self.learning:
            baseline: Dict[str, Dict[str, str]] = {}
            for ip, mac in usable.items():
                baseline.setdefault(mac, {"ip": ip, "first_seen": utc_timestamp()})
            _atomic_write_baseline(self.settings["baseline_file"], baseline)
            self.baseline = baseline
            self.learning = False
            logging.info("Đã lưu baseline %d thiết bị", len(baseline))
        self._apply_mac_data(usable)

    def _apply_mac_data(self, ip_to_mac: Mapping[str, str]) -> None:
        with self.registry_lock:
            for host, target in list(self.registry.items()):
                mac = ip_to_mac.get(host)
                if mac:
                    self.registry[host] = replace(target, mac=mac, known=target.known)
        baseline = self.baseline or {}
        for ip, mac in ip_to_mac.items():
            if mac in baseline or mac in self.alerted_macs:
                continue
            self.alerted_macs.add(mac)
            name = "Thiết bị lạ {}".format(ip)
            target = Target(ip, name, (), known=False, mac=mac)
            with self.registry_lock:
                existing = self.registry.get(ip)
                if existing is None:
                    self.registry[ip] = target
                elif existing.mac is None:
                    self.registry[ip] = replace(existing, mac=mac)
            event = build_event_payload(
                self.settings["agent_id"], "new_device", ip, name,
                "Thiết bị lạ: IP {}, MAC {}".format(ip, mac),
            )
            self.transport.publish_event(event)
            logging.warning("new_device: %s", event["message"])


def _new_state() -> DeviceState:
    return DeviceState(history=deque(maxlen=10))


def _simulate_probe(target: Target, rng: random.Random, cycle: int, down_until: Dict[str, int]) -> ProbeResult:
    if cycle > down_until.get(target.host, 0) and rng.random() < 0.03:
        down_until[target.host] = cycle + rng.randint(4, 8) - 1
    is_down = cycle <= down_until.get(target.host, 0)
    latency = round(min(40.0, max(2.0, rng.gauss(15.0, 9.0))), 1)
    ports = {str(port): (False if is_down else port in (53, 80, 443)) for port in target.ports}
    return ProbeResult(not is_down, None if is_down else latency, ports)


def _emit_event(transport: MQTTTransport, settings: Mapping[str, Any], event_type: str,
                target: Target, message: str) -> None:
    payload = build_event_payload(settings["agent_id"], event_type, target.host, target.name, message)
    transport.publish_event(payload)
    logging.warning("%s: %s", event_type, message)


def _process_result(
    target: Target, result: ProbeResult, state: DeviceState, settings: Mapping[str, Any],
    transport: MQTTTransport, now: float,
) -> Dict[str, Any]:
    state.history.append(not result.online)
    was_initialized = state.online is not None
    if not was_initialized:
        state.online = result.online
        state.failures = 0
    elif result.online:
        state.failures = 0
        if not state.online:
            elapsed = max(0.0, now - (state.down_since if state.down_since is not None else now))
            state.online = True
            state.down_since = None
            _emit_event(transport, settings, "device_up", target, "Đã kết nối lại sau {:.0f} giây".format(elapsed))
    else:
        state.failures += 1
        if state.online and state.failures >= settings["fail_threshold"]:
            state.online = False
            state.down_since = now
            _emit_event(
                transport, settings, "device_down", target,
                "Mất kết nối sau {} lần kiểm tra liên tiếp".format(settings["fail_threshold"]),
            )

    if result.online and state.previous_raw_online:
        for port, is_open in result.ports.items():
            previous = state.previous_ports.get(port)
            if previous is not None and previous != is_open:
                if is_open:
                    _emit_event(transport, settings, "port_opened", target, "Cổng {} đã mở".format(port))
                else:
                    _emit_event(transport, settings, "port_closed", target, "Cổng {} đã đóng".format(port))
    state.previous_raw_online = result.online
    state.previous_ports = dict(result.ports)
    loss_pct = 100.0 * sum(state.history) / len(state.history)
    return build_device_payload(
        settings["agent_id"], target, bool(state.online), result.latency_ms,
        loss_pct, result.ports,
    )


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def _parser() -> argparse.ArgumentParser:
    examples = "Ví dụ:\n  python network_agent.py --config targets.example.json\n  python network_agent.py --targets 192.168.1.1,8.8.8.8 --ports 22,80 --count 3\n  python network_agent.py --discover 192.168.1.0/24 --learn\n  python network_agent.py --simulate --count 60"
    parser = argparse.ArgumentParser(
        description="Kiểm tra thiết bị trong mạng LAN và gửi trạng thái qua MQTT.",
        epilog=examples, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", help="MQTT broker (mặc định broker.hivemq.com)")
    parser.add_argument("--port", type=int, help="Cổng MQTT (1883, TLS mặc định 8883)")
    parser.add_argument("--tls", action="store_true", default=None, help="Bật TLS")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--prefix", help="Prefix topic MQTT, mặc định lanmon")
    parser.add_argument("--agent-id", dest="agent_id", help="Mã agent trong topic")
    parser.add_argument("--config", help="Đường dẫn file JSON cấu hình")
    parser.add_argument("--targets", help="Danh sách host phân cách bằng dấu phẩy")
    parser.add_argument("--ports", help="Danh sách cổng áp dụng cho --targets")
    parser.add_argument("--interval", type=float, help="Chu kỳ kiểm tra (giây, tối thiểu 2)")
    parser.add_argument("--fail-threshold", dest="fail_threshold", type=int, help="Số lần thất bại liên tiếp trước khi báo down")
    parser.add_argument("--ping-timeout", dest="ping_timeout", type=float, help="Timeout ping bằng giây")
    parser.add_argument("--port-timeout", dest="port_timeout", type=float, help="Timeout TCP connect bằng giây")
    parser.add_argument("--discover", help="CIDR private IPv4 hoặc auto để đoán mạng /24")
    parser.add_argument("--discover-every", dest="discover_every", type=float, help="Khoảng quét lại mạng (giây)")
    parser.add_argument("--baseline-file", dest="baseline_file", help="File baseline MAC (mặc định baseline.json)")
    parser.add_argument("--learn", action="store_true", help="Ghi lại baseline từ lần quét đầu")
    parser.add_argument("--simulate", action="store_true", help="Tạo dữ liệu giả, không kiểm tra mạng thật")
    parser.add_argument("--count", type=int, default=0, help="Số chu kỳ; 0 chạy liên tục")
    parser.add_argument("--verbose", action="store_true", help="Bật log chi tiết")
    return parser


def run(settings: Mapping[str, Any]) -> int:
    _configure_logging(bool(settings["verbose"]))
    registry: Dict[str, Target] = {target.host: target for target in settings["targets"]}
    registry_lock = threading.Lock()

    def target_count() -> int:
        with registry_lock:
            return len(registry)

    try:
        transport = MQTTTransport(settings, target_count)
    except RuntimeError as exc:
        logging.error("%s", exc)
        return 2
    transport.start()

    stop_event = threading.Event()
    discovery: Optional[DiscoveryWorker] = None
    if settings["discover"] is not None and not settings["simulate"]:
        try:
            discovery = DiscoveryWorker(settings, registry, registry_lock, transport, stop_event)
            discovery.start()
        except ValueError as exc:
            logging.error("%s", exc)
            transport.stop()
            return 2

    states: Dict[str, DeviceState] = {}
    rng = random.Random()
    simulated_down_until: Dict[str, int] = {}
    cycle = 0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="device-check")
    try:
        while True:
            cycle += 1
            with registry_lock:
                current_targets = list(registry.values())
            if settings["simulate"] and cycle % 40 == 0:
                host = "192.168.1.{}".format(50 + (cycle // 40))
                if host not in {target.host for target in current_targets}:
                    unknown = Target(host, "Thiết bị lạ {}".format(host), (), known=False,
                                     mac="02:00:00:{:02x}:{:02x}:{:02x}".format((cycle // 40) & 255, (cycle // 40) & 255, cycle & 255))
                    with registry_lock:
                        registry[host] = unknown
                    current_targets.append(unknown)
                    _emit_event(
                        transport, settings, "new_device", unknown,
                        "Thiết bị lạ: IP {}, MAC {}".format(host, unknown.mac),
                    )
            started = time.monotonic()
            futures: Dict[str, concurrent.futures.Future[ProbeResult]] = {}
            for target in current_targets:
                states.setdefault(target.host, _new_state())
                if settings["simulate"]:
                    future = executor.submit(_simulate_probe, target, rng, cycle, simulated_down_until)
                else:
                    future = executor.submit(_probe_target, target, settings["ping_timeout"], settings["port_timeout"])
                futures[target.host] = future
            online_count = 0
            latencies: List[float] = []
            for target in current_targets:
                try:
                    result = futures[target.host].result()
                    payload = _process_result(target, result, states[target.host], settings, transport, time.monotonic())
                except Exception as exc:
                    logging.exception("Lỗi kiểm tra %s: %s", target.host, exc)
                    failed = ProbeResult(False, None, {str(port): False for port in target.ports})
                    payload = _process_result(target, failed, states[target.host], settings, transport, time.monotonic())
                transport.publish_device(payload)
                if payload["online"]:
                    online_count += 1
                if payload["latency_ms"] is not None:
                    latencies.append(float(payload["latency_ms"]))
            if latencies:
                average = "{:.1f} ms".format(sum(latencies) / len(latencies))
            else:
                average = "không có"
            logging.info("%d thiết bị: %d online, %d offline | độ trễ TB %s",
                         len(current_targets), online_count, len(current_targets) - online_count, average)
            if settings["count"] and cycle >= settings["count"]:
                break
            remaining = float(settings["interval"]) - (time.monotonic() - started)
            if remaining > 0:
                stop_event.wait(remaining)
    except KeyboardInterrupt:
        logging.info("Đã nhận Ctrl+C, đang thoát")
    finally:
        stop_event.set()
        if discovery is not None:
            discovery.join(timeout=2.0)
        executor.shutdown(wait=True, cancel_futures=False)
        transport.stop()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    for output in (sys.stdout, sys.stderr):
        if hasattr(output, "reconfigure"):
            output.reconfigure(encoding="utf-8", errors="replace")
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        settings = resolve_settings(args)
    except ValueError as exc:
        parser.error(str(exc))
    return run(settings)


if __name__ == "__main__":
    sys.exit(main())
