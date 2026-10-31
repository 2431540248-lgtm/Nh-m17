"""Publish fake temperature and humidity values for the MQTT Dashboard demo.

Usage:
    python test_publisher.py --host localhost --port 1883
    python test_publisher.py --host broker.emqx.io --port 1883 --interval 1

Press Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import logging
import random
import threading
import time

import paho.mqtt.client as mqtt

TOPIC_TEMP = "demo/sensor/temp"
TOPIC_HUMI = "demo/sensor/humi"

logger = logging.getLogger("test_publisher")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish fake temperature/humidity data for MQTT Dashboard."
    )
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Broker port (default: 8883 with --tls, otherwise 1883)",
    )
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Enable TLS with paho's default certificate settings",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between sensor samples (default: 2.0)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of samples to publish; 0 means run until Ctrl+C",
    )
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=0)
    parser.add_argument(
        "--retain",
        action="store_true",
        help="Publish retained messages",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.interval <= 0:
        raise SystemExit("--interval phải lớn hơn 0.")
    if args.count < 0:
        raise SystemExit("--count không được âm.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    port = args.port if args.port is not None else (8883 if args.tls else 1883)
    connected = threading.Event()

    def on_connect(
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            logger.error("Broker refused connection: %s", reason_code)
            return
        logger.info("Connected to %s:%s", args.host, port)
        connected.set()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"demo-pub-{random.randint(1000, 9999)}",
        clean_session=True,
    )
    client.on_connect = on_connect

    if args.username is not None:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        client.tls_set()

    logger.info("Connecting to %s:%s (tls=%s)", args.host, port, args.tls)
    client.connect(args.host, port, keepalive=60)
    client.loop_start()

    try:
        if not connected.wait(timeout=10):
            raise RuntimeError("Không nhận được CONNACK từ broker trong 10 giây.")

        temperature = 25.0
        humidity = 60.0
        sent = 0

        while args.count == 0 or sent < args.count:
            # Random walk keeps the chart values realistic.
            temperature = min(35.0, max(15.0, temperature + random.uniform(-0.8, 0.8)))
            humidity = min(90.0, max(30.0, humidity + random.uniform(-2.0, 2.0)))

            temp_info = client.publish(
                TOPIC_TEMP,
                f"{temperature:.2f}",
                qos=args.qos,
                retain=args.retain,
            )
            humi_info = client.publish(
                TOPIC_HUMI,
                f"{humidity:.2f}",
                qos=args.qos,
                retain=args.retain,
            )
            if (
                temp_info.rc != mqtt.MQTT_ERR_SUCCESS
                or humi_info.rc != mqtt.MQTT_ERR_SUCCESS
            ):
                logger.error(
                    "Publish failed (temp rc=%s, humidity rc=%s)",
                    temp_info.rc,
                    humi_info.rc,
                )
            sent += 1
            logger.info(
                "Sample %d: %s=%.2f, %s=%.2f",
                sent,
                TOPIC_TEMP,
                temperature,
                TOPIC_HUMI,
                humidity,
            )
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        try:
            client.disconnect()
        finally:
            client.loop_stop()


if __name__ == "__main__":
    main()
