import asyncio
import contextlib
import logging
import time

import websockets

from .config import Config
from .metrics import classify_topic
from .metrics import CONNECTION_UP
from .metrics import HEARTBEAT_ERRORS_TOTAL
from .metrics import HEARTBEAT_LAST_SENT_UNIX
from .metrics import HEARTBEATS_SENT_TOTAL
from .metrics import MESSAGES_TOTAL
from .metrics import PARSE_ERRORS_TOTAL
from .metrics import RECONNECTS_TOTAL
from .metrics import TOPIC_MESSAGES_TOTAL
from .parser import Bet365Parser
from .state_manager import OddsStateManager

logger = logging.getLogger(__name__)

class Bet365Client:
    """
    A WebSocket client for connecting to the Bet365 zap-protocol-v1 endpoint.
    """

    def __init__(self, url: str, session_cookie: str):
        self.url = url
        self.session_cookie = session_cookie
        self.state_manager = OddsStateManager()
        self._last_state_log_time = 0.0
        self._last_heartbeat_monotonic: float | None = None
        self.extra_headers = [
            ("Accept-Language", Config.ACCEPT_LANGUAGE)
        ]

    def _maybe_log_state_summary(self) -> None:
        now = time.monotonic()
        if now - self._last_state_log_time < Config.STATE_SUMMARY_INTERVAL_SECONDS:
            return

        summary = self.state_manager.snapshot()
        top_topics = self.state_manager.topic_summaries(limit=3)
        logger.info(
            "[STATE] topics=%s handled=%s ignored=%s unknown=%s stale_dropped=%s top=%s",
            summary["topic_count"],
            summary["handled_messages"],
            summary["ignored_messages"],
            summary["unknown_types"],
            summary["out_of_order_dropped"],
            top_topics,
        )
        self._last_state_log_time = now

    async def _send_heartbeat(self, websocket, source: str) -> None:
        heartbeat_msg = Bet365Parser.create_handshake_message(self.session_cookie)
        await websocket.send(heartbeat_msg)
        HEARTBEATS_SENT_TOTAL.inc()
        HEARTBEAT_LAST_SENT_UNIX.set(time.time())
        self._last_heartbeat_monotonic = time.monotonic()
        logger.info(
            "[HEARTBEAT] Sent keepalive (%s) | payload_len=%s | prefix=%r",
            source,
            len(heartbeat_msg),
            heartbeat_msg[:12],
        )

    async def _heartbeat_loop(self, websocket) -> None:
        interval = Config.HEARTBEAT_INTERVAL_SECONDS
        if interval <= 0:
            logger.warning("Heartbeat disabled because HEARTBEAT_INTERVAL_SECONDS <= 0")
            return

        logger.info("[HEARTBEAT] Loop started (interval=%ss)", interval)

        while True:
            await asyncio.sleep(interval)
            try:
                await self._send_heartbeat(websocket, source="periodic")
            except asyncio.CancelledError:
                logger.info("[HEARTBEAT] Loop cancelled")
                raise
            except Exception as e:
                HEARTBEAT_ERRORS_TOTAL.inc()
                logger.warning("[HEARTBEAT_ERROR] Failed to send heartbeat: %s", e)

    async def connect(self):
        """
        Establishes the WebSocket connection, sends the handshake, and starts listening.
        """
        try:
            async with websockets.connect(
                uri=self.url,
                origin=Config.ORIGIN,
                subprotocols=[Config.SUBPROTOCOL],
                user_agent_header=Config.USER_AGENT,
                additional_headers=self.extra_headers,
                ping_interval=Config.WEBSOCKET_PING_INTERVAL_SECONDS,
                ping_timeout=Config.WEBSOCKET_PING_TIMEOUT_SECONDS,
            ) as websocket:
                logger.info("Connected to Bet365 WebSocket.")
                logger.info(
                    "[WS_KEEPALIVE] ping_interval=%s ping_timeout=%s",
                    Config.WEBSOCKET_PING_INTERVAL_SECONDS,
                    Config.WEBSOCKET_PING_TIMEOUT_SECONDS,
                )
                CONNECTION_UP.set(1)
                heartbeat_task: asyncio.Task | None = None

                # Send Handshake immediately after connecting to establish the session and keep the connection alive
                await self._send_heartbeat(websocket, source="initial")
                if Config.ENABLE_PERIODIC_HEARTBEAT:
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket))
                else:
                    logger.info("[HEARTBEAT] Periodic heartbeat disabled (initial keepalive only)")

                try:
                    await self._listen(websocket)
                finally:
                    if heartbeat_task is not None:
                        heartbeat_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat_task

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            CONNECTION_UP.set(0)
            RECONNECTS_TOTAL.inc()

    async def _listen(self, websocket):
        """
        Internal loop to receive and parse messages continuously.
        """
        while True:
            try:
                message = await websocket.recv()
                # Parse the message
                parsed_messages = Bet365Parser.parse_message(message)

                for pm in parsed_messages:
                    message_type = pm.get("type", "UNKNOWN")
                    MESSAGES_TOTAL.labels(type=message_type).inc()
                    self.state_manager.apply_message(pm)

                    if pm['type'] == 'CONFIG_100':
                         logger.info(f"[CONFIG] Payload: {pm['payload']}")
                    elif pm['type'] == 'TOPIC_LOAD':
                        topic_class = classify_topic(pm["topic"])
                        TOPIC_MESSAGES_TOTAL.labels(topic_class=topic_class, topic=pm["topic"]).inc()
                        logger.info(f"[LOAD] Topic: {pm['topic']}")
                        topic_state = self.state_manager.topics.get(pm["topic"])
                        if topic_state is not None:
                            section_count = topic_state.entities.get("section_count")
                            logger.debug("[LOAD_PARSED] Topic: %s | Sections: %s", pm["topic"], section_count)
                    elif pm['type'] == 'DELTA':
                        topic_class = classify_topic(pm["topic"])
                        TOPIC_MESSAGES_TOTAL.labels(topic_class=topic_class, topic=pm["topic"]).inc()
                        logger.info(f"[DELTA] Topic: {pm['topic']} | Diff Len: {len(pm['diff'])}")
                    elif pm['type'] == 'HANDSHAKE_RESPONSE':
                        logger.info(f"[HANDSHAKE_ACK] {pm['payload']}")
                    else:
                        logger.warning(f"[UNKNOWN] {pm}")

                self._maybe_log_state_summary()

            except websockets.ConnectionClosed as e:
                seconds_since_heartbeat: float | None = None
                if self._last_heartbeat_monotonic is not None:
                    seconds_since_heartbeat = time.monotonic() - self._last_heartbeat_monotonic

                logger.error(
                    "Connection closed: code=%s reason=%r rcvd=%s sent=%s since_last_heartbeat=%.2fs. Reconnecting...",
                    e.code,
                    e.reason,
                    e.rcvd,
                    e.sent,
                    seconds_since_heartbeat if seconds_since_heartbeat is not None else -1.0,
                )
                CONNECTION_UP.set(0)
                RECONNECTS_TOTAL.inc()
                break
            except Exception as e:
                logger.error(f"Error while processing websocket message: {e}")
                PARSE_ERRORS_TOTAL.inc()
