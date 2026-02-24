import asyncio
import contextlib
import logging
import time

import websockets

from . import output
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
        output.log_state_summary(logger, summary, top_topics)
        self._last_state_log_time = now

    def _topic_key_values_snapshot(self, topic: str) -> dict[str, str]:
        topic_state = self.state_manager.topics.get(topic)
        if topic_state is None or not isinstance(topic_state.entities, dict):
            return {}

        key_values = topic_state.entities.get("key_values")
        if not isinstance(key_values, dict):
            return {}

        return {str(key): str(value) for key, value in key_values.items()}

    async def _send_heartbeat(self, websocket, source: str) -> None:
        heartbeat_msg = Bet365Parser.create_handshake_message(self.session_cookie)
        await websocket.send(heartbeat_msg)
        HEARTBEATS_SENT_TOTAL.inc()
        HEARTBEAT_LAST_SENT_UNIX.set(time.time())
        self._last_heartbeat_monotonic = time.monotonic()
        output.log_heartbeat_sent(logger, source, len(heartbeat_msg), heartbeat_msg[:12])

    async def _heartbeat_loop(self, websocket) -> None:
        interval = Config.HEARTBEAT_INTERVAL_SECONDS
        if interval <= 0:
            output.log_heartbeat_disabled_interval(logger)
            return

        output.log_heartbeat_loop_started(logger, interval)

        while True:
            await asyncio.sleep(interval)
            try:
                await self._send_heartbeat(websocket, source="periodic")
            except asyncio.CancelledError:
                output.log_heartbeat_loop_cancelled(logger)
                raise
            except Exception as e:
                HEARTBEAT_ERRORS_TOTAL.inc()
                output.log_heartbeat_error(logger, e)

    async def connect(self):
        """
        Establishes and maintains the WebSocket connection with reconnects.
        """
        attempt = 0
        while True:
            heartbeat_task: asyncio.Task | None = None
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
                    output.log_connected(logger)
                    output.log_ws_keepalive(
                        logger,
                        Config.WEBSOCKET_PING_INTERVAL_SECONDS,
                        Config.WEBSOCKET_PING_TIMEOUT_SECONDS,
                    )
                    CONNECTION_UP.set(1)
                    attempt = 0

                    # Send Handshake immediately after connecting to establish the session and keep the connection alive
                    await self._send_heartbeat(websocket, source="initial")

                    # Subscribe to topics (e.g., In-Play overview to get game odds)
                    # You can change these topics based on what you need (e.g., specific match IDs)
                    topics_to_subscribe = ["CONFIG_1_3", "OVInPlay_1_3", "Media_L1_Z3"]
                    subscribe_msg = Bet365Parser.create_subscribe_message(topics_to_subscribe)
                    await websocket.send(subscribe_msg)
                    logger.info(f"[SUBSCRIBE] Sent subscription for topics: {topics_to_subscribe}")

                    if Config.ENABLE_PERIODIC_HEARTBEAT:
                        heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket))
                    else:
                        output.log_heartbeat_disabled_periodic(logger)

                    await self._listen(websocket)

            except asyncio.CancelledError:
                CONNECTION_UP.set(0)
                raise
            except Exception as e:
                output.log_connection_failed(logger, e)
            finally:
                CONNECTION_UP.set(0)
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task

            attempt += 1
            RECONNECTS_TOTAL.inc()
            output.log_reconnecting(logger, Config.RECONNECT_DELAY_SECONDS, attempt)
            await asyncio.sleep(Config.RECONNECT_DELAY_SECONDS)

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

                    before_values: dict[str, str] = {}
                    if message_type == "DELTA":
                        topic = pm.get("topic")
                        if isinstance(topic, str):
                            before_values = self._topic_key_values_snapshot(topic)

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
                        after_values = self._topic_key_values_snapshot(pm["topic"])
                        output.log_delta(logger, pm["topic"], len(pm["diff"]), before_values, after_values)
                    elif pm['type'] == 'HANDSHAKE_RESPONSE':
                        logger.info(f"[HANDSHAKE_ACK] {pm['payload']}")
                    else:
                        logger.warning(f"[UNKNOWN] {pm}")

                self._maybe_log_state_summary()

            except websockets.ConnectionClosed as e:
                seconds_since_heartbeat: float | None = None
                if self._last_heartbeat_monotonic is not None:
                    seconds_since_heartbeat = time.monotonic() - self._last_heartbeat_monotonic

                output.log_connection_closed(
                    logger,
                    e.code,
                    e.reason,
                    e.rcvd,
                    e.sent,
                    seconds_since_heartbeat if seconds_since_heartbeat is not None else -1.0,
                )
                break
            except Exception as e:
                logger.error(f"Error while processing websocket message: {e}")
                PARSE_ERRORS_TOTAL.inc()
