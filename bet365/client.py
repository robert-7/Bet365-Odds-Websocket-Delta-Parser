import asyncio
import logging
import websockets

from .parser import Bet365Parser

logger = logging.getLogger(__name__)

class Bet365Client:
    """
    A WebSocket client for connecting to the Bet365 zap-protocol-v1 endpoint.
    """
    
    ORIGIN = "https://www.on.bet365.ca"
    SUBPROTOCOL = "zap-protocol-v1"
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )

    def __init__(self, url: str, session_cookie: str):
        self.url = url
        self.session_cookie = session_cookie
        self.extra_headers = [
            ("Accept-Language", "en-CA,en-US;q=0.9,en;q=0.8")
        ]

    async def connect(self):
        """
        Establishes the WebSocket connection, sends the handshake, and starts listening.
        """
        try:
            async with websockets.connect(
                uri=self.url,
                origin=self.ORIGIN,
                subprotocols=[self.SUBPROTOCOL],
                user_agent_header=self.UA,
                additional_headers=self.extra_headers
            ) as websocket:
                logger.info("Connected to Bet365 WebSocket.")

                # Send Handshake immediately after connecting to establish the session and keep the connection alive
                handshake_msg = Bet365Parser.create_handshake_message(self.session_cookie)
                await websocket.send(handshake_msg)
                logger.debug(f"Sent handshake: {handshake_msg!r}")

                await self._listen(websocket)

        except Exception as e:
            logger.error(f"Connection failed: {e}")

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
                    if pm['type'] == 'CONFIG_100':
                         logger.info(f"[CONFIG] Payload: {pm['payload']}")
                    elif pm['type'] == 'TOPIC_LOAD':
                        logger.info(f"[LOAD] Topic: {pm['topic']}")
                    elif pm['type'] == 'DELTA':
                        logger.info(f"[DELTA] Topic: {pm['topic']} | Diff Len: {len(pm['diff'])}")
                    elif pm['type'] == 'HANDSHAKE_RESPONSE':
                        logger.info(f"[HANDSHAKE_ACK] {pm['payload']}")
                    else:
                        logger.warning(f"[UNKNOWN] {pm}")

            except websockets.ConnectionClosed as e:
                logger.error(f"Connection closed: {e}. Reconnecting...")
                break
