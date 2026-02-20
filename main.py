import asyncio
import os
import logging
from dotenv import load_dotenv
import websockets

from bet365 import Bet365Parser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()
BET365_WEBSOCKET_URL = os.getenv("BET365_WEBSOCKET_URL")
TSTK_COOKIE = os.getenv("TSTK_COOKIE")

ORIGIN = "https://www.on.bet365.ca"
SUBPROTOCOL = "zap-protocol-v1"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

async def run():
    extra_headers = [
        ("Accept-Language", "en-CA,en-US;q=0.9,en;q=0.8")
    ]
    # Connect to the Bet365 WebSocket and listen for messages indefinitely
    async with websockets.connect(
        uri=BET365_WEBSOCKET_URL,
        origin=ORIGIN,                      # matches DevTools
        subprotocols=[SUBPROTOCOL],         # matches Sec-WebSocket-Protocol
        user_agent_header=UA,               # matches browser-ish UA (optional but helpful)
        additional_headers=extra_headers    # matches other headers from DevTools (optional but helpful)
    ) as websocket:
        logger.info("Connected to Bet365 WebSocket.")

        # Send Handshake immediately
        handshake_msg = Bet365Parser.create_handshake_message(TSTK_COOKIE)
        await websocket.send(handshake_msg)
        logger.debug(f"Sent handshake: {handshake_msg!r}")

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

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("\nStopped.")
