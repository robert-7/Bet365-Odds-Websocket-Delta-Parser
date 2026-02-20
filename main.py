import asyncio
import os
import logging
from dotenv import load_dotenv
import websockets

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

# Protocol Constants
path_sep = "\x14"   # (20) Topic Load
delta_sep = "\x15"  # (21) Delta Update
handshake_sep = "\x23" # (35) Handshake

# Delimiters
msg_del = "\x08"    # Message delimiter
record_del = "\x01" # Record delimiter
field_del = "\x02"  # Field delimiter


def create_handshake_message(session_id):
    """
    Format: \x23\x03P\x01__time,S_{session_id}\x00
    """
    return f"\x23\x03P\x01__time,S_{session_id}\x00"

def parse_message(message):
    """
    Parses the raw message string from the WebSocket.
    """
    # Messages can be concatenated with \x08
    parts = message.split(msg_del)
    parsed_data = []

    for part in parts:
        if not part:
            continue
        
        # Check for numeric type like "100" (Connection/Config)
        if part.startswith("100" + field_del):
            parsed_data.append({"type": "CONFIG_100", "payload": part[4:]})
            continue

        msg_type_char = part[0]
        payload = part[1:]

        if msg_type_char == path_sep: # \x14 - Initial Topic Load
            # Structure: \x14{Topic}\x01{Data}
            split_payload = payload.split(record_del, 1)
            topic = split_payload[0]
            data = split_payload[1] if len(split_payload) > 1 else ""
            parsed_data.append({
                "type": "TOPIC_LOAD",
                "topic": topic,
                "data": data
            })
            
        elif msg_type_char == delta_sep: # \x15 - Delta Update
             # Structure: \x15{Topic}\x01{Diff}
            split_payload = payload.split(record_del, 1)
            topic = split_payload[0]
            diff = split_payload[1] if len(split_payload) > 1 else ""
            parsed_data.append({
                "type": "DELTA",
                "topic": topic,
                "diff": diff
            })

        elif msg_type_char == handshake_sep:
            parsed_data.append({"type": "HANDSHAKE_RESPONSE", "payload": payload})
            
        else:
            parsed_data.append({"type": "UNKNOWN", "raw": part, "char_code": ord(msg_type_char)})
            
    return parsed_data

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
        handshake_msg = create_handshake_message(TSTK_COOKIE)
        await websocket.send(handshake_msg)
        logger.debug(f"Sent handshake: {handshake_msg!r}")

        while True:
            try:
                message = await websocket.recv()
                # Parse the message
                parsed_messages = parse_message(message)
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
