from .constants import Delimiters
from .constants import ProtocolConstants

class Bet365Parser:
    """
    Parser for the Bet365 zap-protocol-v1 WebSocket messages.
    """

    @staticmethod
    def create_handshake_message(session_id: str) -> str:
        """
        Constructs the initial handshake message required to keep the connection alive.
        Format: \x23\x03P\x01__time,S_{session_id}\x00
        """
        return f"{ProtocolConstants.HANDSHAKE}{Delimiters.HANDSHAKE}P{Delimiters.RECORD}__time,S_{session_id}{Delimiters.NULL}"

    @staticmethod
    def create_subscribe_message(topics: list[str]) -> str:
        """
        Constructs a message to subscribe to specific topics (e.g., game odds).
        Format: \x16\x00{topic_1},{topic_2}\x01
        """
        topic_string = ",".join(topics)
        return f"{ProtocolConstants.SUBSCRIBE}{Delimiters.NULL}{topic_string}{Delimiters.RECORD}"

    @staticmethod
    def parse_message(message: str) -> list[dict]:
        """
        Parses the raw message string from the WebSocket into a list of structured dictionaries.
        """
        # Messages can be concatenated with \x08
        parts = message.split(Delimiters.MESSAGE)
        parsed_data = []

        for part in parts:
            if not part:
                continue

            # Check for numeric type like "100" (Connection/Config)
            if part.startswith("100" + Delimiters.FIELD):
                parsed_data.append({"type": "CONFIG_100", "payload": part[4:]})
                continue

            msg_type_char = part[0]
            payload = part[1:]

            if msg_type_char == ProtocolConstants.TOPIC_LOAD: # \x14 - Initial Topic Load
                # Structure: \x14{Topic}\x01{Data}
                split_payload = payload.split(Delimiters.RECORD, 1)
                topic = split_payload[0]
                data = split_payload[1] if len(split_payload) > 1 else ""
                parsed_data.append({
                    "type": "TOPIC_LOAD",
                    "topic": topic,
                    "data": data
                })

            elif msg_type_char == ProtocolConstants.DELTA: # \x15 - Delta Update
                 # Structure: \x15{Topic}\x01{Diff}
                split_payload = payload.split(Delimiters.RECORD, 1)
                topic = split_payload[0]
                diff = split_payload[1] if len(split_payload) > 1 else ""
                parsed_data.append({
                    "type": "DELTA",
                    "topic": topic,
                    "diff": diff
                })

            elif msg_type_char == ProtocolConstants.HANDSHAKE:
                parsed_data.append({"type": "HANDSHAKE_RESPONSE", "payload": payload})

            else:
                parsed_data.append({"type": "UNKNOWN", "raw": part, "char_code": ord(msg_type_char)})

        return parsed_data
