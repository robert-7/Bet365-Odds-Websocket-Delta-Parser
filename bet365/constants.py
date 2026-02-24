from enum import StrEnum

class ProtocolConstants(StrEnum):
    """
    Constants used in the Bet365 zap-protocol-v1.
    These bytes act as Message Types or Commands. They tell the parser *what kind of data*
    it is currently looking at (e.g., a snapshot, a diff, or a handshake).
    They usually appear at the very beginning of a message block.
    """
    TOPIC_LOAD = "\x14"   # (20) Initial Topic Load (Snapshot)
    DELTA = "\x15"        # (21) Delta Update (Diff)
    SUBSCRIBE = "\x16"    # (22) Subscribe to topics
    HANDSHAKE = "\x23"    # (35) Handshake

class Delimiters(StrEnum):
    """
    Delimiters used to separate messages, records, and fields.
    These bytes act as Separators. They don't carry meaning on their own; they just tell
    the parser *how the data is structured* and where one piece of data ends and the next begins.
    """
    NULL = "\x00"         # Null terminator (Marks the absolute end of a string or payload)
    RECORD = "\x01"       # Record delimiter (Separates the Topic ID from the Payload/Data)
    FIELD = "\x02"        # Field delimiter (Separates individual key-value pairs within the payload)
    HANDSHAKE = "\x03"    # Handshake delimiter
    MESSAGE = "\x08"      # Message delimiter (Separates multiple distinct messages sent in a single WebSocket frame)
