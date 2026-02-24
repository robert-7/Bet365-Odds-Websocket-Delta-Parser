import re

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import start_http_server

MESSAGES_TOTAL = Counter(
    "bet365_messages_total",
    "Total number of parsed websocket messages by type.",
    ["type"],
)

TOPIC_MESSAGES_TOTAL = Counter(
    "bet365_topic_messages_total",
    "Total number of messages per topic class and topic.",
    ["topic_class", "topic"],
)

CONNECTION_UP = Gauge(
    "bet365_connection_up",
    "Whether websocket connection is currently up (1) or down (0).",
)

RECONNECTS_TOTAL = Counter(
    "bet365_reconnects_total",
    "Total number of websocket reconnect/close events.",
)

PARSE_ERRORS_TOTAL = Counter(
    "bet365_parse_errors_total",
    "Total number of message processing errors.",
)

HEARTBEATS_SENT_TOTAL = Counter(
    "bet365_heartbeats_sent_total",
    "Total number of heartbeat messages sent to keep websocket session alive.",
)

HEARTBEAT_ERRORS_TOTAL = Counter(
    "bet365_heartbeat_errors_total",
    "Total number of heartbeat send errors.",
)

HEARTBEAT_LAST_SENT_UNIX = Gauge(
    "bet365_heartbeat_last_sent_unix",
    "Unix timestamp of the most recent successfully sent heartbeat.",
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)


def classify_topic(topic: str) -> str:
    if topic == "__time":
        return "time"
    if topic.startswith("S_"):
        return "session"
    if topic.startswith("OV"):
        return "overview"
    if topic.startswith("CONFIG"):
        return "config"
    if re.match(r"^[0-9]+V", topic) or topic.startswith("EV"):
        return "event_match"
    if re.match(r"^[A-Za-z]+_", topic):
        return "named"
    return "other"
