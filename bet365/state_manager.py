from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from typing import Any


@dataclass
class TopicState:
    """
    Canonical in-memory state for a single Bet365 topic.

    This model is intentionally generic for now. Later steps will define
    how TOPIC_LOAD and DELTA payloads are normalized into `entities`.
    """

    topic: str
    entities: dict[str, Any] = field(default_factory=dict)
    raw_payload: str | None = None
    last_update_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    update_count: int = 0


@dataclass
class OddsStateManager:
    """
    Holds all live topic states.

    Step 1 scope: define a canonical state container only.
    Future steps will add message application (`apply_topic_load`, `apply_delta`)
    and query helpers.
    """

    topics: dict[str, TopicState] = field(default_factory=dict)
