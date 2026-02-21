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
    handled_messages: int = 0
    ignored_messages: int = 0
    unknown_types: int = 0

    def apply_message(self, message: dict[str, Any]) -> None:
        """
        Public entry point for applying one parsed websocket message.
        """
        message_type = message.get("type")

        # TOPIC_LOAD carries a full snapshot for a topic. We parse a structured
        # snapshot and replace topic entities with this authoritative state.
        if message_type == "TOPIC_LOAD":
            topic = message.get("topic")
            data = message.get("data")
            if isinstance(topic, str) and isinstance(data, str):
                self._apply_topic_load(topic=topic, data=data)
                self.handled_messages += 1
                return
        # DELTA carries incremental updates for a topic. For now we store the
        # latest raw delta payload and update metadata; patch semantics come later.
        elif message_type == "DELTA":
            topic = message.get("topic")
            diff = message.get("diff")
            if isinstance(topic, str) and isinstance(diff, str):
                self._apply_delta(topic=topic, diff=diff)
                self.handled_messages += 1
                return
        # CONFIG_100 and HANDSHAKE_RESPONSE are connection/control-plane events,
        # not odds/topic state updates, so we intentionally ignore them for now.
        elif message_type in {"CONFIG_100", "HANDSHAKE_RESPONSE"}:
            self.ignored_messages += 1
            return

        # Any other type (or malformed payload) is tracked as unknown so we can
        # inspect protocol variations later without breaking the listener.
        self.unknown_types += 1

    def _ensure_topic(self, topic: str) -> TopicState:
        """
        Return the existing `TopicState` for a topic, or create it if missing.
        """
        topic_state = self.topics.get(topic)
        if topic_state is None:
            topic_state = TopicState(topic=topic)
            self.topics[topic] = topic_state
        return topic_state

    def _touch_topic(self, topic_state: TopicState) -> None:
        """
        Update common per-topic metadata when a state-changing message is applied.
        """
        topic_state.last_update_utc = datetime.now(timezone.utc)
        topic_state.update_count += 1

    def _apply_topic_load(self, topic: str, data: str) -> None:
        """
        Parse and apply a full topic snapshot.

        TOPIC_LOAD is authoritative for the topic, so entities are replaced.
        """
        topic_state = self._ensure_topic(topic)
        try:
            topic_state.entities = self._parse_topic_load_snapshot(data)
        except Exception as exc:
            topic_state.entities = {
                "_raw": data,
                "_parse_error": str(exc),
            }
        topic_state.raw_payload = data
        self._touch_topic(topic_state)

    def _apply_delta(self, topic: str, diff: str) -> None:
        """
        Step 2 behavior: store latest raw delta payload for the topic.
        Step 4 will parse delta operations and patch normalized entities.
        """
        topic_state = self._ensure_topic(topic)
        topic_state.raw_payload = diff
        self._touch_topic(topic_state)

    def _parse_topic_load_snapshot(self, data: str) -> dict[str, Any]:
        """
        Parse a raw TOPIC_LOAD payload into a structured dictionary.

        Current strategy intentionally parses a safe subset:
        - split sections by '|'
        - parse key/value tokens in each section (e.g., TI=..., UF=...)
        - preserve unknown tokens for later protocol refinement
        """
        sections: list[dict[str, Any]] = []
        key_values: dict[str, str] = {}

        for raw_section in data.split("|"):
            section_text = raw_section.strip()
            if not section_text:
                continue

            section_values: dict[str, str] = {}
            section_tokens: list[str] = []

            for token in section_text.split(";"):
                token_text = token.strip()
                if not token_text:
                    continue

                if "=" in token_text:
                    key, value = token_text.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if key:
                        section_values[key] = value
                        key_values[key] = value
                    else:
                        section_tokens.append(token_text)
                else:
                    section_tokens.append(token_text)

            sections.append(
                {
                    "raw": section_text,
                    "values": section_values,
                    "tokens": section_tokens,
                }
            )

        return {
            "sections": sections,
            "key_values": key_values,
            "section_count": len(sections),
        }
