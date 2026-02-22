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
    last_message_type: str | None = None
    last_seen_sequence: int | None = None
    last_seen_topic_ts: str | None = None
    stale_updates_dropped: int = 0
    parse_errors: int = 0
    last_error: str | None = None


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
    delta_ops_applied: int = 0
    delta_ops_skipped: int = 0
    delta_parse_errors: int = 0
    out_of_order_dropped: int = 0
    missing_baseline_deltas: int = 0
    malformed_messages: int = 0
    stale_by_sequence: int = 0
    stale_by_topic_time: int = 0

    def get_topic(self, topic: str) -> TopicState | None:
        """
        Return the current state for a single topic, or None if not found.
        """
        return self.topics.get(topic)

    def get_topics(self) -> dict[str, TopicState]:
        """
        Return all topic states keyed by topic.
        """
        return self.topics

    def snapshot(self) -> dict[str, Any]:
        """
        Return a JSON-serializable snapshot of manager and topic state.
        """
        topics_snapshot: dict[str, dict[str, Any]] = {}
        for topic, topic_state in self.topics.items():
            topics_snapshot[topic] = {
                "topic": topic_state.topic,
                "entities": topic_state.entities,
                "raw_payload": topic_state.raw_payload,
                "last_update_utc": topic_state.last_update_utc.isoformat(),
                "update_count": topic_state.update_count,
                "last_message_type": topic_state.last_message_type,
                "last_seen_sequence": topic_state.last_seen_sequence,
                "last_seen_topic_ts": topic_state.last_seen_topic_ts,
                "stale_updates_dropped": topic_state.stale_updates_dropped,
                "parse_errors": topic_state.parse_errors,
                "last_error": topic_state.last_error,
            }

        return {
            "handled_messages": self.handled_messages,
            "ignored_messages": self.ignored_messages,
            "unknown_types": self.unknown_types,
            "delta_ops_applied": self.delta_ops_applied,
            "delta_ops_skipped": self.delta_ops_skipped,
            "delta_parse_errors": self.delta_parse_errors,
            "out_of_order_dropped": self.out_of_order_dropped,
            "missing_baseline_deltas": self.missing_baseline_deltas,
            "malformed_messages": self.malformed_messages,
            "stale_by_sequence": self.stale_by_sequence,
            "stale_by_topic_time": self.stale_by_topic_time,
            "topic_count": len(self.topics),
            "topics": topics_snapshot,
        }

    def topic_summaries(self, limit: int = 5) -> list[dict[str, Any]]:
        """
        Return compact per-topic summaries ordered by most recently updated.
        """
        ordered_topics = sorted(
            self.topics.values(),
            key=lambda t: t.last_update_utc,
            reverse=True,
        )

        summaries: list[dict[str, Any]] = []
        for topic_state in ordered_topics[:limit]:
            key_values = topic_state.entities.get("key_values", {}) if isinstance(topic_state.entities, dict) else {}
            summaries.append(
                {
                    "topic": topic_state.topic,
                    "update_count": topic_state.update_count,
                    "last_message_type": topic_state.last_message_type,
                    "last_seen_topic_ts": topic_state.last_seen_topic_ts,
                    "key_count": len(key_values) if isinstance(key_values, dict) else 0,
                    "stale_updates_dropped": topic_state.stale_updates_dropped,
                    "parse_errors": topic_state.parse_errors,
                }
            )

        return summaries

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
            self.malformed_messages += 1
        # DELTA carries incremental updates for a topic. For now we store the
        # latest raw delta payload and update metadata; patch semantics come later.
        elif message_type == "DELTA":
            topic = message.get("topic")
            diff = message.get("diff")
            if isinstance(topic, str) and isinstance(diff, str):
                self._apply_delta(topic=topic, diff=diff)
                self.handled_messages += 1
                return
            self.malformed_messages += 1
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

    def _record_applied_update(
        self,
        topic_state: TopicState,
        message_type: str,
        sequence: int | None,
        topic_ts: str | None,
    ) -> None:
        topic_state.last_message_type = message_type
        if sequence is not None:
            topic_state.last_seen_sequence = sequence
        if topic_ts is not None:
            topic_state.last_seen_topic_ts = topic_ts
        topic_state.last_error = None
        self._touch_topic(topic_state)

    def _drop_as_stale(self, topic_state: TopicState, reason: str) -> None:
        topic_state.stale_updates_dropped += 1
        topic_state.last_error = reason
        self.out_of_order_dropped += 1

    def _should_apply_update(
        self,
        topic_state: TopicState,
        sequence: int | None,
        topic_ts: str | None,
    ) -> tuple[bool, str | None]:
        if sequence is not None and topic_state.last_seen_sequence is not None:
            if sequence <= topic_state.last_seen_sequence:
                self.stale_by_sequence += 1
                return False, "stale_by_sequence"

        if topic_ts is not None and topic_state.last_seen_topic_ts is not None:
            if topic_ts <= topic_state.last_seen_topic_ts:
                self.stale_by_topic_time += 1
                return False, "stale_by_topic_time"

        return True, None

    def _extract_topic_ts(self, key_values: dict[str, str]) -> str | None:
        topic_ts = key_values.get("TI")
        if isinstance(topic_ts, str) and topic_ts:
            return topic_ts
        return None

    def _extract_sequence(self, key_values: dict[str, str]) -> int | None:
        for key in ("SEQ", "SN", "SE"):
            raw = key_values.get(key)
            if not isinstance(raw, str) or not raw:
                continue
            try:
                return int(raw)
            except ValueError:
                continue
        return None

    def _apply_topic_load(self, topic: str, data: str) -> None:
        """
        Parse and apply a full topic snapshot.

        TOPIC_LOAD is authoritative for the topic, so entities are replaced.
        """
        topic_state = self._ensure_topic(topic)
        try:
            parsed_snapshot = self._parse_topic_load_snapshot(data)
            key_values = parsed_snapshot.get("key_values", {})
            if not isinstance(key_values, dict):
                key_values = {}

            sequence = self._extract_sequence(key_values)
            topic_ts = self._extract_topic_ts(key_values)
            should_apply, reason = self._should_apply_update(topic_state, sequence, topic_ts)
            if not should_apply and reason is not None:
                self._drop_as_stale(topic_state, reason)
                return

            topic_state.entities = parsed_snapshot
        except Exception as e:
            self.malformed_messages += 1
            topic_state.parse_errors += 1
            topic_state.entities = {
                "_raw": data,
                "_parse_error": str(e),
            }
            topic_state.last_error = str(e)
            self._touch_topic(topic_state)
            return

        topic_state.raw_payload = data
        self._record_applied_update(
            topic_state=topic_state,
            message_type="TOPIC_LOAD",
            sequence=sequence,
            topic_ts=topic_ts,
        )

    def _apply_delta(self, topic: str, diff: str) -> None:
        """
        Parse and apply incremental updates for an existing topic state.

        Step 4 behavior patches the `entities["key_values"]` map with
        upserts parsed from DELTA payloads, while preserving unknown tokens
        for future protocol refinement.
        """
        is_new_topic = topic not in self.topics
        topic_state = self._ensure_topic(topic)
        if is_new_topic:
            self.missing_baseline_deltas += 1

        if not isinstance(topic_state.entities, dict):
            topic_state.entities = {}

        key_values = topic_state.entities.get("key_values")
        if not isinstance(key_values, dict):
            key_values = {}
            topic_state.entities["key_values"] = key_values

        try:
            delta_ops = self._parse_delta_operations(diff)

            sequence = self._extract_sequence(delta_ops["upserts"])
            topic_ts = self._extract_topic_ts(delta_ops["upserts"])
            should_apply, reason = self._should_apply_update(topic_state, sequence, topic_ts)
            if not should_apply and reason is not None:
                self._drop_as_stale(topic_state, reason)
                return

            for key, value in delta_ops["upserts"].items():
                key_values[key] = value
                self.delta_ops_applied += 1

            if delta_ops["unparsed_tokens"]:
                topic_state.entities["delta_unparsed_tokens"] = delta_ops["unparsed_tokens"]
                self.delta_ops_skipped += len(delta_ops["unparsed_tokens"])
        except Exception as e:
            self.delta_parse_errors += 1
            self.malformed_messages += 1
            topic_state.parse_errors += 1
            topic_state.entities["delta_parse_error"] = str(e)
            topic_state.last_error = str(e)
            self._touch_topic(topic_state)
            return

        topic_state.raw_payload = diff
        self._record_applied_update(
            topic_state=topic_state,
            message_type="DELTA",
            sequence=sequence,
            topic_ts=topic_ts,
        )

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

    def _parse_delta_operations(self, diff: str) -> dict[str, Any]:
        """
        Parse a DELTA payload into patch operations.

        Current safe subset:
        - `key=value` tokens are treated as upserts to key-values state
        - non `key=value` tokens are preserved as unparsed tokens
        """
        upserts: dict[str, str] = {}
        unparsed_tokens: list[str] = []

        for section in diff.split("|"):
            section_text = section.strip()
            if not section_text:
                continue

            for token in section_text.split(";"):
                token_text = token.strip()
                if not token_text:
                    continue

                if "=" not in token_text:
                    unparsed_tokens.append(token_text)
                    continue

                key, value = token_text.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    unparsed_tokens.append(token_text)
                    continue

                upserts[key] = value

        return {
            "upserts": upserts,
            "unparsed_tokens": unparsed_tokens,
        }
