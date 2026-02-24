import logging


def log_state_summary(logger: logging.Logger, summary: dict, top_topics: list[dict]) -> None:
    logger.info(
        "[STATE] topics=%s handled=%s ignored=%s(config=%s,handshake=%s) unknown=%s stale_dropped=%s top=%s",
        summary["topic_count"],
        summary["handled_messages"],
        summary["ignored_messages"],
        summary["ignored_config"],
        summary["ignored_handshake_response"],
        summary["unknown_types"],
        summary["out_of_order_dropped"],
        top_topics,
    )


def _delta_change_summary(before: dict[str, str], after: dict[str, str], limit: int = 6) -> tuple[int, str]:
    changed_keys = sorted({*before.keys(), *after.keys()})
    changed_keys = [key for key in changed_keys if before.get(key) != after.get(key)]

    if not changed_keys:
        return 0, ""

    entries: list[str] = []
    for key in changed_keys[:limit]:
        entries.append(f"{key}: {before.get(key, '<missing>')} -> {after.get(key, '<missing>')}")

    if len(changed_keys) > limit:
        entries.append(f"... +{len(changed_keys) - limit} more")

    return len(changed_keys), "; ".join(entries)


def log_delta(logger: logging.Logger, topic: str, diff_len: int, before_values: dict[str, str], after_values: dict[str, str]) -> None:
    logger.info("[DELTA] Topic: %s | Diff Len: %s", topic, diff_len)

    changed_count, changed_summary = _delta_change_summary(before_values, after_values)
    if changed_count > 0:
        logger.info(
            "[DELTA_APPLIED] Topic: %s | Changed Keys: %s | %s",
            topic,
            changed_count,
            changed_summary,
        )
    else:
        logger.info("[DELTA_APPLIED] Topic: %s | No key-value changes", topic)


def log_connected(logger: logging.Logger) -> None:
    logger.info("Connected to Bet365 WebSocket.")


def log_ws_keepalive(logger: logging.Logger, ping_interval: int | None, ping_timeout: int | None) -> None:
    logger.info(
        "[WS_KEEPALIVE] ping_interval=%s ping_timeout=%s",
        ping_interval,
        ping_timeout,
    )


def log_heartbeat_sent(logger: logging.Logger, source: str, payload_len: int, payload_prefix: str) -> None:
    logger.info(
        "[HEARTBEAT] Sent keepalive (%s) | payload_len=%s | prefix=%r",
        source,
        payload_len,
        payload_prefix,
    )


def log_heartbeat_loop_started(logger: logging.Logger, interval: int) -> None:
    logger.info("[HEARTBEAT] Loop started (interval=%ss)", interval)


def log_heartbeat_loop_cancelled(logger: logging.Logger) -> None:
    logger.info("[HEARTBEAT] Loop cancelled")


def log_heartbeat_disabled_interval(logger: logging.Logger) -> None:
    logger.warning("Heartbeat disabled because HEARTBEAT_INTERVAL_SECONDS <= 0")


def log_heartbeat_disabled_periodic(logger: logging.Logger) -> None:
    logger.info("[HEARTBEAT] Periodic heartbeat disabled (initial keepalive only)")


def log_heartbeat_error(logger: logging.Logger, error: Exception) -> None:
    logger.warning("[HEARTBEAT_ERROR] Failed to send heartbeat: %s", error)


def log_connection_failed(logger: logging.Logger, error: Exception) -> None:
    logger.error("Connection failed: %s", error)


def log_reconnecting(logger: logging.Logger, delay_seconds: int, attempt: int) -> None:
    logger.info("Reconnecting in %ss (attempt=%s)", delay_seconds, attempt)


def log_connection_closed(
    logger: logging.Logger,
    code: int,
    reason: str,
    received: str | None,
    sent: str | None,
    seconds_since_heartbeat: float,
) -> None:
    logger.error(
        "Connection closed: code=%s reason=%r rcvd=%s sent=%s since_last_heartbeat=%.2fs.",
        code,
        reason,
        received,
        sent,
        seconds_since_heartbeat,
    )
