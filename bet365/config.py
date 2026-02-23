class Config:
    """
    Configuration settings for the Bet365 WebSocket connection.
    """
    ORIGIN = "https://www.on.bet365.ca"
    SUBPROTOCOL = "zap-protocol-v1"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
    ACCEPT_LANGUAGE = "en-CA,en-US;q=0.9,en;q=0.8"
    STATE_SUMMARY_INTERVAL_SECONDS = 30
    HEARTBEAT_INTERVAL_SECONDS = 20
