import json
import logging
import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from typing import Any
from typing import Callable

logger = logging.getLogger(__name__)


class _StateRequestHandler(BaseHTTPRequestHandler):
    get_state_snapshot: Callable[[], dict[str, Any]] | None = None

    def do_GET(self) -> None:
        if self.path not in {"/state", "/state/"}:
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            payload = {"error": "not_found", "path": self.path}
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))
            return

        if self.get_state_snapshot is None:
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            payload = {"error": "state_provider_not_configured"}
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))
            return

        snapshot = self.get_state_snapshot()
        body = json.dumps(snapshot, indent=2, sort_keys=True).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        logger.debug("state_server: " + format, *args)


def start_state_server(port: int, get_state_snapshot: Callable[[], dict[str, Any]]) -> ThreadingHTTPServer:
    _StateRequestHandler.get_state_snapshot = get_state_snapshot

    server = ThreadingHTTPServer(("0.0.0.0", port), _StateRequestHandler)
    thread = threading.Thread(target=server.serve_forever, name="state-server", daemon=True)
    thread.start()
    return server
