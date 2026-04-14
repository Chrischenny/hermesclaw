"""WeChat/iLink communication SDK used by HermesClaw.

This module focuses only on the communication layer:
1) building iLink-compliant HTTP requests,
2) performing long-poll updates and sendmessage calls,
3) exposing a reusable local proxy handler for gateway processes.

The routing strategy (Hermes/OpenClaw/Both) is intentionally NOT owned by this
SDK. Business code can inject small callbacks to decide whether tagging should
be applied for a target user.
"""

from __future__ import annotations

import json
import logging
import secrets
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

import requests

# Public logger for this SDK.  The host app can configure global logging.
log = logging.getLogger("hermesclaw.wechat_sdk")

# iLink protocol constants exposed for callers that need compatibility.
ILINK_CHANNEL_VERSION = "2.1.7"
ILINK_CLIENT_VERSION = "65547"
DEFAULT_POLL_SEC = 35

# iLink item type used by text message payload construction.
TEXT_ITEM_TYPE = 1

# Endpoint allowlist for the local proxy server.
PROXY_ALLOWLIST = frozenset([
    "ilink/bot/getupdates",
    "ilink/bot/sendmessage",
    "ilink/bot/getuploadurl",
    "ilink/bot/sendtyping",
    "ilink/bot/getconfig",
    "ilink/bot/get_bot_qrcode",
    "ilink/bot/get_qrcode_status",
])


def build_ilink_headers(token: str, body: str | bytes = "") -> dict[str, str]:
    """Build standard iLink HTTP headers.

    Args:
        token: iLink bot token used in Authorization header.
        body: JSON payload text/bytes; used to compute Content-Length precisely.

    Returns:
        A header dictionary that follows iLink gateway expectations.

    Notes:
        - Content-Length is calculated from UTF-8 encoded bytes.
        - Caller can pass empty token for unauthenticated scenarios in tests.
    """
    body_size = len(body if isinstance(body, bytes) else body.encode())
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "Content-Length": str(body_size),
        "iLink-App-Id": "",
        "iLink-App-ClientVersion": ILINK_CLIENT_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


class ILinkClient:
    """Tiny SDK client for iLink communication.

    This class is intentionally small and synchronous so it can be embedded
    directly into simple proxy services without extra runtime dependencies.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        channel_version: str = ILINK_CHANNEL_VERSION,
        default_poll_sec: int = DEFAULT_POLL_SEC,
    ) -> None:
        # Store immutable connection config for all requests.
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.channel_version = channel_version
        self.default_poll_sec = default_poll_sec

    def post(self, endpoint: str, payload: dict, timeout: int = 30) -> dict:
        """POST JSON to one iLink endpoint and return decoded JSON response."""
        url = self.base_url + "/" + endpoint.lstrip("/")
        body = json.dumps(payload)
        response = requests.post(
            url,
            headers=build_ilink_headers(self.token, body),
            data=body.encode(),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def forward_raw(self, endpoint: str, body: bytes, timeout: int = 30) -> requests.Response:
        """Forward already-serialized request body to iLink without mutation.

        This is used by the HTTP proxy path where body content is controlled by
        gateway processes and should be relayed as-is.
        """
        url = self.base_url + "/" + endpoint.lstrip("/")
        return requests.post(
            url,
            headers=build_ilink_headers(self.token, body),
            data=body,
            timeout=timeout,
        )

    def get_updates(self, cursor: str = "", poll_sec: int | None = None) -> dict:
        """Long-poll iLink updates with timeout-safe fallback behavior.

        Returns:
            Normal iLink response when successful.
            Synthetic empty response on timeout to keep loop stable.
            Synthetic error response on unexpected exceptions.
        """
        if poll_sec is None:
            poll_sec = self.default_poll_sec
        try:
            return self.post(
                "ilink/bot/getupdates",
                {
                    "get_updates_buf": cursor,
                    "base_info": {"channel_version": self.channel_version},
                },
                timeout=poll_sec + 5,  # HTTP timeout > long-poll timeout.
            )
        except requests.exceptions.Timeout:
            # Timeout is expected for long-poll APIs; return an empty batch.
            return {"ret": 0, "msgs": [], "get_updates_buf": cursor}
        except Exception as exc:
            # Keep caller alive by returning normalized error shape.
            log.warning("getUpdates failed: %s", exc)
            return {"ret": -1, "msgs": [], "get_updates_buf": cursor}

    def send_text(self, to_user_id: str, text: str, context_token: str | None = None) -> dict:
        """Send a plain text message through iLink sendmessage API.

        Args:
            to_user_id: Receiver user ID in WeChat/iLink channel.
            text: Final text to send.
            context_token: Optional reply context from inbound message.
        """
        msg = {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": "hc-" + secrets.token_hex(8),
            "message_type": 2,
            "message_state": 2,
            "item_list": [{"type": TEXT_ITEM_TYPE, "text_item": {"text": text}}],
        }
        if context_token:
            msg["context_token"] = context_token
        return self.post(
            "ilink/bot/sendmessage",
            {"msg": msg, "base_info": {"channel_version": self.channel_version}},
        )


def make_gateway_proxy_handler(
    queue,
    client: ILinkClient,
    *,
    tag: str = "",
    should_tag_text=None,
    allowlist=PROXY_ALLOWLIST,
    poll_sec: int = DEFAULT_POLL_SEC,
):
    """Create a reusable iLink-compatible proxy handler class.

    Args:
        queue: Thread-safe queue object with `dequeue_all(timeout)` method.
        client: SDK client used to forward API calls to real iLink endpoint.
        tag: Prefix text inserted into outgoing sendmessage items when
            `should_tag_text(to_user_id)` returns True.
        should_tag_text: Optional callback `(to_user_id) -> bool`.
        allowlist: Endpoint allowlist, defaulting to iLink proxy-safe endpoints.
        poll_sec: Long-poll wait time for `getupdates` simulation.

    Returns:
        A `BaseHTTPRequestHandler` subclass ready for HTTPServer/ThreadingHTTPServer.

    Interface contract:
        - `POST /ilink/bot/getupdates`: pulls messages from queue.
        - `POST /ilink/bot/sendmessage`: forwards to iLink, optional text tagging.
        - Other allowlisted endpoints: transparent passthrough.
        - Non-allowlisted endpoints: 404 with normalized JSON error payload.
    """

    class ILinkGatewayProxyHandler(BaseHTTPRequestHandler):
        """Concrete request handler bound to one queue+client instance."""

        def do_POST(self):
            # Read endpoint and request body early; all sub-handlers reuse them.
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            endpoint = urlparse(self.path).path.lstrip("/")

            # Defensive default-deny to avoid exposing full iLink API surface.
            if endpoint not in allowlist:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'{"ret":-1,"errmsg":"not allowed"}')
                log.warning("Proxy blocked endpoint: %s", endpoint)
                return

            try:
                if endpoint == "ilink/bot/getupdates":
                    self._handle_getupdates(body)
                elif endpoint == "ilink/bot/sendmessage":
                    self._handle_sendmessage(body)
                else:
                    self._proxy_passthrough(body)
            except BrokenPipeError:
                log.debug("Client disconnected (BrokenPipeError)")
            except ConnectionResetError:
                log.debug("Client disconnected (ConnectionResetError)")

        def _handle_getupdates(self, body: bytes):
            """Serve queued messages to a local gateway via long-poll semantics."""
            try:
                request_json = json.loads(body) if body else {}
            except Exception:
                request_json = {}
            cursor = request_json.get("get_updates_buf", "")

            # Queue controls long-poll waiting; proxy just formats response shape.
            msgs = queue.dequeue_all(timeout=poll_sec)
            response = {"ret": 0, "msgs": msgs, "get_updates_buf": cursor}
            self._write_json(200, response)
            if msgs:
                log.info("Proxy [%s] getupdates -> %d msgs", tag or "?", len(msgs))

        def _handle_sendmessage(self, body: bytes):
            """Forward sendmessage request, with optional tag prefix in text items."""
            try:
                request_json = json.loads(body) if body else {}
            except Exception:
                request_json = {}

            # Optional attribution prefix is only applied when callback approves.
            if tag and should_tag_text:
                msg = request_json.get("msg", {})
                to_user = msg.get("to_user_id", "")
                should_tag = bool(to_user) and bool(should_tag_text(to_user))
                if should_tag:
                    for item in msg.get("item_list", []):
                        if item.get("type") == TEXT_ITEM_TYPE:
                            text_item = item.get("text_item", {})
                            original = text_item.get("text", "")
                            if original:
                                text_item["text"] = f"{tag} {original}"

            self._forward_to_ilink(
                "ilink/bot/sendmessage",
                json.dumps(request_json).encode() if request_json else body,
            )

        def _proxy_passthrough(self, body: bytes):
            """Forward other allowlisted endpoints without payload mutation."""
            endpoint = urlparse(self.path).path.lstrip("/")
            self._forward_to_ilink(endpoint, body)

        def _forward_to_ilink(self, endpoint: str, body: bytes):
            """Proxy one request to iLink and stream status/body back to caller."""
            try:
                response = client.forward_raw(endpoint, body, timeout=30)
                self.send_response(response.status_code)
                for key, value in response.headers.items():
                    if key.lower() not in (
                        "transfer-encoding",
                        "content-encoding",
                        "connection",
                    ):
                        self.send_header(key, value)
                self.end_headers()
                self.wfile.write(response.content)
            except BrokenPipeError:
                log.debug("BrokenPipe on write-back (benign): %s", endpoint)
            except Exception as exc:
                log.error("Proxy forward error: %s", exc)
                try:
                    err = json.dumps({"ret": -1, "errmsg": str(exc)}).encode()
                    self.send_response(502)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err)))
                    self.end_headers()
                    self.wfile.write(err)
                except BrokenPipeError:
                    log.debug("BrokenPipe writing error response (benign)")

        def _write_json(self, code: int, obj: dict):
            """Write JSON response with explicit content headers."""
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            # Silence BaseHTTPRequestHandler default logs; app logger is enough.
            pass

    return ILinkGatewayProxyHandler
