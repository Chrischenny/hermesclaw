"""HermesClaw v2: dual-gateway proxy router for WeChat.

Takes over one iLink token, polls for messages, and distributes them
to two independent proxy servers -- one for OpenClaw's clawbot and one
for Hermes Agent's WeChat gateway.  Each gateway believes it is talking
directly to the iLink API.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from enum import Enum
from http.server import ThreadingHTTPServer
from pathlib import Path

from wechat_sdk import (
    DEFAULT_POLL_SEC as SDK_DEFAULT_POLL_SEC,
    ILINK_CHANNEL_VERSION,
    ILINK_CLIENT_VERSION,
    PROXY_ALLOWLIST,
    ILinkClient,
    build_ilink_headers,
    make_gateway_proxy_handler,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

T, VO = 1, 3  # iLink message types: text, voice
ILINK_VER = ILINK_CHANNEL_VERSION
ILINK_CV = ILINK_CLIENT_VERSION
QUEUE_CAP = 200
DEFAULT_POLL_SEC = SDK_DEFAULT_POLL_SEC

log = logging.getLogger("hermesclaw")

# ---------------------------------------------------------------------------
# Route enum & persistent state
# ---------------------------------------------------------------------------


class Route(str, Enum):
    HERMES = "hermes"
    OPENCLAW = "openclaw"
    BOTH = "both"


class State:
    """Per-user routing state, persisted to JSON."""

    def __init__(self, fp):
        self.fp = Path(fp)
        self.d = {}
        self.lock = threading.Lock()
        if self.fp.exists():
            try:
                self.d = json.loads(self.fp.read_text())
                log.info("Loaded %d route states", len(self.d))
            except Exception:
                pass

    def save(self):
        t = self.fp.with_suffix(".tmp")
        t.write_text(json.dumps(self.d, indent=2, ensure_ascii=False))
        t.replace(self.fp)

    def get(self, uid):
        with self.lock:
            if uid not in self.d:
                self.d[uid] = {"route": Route.HERMES.value, "status_shown": False}
            entry = self.d[uid]
            # Migrate v1 format {"b": ...} to v2 {"route": ...}
            raw = entry.get("route") or entry.get("b", Route.HERMES.value)
            return Route(raw)

    def should_show_status(self, uid):
        with self.lock:
            if uid not in self.d:
                self.d[uid] = {"route": Route.HERMES.value, "status_shown": False}
            return not self.d[uid].get("status_shown", False)

    def mark_status_shown(self, uid):
        with self.lock:
            if uid not in self.d:
                self.d[uid] = {"route": Route.HERMES.value, "status_shown": True}
            else:
                self.d[uid]["status_shown"] = True
            self.save()

    def set(self, uid, route):
        with self.lock:
            status_shown = self.d.get(uid, {}).get("status_shown", False)
            self.d[uid] = {"route": route.value, "status_shown": status_shown}
            self.save()
            log.info("User %s -> %s", uid[:16], route.value)


# ---------------------------------------------------------------------------
# Text extraction (voice -> transcription)
# ---------------------------------------------------------------------------


def extract_text(items):
    """Return combined text from iLink items.

    Voice items contribute only their iLink transcription by design.
    """
    parts = []
    for it in items:
        tp = it.get("type", 0)
        if tp == T:
            x = it.get("text_item", {}).get("text", "")
            if x:
                parts.append(x)
        elif tp == VO:
            x = it.get("voice_item", {}).get("text", "")
            if x:
                parts.append(
                    f'[The user sent a voice message. Here\'s what they said: "{x}"]'
                )
    return "\n".join(parts).strip()


# ---------------------------------------------------------------------------
# Router commands
# ---------------------------------------------------------------------------


def route_label(r):
    if r == Route.HERMES:
        return "Hermes"
    if r == Route.OPENCLAW:
        return "OpenClaw"
    return "Hermes + OpenClaw"


def cmd(state, uid, text):
    """Process a slash command.  Returns reply text or None for passthrough."""
    c = text.strip().lower()
    if c == "/hermes":
        state.set(uid, Route.HERMES)
        return "Switched to **Hermes**."
    if c == "/openclaw":
        state.set(uid, Route.OPENCLAW)
        return "Switched to **OpenClaw**."
    if c == "/both":
        state.set(uid, Route.BOTH)
        return "Switched to **Hermes + OpenClaw**."
    if c == "/whoami":
        route = state.get(uid)
        return (
            f"**HermesClaw** by X @AaronYonW\n"
            f"**Current route**: **{route_label(route)}**\n"
            f"**/hermes** → Hermes only\n"
            f"**/openclaw** → OpenClaw only\n"
            f"**/both** → both reply\n"
            f"**/whoami** → this status"
        )
    return None


# ---------------------------------------------------------------------------
# iLink helpers
# ---------------------------------------------------------------------------


def hdrs(tok, body=""):
    """Backward-compatible wrapper around SDK header builder.

    New code should use `wechat_sdk.build_ilink_headers` directly.
    """
    return build_ilink_headers(tok, body)


def _client(base_url, tok):
    """Build SDK client with project-level protocol defaults."""
    return ILinkClient(
        base_url=base_url,
        token=tok,
        channel_version=ILINK_VER,
        default_poll_sec=DEFAULT_POLL_SEC,
    )


def ilink_post(base_url, ep, bd, tok, to=30):
    """Backward-compatible wrapper to keep test and caller imports stable."""
    return _client(base_url, tok).post(ep, bd, timeout=to)


def get_updates_real(base_url, tok, buf="", to=None):
    """Backward-compatible wrapper for SDK long-poll call."""
    return _client(base_url, tok).get_updates(cursor=buf, poll_sec=to)


def send_text_ilink(base_url, tok, to_user, text, ctx=None):
    """Backward-compatible wrapper for SDK send_text."""
    return _client(base_url, tok).send_text(
        to_user_id=to_user,
        text=text,
        context_token=ctx,
    )


# ---------------------------------------------------------------------------
# Message queue with event-based long-poll
# ---------------------------------------------------------------------------


class MessageQueue:
    """Thread-safe queue with blocking dequeue for long-poll simulation."""

    def __init__(self, capacity=QUEUE_CAP):
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.msgs = []
        self.capacity = capacity

    def enqueue(self, msg):
        with self.lock:
            if len(self.msgs) >= self.capacity:
                self.msgs.pop(0)
                log.warning("Queue full (%d), dropped oldest", self.capacity)
            self.msgs.append(msg)
        self.event.set()

    def dequeue_all(self, timeout=None):
        """Return all queued messages, blocking up to *timeout* seconds."""
        if timeout is not None and timeout > 0:
            self.event.wait(timeout=timeout)
        with self.lock:
            batch = list(self.msgs)
            self.msgs.clear()
            self.event.clear()
        return batch

    def size(self):
        with self.lock:
            return len(self.msgs)


def make_proxy_handler(queue, ilink_base_url, ilink_token, state, tag):
    """Factory wrapper: keep legacy interface while delegating to SDK."""
    client = _client(ilink_base_url, ilink_token)

    def should_tag_text(to_user):
        """Tag only when current route is BOTH for the destination user."""
        try:
            return state.get(to_user) == Route.BOTH
        except Exception:
            return False

    return make_gateway_proxy_handler(
        queue=queue,
        client=client,
        tag=tag,
        should_tag_text=should_tag_text,
        allowlist=PROXY_ALLOWLIST,
        poll_sec=DEFAULT_POLL_SEC,
    )


# ---------------------------------------------------------------------------
# Message routing
# ---------------------------------------------------------------------------


def route_message(uid, msg, route, hermes_q, openclaw_q):
    """Enqueue *msg* to the correct proxy queue(s) based on *route*."""
    if route in (Route.HERMES, Route.BOTH):
        if hermes_q:
            hermes_q.enqueue(msg)
        else:
            log.warning("Hermes queue not available for %s", uid[:16])
    if route in (Route.OPENCLAW, Route.BOTH):
        if openclaw_q:
            openclaw_q.enqueue(msg)
        else:
            log.warning("OpenClaw queue not available for %s", uid[:16])


def proc_msg(msg, state, base_url, token, hermes_q, openclaw_q):
    """Process one inbound iLink message."""
    uid = msg.get("from_user_id", "")
    ctx = msg.get("context_token", "")
    items = msg.get("item_list", [])
    if msg.get("message_type", 1) != 1:
        return
    log.info("Msg from=%s... items=%d", uid[:16], len(items))

    txt = extract_text(items)
    has_any = txt or any(it.get("type", 0) != 0 for it in items)
    if not has_any:
        return

    # Show status on first contact.
    if state.should_show_status(uid) and not txt.startswith("/"):
        reply = cmd(state, uid, "/whoami")
        send_text_ilink(base_url, token, uid, reply, ctx)
        state.mark_status_shown(uid)

    # Slash commands are text-only; never forwarded to gateways.
    if txt.startswith("/"):
        r = cmd(state, uid, txt)
        if r:
            send_text_ilink(base_url, token, uid, r, ctx)
            return

    route = state.get(uid)
    route_message(uid, msg, route, hermes_q, openclaw_q)


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------

MAX_FAILS = 3
BACKOFF = 30


def poll_loop(base_url, token, state, hermes_q, openclaw_q, poll_sec=None):
    if poll_sec is None:
        poll_sec = DEFAULT_POLL_SEC
    buf = ""
    fails = 0
    while True:
        try:
            resp = get_updates_real(base_url, token, buf, poll_sec)
            ret, ec = resp.get("ret"), resp.get("errcode")
            if ret not in (0, None) or ec not in (0, None):
                log.warning("getUpdates err: ret=%s ec=%s", ret, ec)
                fails += 1
                if fails >= MAX_FAILS:
                    time.sleep(BACKOFF)
                    fails = 0
                else:
                    time.sleep(2)
                continue
            fails = 0
            if resp.get("get_updates_buf"):
                buf = resp["get_updates_buf"]
            for m in resp.get("msgs", []):
                try:
                    proc_msg(m, state, base_url, token, hermes_q, openclaw_q)
                except Exception as e:
                    log.error("proc: %s", e, exc_info=True)
        except Exception as e:
            log.error("Loop: %s", e, exc_info=True)
            fails += 1
            if fails >= MAX_FAILS:
                time.sleep(BACKOFF)
                fails = 0
            else:
                time.sleep(2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env")

    base_url = os.getenv("ILINK_BASE_URL", "https://ilinkai.weixin.qq.com")
    token = os.getenv("ILINK_TOKEN", "")
    hermes_port = int(os.getenv("HERMES_PROXY_PORT", "19998"))
    oc_port = int(os.getenv("OPENCLAW_PROXY_PORT", "19999"))
    state_file = os.getenv("STATE_FILE", str(Path(__file__).parent / "router_state.json"))
    log_file = os.getenv("LOG_FILE", str(Path(__file__).parent / "hermesclaw.log"))
    poll_sec = int(os.getenv("LONG_POLL_TIMEOUT", "35"))
    hermes_on = os.getenv("HERMES_ENABLED", "true").lower() in ("true", "1", "yes")
    oc_on = os.getenv("OPENCLAW_ENABLED", "true").lower() in ("true", "1", "yes")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    if not token:
        log.error("No ILINK_TOKEN set!")
        sys.exit(1)

    state = State(state_file)
    hermes_q = MessageQueue() if hermes_on else None
    oc_q = MessageQueue() if oc_on else None

    log.info("=" * 60)
    log.info("HermesClaw v2 -- dual gateway proxy")
    log.info("iLink: %s", base_url)
    log.info("Hermes proxy: :%d (enabled=%s)", hermes_port, hermes_on)
    log.info("OpenClaw proxy: :%d (enabled=%s)", oc_port, oc_on)
    log.info("Default route: %s", Route.HERMES.value)
    log.info("=" * 60)

    servers = []

    if hermes_on:
        h_handler = make_proxy_handler(
            hermes_q, base_url, token, state, tag="[Hermes Agent]",
        )
        h_srv = ThreadingHTTPServer(("127.0.0.1", hermes_port), h_handler)
        threading.Thread(target=h_srv.serve_forever, daemon=True).start()
        servers.append(h_srv)
        log.info("Hermes proxy started on :%d", hermes_port)

    if oc_on:
        oc_handler = make_proxy_handler(
            oc_q, base_url, token, state, tag="[OpenClaw]",
        )
        oc_srv = ThreadingHTTPServer(("127.0.0.1", oc_port), oc_handler)
        threading.Thread(target=oc_srv.serve_forever, daemon=True).start()
        servers.append(oc_srv)
        log.info("OpenClaw proxy started on :%d", oc_port)

    poll_thread = threading.Thread(
        target=poll_loop,
        args=(base_url, token, state, hermes_q, oc_q, poll_sec),
        daemon=True,
    )
    poll_thread.start()

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda s, f: stop.set())
    signal.signal(signal.SIGTERM, lambda s, f: stop.set())

    while not stop.is_set():
        time.sleep(1)

    log.info("Shutting down...")
    for s in servers:
        s.shutdown()
    log.info("Stopped")


if __name__ == "__main__":
    main()
