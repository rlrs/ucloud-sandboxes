"""Push this node's heartbeat to the gateway from a node-agent thread (C4.4).

This replaces the oneshot systemd timer that started a Python process every
20 s to GET ``/v1/heartbeat`` and POST the result. The wire contract is
unchanged: the body is ``heartbeat_to_dict`` of the heartbeat that
``GET /v1/heartbeat`` serves, with the configured labels merged over the
node's own, sent with the heartbeat bearer token.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import logging
import math
import random
import threading
from typing import Callable, Mapping
from urllib.parse import urlsplit

from .agent import HEARTBEAT_POST_TIMEOUT_SECONDS, HeartbeatPostResult, post_heartbeat_with_headers
from .models import NodeHeartbeat

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 20
# Answers that say "try again soon"; any other rejection waits a full period.
_TRANSIENT_STATUSES = frozenset({408, 429})
_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeartbeatSenderConfig:
    url: str
    bearer_token: str = field(repr=False)
    interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    labels: Mapping[str, str] = field(default_factory=dict)
    # Each wait is drawn from [1 - jitter, 1 + jitter] times its base, so
    # nodes restarted together drift apart instead of posting in lockstep.
    jitter: float = 0.2
    # Base wait after the first consecutive transient failure; it doubles per
    # further failure and is capped at one interval.
    retry_initial_seconds: float = 1.0

    def __post_init__(self) -> None:
        target = urlsplit(self.url)
        if target.scheme not in {"http", "https"} or not target.hostname or "@" in target.netloc:
            raise ValueError("heartbeat url must be an absolute http(s) URL without credentials")
        token = self.bearer_token
        if not token or token != token.strip() or not token.isprintable():
            raise ValueError("heartbeat bearer token must be a non-empty single line")
        if not (math.isfinite(self.interval_seconds) and self.interval_seconds > 0):
            raise ValueError("heartbeat interval must be positive and finite")
        if not 0 <= self.jitter < 1:
            raise ValueError("heartbeat jitter must be in [0, 1)")
        if not 0 < self.retry_initial_seconds <= self.interval_seconds:
            raise ValueError("heartbeat retry delay must be positive and at most the interval")
        labels = dict(self.labels)
        if not all(isinstance(k, str) and k and isinstance(v, str) for k, v in labels.items()):
            raise ValueError("heartbeat labels must map non-empty strings to strings")
        object.__setattr__(self, "labels", labels)


class NodeHeartbeatSender:
    """Send heartbeats from one thread, which no request thread waits on.

    Attempts are sequential and each samples a fresh heartbeat, so a retry
    never resends stale inventory. The first attempt starts at once: a new
    node-agent process announces itself. Then a delivered heartbeat, or a
    rejection that retrying cannot fix, waits one jittered interval. A
    transient failure (transport, sampling, 408, 429, 5xx) waits
    ``retry_initial_seconds`` doubled per consecutive failure, capped at one
    interval, so retries are bounded in rate and never slower than the
    regular cadence.
    """

    def __init__(self, source: Callable[[], NodeHeartbeat], config: HeartbeatSenderConfig, *,
                 post: Callable[..., HeartbeatPostResult] = post_heartbeat_with_headers,
                 rng: random.Random | None = None) -> None:
        self.config = config
        self._source = source
        self._post = post
        self._rng = rng or random.Random()
        self._headers = {"Authorization": f"Bearer {config.bearer_token}"}
        self._changed = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stopping = False
        # send_now() tickets: issued, and covered by a completed attempt that
        # started after they were issued.
        self._requested = 0
        self._served = 0
        self._outcome: HeartbeatPostResult | Exception | None = None
        self._failures = 0  # consecutive; owned by the sender thread

    def start(self) -> None:
        with self._changed:
            if self._thread is not None or self._stopping:
                raise RuntimeError("a heartbeat sender starts once")
            self._thread = threading.Thread(target=self._run, name="node-heartbeat", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop for good; waits for a POST in flight up to its timeout. A sample
        still being taken is never sent."""
        with self._changed:
            self._stopping = True
            self._changed.notify_all()
            thread = self._thread
        if thread is None or thread is threading.current_thread():
            return
        thread.join(HEARTBEAT_POST_TIMEOUT_SECONDS + 5)
        if thread.is_alive():
            _LOG.warning("heartbeat sender is still in an attempt after stop")

    def send_now(self, timeout: float = 30.0) -> HeartbeatPostResult:
        """Wake the thread for a heartbeat sampled after this call; return the answer."""
        with self._changed:
            if self._stopping:
                raise RuntimeError("heartbeat sender is stopped")
            self._requested += 1
            ticket = self._requested
            self._changed.notify_all()
            if not self._changed.wait_for(lambda: self._served >= ticket or self._stopping, timeout):
                raise TimeoutError("heartbeat was not sent in time")
            if self._served < ticket:
                raise RuntimeError("heartbeat sender stopped")
            outcome = self._outcome
        if isinstance(outcome, Exception):
            raise RuntimeError(f"heartbeat failed: {outcome}") from outcome
        assert outcome is not None
        return outcome

    def _run(self) -> None:
        delay = 0.0
        while True:
            with self._changed:
                self._changed.wait_for(lambda: self._stopping or self._requested > self._served, delay)
                if self._stopping:
                    return
                serving = self._requested
            outcome = self._attempt()
            if outcome is None:
                return
            delay = self._next_delay(outcome)
            with self._changed:
                self._served = serving
                self._outcome = outcome
                self._changed.notify_all()

    def _attempt(self) -> HeartbeatPostResult | Exception | None:
        """One fresh sample and POST; None when stop() came during the sample."""
        try:
            heartbeat = self._source()
            if self.config.labels:
                heartbeat = replace(heartbeat, labels={**heartbeat.labels, **self.config.labels})
            with self._changed:
                if self._stopping:  # serving has ended: advertise nothing more
                    return None
            return self._post(self.config.url, heartbeat, self._headers)
        except Exception as exc:  # the thread outlives any one failed attempt
            return exc

    def _next_delay(self, outcome: HeartbeatPostResult | Exception) -> float:
        interval = self.config.interval_seconds
        if not isinstance(outcome, Exception) and 200 <= outcome.status < 300:
            if self._failures:
                _LOG.warning("heartbeat to %s delivered after %d failed attempts",
                             self.config.url, self._failures)
            self._failures = 0
            base = interval
        else:
            self._failures += 1
            transient = isinstance(outcome, Exception) or (
                outcome.status in _TRANSIENT_STATUSES or outcome.status >= 500)
            backoff = self.config.retry_initial_seconds * 2 ** min(self._failures - 1, 32)
            base = min(interval, backoff) if transient else interval
        jitter = self.config.jitter
        delay = base * self._rng.uniform(1 - jitter, 1 + jitter)
        if self._failures:
            detail = (f"{type(outcome).__name__}: {outcome}" if isinstance(outcome, Exception)
                      else f"HTTP {outcome.status}: {str(outcome.payload)[:200]}")
            _LOG.warning("heartbeat to %s failed (%d in a row, next in %.1f s): %s",
                         self.config.url, self._failures, delay, detail)
        return delay
