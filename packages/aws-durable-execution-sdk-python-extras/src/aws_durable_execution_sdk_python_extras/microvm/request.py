# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Delivery of one job to a MicroVM route over HTTP.

This module is the first retry tier of the request step. It runs inside one
step attempt. The step retry strategy is the second tier.
"""

from __future__ import annotations

import http.client
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from aws_durable_execution_sdk_python.types import LoggerInterface


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_extras.microvm.config import (
        LambdaMicrovmsClient,
    )


DEFAULT_MICROVM_PORT = 8080
"""The MicroVM endpoint forwards to this port when no port header is sent."""

DEFAULT_MICROVM_JOB_PATH = "/durable-execution/v1/job"
"""The path that receives a job over HTTP when the config names no route.

The worker package serves this path with its ``handler``. So the same handler
receives a job whether it arrives in the ``run`` hook or over HTTP. The value
must equal ``MICROVM_JOB_PATH`` in ``aws-durable-execution-sdk-python-microvm-worker``
and ``DEFAULT_MICROVM_JOB_PATH`` in the JavaScript SDK.
"""

DEFAULT_REQUEST_RETRY_WINDOW_SECONDS = 60
"""The default length of the first retry tier."""

MAX_REQUEST_RETRY_WINDOW_SECONDS = 600
"""The longest retry window. A longer window would outlive the auth token
and its one refresh."""

INVOCATION_RESERVE_SECONDS = 10.0
"""The first retry tier stops this long before the invocation times out.

A Lambda timeout during a step attempt records no outcome for the attempt.
So the step keeps time to raise and to checkpoint the second-tier retry.
"""

MIN_CALL_SECONDS = 1.0
"""A call that would start with less than this much time before the reserve
does not start. A shorter call is unlikely to finish, and the step retries
the job in a later invocation anyway."""

_INITIAL_DELAY_SECONDS = 0.25
_MAX_DELAY_SECONDS = 4.0
_REQUEST_TIMEOUT_SECONDS = 10.0
_AUTH_TOKEN_MINUTES = 5
_PORT_HEADER = "X-aws-proxy-port"

_T = TypeVar("_T")


class MicrovmRequestRejectedError(Exception):
    """The endpoint rejected the job with a status that a retry cannot change.

    Examples are 400, 404, and 413. The default step retry strategy does not
    retry it.
    """

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class MicrovmRequestUnauthorizedError(Exception):
    """The endpoint refused the job with 401 or 403 after a new auth token.

    The default step retry strategy retries it, outside the invocation.
    """

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class MicrovmEndpointUnavailableError(Exception):
    """The first retry tier ended without an accepted request.

    The default step retry strategy retries it, outside the invocation.
    """


def reserve_starts_at(
    remaining_seconds: Callable[[], float | None],
) -> float | None:
    """Return the ``time.monotonic()`` time at which the reserve starts.

    Every call of the first tier must end by then. ``None`` means that the
    compute reports no deadline.
    """
    remaining = remaining_seconds()
    if remaining is None:
        return None
    return time.monotonic() + remaining - INVOCATION_RESERVE_SECONDS


def _call_budget(
    hard_deadline: float | None,
    cap: float,
    on_expired: Callable[[], Exception],
) -> float:
    """Return how long the next call may run.

    It is at most ``cap``, and it never runs past ``hard_deadline``.

    Raises:
        Exception: The result of ``on_expired``, when less than
            :data:`MIN_CALL_SECONDS` remains before ``hard_deadline``.
    """
    if hard_deadline is None:
        return cap
    left = hard_deadline - time.monotonic()
    if left < MIN_CALL_SECONDS:
        raise on_expired()
    return min(cap, left)


def call_before_deadline(
    call: Callable[[], _T],
    hard_deadline: float | None,
    on_expired: Callable[[], Exception],
) -> _T:
    """Run one boto3 call that must end before ``hard_deadline``.

    1. Without a deadline, the call runs with the client's own settings.
    2. With a deadline, the call runs in a daemon thread, and this function
       waits for it until the deadline. boto3 cannot cancel a call, and its
       own retries can run longer than the time that remains.
    3. A call that has not ended at the deadline raises the result of
       ``on_expired``. That error is retryable, so the step retries the job
       in a later invocation. The thread keeps running until the call
       returns. Its result is discarded.

    Raises:
        Exception: The exception that the call raised, or the result of
            ``on_expired``.
    """
    if hard_deadline is None:
        return call()
    budget = _call_budget(hard_deadline, float("inf"), on_expired)
    outcome: dict[str, Any] = {}
    done = threading.Event()

    def run() -> None:
        try:
            outcome["value"] = call()
        except Exception as error:  # noqa: BLE001 - re-raised by the caller below
            outcome["error"] = error
        finally:
            done.set()

    threading.Thread(target=run, name="microvm-call", daemon=True).start()
    if not done.wait(budget):
        raise on_expired()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


@dataclass(frozen=True, kw_only=True)
class SendJobOptions:
    """The inputs of :func:`send_job`."""

    client: LambdaMicrovmsClient
    microvm_id: str
    endpoint: str
    path: str
    port: int
    body: bytes
    retry_window_seconds: float
    """The first-tier retry window."""
    remaining_seconds: Callable[[], float | None]
    """The remaining invocation time. ``None`` means that the compute reports
    no deadline, and only the retry window applies."""
    logger: LoggerInterface
    sleep: Callable[[float], None] = time.sleep


def send_job(options: SendJobOptions) -> int:
    """Deliver one job to a MicroVM route, with the first retry tier.

    The function handles each failure this way:

    1. A connection error, a timeout, 429, or any 5xx: wait and retry. The
       endpoint answers this way while the MicroVM starts.
    2. 401 or 403: create a new auth token and retry once. A second 401 or
       403 raises :class:`MicrovmRequestUnauthorizedError`, and the step
       retry strategy takes over.
    3. Any other 4xx: raise :class:`MicrovmRequestRejectedError` at once.

    The delay starts at 250 milliseconds and doubles up to 4 seconds. The
    tier ends at the retry window, or 10 seconds before the invocation times
    out, whichever comes first. It then raises
    :class:`MicrovmEndpointUnavailableError`.

    The two limits differ:

    1. The retry window limits when an attempt may start. An attempt that
       starts inside the window may end after it.
    2. The invocation reserve limits when every call must end. Each request
       and each ``CreateMicrovmAuthToken`` call stops at the reserve. A call
       that would start with less than 1 second before the reserve does not
       start. A Lambda timeout during the attempt would record no outcome,
       and the reserve prevents that.

    The endpoint can accept a request before Lambda has sent the ``run``
    hook. So the body carries the MicroVM ID too.

    A request can reach the MicroVM while its response is lost. The next
    request then delivers the same job again. The worker ignores a second
    request for a callback ID that runs or recently ended, so the job still
    runs once.

    Returns:
        The status that accepted the job.

    Raises:
        MicrovmRequestRejectedError: When the endpoint rejects the job.
        MicrovmRequestUnauthorizedError: When a new auth token is refused.
        MicrovmEndpointUnavailableError: When the tier ends.
        Exception: When ``CreateMicrovmAuthToken`` fails.
    """
    started = time.monotonic()
    window_ends = started + options.retry_window_seconds
    attempt = 0
    last_failure = "no attempt"

    def unavailable() -> Exception:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        return MicrovmEndpointUnavailableError(
            f"The MicroVM did not accept the job at {options.path} after "
            f"{attempt} attempts in {elapsed_ms} ms. Last failure: {last_failure}."
        )

    def deadline() -> float:
        reserve = reserve_starts_at(options.remaining_seconds)
        return window_ends if reserve is None else min(window_ends, reserve)

    def new_token() -> Mapping[str, str]:
        return call_before_deadline(
            lambda: _create_auth_token(options),
            reserve_starts_at(options.remaining_seconds),
            unavailable,
        )

    url = _endpoint_url(options.endpoint, options.path)
    token = new_token()
    refreshed_token = False
    delay = _INITIAL_DELAY_SECONDS

    while True:
        attempt += 1
        # The request must end before the reserve, like every call in the
        # tier.
        timeout = _call_budget(
            reserve_starts_at(options.remaining_seconds),
            _REQUEST_TIMEOUT_SECONDS,
            unavailable,
        )
        headers = {"content-type": "application/json", **token}
        if options.port != DEFAULT_MICROVM_PORT:
            headers[_PORT_HEADER] = str(options.port)
        status, failure = _post(url, options.body, headers, timeout)

        if status is not None and 200 <= status < 300:
            options.logger.info(
                "MicroVM job accepted", extra={"attempt": attempt, "status": status}
            )
            return status
        if status is None:
            last_failure = failure
        else:
            last_failure = f"HTTP {status}"
            if status in {401, 403}:
                if refreshed_token:
                    msg = (
                        f"The MicroVM refused a new auth token at {options.path} "
                        f"with HTTP {status}"
                    )
                    raise MicrovmRequestUnauthorizedError(msg, status)
                refreshed_token = True
                # The refresh checks the reserve like every other call. The
                # next request then checks it again.
                token = new_token()
                options.logger.info(
                    "MicroVM auth token refreshed",
                    extra={"attempt": attempt, "status": status},
                )
                continue
            if 400 <= status < 500 and status != 429:  # noqa: PLR2004
                msg = (
                    f"The MicroVM rejected the job at {options.path} with HTTP {status}"
                )
                raise MicrovmRequestRejectedError(msg, status)

        if time.monotonic() + delay > deadline():
            raise unavailable()
        options.logger.info(
            "MicroVM job not accepted yet",
            extra={"attempt": attempt, "failure": last_failure},
        )
        options.sleep(delay)
        delay = min(delay * 2, _MAX_DELAY_SECONDS)


def _create_auth_token(options: SendJobOptions) -> Mapping[str, str]:
    response = options.client.create_microvm_auth_token(
        microvmIdentifier=options.microvm_id,
        expirationInMinutes=_AUTH_TOKEN_MINUTES,
        allowedPorts=[{"port": options.port}],
    )
    token = response.get("authToken")
    if not token:
        msg = "CreateMicrovmAuthToken returned no authToken"
        raise RuntimeError(msg)
    return token


# Proxy settings from the environment do not apply. The request goes to the
# MicroVM endpoint directly, as the JavaScript SDK's fetch sends it.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _post(
    url: str, body: bytes, headers: Mapping[str, str], timeout: float
) -> tuple[int | None, str]:
    """POST ``body``, and return the status, or ``None`` and the failure.

    The body of the response is not used. So the function reads no body, and
    closes the response.

    ``timeout`` applies to each socket operation: the connect, and each send
    and receive. The endpoint answers at once, so it bounds the request.
    """
    request = urllib.request.Request(  # noqa: S310 - the URL is https or the endpoint's own
        url, data=body, headers=dict(headers), method="POST"
    )
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            return response.status, ""
    except urllib.error.HTTPError as error:
        error.close()
        return error.code, ""
    except (OSError, http.client.HTTPException, ValueError) as error:
        # URLError, a refused connection, and a timeout are OSError. An
        # invalid response from the endpoint raises an HTTPException.
        return None, f"{type(error).__name__}: {error}"


def _endpoint_url(endpoint: str, path: str) -> str:
    """Join the endpoint and the path.

    ``RunMicrovm`` returns the endpoint as a host name without a scheme.
    """
    base = (
        endpoint
        if endpoint.startswith(("https://", "http://"))
        else (f"https://{endpoint}")
    )
    return f"{base.rstrip('/')}{path}"
