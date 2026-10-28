"""Minimal REST client for the Amazon Connect ACXD SDK API (restJson1, API key auth).

Wire format follows amazon-connect-acxd-sdk@0.2.0 (design §2.1, §6.5).  The API
key is held privately and never logged; only mask_key() output appears in logs.
"""

from __future__ import annotations

import json
import logging
import random
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

import config

logger = logging.getLogger("acxd_client")

USER_AGENT = "acxd-device-fault-deploy/1.0"
RETRY_STATUSES = (429, 500, 502, 503, 504)
BACKOFF_SECONDS = (1, 2, 4, 8, 16)


# Response fields that carry credentials.  Live data: GetApplicationDeployment returns "apiKey"
# (a runtime key of the deployed application) next to deploymentAlias.  These values must never reach
# a log line, an artifact or the terminal.
SECRET_FIELD_NAMES = frozenset({"apikey", "deploymentkey", "secret", "secretkey", "token", "accesstoken",
                                "participanttoken", "password", "signingkey"})
REDACTED = "<redacted>"


def redact(obj):
    """Deep copy of obj with every credential-like field replaced by REDACTED."""
    if isinstance(obj, dict):
        return {k: (REDACTED if isinstance(k, str) and k.replace("_", "").replace("-", "").lower()
                    in SECRET_FIELD_NAMES and v not in (None, "") else redact(v))
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    return obj


def redact_text(raw: bytes, limit: int) -> str:
    """Response body for DEBUG logs: JSON bodies redacted, anything else truncated."""
    try:
        return json.dumps(redact(json.loads(raw.decode("utf-8"))), ensure_ascii=False)[:limit]
    except (ValueError, UnicodeDecodeError):
        return raw[:limit].decode("utf-8", "replace")


def mask_key(key: str | None) -> str:
    """<prefix>.<secret> -> ****<last 4 chars of prefix>.****

    Neither the secret nor the fixed key prefix ever appears in the output.
    """
    if not key:
        return "<none>"
    if "." in key:
        return "****" + key.split(".", 1)[0][-4:] + ".****"
    return "****"


def endpoint_for_region(region: str) -> str:
    return config.ACXD_ENDPOINT_TEMPLATE.format(region=region)


class AcxdApiError(Exception):
    def __init__(self, status, error_type, message, request_id=None):
        self.status = status
        self.error_type = error_type
        self.message = message
        self.request_id = request_id
        super().__init__(str(self))

    def __str__(self):
        rid = f", requestId={self.request_id}" if self.request_id else ""
        return f"ACXD error status={self.status} type={self.error_type}: {self.message}{rid}"


class AcxdReadOnlyViolation(RuntimeError):
    """A non-GET request on a read_only client (--preflight); raised before any network I/O."""


class AcxdAmbiguousWriteError(AcxdApiError):
    """A create-type POST failed with 5xx / network error: it may or may not have happened."""

    def __init__(self, op, cause):
        status = getattr(cause, "status", None)
        error_type = getattr(cause, "error_type", None) or type(cause).__name__
        super().__init__(status, error_type, f"{op}: result unknown ({cause})",
                         getattr(cause, "request_id", None))
        self.op = op
        self.cause = cause


def _parse_error(status, body: bytes, headers) -> AcxdApiError:
    """A16: type from body.type / body.__type / body.code, then x-amzn-ErrorType header."""
    error_type = None
    message = ""
    try:
        data = json.loads(body.decode("utf-8")) if body else {}
    except (ValueError, UnicodeDecodeError):
        data = {}
    if isinstance(data, dict):
        error_type = data.get("type") or data.get("__type") or data.get("code")
        message = data.get("message") or data.get("Message") or ""
    if not error_type and headers is not None:
        error_type = headers.get("x-amzn-ErrorType")
    if error_type:
        error_type = str(error_type).split(":", 1)[0]
        if "#" in error_type:
            error_type = error_type.rsplit("#", 1)[1]
    request_id = headers.get("x-amzn-RequestId") if headers is not None else None
    return AcxdApiError(status, error_type or "Unknown", message, request_id)


class AcxdClient:
    def __init__(self, endpoint: str, api_key: str, workspace_id: str | None, timeout: int = 30,
                 sleep=time.sleep, read_only: bool = False):
        self.endpoint = endpoint.rstrip("/")
        self.__api_key = api_key
        self.workspace_id = workspace_id
        self.timeout = timeout
        self._sleep = sleep
        self.read_only = read_only

    def __repr__(self):
        return (f"AcxdClient(endpoint={self.endpoint!r}, api_key={mask_key(self.__api_key)!r}, "
                f"workspace_id={self.workspace_id!r})")

    # -- transport ---------------------------------------------------------

    def _headers(self):
        headers = {
            "x-api-key": self.__api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        # Account-level operations (ListWorkspaces) are sent without x-workspace-id (SDK wrapper.js).
        if self.workspace_id:
            headers["x-workspace-id"] = self.workspace_id
        return headers

    def _request(self, method, path, body=None, query=None, expected=(200, 201, 204),
                 retry_policy="idempotent", op=None):
        op = op or f"{method} {path}"
        if self.read_only and method != "GET":
            raise AcxdReadOnlyViolation(f"preflight is read-only: {op}")
        url = self.endpoint + path
        if query:
            url += "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        attempt = 0
        while True:
            req = urllib.request.Request(url, data=data, method=method, headers=self._headers())
            logger.debug("ACXD %s %s", method, path)
            retryable = False
            err = None
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    status = resp.status
                    raw = resp.read()
                    headers = resp.headers
                if status not in expected:
                    raise _parse_error(status, raw, headers)
                logger.debug("ACXD %s %s -> %s %s", method, path, status, redact_text(raw, 2000))
                if not raw:
                    return {}
                try:
                    return json.loads(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    logger.debug("non-JSON response (first 500 bytes): %r", raw[:500])
                    raise AcxdApiError(status, "InvalidResponse", f"{op}: response is not JSON",
                                       headers.get("x-amzn-RequestId") if headers else None)
            except urllib.error.HTTPError as exc:
                try:
                    raw = exc.read()
                except Exception:  # noqa: BLE001 - body may be unreadable
                    raw = b""
                finally:
                    exc.close()
                err = _parse_error(exc.code, raw, exc.headers)
                logger.debug("ACXD %s %s -> %s %s", method, path, exc.code, redact_text(raw, 500))
                if exc.code == 429:
                    retryable = True
                elif exc.code in RETRY_STATUSES:
                    if retry_policy == "idempotent":
                        retryable = True
                    else:
                        raise AcxdAmbiguousWriteError(op, err) from None
                else:
                    raise err from None
            except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as exc:
                if retry_policy == "idempotent":
                    retryable = True
                    err = AcxdApiError(None, type(exc).__name__, f"{op}: {exc}")
                else:
                    raise AcxdAmbiguousWriteError(op, exc) from None

            if not retryable or attempt >= len(BACKOFF_SECONDS):
                raise err
            delay = BACKOFF_SECONDS[attempt] + random.uniform(0, 0.5)
            attempt += 1
            logger.warning("ACXD %s failed (status=%s type=%s), retry %d/%d in %.1fs",
                           op, err.status, err.error_type, attempt, len(BACKOFF_SECONDS), delay)
            self._sleep(delay)

    def _paginate(self, path, op):
        items = []
        token = None
        while True:
            query = {"maxResults": 100}
            if token:
                query["nextToken"] = token
            page = self._request("GET", path, query=query, op=op)
            items.extend(page.get("items") or [])
            token = page.get("nextToken")
            if not token:
                return items

    @staticmethod
    def _q(value) -> str:
        return urllib.parse.quote(str(value), safe="")

    # -- workspaces (account level, no x-workspace-id) ------------------------

    def list_workspaces(self):
        return self._paginate("/sdk/workspaces", "ListWorkspaces")

    # -- context variables -------------------------------------------------

    def list_context_variables(self):
        return self._paginate("/sdk/context-variables", "ListContextVariables")

    def create_context_variable(self, body):
        return self._request("POST", "/sdk/context-variables", body, retry_policy="throttle_only",
                             op="CreateContextVariable")

    # -- flows ---------------------------------------------------------------

    def list_flows(self):
        return self._paginate("/sdk/flows", "ListFlows")

    def get_flow(self, flow_id):
        return self._request("GET", f"/sdk/flows/{self._q(flow_id)}", op="GetFlow")

    def create_flow(self, body):
        return self._request("POST", "/sdk/flows", body, retry_policy="throttle_only", op="CreateFlow")

    def update_flow(self, flow_id, body):
        return self._request("PATCH", f"/sdk/flows/{self._q(flow_id)}", body, op="UpdateFlow")

    # -- applications --------------------------------------------------------

    def list_applications(self):
        return self._paginate("/sdk/applications", "ListApplications")

    def get_application(self, app_id):
        return self._request("GET", f"/sdk/applications/{self._q(app_id)}", op="GetApplication")

    def create_application(self, body):
        return self._request("POST", "/sdk/applications", body, retry_policy="throttle_only",
                             op="CreateApplication")

    def update_application(self, app_id, body):
        return self._request("PATCH", f"/sdk/applications/{self._q(app_id)}", body, op="UpdateApplication")

    # -- builds --------------------------------------------------------------

    def list_builds(self, app_id):
        return self._paginate(f"/sdk/applications/{self._q(app_id)}/builds", "ListApplicationBuilds")

    def create_build(self, app_id, body):
        return self._request("POST", f"/sdk/applications/{self._q(app_id)}/builds", body,
                             retry_policy="none", op="CreateApplicationBuild")

    def get_build(self, app_id, build_id):
        return self._request("GET", f"/sdk/applications/{self._q(app_id)}/builds/{self._q(build_id)}",
                             op="GetApplicationBuild")

    # -- deployments ---------------------------------------------------------

    def list_deployments(self, app_id):
        return self._paginate(f"/sdk/applications/{self._q(app_id)}/deployments", "ListApplicationDeployments")

    def create_deployment(self, app_id, body):
        return self._request("POST", f"/sdk/applications/{self._q(app_id)}/deployments", body,
                             retry_policy="throttle_only", op="CreateApplicationDeployment")

    def update_deployment(self, app_id, deployment_id, body):
        return self._request(
            "PATCH", f"/sdk/applications/{self._q(app_id)}/deployments/{self._q(deployment_id)}", body,
            op="UpdateApplicationDeployment")

    def get_deployment(self, app_id, deployment_id):
        return self._request(
            "GET", f"/sdk/applications/{self._q(app_id)}/deployments/{self._q(deployment_id)}",
            op="GetApplicationDeployment")

    # -- audit trail (diagnostics; a query, no resource is changed) --------------------------

    def start_trail_query(self, body):
        # POST, but read-only in effect: it only starts a query over the workspace audit trail.
        if self.read_only:
            raise AcxdReadOnlyViolation("preflight is read-only: StartTrailQuery")
        return self._request("POST", "/sdk/trails/query", body, expected=(200, 202), op="StartTrailQuery")

    def get_trail_query_results(self, result_id):
        return self._request("GET", f"/sdk/trails/results/{self._q(result_id)}", op="GetTrailQueryResults")
