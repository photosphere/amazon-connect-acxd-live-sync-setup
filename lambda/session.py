"""POST /api/session (CloudFront -> API Gateway HTTP API -> this function).

Body {"token": "..."} from the email link.  Returns the Live Sync connection of the caller's
phone call only when the token signature is valid, it has not expired and the call is still
active (connect:DescribeContact has no DisconnectTimestamp):
  {"contactId", "languageCode", "region", "expiresAt", "liveSync": {"apiKey", "deploymentKey"}}
The Live Sync keys live in the Secrets Manager secret SECRET_ARN (set by deploy_livesync.py), never in
the static page.  Responses are no-store; the token is never logged.
"""
import base64
import hashlib
import hmac
import json
import os
import re
import time

import boto3

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
MAX_BODY = 4096
SECRET_TTL = 60  # seconds: keys updated by deploy_livesync.py are picked up within a minute
_secret = {"value": None, "at": 0.0}


class Rejected(Exception):
    def __init__(self, status, code):
        super().__init__(code)
        self.status = status
        self.code = code


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def verify_token(token: str, key: str, now: float | None = None) -> dict:
    """Payload {"c", "x", "l"} of a valid, unexpired token; Rejected otherwise."""
    if not isinstance(token, str) or token.count(".") != 1 or len(token) > 1024:
        raise Rejected(400, "INVALID_TOKEN")
    body, sig = token.split(".")
    expected = hmac.new(key.encode("utf-8"), body.encode("ascii", "ignore"), hashlib.sha256).digest()
    try:
        given = _unb64(sig)
        payload = json.loads(_unb64(body))
    except (ValueError, TypeError):
        raise Rejected(400, "INVALID_TOKEN") from None
    if not hmac.compare_digest(expected, given):
        raise Rejected(401, "INVALID_TOKEN")
    if not isinstance(payload, dict) or not UUID_RE.match(str(payload.get("c", ""))):
        raise Rejected(400, "INVALID_TOKEN")
    if not isinstance(payload.get("x"), int) or payload["x"] < (now or time.time()):
        raise Rejected(401, "LINK_EXPIRED")
    return payload


def _secret_doc() -> dict:
    if _secret["value"] is None or time.time() - _secret["at"] > SECRET_TTL:
        sm = boto3.client("secretsmanager")
        _secret["value"] = json.loads(sm.get_secret_value(SecretId=os.environ["SECRET_ARN"])["SecretString"])
        _secret["at"] = time.time()
    return _secret["value"]


def call_active(contact_id: str) -> bool:
    contact = boto3.client("connect").describe_contact(
        InstanceId=os.environ["CONNECT_INSTANCE_ID"], ContactId=contact_id)["Contact"]
    return not contact.get("DisconnectTimestamp")


def _respond(status: int, payload: dict) -> dict:
    return {"statusCode": status,
            "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "body": json.dumps(payload)}


def handler(event, context):
    try:
        if ((event.get("requestContext") or {}).get("http") or {}).get("method") != "POST":
            raise Rejected(405, "METHOD_NOT_ALLOWED")
        raw = event.get("body") or ""
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode("utf-8", "replace")
        if not raw or len(raw) > MAX_BODY:
            raise Rejected(400, "INVALID_BODY")
        try:
            token = (json.loads(raw) or {}).get("token")
        except (ValueError, AttributeError):
            raise Rejected(400, "INVALID_BODY") from None
        doc = _secret_doc()
        payload = verify_token(token, doc.get("signingKey") or "")
        api_key, dep_key = doc.get("liveSyncApiKey") or "", doc.get("liveSyncDeploymentKey") or ""
        if not api_key or not dep_key:
            raise Rejected(503, "LIVE_SYNC_KEYS_NOT_CONFIGURED")
        try:
            active = call_active(payload["c"])
        except Exception as exc:  # noqa: BLE001 - fail closed
            code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
            print(json.dumps({"outcome": "describe_contact_failed", "code": code}))
            raise Rejected(503, "CALL_STATUS_UNAVAILABLE") from None
        if not active:
            raise Rejected(410, "CALL_ENDED")
    except Rejected as rej:
        print(json.dumps({"outcome": "rejected", "code": rej.code}))
        return _respond(rej.status, {"error": rej.code})
    print(json.dumps({"outcome": "ok", "contactId": payload["c"]}))
    return _respond(200, {
        "contactId": payload["c"], "languageCode": payload.get("l") or "en-US",
        "region": os.environ.get("AWS_REGION", ""), "expiresAt": payload["x"],
        "liveSync": {"apiKey": api_key, "deploymentKey": dep_key},
    })
