"""Invoked by the Connect inbound flow (InvokeLambdaFunction, synchronous, <= 8 s).

Emails the caller a link to the Live Sync page: <PAGE_URL>#t=<token>.  The token is
base64url(JSON {"c": contactId, "x": expiry epoch, "l": language}) + "." + base64url(HMAC-SHA256),
signed with signingKey from the Secrets Manager secret SECRET_ARN.  The fragment (#t=) is never
sent to CloudFront, so the token stays out of access logs and Referer headers.

Recipient: TEST_EMAIL when set (demo mode: every caller's link goes there), otherwise the
CustomerEmail flow parameter (Customer Profiles lookup).  Returns a flat string map
($.External.linkSent / $.External.reason).  Never logs the token, the link or the full address.
"""
import base64
import hashlib
import hmac
import html
import json
import os
import re
import time

import boto3

EMAIL_RE = re.compile(r"^[^@\s<>\"',;]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,63}$")
_secret = {"value": None, "at": 0.0}


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_token(contact_id: str, ttl_seconds: int, language: str, key: str, now: float | None = None) -> str:
    payload = json.dumps({"c": contact_id, "x": int((now or time.time()) + ttl_seconds), "l": language},
                         separators=(",", ":")).encode("utf-8")
    body = _b64(payload)
    sig = hmac.new(key.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return body + "." + _b64(sig)


def mask_email(address: str) -> str:
    local, _, domain = address.partition("@")
    return (local[:1] + "***@" + domain) if domain else "***"


def pick_recipient(test_email: str, customer_email: str):
    """(address, source) or (None, reason)."""
    for value, source in ((test_email, "test"), (customer_email, "profile")):
        value = (value or "").strip()
        if value:
            if EMAIL_RE.match(value):
                return value, source
            if source == "test":
                return None, "invalid_test_email"
    return None, "no_email"


def _signing_key() -> str:
    # Cached per execution environment; the signing key never changes after stack creation.
    if _secret["value"] is None or time.time() - _secret["at"] > 300:
        sm = boto3.client("secretsmanager")
        _secret["value"] = json.loads(sm.get_secret_value(SecretId=os.environ["SECRET_ARN"])["SecretString"])
        _secret["at"] = time.time()
    key = _secret["value"].get("signingKey") or ""
    if len(key) < 32:
        raise RuntimeError("signingKey missing from the secret")
    return key


def build_email(link: str, minutes: int):
    subject = os.environ["EMAIL_SUBJECT"]
    text = os.environ["EMAIL_BODY"].replace("{link}", link).replace("{minutes}", str(minutes))
    paragraphs = os.environ["EMAIL_BODY"].replace("{minutes}", str(minutes)).split("\n")
    rendered = []
    for p in paragraphs:
        if "{link}" in p:
            rendered.append('<p><a href="' + html.escape(link, quote=True) + '">'
                            + html.escape(p.replace("{link}", "").strip() or link) + "</a></p>")
        elif p.strip():
            rendered.append("<p>" + html.escape(p) + "</p>")
    return subject, text, "".join(rendered)


def handler(event, context):
    details = (event or {}).get("Details") or {}
    contact_id = ((details.get("ContactData") or {}).get("ContactId") or "").strip()
    params = details.get("Parameters") or {}
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", contact_id):
        print(json.dumps({"outcome": "bad_event"}))
        return {"linkSent": "false", "reason": "no_contact_id"}
    recipient, source = pick_recipient(os.environ.get("TEST_EMAIL", ""), params.get("CustomerEmail", ""))
    if recipient is None:
        print(json.dumps({"outcome": "skipped", "reason": source, "contactId": contact_id}))
        return {"linkSent": "false", "reason": source}
    ttl = int(os.environ.get("LINK_TTL_SECONDS", "1800"))
    try:
        token = sign_token(contact_id, ttl, os.environ.get("LANGUAGE", "en-US"), _signing_key())
        link = os.environ["PAGE_URL"].rstrip("/") + "/#t=" + token
        subject, text, body_html = build_email(link, ttl // 60)
        boto3.client("sesv2").send_email(
            FromEmailAddress=os.environ["EMAIL_FROM"],
            Destination={"ToAddresses": [recipient]},
            Content={"Simple": {"Subject": {"Data": subject, "Charset": "UTF-8"},
                                "Body": {"Text": {"Data": text, "Charset": "UTF-8"},
                                         "Html": {"Data": body_html, "Charset": "UTF-8"}}}},
        )
    except Exception as exc:  # noqa: BLE001 - the call must go on without the page
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
        print(json.dumps({"outcome": "failed", "code": code, "contactId": contact_id}))
        return {"linkSent": "false", "reason": "send_failed"}
    print(json.dumps({"outcome": "sent", "source": source, "to": mask_email(recipient), "contactId": contact_id}))
    return {"linkSent": "true", "reason": source, "recipient": mask_email(recipient)}
