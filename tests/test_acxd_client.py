import email.message
import io
import json
import os
import socket
import sys
import unittest
import urllib.error
import urllib.parse
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import acxd_client  # noqa: E402
import config  # noqa: E402
from acxd_client import AcxdAmbiguousWriteError, AcxdApiError, AcxdClient  # noqa: E402

# Fake keys only; the prefix comes from config so the literal never appears in source files.
KEY = config.API_KEY_PREFIX + "abc.TOPSECRET"
FAKE_KEY = config.API_KEY_PREFIX + "test.SECRETVALUE"
WS = "67898477-f36f-4b06-bdca-0d9a1f2eb9cc"


def headers(d=None):
    m = email.message.Message()
    for k, v in (d or {}).items():
        m[k] = v
    return m


class FakeResp:
    def __init__(self, status=200, body=None, raw=None, hdrs=None):
        self.status = status
        self._raw = raw if raw is not None else json.dumps(body if body is not None else {}).encode()
        self.headers = headers(hdrs)

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def http_error(code, body=None, hdrs=None):
    raw = json.dumps(body).encode() if body is not None else b""
    return urllib.error.HTTPError("https://x", code, "err", headers(hdrs), io.BytesIO(raw))


def client():
    return AcxdClient(acxd_client.endpoint_for_region("us-east-1"), KEY, WS, sleep=lambda s: None)


class AcxdClientTests(unittest.TestCase):
    def test_endpoint_and_headers(self):
        self.assertEqual(acxd_client.endpoint_for_region("eu-west-2"), "https://api.acxd.connect.eu-west-2.amazonaws.com")
        with mock.patch("urllib.request.urlopen", return_value=FakeResp(201, {"name": "DeviceName"})) as m:
            body = {"name": "DeviceName", "schema": {"type": "string"}}
            client().create_context_variable(body)
        req = m.call_args[0][0]
        self.assertEqual(req.full_url, "https://api.acxd.connect.us-east-1.amazonaws.com/sdk/context-variables")
        self.assertEqual(req.get_method(), "POST")
        hdrs = {k.lower(): v for k, v in req.header_items()}
        self.assertEqual(hdrs["x-api-key"], KEY)
        self.assertEqual(hdrs["x-workspace-id"], WS)
        self.assertEqual(hdrs["content-type"], "application/json")
        self.assertEqual(json.loads(req.data), body)

    def test_path_encoding(self):
        with mock.patch("urllib.request.urlopen", return_value=FakeResp(200, {})) as m:
            client().update_flow("a/b c", {})
        req = m.call_args[0][0]
        self.assertTrue(req.full_url.endswith("/sdk/flows/a%2Fb%20c"))
        self.assertEqual(req.get_method(), "PATCH")

    def test_pagination(self):
        for method, path in (("list_context_variables", "/sdk/context-variables"), ("list_flows", "/sdk/flows"),
                             ("list_applications", "/sdk/applications"),
                             ("list_deployments", "/sdk/applications/APP/deployments")):
            pages = [FakeResp(200, {"items": [{"n": 1}], "nextToken": "T2"}), FakeResp(200, {"items": [{"n": 2}]})]
            with mock.patch("urllib.request.urlopen", side_effect=pages) as m:
                fn = getattr(client(), method)
                items = fn("APP") if method == "list_deployments" else fn()
            self.assertEqual(items, [{"n": 1}, {"n": 2}], method)
            second = urllib.parse.urlparse(m.call_args_list[1][0][0].full_url)
            self.assertEqual(second.path, path)
            q = urllib.parse.parse_qs(second.query)
            self.assertEqual(q["nextToken"], ["T2"])
            self.assertEqual(q["maxResults"], ["100"])

    def test_read_routes(self):  # GetApplication / GetFlow / ListApplicationBuilds (confirmed live, 200)
        fake_key = FAKE_KEY
        c = AcxdClient(acxd_client.endpoint_for_region("us-west-2"), fake_key, WS, sleep=lambda s: None)
        cases = (
            (lambda: c.get_application("APP 1"), "/sdk/applications/APP%201", {"applicationId": "APP 1"}),
            (lambda: c.get_flow("Maintplink"), "/sdk/flows/Maintplink", {"flowId": "Maintplink"}),
        )
        for call, path, body in cases:
            with self.assertLogs("acxd_client", "DEBUG") as logs, \
                    mock.patch("urllib.request.urlopen", return_value=FakeResp(200, body)) as m:
                self.assertEqual(call(), body)
            req = m.call_args[0][0]
            self.assertEqual(req.get_method(), "GET")
            self.assertEqual(urllib.parse.urlparse(req.full_url).path, path)
            self.assertIsNone(req.data)
            hdrs = {k.lower(): v for k, v in req.header_items()}
            self.assertEqual((hdrs["x-api-key"], hdrs["x-workspace-id"]), (fake_key, WS))
            self.assertFalse(any("SECRETVALUE" in line for line in logs.output))
        pages = [FakeResp(200, {"items": [{"buildId": "B1"}], "nextToken": "T2"}),
                 FakeResp(200, {"items": [{"buildId": "B2"}]})]
        with self.assertLogs("acxd_client", "DEBUG") as logs, \
                mock.patch("urllib.request.urlopen", side_effect=pages) as m:
            self.assertEqual(c.list_builds("APP"), [{"buildId": "B1"}, {"buildId": "B2"}])
        for call in m.call_args_list:
            req = call[0][0]
            self.assertEqual(req.get_method(), "GET")
            self.assertEqual(urllib.parse.urlparse(req.full_url).path, "/sdk/applications/APP/builds")
        q = urllib.parse.parse_qs(urllib.parse.urlparse(m.call_args_list[1][0][0].full_url).query)
        self.assertEqual(q["nextToken"], ["T2"])
        self.assertFalse(any("SECRETVALUE" in line for line in logs.output))

    def test_read_only_blocks_writes(self):  # --preflight guard
        c = AcxdClient(acxd_client.endpoint_for_region("us-west-2"), FAKE_KEY, WS,
                       sleep=lambda s: None, read_only=True)
        writes = (
            ("CreateContextVariable", lambda: c.create_context_variable({"name": "x"})),
            ("CreateFlow", lambda: c.create_flow({"flowId": "f"})),
            ("UpdateFlow", lambda: c.update_flow("f", {})),
            ("CreateApplication", lambda: c.create_application({})),
            ("UpdateApplication", lambda: c.update_application("APP", {})),
            ("CreateApplicationBuild", lambda: c.create_build("APP", {})),
            ("CreateApplicationDeployment", lambda: c.create_deployment("APP", {})),
            ("UpdateApplicationDeployment", lambda: c.update_deployment("APP", "D", {})),
        )
        with mock.patch("urllib.request.urlopen", side_effect=AssertionError("network")) as m:
            for op, call in writes:
                with self.assertRaises(acxd_client.AcxdReadOnlyViolation) as cm:
                    call()
                self.assertIn(op, str(cm.exception))
                self.assertNotIn("SECRETVALUE", str(cm.exception))
        m.assert_not_called()
        self.assertTrue(issubclass(acxd_client.AcxdReadOnlyViolation, RuntimeError))
        reads = (lambda: c.get_application("APP"), lambda: c.list_flows(), lambda: c.list_context_variables(),
                 lambda: c.list_applications(), lambda: c.list_deployments("APP"), lambda: c.get_flow("f"))
        for call in reads:
            with mock.patch("urllib.request.urlopen", return_value=FakeResp(200, {"items": []})) as m:
                call()
            self.assertEqual(m.call_args[0][0].get_method(), "GET")
        self.assertFalse(client().read_only)

    def test_list_workspaces_account_level(self):  # two-input (a)-(d)
        c = AcxdClient(acxd_client.endpoint_for_region("us-west-2"), FAKE_KEY, None, sleep=lambda s: None,
                       read_only=True)
        pages = [FakeResp(200, {"items": [{"workspaceId": "W1", "name": "a"}], "nextToken": "T2"}),
                 FakeResp(200, {"items": [{"workspaceId": "W2", "name": "b"}]})]
        with self.assertLogs("acxd_client", "DEBUG") as logs, \
                mock.patch("urllib.request.urlopen", side_effect=pages) as m:
            items = c.list_workspaces()
        self.assertEqual([i["workspaceId"] for i in items], ["W1", "W2"])
        for call in m.call_args_list:
            req = call[0][0]
            self.assertEqual(req.get_method(), "GET")
            self.assertEqual(urllib.parse.urlparse(req.full_url).path, "/sdk/workspaces")
            self.assertIsNone(req.get_header("X-workspace-id"))
            self.assertEqual(req.get_header("X-api-key"), FAKE_KEY)
        q = urllib.parse.parse_qs(urllib.parse.urlparse(m.call_args_list[1][0][0].full_url).query)
        self.assertEqual(q["nextToken"], ["T2"])
        self.assertNotIn("SECRETVALUE", repr(c))
        self.assertIn("workspace_id=None", repr(c))
        self.assertFalse(any("SECRETVALUE" in line for line in logs.output))
        with mock.patch("urllib.request.urlopen", side_effect=http_error(403, {"type": "AccessDeniedException"})):
            with self.assertRaises(AcxdApiError) as cm:
                c.list_workspaces()
        self.assertEqual(cm.exception.status, 403)
        self.assertNotIn("SECRETVALUE", str(cm.exception))
        # A workspace-scoped client still sends the header.
        with mock.patch("urllib.request.urlopen", return_value=FakeResp(200, {"items": []})) as m:
            client().list_flows()
        self.assertEqual(m.call_args[0][0].get_header("X-workspace-id"), WS)

    def test_get_application_404(self):
        with mock.patch("urllib.request.urlopen", side_effect=http_error(404, {"type": "ResourceNotFoundException"})):
            with self.assertRaises(AcxdApiError) as cm:
                client().get_application("APP")
        self.assertEqual(cm.exception.status, 404)

    def test_get_retries_503(self):
        with mock.patch("urllib.request.urlopen", side_effect=[http_error(503), FakeResp(200, {"status": "BUILT"})]) as m:
            self.assertEqual(client().get_build("A", "B")["status"], "BUILT")
        self.assertEqual(m.call_count, 2)

    def test_throttle_only_retries_429(self):
        with mock.patch("urllib.request.urlopen",
                        side_effect=[http_error(429, {"type": "ThrottlingException"}), FakeResp(201, {"applicationId": "X"})]) as m:
            self.assertEqual(client().create_application({})["applicationId"], "X")
        self.assertEqual(m.call_count, 2)

    def test_throttle_only_no_retry_on_5xx_or_timeout(self):
        for exc in (http_error(503), socket.timeout("timed out"), urllib.error.URLError("down")):
            with mock.patch("urllib.request.urlopen", side_effect=[exc, FakeResp(201, {})]) as m:
                with self.assertRaises(AcxdAmbiguousWriteError):
                    client().create_application({})
            self.assertEqual(m.call_count, 1)

    def test_create_build_no_retry(self):
        with mock.patch("urllib.request.urlopen", side_effect=[http_error(500), FakeResp(201, {})]) as m:
            with self.assertRaises(AcxdAmbiguousWriteError):
                client().create_build("A", {})
        self.assertEqual(m.call_count, 1)

    def test_retry_exhaustion(self):
        with mock.patch("urllib.request.urlopen", side_effect=[http_error(500)] * 6) as m:
            with self.assertRaises(AcxdApiError) as cm:
                client().get_build("A", "B")
        self.assertEqual(m.call_count, 6)
        self.assertEqual(cm.exception.status, 500)

    def test_error_types(self):
        for code, typ in ((404, "ResourceNotFoundException"), (409, "ConflictException"), (422, "UnprocessableEntity")):
            with mock.patch("urllib.request.urlopen",
                            side_effect=http_error(code, {"type": typ, "message": "m"}, {"x-amzn-RequestId": "R1"})):
                with self.assertRaises(AcxdApiError) as cm:
                    client().list_flows()
            self.assertEqual((cm.exception.status, cm.exception.error_type), (code, typ))
            self.assertEqual(cm.exception.request_id, "R1")

    def test_error_type_from_header(self):
        with mock.patch("urllib.request.urlopen",
                        side_effect=http_error(400, None, {"x-amzn-ErrorType": "ValidationException:http://x"})):
            with self.assertRaises(AcxdApiError) as cm:
                client().list_flows()
        self.assertEqual(cm.exception.error_type, "ValidationException")

    def test_invalid_response(self):
        with mock.patch("urllib.request.urlopen", return_value=FakeResp(200, raw=b"<html>")):
            with self.assertRaises(AcxdApiError) as cm:
                client().get_build("A", "B")
        self.assertEqual(cm.exception.error_type, "InvalidResponse")

    def test_secret_not_in_str_or_repr(self):
        c = client()
        self.assertNotIn("TOPSECRET", repr(c))
        self.assertIn("****_abc.****", repr(c))
        self.assertNotIn(config.API_KEY_PREFIX, repr(c))
        with mock.patch("urllib.request.urlopen", side_effect=http_error(403, {"type": "AccessDeniedException"})):
            with self.assertRaises(AcxdApiError) as cm:
                c.list_flows()
        self.assertNotIn("TOPSECRET", str(cm.exception))
        self.assertEqual(acxd_client.mask_key(KEY), "****_abc.****")


if __name__ == "__main__":
    unittest.main()


class RedactTests(unittest.TestCase):
    def test_redacts_credential_fields_at_any_depth(self):
        from acxd_client import REDACTED, redact, redact_text
        doc = {"deploymentAlias": "Alias1", "apiKey": "YmjA-secret", "nested": [{"deployment_key": "dk",
               "participantToken": "pt", "name": "keep"}], "empty": {"apiKey": ""}}
        out = redact(doc)
        self.assertEqual(out["apiKey"], REDACTED)
        self.assertEqual(out["nested"][0]["deployment_key"], REDACTED)
        self.assertEqual(out["nested"][0]["participantToken"], REDACTED)
        self.assertEqual((out["deploymentAlias"], out["nested"][0]["name"], out["empty"]["apiKey"]),
                         ("Alias1", "keep", ""))
        self.assertEqual(doc["apiKey"], "YmjA-secret")  # input untouched
        self.assertNotIn("YmjA-secret", redact_text(json.dumps(doc).encode(), 2000))
        self.assertEqual(redact_text(b"not json", 3), "not")
