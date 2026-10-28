import contextlib
import copy
import io
import json
import os
import socket
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
from pathlib import Path
from unittest import mock

import botocore.session
from botocore.stub import Stubber

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import config  # noqa: E402
import deploy  # noqa: E402
import flowlib  # noqa: E402

INSTANCE = "11111111-2222-4333-8444-555555555555"
QID = "66666666-7777-4888-9999-000000000000"
QARN = f"arn:aws:connect:us-east-1:123456789012:instance/{INSTANCE}/queue/{QID}"
WS = "67898477-f36f-4b06-bdca-0d9a1f2eb9cc"
WS2 = "77898477-f36f-4b06-bdca-0d9a1f2eb9cd"
# Fake key; the prefix comes from config so the literal never appears in source files.
KEY = config.API_KEY_PREFIX + "test.SECRETVALUE"
TARN = f"arn:aws:connect:us-east-1:123456789012:instance/{INSTANCE}"
REAL_ARN = "arn:aws:connect:us-west-2:123456789012:instance/00000000-0000-4000-8000-000000000001"
BQ_ARN = f"{TARN}/queue/00000000-0000-4000-8000-000000000007"
OTHER_Q_ARN = f"{TARN}/queue/00000000-0000-4000-8000-000000000008"
SP_ARN = f"arn:aws:connect:us-east-1:123456789012:instance/{INSTANCE}/contact-flow/aaaaaaaa-0000-4000-8000-000000000001"
IN_ARN = f"arn:aws:connect:us-east-1:123456789012:instance/{INSTANCE}/contact-flow/aaaaaaaa-0000-4000-8000-000000000002"
REF_ID = "bbbbbbbb-0000-4000-8000-000000000001"

ENV_VARS = ("ACXD_API_KEY", "ACXD_WORKSPACE_ID", "ACXD_REGION", "ACXD_ENDPOINT", "AWS_REGION",
            "CONNECT_INSTANCE_ID", "CONNECT_INSTANCE_ARN", "CONNECT_QUEUE_ARN", "CONNECT_QUEUE_NAME",
            "ACXD_DEPLOYMENT_ALIAS", "NAME_PREFIX", "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE")
DRY_FILES = {"acxd_context_variables.json", "acxd_flow.json", "acxd_application.json", "acxd_build.json",
             "acxd_deployment.json", "screenpop_flow.json", "inbound_acxd_flow.json", "deploy_state.json"}

SYNTH_ACTION = {"Identifier": "orig", "Type": "ConnectAgenticCX",
                "Parameters": {"ApplicationId": "APP1", "Alias": "production"},
                "Transitions": {"NextAction": "x", "Errors": [{"ErrorType": "NoMatchingError", "NextAction": "x"}]}}


@contextlib.contextmanager
def clean_env(**values):
    with mock.patch.dict(os.environ):
        for k in ENV_VARS:
            os.environ.pop(k, None)
        os.environ.update(values)
        yield


def run_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = deploy.main(argv)
    return code, out.getvalue(), err.getvalue()


def dry_args(tmp, *extra):
    return ["--dry-run", "--region", "us-east-1", "--instance-id", INSTANCE, "--queue-arn", QARN,
            "--acxd-workspace-id", WS, "--output-dir", tmp, *extra]


def connect_args(tmp, *extra):
    return ["--phase", "connect", "--region", "us-east-1", "--instance-id", INSTANCE, "--queue-arn", QARN,
            "--output-dir", tmp, *extra]


def acxd_args(tmp, *extra):
    return ["--phase", "acxd", "--region", "us-east-1", "--acxd-workspace-id", WS, "--output-dir", tmp, *extra]


def load(tmp, *parts):
    with open(os.path.join(tmp, *parts), encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Fake ACXD service
# ---------------------------------------------------------------------------

class FakeResp:
    def __init__(self, status, body):
        self.status = status
        self._raw = json.dumps(body).encode()
        self.headers = {}

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def http_error(code, typ="Error", message=None):
    body = {"type": typ, **({"message": message} if message else {})}
    return urllib.error.HTTPError("https://x", code, "err", {}, io.BytesIO(json.dumps(body).encode()))


class FakeAcxd:
    def __init__(self):
        self.calls = []
        self.bodies = []  # (method, path, body)
        self.ws_headers = []  # x-workspace-id of every request (None when absent)
        self.workspaces = [{"workspaceId": WS, "name": "ws1"}]
        self.workspaces_error = None
        self.cvars, self.flows, self.apps, self.deps = [], [], [], []
        self.flow_nodes_mode = "same"
        self.build_statuses = ["PENDING", "BUILT"]  # B1
        self.build_plan = {}  # buildId -> statuses for B2, B3, ... (default BUILT)
        self.build_details = {}  # buildId -> details of the build object (default "x")
        self.builds_created = 0
        self.alias_missing = False
        self.deployment_failed = False  # GET deployment -> "failed" with the runtime apiKey (live shape)
        self.flow_post_error_once = None
        self.app_post_timeout = False
        self.first_call_error = None
        self.build_post_error = None
        self.app_id = "APP1"
        self.app_doc = None  # GET /sdk/applications/<app_id> response (reuse mode)
        # metadata of the default GET /sdk/applications/<app_id> response (ownership check);
        # tagged like an application created by deploy.py unless a test overrides it.
        self.app_metadata = copy.deepcopy(config.APPLICATION_METADATA)
        self.flow_metadata = copy.deepcopy(config.FLOW_METADATA)  # GET /sdk/flows/<flowId> (ownership check)
        self.app_patches = []

    def count(self, method, path):
        return sum(1 for c in self.calls if c == (method, path))

    def __call__(self, req, timeout=None):
        method = req.get_method()
        path = urllib.parse.urlparse(req.full_url).path
        body = json.loads(req.data) if req.data else None
        self.calls.append((method, path))
        self.bodies.append((method, path, body))
        self.ws_headers.append(req.get_header("X-workspace-id"))
        if self.first_call_error is not None:
            err, self.first_call_error = self.first_call_error, None
            raise err
        app = f"/sdk/applications/{self.app_id}"
        if path == "/sdk/workspaces" and method == "GET":
            if self.workspaces_error is not None:
                raise self.workspaces_error
            return FakeResp(200, {"items": self.workspaces})
        if path == "/sdk/context-variables":
            if method == "GET":
                return FakeResp(200, {"items": self.cvars})
            self.cvars.append(body)
            return FakeResp(201, body)
        if path == "/sdk/flows":
            if method == "GET":
                return FakeResp(200, {"items": self.flows})
            if self.flow_post_error_once is not None:
                err, self.flow_post_error_once = self.flow_post_error_once, None
                raise err
            self.flows.append({"flowId": body["flowId"]})
            return FakeResp(201, self._flow_resp(body))
        if path.startswith("/sdk/flows/") and method == "PATCH":
            return FakeResp(200, self._flow_resp(body))
        if path.startswith("/sdk/flows/") and method == "GET":
            flow_id = urllib.parse.unquote(path.rsplit("/", 1)[1])
            if not any(f.get("flowId") == flow_id for f in self.flows):
                raise http_error(404, "ResourceNotFoundException")
            return FakeResp(200, {"flowId": flow_id, "metadata": self.flow_metadata})
        if path == "/sdk/applications":
            if method == "GET":
                return FakeResp(200, {"items": self.apps})
            self.apps.append({"applicationId": "APP1", "name": body["name"]})
            if self.app_post_timeout:
                self.app_post_timeout = False
                raise socket.timeout("timed out")
            return FakeResp(201, {"applicationId": "APP1"})
        if path == app and method == "PATCH":
            self.app_patches.append(body)
            return FakeResp(200, {"applicationId": self.app_id})
        if path == app and method == "GET" and self.app_doc is not None:
            return FakeResp(200, self.app_doc)
        if path == app and method == "GET":
            name = next((a.get("name") for a in self.apps if a.get("applicationId") == self.app_id), None)
            if name is None:
                raise http_error(404, "ResourceNotFoundException")
            return FakeResp(200, {"applicationId": self.app_id, "name": name, "metadata": self.app_metadata})
        if path == app + "/builds" and method == "POST":
            if self.build_post_error is not None:
                raise self.build_post_error
            self.builds_created += 1
            return FakeResp(201, {"buildId": f"B{self.builds_created}", "status": "PENDING"})
        if path.startswith(app + "/builds/B"):
            build_id = path.rsplit("/", 1)[1]
            statuses = self.build_statuses if build_id == "B1" else self.build_plan.setdefault(build_id, ["BUILT"])
            status = statuses.pop(0) if len(statuses) > 1 else statuses[0]
            return FakeResp(200, {"buildId": build_id, "status": status,
                                  "details": self.build_details.get(build_id, "x"),
                                  "languageSettings": [{"languageCode": "zh-CN", "useLex3pAsr": False}]})
        if path == app + "/deployments":
            if method == "GET":
                return FakeResp(200, {"items": self.deps})
            self.deps.append({"deploymentId": "D1", "environment": body["environment"],
                              "deploymentAlias": None if self.alias_missing else "ALIAS1"})
            return FakeResp(201, {"deploymentId": "D1", "deploymentStatus": "pending"})
        if path == app + "/deployments/D1":
            if method == "PATCH":
                return FakeResp(200, {"deploymentId": "D1", "deploymentStatus": "pending"})
            resp = {"deploymentId": "D1", "deploymentStatus": "deployed"}
            if self.deployment_failed:
                resp.update(deploymentStatus="failed", apiKey="RUNTIMEKEYVALUE-1234", contextVariables=[])
            if not self.alias_missing:
                resp["deploymentAlias"] = "ALIAS1"
            return FakeResp(200, resp)
        raise AssertionError(f"unexpected ACXD call {method} {path}")

    def _flow_resp(self, body):
        resp = {"flowId": "AgenticCareIntake", "saveId": "s1"}
        if self.flow_nodes_mode == "same":
            resp["nodes"] = body["nodes"]
        elif self.flow_nodes_mode == "different":
            resp["nodes"] = {"server-" + k[-2:]: v for k, v in body["nodes"].items()}
        return resp


# ---------------------------------------------------------------------------
# Connect stub helpers
# ---------------------------------------------------------------------------

def connect_client():
    c = botocore.session.get_session().create_client(
        "connect", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    return c, Stubber(c)


def stub_instance_queue(stub, flows=None, tags=None):
    """DescribeInstance + DescribeQueue + the same-name flow precheck (connect_prechecks)."""
    stub.add_response("describe_instance", {"Instance": {"Id": INSTANCE}}, {"InstanceId": INSTANCE})
    stub.add_response("describe_queue", {"Queue": {"Name": "Support", "QueueArn": QARN, "QueueId": QID}},
                      {"InstanceId": INSTANCE, "QueueId": QID})
    stub_precheck(stub, flows, tags)


def stub_precheck(stub, flows=None, tags=None):
    """precheck_contact_flows: one ListContactFlows, then DescribeContactFlow of every derived-name flow
    (sorted by name), tagged as created by deploy.py unless `tags` says otherwise."""
    stub_list(stub, flows)
    wanted = ("AgenticCare ScreenPop Flow", "AgenticCare Inbound ACXD Flow")
    for f in sorted((f for f in flows or [] if f["Name"] in wanted), key=lambda f: f["Name"]):
        stub.add_response(
            "describe_contact_flow",
            {"ContactFlow": {"Id": f["Id"], "Arn": f["Arn"], "Name": f["Name"], "Content": "{}",
                             "Tags": dict(config.CONNECT_OWNER_TAGS) if tags is None else tags}},
            {"InstanceId": INSTANCE, "ContactFlowId": f["Id"]})


def stub_reads(stub, flows=None):
    stub_instance_queue(stub, flows)
    stub_list(stub, flows)


def stub_list(stub, flows=None):
    stub.add_response("list_contact_flows", {"ContactFlowSummaryList": flows or []},
                      {"InstanceId": INSTANCE, "ContactFlowTypes": ["CONTACT_FLOW"]})


def capture_params(client, captured):
    def hook(params, model, **_):
        captured.append((model.name, dict(params)))
    client.meta.events.register("provide-client-params.connect.*", hook)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

class DryRunTests(unittest.TestCase):
    def test_dry_run_end_to_end(self):  # AC2, AC3, AC6, AC13
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            state = Path(tmp) / "deploy_state.json"
            state.write_text('{"keep": true}', encoding="utf-8")
            os.utime(state, (1_000_000, 1_000_000))
            before = (state.read_text(), state.stat().st_mtime)
            urlopen = mock.Mock(side_effect=AssertionError("network"))
            boto = mock.Mock(side_effect=AssertionError("boto3"))
            with mock.patch("urllib.request.urlopen", urlopen), mock.patch("deploy.boto3.client", boto):
                code, out, _ = run_main(dry_args(tmp))
            self.assertEqual(code, 0)
            urlopen.assert_not_called()
            boto.assert_not_called()
            dry = Path(tmp) / "dry-run"
            self.assertEqual({p.name for p in dry.iterdir()}, DRY_FILES)
            for p in dry.iterdir():
                json.loads(p.read_text(encoding="utf-8"))
            st = load(tmp, "dry-run", "deploy_state.json")
            self.assertIs(st["dryRun"], True)
            self.assertEqual(st["schemaVersion"], 1)
            self.assertIsNone(st["acxd"]["flowNodeIdsRemapped"])
            self.assertEqual((state.read_text(), state.stat().st_mtime), before)
            app = load(tmp, "dry-run", "acxd_application.json")
            self.assertEqual(list(app["settings"]["defaultFlows"]), ["ACXD.Welcome"])
            self.assertNotIn("conversationTTL", app["settings"])
            self.assertNotIn("thresholds", app["settings"])
            expected_ls = [{"languageCode": "zh-CN", "useLex3pAsr": False}]
            self.assertEqual(app["settings"]["languageSettings"], expected_ls)
            self.assertEqual(load(tmp, "dry-run", "acxd_build.json")["languageSettings"], expected_ls)
            self.assertEqual(load(tmp, "dry-run", "acxd_deployment.json")["buildIdentifier"], "DRYRUN-BUILD-ID")
            self.assertEqual(len(load(tmp, "dry-run", "acxd_context_variables.json")), 3)
            inbound = load(tmp, "dry-run", "inbound_acxd_flow.json")
            hook = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
            self.assertTrue(hook["Parameters"]["EventHooks"]["DefaultAgentUI"].endswith("DRYRUN-SCREENPOP"))
            self.assertIn("A20", out)

    def test_language_setting_extra(self):  # AC13
        with tempfile.TemporaryDirectory() as tmp, clean_env(), \
                mock.patch.object(config, "LANGUAGE_SETTING_EXTRA", {"voice": "X"}):
            self.assertEqual(run_main(dry_args(tmp))[0], 0)
            expected = [{"languageCode": "zh-CN", "voice": "X"}]
            self.assertEqual(load(tmp, "dry-run", "acxd_application.json")["settings"]["languageSettings"], expected)
            self.assertEqual(load(tmp, "dry-run", "acxd_build.json")["languageSettings"], expected)

    def test_en_us(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            self.assertEqual(run_main(dry_args(tmp, "--language", "en-US"))[0], 0)
            app = load(tmp, "dry-run", "acxd_application.json")
            self.assertEqual(app["settings"]["languageCode"], "en-US")
            self.assertTrue(app["description"].startswith("Device fault"))
            inbound = load(tmp, "dry-run", "inbound_acxd_flow.json")
            tts = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactTextToSpeechVoice"][0]
            self.assertEqual(tts["Parameters"]["TextToSpeechVoice"], "Joanna")
            text = json.dumps(inbound, ensure_ascii=False)
            self.assertIn('"CustomerName": "Not found"', text)
            self.assertIn('"CustomerEmail": "Not provided"', text)
            self.assertIn('"$.Customer.FirstName $.Customer.LastName"', text)
            bar = action_of(load(tmp, "dry-run", "screenpop_flow.json"), "ShowView")["Parameters"]["ViewData"]
            self.assertEqual([b["Label"] for b in bar["AttributeBar"]][:3],
                             ["Customer name", "Phone number", "Email"])

    def _agentic(self, tmp):
        inbound = load(tmp, "dry-run", "inbound_acxd_flow.json")
        action = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        return action, inbound["Metadata"]["ActionMetadata"][config.AGENTIC_CX_ACTION_ID]

    def test_generated_action_default(self):  # AC2: no reference flow needed
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            code, out, _ = run_main(dry_args(tmp))
            self.assertEqual(code, 0)
            action, am = self._agentic(tmp)
            st = load(tmp, "dry-run", "deploy_state.json")
        self.assertEqual(action["Type"], "ConnectParticipantWithAgenticCX")
        self.assertEqual(action["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": "DRYRUN-APPLICATION-ID",
                          "Alias": "DRYRUN-DEPLOYMENT-ALIAS"})
        errors = {e["ErrorType"]: e["NextAction"] for e in action["Transitions"]["Errors"]}
        self.assertEqual(errors["InputTimeLimitExceeded"], "d1a9e0c0-0000-4000-8000-000000000009")
        self.assertEqual(errors["NoMatchingCondition"], "d1a9e0c0-0000-4000-8000-000000000005")
        self.assertEqual(am["position"], {"x": 880, "y": 40})
        self.assertEqual(am["parameters"]["AgentConfiguration"]["Alias"], {"displayName": "Production"})
        self.assertEqual(st["connect"]["agenticCxActionSource"], "generated")
        self.assertEqual(st["acxd"]["deploymentAlias"], "DRYRUN-DEPLOYMENT-ALIAS")
        self.assertNotIn("获取方法", out)

    def test_generated_action_explicit_ids(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            code, _, _ = run_main(dry_args(tmp, "--acxd-application-id", "APP9", "--acxd-deployment-alias", "AL_9-x"))
            self.assertEqual(code, 0)
            action, _ = self._agentic(tmp)
            st = load(tmp, "dry-run", "deploy_state.json")
        self.assertEqual(action["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": "APP9", "Alias": "AL_9-x"})
        self.assertEqual(st["acxd"]["deploymentAlias"], "AL_9-x")

    def test_connect_dry_run_without_ids_uses_fallback(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            argv = ["--dry-run", "--phase", "connect", "--region", "us-east-1", "--instance-id", INSTANCE,
                    "--queue-arn", QARN, "--acxd-workspace-id", WS, "--output-dir", tmp]
            code, out, _ = run_main(argv)
            self.assertEqual(code, 0)
            action, _ = self._agentic(tmp)
        self.assertEqual(action["Type"], config.UNVERIFIED_ACTION_TYPE)
        self.assertIn("获取方法", out)

    def test_secret_never_written(self):  # AC7
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY):
            code, out, err = run_main(dry_args(tmp, "--verbose"))
            self.assertEqual(code, 0)
            self.assertNotIn("SECRETVALUE", out + err)
            for p in Path(tmp).rglob("*"):
                if p.is_file():
                    self.assertNotIn("SECRETVALUE", p.read_text(encoding="utf-8"), str(p))


class ValidationTests(unittest.TestCase):
    def test_missing_key_non_tty(self):  # AC10
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("sys.stdin.isatty", return_value=False):
            code, _, err = run_main(acxd_args(tmp))
        self.assertEqual(code, 2)
        self.assertIn("ACXD_API_KEY", err)

    def test_getpass_called_for_acxd_tty(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("sys.stdin.isatty", return_value=True), \
                mock.patch("deploy.getpass.getpass", return_value="") as gp:
            code, _, _ = run_main(acxd_args(tmp))
        self.assertEqual(code, 2)
        gp.assert_called_once()

    def test_bad_configs(self):  # AC10
        cases = [
            (["--dry-run", "--instance-id", INSTANCE, "--queue-arn", QARN, "--acxd-workspace-id", WS], "--region"),
            (["--dry-run", "--region", "us-east-1", "--queue-arn", QARN, "--acxd-workspace-id", WS], "--instance-id"),
            (dry_args("x", "--queue-name", "Q"), "--queue-arn"),
            (["--dry-run", "--region", "us-east-1", "--instance-id", "21111111-2222-4333-8444-555555555555",
              "--queue-arn", QARN, "--acxd-workspace-id", WS], "--queue-arn"),
            (dry_args("x", "--acxd-endpoint", "http://example.com"), "https://"),
            (dry_args("x", "--acxd-deployment-alias", "bad alias!"), "--acxd-deployment-alias"),
            (dry_args("x", "--acxd-workspace-id", "not-a-uuid"), "--acxd-workspace-id"),
        ]
        for argv, needle in cases:
            with clean_env(), mock.patch("sys.stdin.isatty", return_value=False):
                code, _, err = run_main(argv)
            self.assertEqual(code, 2, argv)
            self.assertIn(needle, err, argv)
        # Two-input change: a dry-run without --acxd-workspace-id is valid (placeholder workspace).
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            argv = ["--dry-run", "--region", "us-east-1", "--instance-id", INSTANCE, "--queue-arn", QARN,
                    "--output-dir", tmp]
            self.assertEqual(run_main(argv)[0], 0)

    def test_load_state(self):  # AC11
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "s.json"
            self.assertIsNone(deploy.load_state(p))
            for text in ('{"schemaVersion": 1, "dryRun": true}', "{bad", '{"schemaVersion": 2, "dryRun": false}'):
                p.write_text(text)
                self.assertIsNone(deploy.load_state(p), text)
            p.write_text('{"schemaVersion": 1, "dryRun": false, "acxd": {"applicationId": "A"}}')
            self.assertEqual(deploy.load_state(p)["acxd"]["applicationId"], "A")

    def _cfg(self, *argv):
        with clean_env():
            return deploy.validate_config(deploy.build_parser().parse_args(list(argv)))

    def test_builders(self):
        cfg = self._cfg(*dry_args("x"))
        self.assertLessEqual(len(deploy.build_build_body(cfg)["version"]), 16)
        body = deploy.build_flow_body(cfg, {"n": {}})
        self.assertNotIn("flowId", deploy.update_flow_body(body))
        self.assertEqual(body["flowId"], "AgenticCareIntake")

    def test_resolve_application_id_ignores_dry_state(self):  # AC11
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "deploy_state.json"
            p.write_text(json.dumps({"schemaVersion": 1, "dryRun": True, "acxd": {"applicationId": "DRYRUN-APPLICATION-ID"}}))
            cfg = self._cfg(*connect_args(tmp))
            self.assertIsNone(deploy.resolve_application_id(cfg, deploy.load_state(p)))


# ---------------------------------------------------------------------------
# ACXD phase
# ---------------------------------------------------------------------------

class AcxdPhaseTests(unittest.TestCase):
    def run_acxd(self, fake, tmp):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.time.sleep"):
            return run_main(acxd_args(tmp, "--poll-interval", "1"))

    def test_success(self):
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
            self.assertIs(st["dryRun"], False)
            a = st["acxd"]
            self.assertEqual((a["applicationId"], a["buildId"], a["deploymentId"], a["deploymentAlias"]),
                             ("APP1", "B1", "D1", "ALIAS1"))
            self.assertIs(a["flowNodeIdsRemapped"], False)
            self.assertEqual(st["completedSteps"], list(deploy.STEP_NAMES[:5]))
            self.assertGreaterEqual(fake.count("GET", "/sdk/applications/APP1/builds/B1"), 2)
            self.assertEqual(fake.count("GET", "/sdk/applications/APP1/deployments/D1"), 1)
            self.assertEqual(fake.count("POST", "/sdk/context-variables"), 3)
            self.assertEqual(fake.count("GET", "/sdk/workspaces"), 0)  # explicit --acxd-workspace-id
            self.assertEqual(set(fake.ws_headers), {WS})
            self.assertNotIn("SECRETVALUE", out + err)
            for p in Path(tmp).rglob("*.json"):
                self.assertNotIn("SECRETVALUE", p.read_text(encoding="utf-8"))

    def test_application_post_timeout_then_update(self):  # AC12
        fake = FakeAcxd()
        fake.app_post_timeout = True
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 0, err)
        self.assertEqual(fake.count("POST", "/sdk/applications"), 1)
        self.assertEqual(fake.count("PATCH", "/sdk/applications/APP1"), 1)

    def test_foreign_same_name_application_stops_before_any_write(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = {"path": "", "tags": []}  # created by someone else
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertIn("不是本脚本创建的", err)
        self.assertIn("--name-prefix", err)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"}, fake.calls)
        self.assertEqual(fake.calls, [("GET", "/sdk/applications"), ("GET", "/sdk/applications/APP1")])

    def test_foreign_application_without_metadata_also_stops(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = None
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertEqual(fake.count("PATCH", "/sdk/applications/APP1"), 0)

    def test_owned_same_name_application_is_updated(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 0, err)
        self.assertEqual(fake.count("POST", "/sdk/applications"), 0)
        self.assertEqual(fake.count("PATCH", "/sdk/applications/APP1"), 1)
        # Checked once before any write and once right before the PATCH.
        self.assertEqual(fake.count("GET", "/sdk/applications/APP1"), 2)

    def test_overwrite_untagged_application_flag(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = {"path": "", "tags": ["other"]}
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), \
                mock.patch("urllib.request.urlopen", fake), mock.patch("deploy.time.sleep"):
            code, _, err = run_main(acxd_args(tmp, "--poll-interval", "1", "--overwrite-untagged-application"))
        self.assertEqual(code, 0, err)
        self.assertIn("--overwrite-untagged-resources", err)
        self.assertEqual(fake.count("PATCH", "/sdk/applications/APP1"), 1)

    def test_conflict_with_foreign_application_created_meanwhile(self):
        fake = FakeAcxd()
        fake.app_metadata = {"path": "", "tags": []}
        orig = fake.__call__

        def racing(req, timeout=None):
            if req.get_method() == "POST" and urllib.parse.urlparse(req.full_url).path == "/sdk/applications":
                fake.calls.append(("POST", "/sdk/applications"))
                fake.apps.append({"applicationId": "APP1", "name": "AgenticCareSelfService"})
                raise http_error(409, "ConflictException")
            return orig(req, timeout)
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(racing, tmp)
        self.assertEqual(code, 2)
        self.assertEqual(fake.count("PATCH", "/sdk/applications/APP1"), 0)

    def test_foreign_acxd_flow_stops_before_any_write(self):
        fake = FakeAcxd()
        fake.flows = [{"flowId": "AgenticCareIntake"}]
        fake.flow_metadata = {"path": "", "tags": ["someone-else"]}
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertIn("ACXD flow AgenticCareIntake", err)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"}, fake.calls)
        self.assertIn(("GET", "/sdk/flows/AgenticCareIntake"), fake.calls)

    def test_owned_acxd_flow_is_updated(self):
        fake = FakeAcxd()
        fake.flows = [{"flowId": "AgenticCareIntake"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 0, err)
        self.assertEqual(fake.count("PATCH", "/sdk/flows/AgenticCareIntake"), 1)
        self.assertEqual(fake.count("POST", "/sdk/flows"), 0)
        self.assertEqual(fake.count("GET", "/sdk/flows/AgenticCareIntake"), 2)  # precheck + before PATCH

    def test_foreign_acxd_flow_with_override(self):
        fake = FakeAcxd()
        fake.flows = [{"flowId": "AgenticCareIntake"}]
        fake.flow_metadata = None
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), \
                mock.patch("urllib.request.urlopen", fake), mock.patch("deploy.time.sleep"):
            code, _, err = run_main(acxd_args(tmp, "--poll-interval", "1", "--overwrite-untagged-resources"))
        self.assertEqual(code, 0, err)
        self.assertEqual(fake.count("PATCH", "/sdk/flows/AgenticCareIntake"), 1)

    def test_failed_deployment_never_leaks_the_runtime_api_key(self):
        fake = FakeAcxd()
        fake.deployment_failed = True
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 1)
            self.assertIn("tools/acxd_diagnose_deployment.py", err)
            self.assertNotIn("RUNTIMEKEYVALUE", out + err)
            saved = load(tmp, "acxd_deployment_result.json")
            self.assertEqual(saved["apiKey"], "<redacted>")
            for p in Path(tmp).rglob("*.json"):
                self.assertNotIn("RUNTIMEKEYVALUE", p.read_text(encoding="utf-8"), p)

    def test_create_build_5xx_no_retry(self):  # AC12
        fake = FakeAcxd()
        fake.build_post_error = http_error(500)
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(fake, tmp)
        self.assertEqual(code, 1)
        self.assertEqual(fake.count("POST", "/sdk/applications/APP1/builds"), 1)

    def test_first_call_404(self):
        fake = FakeAcxd()
        fake.first_call_error = http_error(404, "ResourceNotFoundException")
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)

    def test_flow_node_ids_remapped(self):  # AC17
        for mode, expected in (("same", False), ("different", True), ("missing", None)):
            fake = FakeAcxd()
            fake.flow_nodes_mode = mode
            with tempfile.TemporaryDirectory() as tmp:
                if mode == "different":
                    with self.assertLogs("deploy", "WARNING") as logs:
                        code, _, _ = self.run_acxd(fake, tmp)
                    self.assertTrue(any("node IDs differ" in m and "server-01" in m for m in logs.output))
                else:
                    code, _, _ = self.run_acxd(fake, tmp)
                self.assertEqual(code, 0, mode)
                self.assertIs(load(tmp, "deploy_state.json")["acxd"]["flowNodeIdsRemapped"], expected, mode)

    def test_build_failed(self):
        fake = FakeAcxd()
        fake.build_statuses = ["FAILED"]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(fake, tmp)
            self.assertEqual(code, 1)
            self.assertEqual(load(tmp, "acxd_build_result.json")["status"], "FAILED")


REUSE_APP = "00000000-0000-4000-8000-000000000004"


def workshop_app():
    with open(os.path.join(ROOT, "tests", "fixtures", "acxd_application_workshop1014.json"), encoding="utf-8") as fh:
        return json.load(fh)


class ReuseApplicationTests(unittest.TestCase):
    def test_patch_body(self):  # (a)
        existing = workshop_app()
        patch = deploy.build_reuse_application_patch(existing, "AgenticCareIntake")
        self.assertEqual(set(patch), {"flows", "settings"})
        self.assertEqual(patch["flows"], [{"flowId": "Maintplink"}, {"flowId": "AgenticCareIntake"}])
        df = patch["settings"]["defaultFlows"]
        self.assertEqual(df["ACXD.Welcome"], {"flowId": "AgenticCareIntake"})
        self.assertEqual(df["ACXD.Fallback"], {"flowId": "Maintplink"})
        self.assertEqual(df["ACXD.Unknown"], {"flowId": "Maintplink"})
        for key in ("clusters", "guardrails", "languageSettings", "languageCode", "languageCodes",
                    "conversationTTL", "thresholds", "lifecycleHooks", "repeatOnIncomprehension"):
            self.assertEqual(patch["settings"][key], existing["settings"][key], key)
        self.assertEqual(set(patch["settings"]), set(existing["settings"]))
        # input untouched, idempotent when the flow is already attached
        self.assertEqual(existing, workshop_app())
        again = deploy.build_reuse_application_patch({**existing, **patch}, "AgenticCareIntake")
        self.assertEqual(again["flows"], patch["flows"])

    def run_reuse(self, fake, tmp, language):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.time.sleep"):
            return run_main(acxd_args(tmp, "--poll-interval", "1", "--reuse-acxd-application",
                                      "--acxd-application-id", REUSE_APP, "--language", language))

    def _fake(self):
        fake = FakeAcxd()
        fake.app_id = REUSE_APP
        fake.app_doc = workshop_app()
        return fake

    def test_acxd_phase_reuse(self):  # (b)
        fake = self._fake()
        app = f"/sdk/applications/{REUSE_APP}"
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertLogs("deploy", "WARNING") as logs:
                code, out, err = self.run_reuse(fake, tmp, "en-US")
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
            before = load(tmp, "acxd_application_before.json")
            sent = load(tmp, "acxd_application.json")
        self.assertEqual(fake.count("POST", "/sdk/applications"), 0)
        self.assertEqual(fake.count("GET", "/sdk/applications"), 0)
        self.assertEqual(fake.count("PATCH", app), 1)
        self.assertEqual(fake.count("POST", app + "/builds"), 1)
        self.assertEqual(fake.count("POST", app + "/deployments"), 1)
        writes = [c for c in fake.calls if c[0] in ("POST", "PATCH")]
        self.assertLess(fake.calls.index(("GET", app)), fake.calls.index(writes[0]))
        self.assertEqual(fake.app_patches, [sent])
        self.assertEqual(set(sent), {"flows", "settings"})
        self.assertEqual(sent["settings"]["defaultFlows"]["ACXD.Welcome"], {"flowId": "AgenticCareIntake"})
        self.assertEqual(before, workshop_app())
        a = st["acxd"]
        self.assertIs(a["applicationReused"], True)
        self.assertEqual((a["applicationId"], a["applicationName"], a["previousWelcomeFlowId"]),
                         (REUSE_APP, "workshop1014", "Maintplink"))
        self.assertEqual(a["deploymentAlias"], "ALIAS1")
        self.assertEqual(st["completedSteps"], list(deploy.STEP_NAMES[:5]))
        self.assertTrue(any("Maintplink -> AgenticCareIntake" in m for m in logs.output))
        self.assertNotIn("SECRETVALUE", out + err)

    def test_language_mismatch(self):  # (c)
        fake = self._fake()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_reuse(fake, tmp, "zh-CN")
        self.assertEqual(code, 2)
        self.assertIn("--language en-US", err)
        self.assertEqual([c for c in fake.calls if c[0] in ("POST", "PATCH")], [])

    def test_application_missing(self):
        fake = self._fake()
        fake.first_call_error = http_error(404, "ResourceNotFoundException")
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_reuse(fake, tmp, "en-US")
        self.assertEqual(code, 2)
        self.assertIn("应用不存在", err)
        self.assertEqual(fake.calls, [("GET", f"/sdk/applications/{REUSE_APP}")])

    def test_flag_validation(self):  # (d)
        cases = [
            (dry_args("x", "--reuse-acxd-application"), "--acxd-application-id"),
            (dry_args("x", "--reuse-acxd-application", "--acxd-application-id", "APP1"), "UUID"),
        ]
        for argv, needle in cases:
            with clean_env():
                code, _, err = run_main(argv)
            self.assertEqual(code, 2, argv)
            self.assertIn(needle, err, argv)
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            argv = ["--dry-run", "--phase", "connect", "--region", "us-east-1", "--instance-id", INSTANCE,
                    "--queue-arn", QARN, "--acxd-workspace-id", WS, "--output-dir", tmp,
                    "--reuse-acxd-application", "--acxd-application-id", REUSE_APP]
            with self.assertLogs("deploy", "WARNING") as logs:
                code, _, _ = run_main(argv)
        self.assertEqual(code, 0)
        self.assertTrue(any("no effect" in m for m in logs.output))

    def test_dry_run_reuse(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            urlopen = mock.Mock(side_effect=AssertionError("network"))
            with mock.patch("urllib.request.urlopen", urlopen), \
                    mock.patch("deploy.boto3.client", mock.Mock(side_effect=AssertionError("boto3"))):
                with self.assertLogs("deploy", "WARNING") as logs:
                    code, _, err = run_main(dry_args(tmp, "--reuse-acxd-application", "--acxd-application-id",
                                                     REUSE_APP, "--acxd-deployment-alias", "AL1",
                                                     "--language", "en-US"))
            self.assertEqual(code, 0, err)
            urlopen.assert_not_called()
            app = load(tmp, "dry-run", "acxd_application.json")
            st = load(tmp, "dry-run", "deploy_state.json")
            inbound = load(tmp, "dry-run", "inbound_acxd_flow.json")
        self.assertEqual(app, {"flows": [{"flowId": "AgenticCareIntake"}],
                               "settings": {"languageCode": "en-US", "languageCodes": ["en-US"],
                                            "defaultFlows": {"ACXD.Welcome": {"flowId": "AgenticCareIntake"}}}})
        self.assertEqual(st["acxd"]["applicationId"], REUSE_APP)
        self.assertIs(st["acxd"]["applicationReused"], True)
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"]["ApplicationId"], REUSE_APP)
        self.assertTrue(any("--preflight" in m for m in logs.output))


# ---------------------------------------------------------------------------
# Connect phase
# ---------------------------------------------------------------------------

class ConnectPhaseTests(unittest.TestCase):
    def test_guard_no_writes(self):  # AC8, AC15
        client, stub = connect_client()
        stub_reads(stub)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("sys.stdin.isatty", return_value=True), \
                mock.patch("deploy.getpass.getpass") as gp, \
                mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError)) as uo:
            code, out, _ = run_main(connect_args(tmp))
            stub.assert_no_pending_responses()
        self.assertEqual(code, 2)
        gp.assert_not_called()
        uo.assert_not_called()
        self.assertIn("获取方法", out)

    def test_key_without_workspace_no_lookup(self):  # AC15 (two-input: only the workspace discovery GET)
        client, stub = connect_client()
        stub_reads(stub)
        fake = FakeAcxd()
        fake.workspaces = []
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", fake):
            code, _, _ = run_main(connect_args(tmp))
        self.assertEqual(code, 2)
        self.assertEqual(fake.calls, [("GET", "/sdk/workspaces")])

    def test_lookup_failures_do_not_change_exit_code(self):  # AC15
        for err in (urllib.error.URLError("down"), http_error(403, "AccessDeniedException")):
            client, stub = connect_client()
            stub_reads(stub)
            with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), stub, \
                    mock.patch("deploy.boto3.client", return_value=client), \
                    mock.patch("deploy.time.sleep"), \
                    mock.patch("urllib.request.urlopen", mock.Mock(side_effect=err)) as uo:
                with self.assertLogs("deploy", "WARNING") as logs:
                    code, _, _ = run_main(connect_args(tmp, "--acxd-workspace-id", WS))
            self.assertEqual(code, 2)
            self.assertTrue(uo.called)
            self.assertTrue(any("name lookup" in m for m in logs.output))
            self.assertFalse(any("SECRETVALUE" in m for m in logs.output))

    def test_application_id_from_state(self):  # AC15
        client, stub = connect_client()
        stub_reads(stub)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            Path(tmp, "deploy_state.json").write_text(json.dumps(
                {"schemaVersion": 1, "dryRun": False, "acxd": {"applicationId": "APPX"}}))
            with self.assertLogs("deploy", "INFO") as logs:
                code, _, _ = run_main(connect_args(tmp))
        self.assertEqual(code, 2)
        self.assertTrue(any("applicationId source: deploy_state.json" in m for m in logs.output))

    def _action_file(self, tmp, obj=SYNTH_ACTION):
        p = Path(tmp, "action.json")
        p.write_text(json.dumps(obj))
        return str(p)

    def test_create_path(self):
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_reads(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            code, out, err = run_main(connect_args(tmp, "--agentic-cx-action-file", self._action_file(tmp),
                                                   "--acxd-application-id", "APP1"))
            stub.assert_no_pending_responses()
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
            self.assertEqual(st["connect"]["screenpopFlowArn"], SP_ARN)
            self.assertEqual(st["connect"]["inboundFlowArn"], IN_ARN)
            self.assertEqual(st["connect"]["agenticCxActionSource"], "file")
        creates = [p for name, p in captured if name == "CreateContactFlow"]
        self.assertEqual([p["Name"] for p in creates], ["AgenticCare ScreenPop Flow", "AgenticCare Inbound ACXD Flow"])
        for p in creates:
            self.assertEqual((p["Type"], p["Status"]), ("CONTACT_FLOW", "PUBLISHED"))
            self.assertEqual(p["Tags"], config.CONNECT_OWNER_TAGS)  # ownership marker
        inbound = json.loads(creates[1]["Content"])
        hook = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
        self.assertEqual(hook["Parameters"]["EventHooks"]["DefaultAgentUI"], SP_ARN)
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(agentic["Type"], "ConnectAgenticCX")
        self.assertIn("associate-phone-number-contact-flow", out)

    def test_update_path(self):
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        flows = [{"Id": "sp", "Arn": SP_ARN, "Name": "AgenticCare ScreenPop Flow"},
                 {"Id": "in", "Arn": IN_ARN, "Name": "AgenticCare Inbound ACXD Flow"}]
        stub_reads(stub, flows)
        stub.add_response("update_contact_flow_content", {}, {"InstanceId": INSTANCE, "ContactFlowId": "sp",
                                                              "Content": mock.ANY})
        stub_list(stub, flows)
        stub.add_response("update_contact_flow_content", {}, {"InstanceId": INSTANCE, "ContactFlowId": "in",
                                                              "Content": mock.ANY})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            code, _, err = run_main(connect_args(tmp, "--agentic-cx-action-file", self._action_file(tmp)))
            stub.assert_no_pending_responses()
        self.assertEqual(code, 0, err)
        updates = [p for name, p in captured if name == "UpdateContactFlowContent"]
        inbound = json.loads(updates[1]["Content"])
        hook = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
        self.assertEqual(hook["Parameters"]["EventHooks"]["DefaultAgentUI"], SP_ARN)

    def test_reference_flow(self):
        ref = {"Version": "2019-10-30", "StartAction": "r1", "Actions": [
            {"Identifier": "r1", "Type": "ConnectAgenticCX", "Parameters": {"ApplicationId": "APP1"},
             "Transitions": {"NextAction": "r2", "Errors": [{"ErrorType": "NoMatchingError", "NextAction": "r2"}]}},
            {"Identifier": "r2", "Type": "DisconnectParticipant", "Parameters": {}, "Transitions": {}}],
            "Metadata": {"ActionMetadata": {"r1": {"position": {"x": 1, "y": 2}, "parameters": {"App": "shown"}}}}}
        client, stub = connect_client()
        stub_instance_queue(stub)
        stub.add_response("describe_contact_flow", {"ContactFlow": {"Content": json.dumps(ref)}},
                          {"InstanceId": INSTANCE, "ContactFlowId": REF_ID})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            code, _, err = run_main(connect_args(tmp, "--agentic-cx-reference-flow-id", REF_ID))
            stub.assert_no_pending_responses()
            self.assertEqual(code, 0, err)
            inbound = load(tmp, "inbound_acxd_flow.json")
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(agentic["Type"], "ConnectAgenticCX")
        self.assertEqual(agentic["Transitions"]["NextAction"], "d1a9e0c0-0000-4000-8000-000000000005")
        am = inbound["Metadata"]["ActionMetadata"][config.AGENTIC_CX_ACTION_ID]
        self.assertEqual(am["parameters"], {"App": "shown"})
        self.assertEqual(am["position"], {"x": 880, "y": 40})

    def _generated_create(self, env, extra, urlopen):
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_reads(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(**env), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", urlopen):
            with self.assertLogs("deploy", "INFO") as logs:
                code, _, err = run_main(connect_args(tmp, "--acxd-workspace-id", WS, *extra))
            stub.assert_no_pending_responses()
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertEqual(st["connect"]["agenticCxActionSource"], "generated")
        creates = [p for name, p in captured if name == "CreateContactFlow"]
        inbound = json.loads(creates[1]["Content"])
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        return agentic, logs.output

    def test_generated_action_explicit(self):
        uo = mock.Mock(side_effect=AssertionError("network"))
        agentic, _ = self._generated_create({}, ["--acxd-application-id", "APP1",
                                                 "--acxd-deployment-alias", "AL1"], uo)
        uo.assert_not_called()
        self.assertEqual(agentic["Type"], "ConnectParticipantWithAgenticCX")
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": "APP1", "Alias": "AL1"})

    def test_generated_action_lookup(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.deps = [{"deploymentId": "D1", "environment": "production", "deploymentAlias": "AL1"}]
        agentic, logs = self._generated_create({"ACXD_API_KEY": KEY}, [], fake)
        self.assertEqual(fake.calls, [("GET", "/sdk/applications"), ("GET", "/sdk/applications/APP1/deployments")])
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": "APP1", "Alias": "AL1"})
        self.assertTrue(any("deploymentAlias source: ACXD" in m for m in logs))
        self.assertFalse(any("SECRETVALUE" in m for m in logs))

    def test_generated_action_app_name_from_state(self):
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_reads(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError("network"))):
            Path(tmp, "deploy_state.json").write_text(json.dumps(
                {"schemaVersion": 1, "dryRun": False,
                 "acxd": {"applicationId": "APPX", "deploymentAlias": "ALX", "applicationName": "workshop1014"}}))
            code, _, err = run_main(connect_args(tmp, "--acxd-workspace-id", WS))
            stub.assert_no_pending_responses()
        self.assertEqual(code, 0, err)
        inbound = json.loads([p for name, p in captured if name == "CreateContactFlow"][1]["Content"])
        am = inbound["Metadata"]["ActionMetadata"][config.AGENTIC_CX_ACTION_ID]
        self.assertEqual(am["parameters"]["AgentConfiguration"]["ApplicationId"], {"displayName": "workshop1014"})

    def test_invalid_contact_flow(self):
        client, stub = connect_client()
        stub_reads(stub)
        stub.add_client_error("create_contact_flow", "InvalidContactFlowException", "bad", 400,
                              modeled_fields={"problems": [{"message": "PROBLEM-ONE"}]})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            code, _, err = run_main(connect_args(tmp, "--agentic-cx-action-file", self._action_file(tmp)))
        self.assertEqual(code, 1)
        self.assertIn("PROBLEM-ONE", err)


# ---------------------------------------------------------------------------
# Preflight (read-only)
# ---------------------------------------------------------------------------

def preflight_args(tmp, *extra):
    return ["--preflight", "--region", "us-east-1", "--instance-id", INSTANCE, "--queue-arn", QARN,
            "--acxd-workspace-id", WS, "--output-dir", tmp, *extra]


class PreflightTests(unittest.TestCase):
    def test_connect_read_only_guard(self):
        client, stub = connect_client()
        deploy.install_connect_read_only_guard(client)
        stub.add_response("describe_instance", {"Instance": {"Id": INSTANCE}}, {"InstanceId": INSTANCE})
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        with stub:
            self.assertEqual(client.describe_instance(InstanceId=INSTANCE)["Instance"]["Id"], INSTANCE)
            with self.assertRaisesRegex(RuntimeError, "preflight is read-only: CreateContactFlow"):
                client.create_contact_flow(InstanceId=INSTANCE, Name="n", Type="CONTACT_FLOW", Content="{}")
        # Without a Stubber the guard raises before any request is sent.
        bare, _ = connect_client()
        deploy.install_connect_read_only_guard(bare)
        with mock.patch("botocore.endpoint.Endpoint.make_request", side_effect=AssertionError("network")) as mr:
            for call in (lambda: bare.update_contact_flow_content(InstanceId=INSTANCE, ContactFlowId="x",
                                                                   Content="{}"),
                         lambda: bare.associate_phone_number_contact_flow(
                             PhoneNumberId="p", InstanceId=INSTANCE, ContactFlowId="x")):
                with self.assertRaisesRegex(RuntimeError, "preflight is read-only"):
                    call()
        mr.assert_not_called()
        # The guard is per client (clients of one session have separate event emitters).
        session = botocore.session.get_session()
        guarded, other = (session.create_client("connect", region_name="us-east-1", aws_access_key_id="x",
                                                aws_secret_access_key="y") for _ in range(2))
        deploy.install_connect_read_only_guard(guarded)
        other_stub = Stubber(other)
        other_stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        with other_stub:
            other.create_contact_flow(InstanceId=INSTANCE, Name="n", Type="CONTACT_FLOW", Content="{}")

    def _run(self, fake, tmp, flows, *extra, key=KEY):
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_instance_queue(stub, flows)
        with clean_env(ACXD_API_KEY=key), stub, mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", fake), mock.patch("deploy.time.sleep"):
            with self.assertLogs(level="DEBUG") as logs:
                # --verbose: DEBUG logs include ACXD response bodies, all checked for the secret below.
                code, out, err = run_main(preflight_args(tmp, "--verbose", *extra))
            stub.assert_no_pending_responses()
        self.assertEqual([name for name, _ in captured],
                         ["DescribeInstance", "DescribeQueue", "ListContactFlows"]
                         + ["DescribeContactFlow"] * len(flows or []))
        self.assertTrue(fake.calls)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"}, fake.calls)
        self.assertFalse(Path(tmp, "deploy_state.json").exists())
        self.assertFalse(Path(tmp, "dry-run").exists())
        self.assertFalse(Path(tmp, "preflight", "deploy_state.json").exists())
        for p in Path(tmp).rglob("*"):
            if p.is_file():
                self.assertNotIn("SECRETVALUE", p.read_text(encoding="utf-8"), p)
        self.assertFalse(any("SECRETVALUE" in m for m in logs.output))
        self.assertNotIn("SECRETVALUE", out + err)
        return code, out, logs.output

    def test_foreign_same_name_application(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = {"path": "", "tags": []}
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), \
                mock.patch("urllib.request.urlopen", fake):
            code, _, err = run_main(preflight_args(tmp, "--phase", "acxd"))
        self.assertEqual(code, 2)
        self.assertIn("不是本脚本创建的", err)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})
        # With the override flag the preflight succeeds and records a warning.
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = {"path": "", "tags": []}
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), \
                mock.patch("urllib.request.urlopen", fake):
            code, _, err = run_main(preflight_args(tmp, "--phase", "acxd", "--overwrite-untagged-application"))
            self.assertEqual(code, 0, err)
            report = load(tmp, "preflight", "preflight_report.json")
        self.assertTrue(any("--overwrite-untagged-resources" in w for w in report["warnings"]))
        self.assertIn(("acxd", "UpdateApplication"),
                      [(w["service"], w["operation"]) for w in report["plannedWrites"]])

    def test_new_application_path(self):  # (a), (d)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, out, _ = self._run(fake, tmp, None, "--phase", "all")
            self.assertEqual(code, 0)
            report = load(tmp, "preflight", "preflight_report.json")
            inbound = load(tmp, "preflight", "inbound_acxd_flow.json")
            files = {p.name for p in Path(tmp, "preflight").iterdir()}
        self.assertEqual(files, (DRY_FILES - {"deploy_state.json"}) | {"preflight_report.json"})
        self.assertEqual(fake.calls, [("GET", "/sdk/context-variables"), ("GET", "/sdk/flows"),
                                      ("GET", "/sdk/applications")])
        self.assertEqual((report["schemaVersion"], report["preflight"], report["region"]), (1, True, "us-east-1"))
        self.assertEqual([(w["service"], w["operation"]) for w in report["plannedWrites"]], [
            ("acxd", "CreateContextVariable"), ("acxd", "CreateContextVariable"), ("acxd", "CreateContextVariable"),
            ("acxd", "CreateFlow"), ("acxd", "CreateApplication"), ("acxd", "CreateApplicationBuild"),
            ("acxd", "CreateApplicationDeployment"), ("connect", "CreateContactFlow"),
            ("connect", "CreateContactFlow")])
        self.assertEqual([w["target"] for w in report["plannedWrites"]][:5],
                         list(config.FIELDS) + ["AgenticCareIntake", "AgenticCareSelfService"])
        self.assertEqual([w["target"] for w in report["plannedWrites"]][-2:],
                         ["AgenticCare ScreenPop Flow", "AgenticCare Inbound ACXD Flow"])
        r = report["resolved"]
        self.assertEqual((r["instanceId"], r["queueArn"], r["workspaceId"], r["applicationId"],
                          r["applicationName"], r["deploymentAlias"], r["screenpopFlowId"], r["inboundFlowId"],
                          r["agenticCxActionSource"]),
                         (INSTANCE, QARN, WS, None, "AgenticCareSelfService", None, None, None, "generated"))
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": deploy.PREFLIGHT_APPLICATION_PENDING,
                          "Alias": deploy.PREFLIGHT_ALIAS_PENDING})
        hook = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
        self.assertTrue(hook["Parameters"]["EventHooks"]["DefaultAgentUI"].endswith(
            f"instance/{INSTANCE}/contact-flow/PREFLIGHT-SCREENPOP"))
        self.assertTrue(any(deploy.PREFLIGHT_ALIAS_PENDING in w for w in report["warnings"]))
        self.assertIn("真实运行将执行以下写操作", out)
        self.assertIn("[connect] CreateContactFlow  AgenticCare Inbound ACXD Flow", out)
        # Prefix block with existence marks (reuses the preflight list results, no extra call).
        self.assertIn("资源名前缀: AgenticCare（默认值）", out)
        self.assertIn("  ACXD flowId: AgenticCareIntake  → 将新建", out)
        self.assertIn("  Generative Journey 节点: AgenticCareJourney\n", out)
        self.assertIn("  ACXD 应用名: AgenticCareSelfService  → 将新建", out)
        self.assertIn("  Connect 入站流: AgenticCare Inbound ACXD Flow  → 将新建", out)

    def test_reuse_application(self):  # (b)
        fake = FakeAcxd()
        fake.app_id = REUSE_APP
        fake.app_doc = workshop_app()
        fake.cvars = [{"name": "DeviceName", "schema": {"type": "number"}}]
        fake.deps = [{"deploymentId": "D9", "environment": "production", "deploymentAlias": "TESTALIAS02"}]
        flows = [{"Id": "sp", "Arn": SP_ARN, "Name": "AgenticCare ScreenPop Flow"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self._run(fake, tmp, flows, "--acxd-application-id", REUSE_APP, "--reuse-acxd-application",
                                   "--language", "en-US")
            self.assertEqual(code, 0)
            report = load(tmp, "preflight", "preflight_report.json")
            inbound = load(tmp, "preflight", "inbound_acxd_flow.json")
            sent = load(tmp, "preflight", "acxd_application.json")
            before = load(tmp, "preflight", "acxd_application_before.json")
        self.assertEqual(fake.app_patches, [])
        self.assertEqual([w["operation"] for w in report["plannedWrites"]], [
            "CreateContextVariable", "CreateContextVariable", "CreateFlow", "UpdateApplication",
            "CreateApplicationBuild", "UpdateApplicationDeployment", "UpdateContactFlowContent", "CreateContactFlow"])
        self.assertIn("TESTALIAS02", report["plannedWrites"][5]["target"])
        r = report["resolved"]
        self.assertEqual((r["applicationId"], r["applicationName"], r["deploymentAlias"], r["screenpopFlowId"]),
                         (REUSE_APP, "workshop1014", "TESTALIAS02", "sp"))
        self.assertEqual(before, workshop_app())
        self.assertEqual(sent, deploy.build_reuse_application_patch(workshop_app(), "AgenticCareIntake"))
        agentic = [a for a in inbound["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": REUSE_APP, "Alias": "TESTALIAS02"})
        am = inbound["Metadata"]["ActionMetadata"][config.AGENTIC_CX_ACTION_ID]
        self.assertEqual(am["parameters"]["AgentConfiguration"]["ApplicationId"], {"displayName": "workshop1014"})
        hook = [a for a in inbound["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
        self.assertEqual(hook["Parameters"]["EventHooks"]["DefaultAgentUI"], SP_ARN)
        self.assertTrue(any("schema.type=number" in w for w in report["warnings"]))
        self.assertTrue(any("Maintplink -> AgenticCareIntake" in w for w in report["warnings"]))
        self.assertFalse(any("PENDING" in w for w in report["warnings"]))

    def test_reuse_language_mismatch(self):
        fake = FakeAcxd()
        fake.app_id = REUSE_APP
        fake.app_doc = workshop_app()
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), \
                mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.boto3.client", mock.Mock(side_effect=AssertionError("boto3"))):
            code, _, err = run_main(preflight_args(tmp, "--acxd-application-id", REUSE_APP,
                                                   "--reuse-acxd-application"))
            self.assertFalse(Path(tmp, "preflight", "preflight_report.json").exists())
        self.assertEqual(code, 2)
        self.assertIn("--language en-US", err)
        self.assertEqual(fake.calls, [("GET", f"/sdk/applications/{REUSE_APP}")])

    def test_config_errors(self):  # (c)
        cases = [
            (preflight_args("x", "--dry-run"), {"ACXD_API_KEY": KEY}, "--dry-run"),
            (preflight_args("x"), {}, "ACXD_API_KEY"),
            (["--preflight", "--phase", "connect", "--region", "us-east-1", "--output-dir", "x"], {}, "--instance-id"),
        ]
        for argv, env, needle in cases:
            with clean_env(**env), mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError)) as uo, \
                    mock.patch("deploy.boto3.client", mock.Mock(side_effect=AssertionError("boto3"))), \
                    mock.patch("sys.stdin.isatty", return_value=False):
                code, _, err = run_main(argv)
            self.assertEqual(code, 2, argv)
            self.assertIn(needle, err, argv)
            uo.assert_not_called()

    def test_connect_phase_unresolvable_action(self):
        client, stub = connect_client()
        stub_instance_queue(stub)
        stub_list(stub)  # existing-flow lookup of resolve_agentic_cx_action
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError)) as uo:
            code, out, _ = run_main(["--preflight", "--phase", "connect", "--region", "us-east-1",
                                     "--instance-id", INSTANCE, "--queue-arn", QARN, "--output-dir", tmp])
            stub.assert_no_pending_responses()
            self.assertFalse(Path(tmp, "deploy_state.json").exists())
        self.assertEqual(code, 2)
        self.assertIn("获取方法", out)
        uo.assert_not_called()

    def test_connect_phase_generated(self):
        client, stub = connect_client()
        stub_instance_queue(stub)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError)) as uo:
            code, _, err = run_main(["--preflight", "--phase", "connect", "--region", "us-east-1",
                                     "--instance-id", INSTANCE, "--queue-arn", QARN, "--output-dir", tmp,
                                     "--acxd-workspace-id", WS, "--acxd-application-id", "APP1",
                                     "--acxd-deployment-alias", "AL1"])
            stub.assert_no_pending_responses()
            self.assertEqual(code, 0, err)
            report = load(tmp, "preflight", "preflight_report.json")
        uo.assert_not_called()
        self.assertEqual([w["operation"] for w in report["plannedWrites"]], ["CreateContactFlow"] * 2)
        self.assertEqual((report["resolved"]["applicationId"], report["resolved"]["deploymentAlias"]),
                         ("APP1", "AL1"))


# ---------------------------------------------------------------------------
# Two inputs: --instance-arn + ACXD API key
# ---------------------------------------------------------------------------

def parse(*argv):
    return deploy.validate_config(deploy.build_parser().parse_args(list(argv)))


def stub_describe_instance(stub, extra=None):
    stub.add_response("describe_instance", {"Instance": {"Id": INSTANCE, "Arn": TARN, **(extra or {})}},
                      {"InstanceId": INSTANCE})


def stub_list_queues(stub, queues):
    summaries = [{"Id": arn.rsplit("/", 1)[1], "Arn": arn, "Name": name, "QueueType": "STANDARD"}
                 for name, arn in queues]
    stub.add_response("list_queues", {"QueueSummaryList": summaries},
                      {"InstanceId": INSTANCE, "QueueTypes": ["STANDARD"]})


DEFAULT_QUEUES = [("Other", OTHER_Q_ARN), ("BasicQueue", BQ_ARN)]
EXISTING_FLOWS = [{"Id": "sp", "Arn": SP_ARN, "Name": "AgenticCare ScreenPop Flow"},
                  {"Id": "in", "Arn": IN_ARN, "Name": "AgenticCare Inbound ACXD Flow"}]


def stub_create_path(stub, queues=DEFAULT_QUEUES, describe_extra=None, confirm_list=False):
    """confirm_list is kept for readability only: the confirmation reuses the precheck's ListContactFlows."""
    stub_describe_instance(stub, describe_extra)
    stub_list_queues(stub, queues)
    stub_precheck(stub)
    stub_list(stub)
    stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
    stub_list(stub)
    stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})


def stub_update_path(stub):
    stub_describe_instance(stub)
    stub_list_queues(stub, DEFAULT_QUEUES)
    stub_precheck(stub, EXISTING_FLOWS)
    stub_list(stub, EXISTING_FLOWS)
    stub.add_response("update_contact_flow_content", {}, {"InstanceId": INSTANCE, "ContactFlowId": "sp",
                                                          "Content": mock.ANY})
    stub_list(stub, EXISTING_FLOWS)
    stub.add_response("update_contact_flow_content", {}, {"InstanceId": INSTANCE, "ContactFlowId": "in",
                                                          "Content": mock.ANY})


def run_two_input(fake, tmp, stub_fn, *extra, env=None, pending_ok=False):
    """deploy.py --instance-arn <TARN> with FakeAcxd and a stubbed Connect client."""
    client, stub = connect_client()
    captured = []
    capture_params(client, captured)
    stub_fn(stub)
    with clean_env(**({"ACXD_API_KEY": KEY} if env is None else env)), stub, \
            mock.patch("deploy.boto3.client", return_value=client), \
            mock.patch("urllib.request.urlopen", fake), mock.patch("deploy.time.sleep"):
        code, out, err = run_main(["--instance-arn", TARN, "--output-dir", tmp, "--poll-interval", "1", *extra])
        if not pending_ok:
            stub.assert_no_pending_responses()
    return code, out, err, captured


def inbound_from(captured, op="CreateContactFlow"):
    return json.loads([p for name, p in captured if name == op][1]["Content"])


def action_of(flow, typ):
    return [a for a in flow["Actions"] if a["Type"] == typ][0]


class InstanceArnTests(unittest.TestCase):
    def test_parse_real_arn(self):
        ref = deploy.parse_instance_arn("  " + REAL_ARN + "\n")
        self.assertEqual((ref.partition, ref.region, ref.account_id, ref.instance_id),
                         ("aws", "us-west-2", "123456789012", "00000000-0000-4000-8000-000000000001"))
        self.assertEqual(ref.arn, REAL_ARN)

    def test_parse_invalid(self):
        cases = [
            "", "arn:aws:connect:us-west-2:123456789012:00000000-0000-4000-8000-000000000001",
            BQ_ARN, "arn:aws:s3:::x", "arn:aws:connect:us-west:123456789012:instance/" + INSTANCE,
            "arn:aws:connect:us-west-2:123456789012:instance/not-a-uuid-not-a-uuid-not-a-uuid-xxxx",
            "arn:aws:connect:us-west-2:12345678901:instance/" + INSTANCE,
        ]
        for value in cases:
            with self.assertRaises(deploy.ConfigError, msg=value) as cm:
                deploy.parse_instance_arn(value)
            self.assertIn("--instance-arn", str(cm.exception), value)
        with self.assertRaises(deploy.ConfigError) as cm:
            deploy.parse_instance_arn(BQ_ARN)
        self.assertIn("队列", str(cm.exception))
        with self.assertRaises(deploy.ConfigError) as cm:
            deploy.parse_instance_arn(SP_ARN)
        self.assertIn("流 ARN", str(cm.exception))

    def test_cli_conflicts_and_precedence(self):
        for extra, needle in ((["--region", "us-west-2"], "--region"),
                              (["--instance-id", "21111111-2222-4333-8444-555555555555"], "--instance-id"),
                              (["--instance-arn", "arn:bad"], "--instance-arn")):
            with clean_env():
                code, _, err = run_main(["--dry-run", "--instance-arn", TARN, *extra])
            self.assertEqual(code, 2, extra)
            self.assertIn(needle, err, extra)
        with clean_env(AWS_REGION="eu-west-2", CONNECT_INSTANCE_ID="21111111-2222-4333-8444-555555555555"):
            cfg = parse("--dry-run", "--instance-arn", REAL_ARN)
        self.assertEqual((cfg.region, cfg.instance_id, cfg.account_id, cfg.instance_arn),
                         ("us-west-2", "00000000-0000-4000-8000-000000000001", "123456789012", REAL_ARN))
        self.assertEqual(cfg.acxd_endpoint, "https://api.acxd.connect.us-west-2.amazonaws.com")
        with clean_env(CONNECT_INSTANCE_ARN=TARN):
            cfg = parse("--dry-run", "--region", "us-east-1", "--instance-id", INSTANCE)
        self.assertEqual((cfg.region, cfg.instance_id), ("us-east-1", INSTANCE))
        with clean_env():
            code, _, err = run_main(["--dry-run", "--phase", "connect", "--region", "us-east-1"])
        self.assertEqual(code, 2)
        self.assertIn("--instance-arn", err)
        with clean_env():
            code, _, err = run_main(["--dry-run", "--instance-arn", TARN, "--queue-arn", QARN, "--queue-name", "Q"])
        self.assertEqual(code, 2)
        self.assertIn("最多提供一个", err)

    def test_two_input_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            urlopen = mock.Mock(side_effect=AssertionError("network"))
            boto = mock.Mock(side_effect=AssertionError("boto3"))
            with mock.patch("urllib.request.urlopen", urlopen), mock.patch("deploy.boto3.client", boto):
                code, out, err = run_main(["--dry-run", "--instance-arn", REAL_ARN, "--output-dir", tmp])
            self.assertEqual(code, 0, err)
            urlopen.assert_not_called()
            boto.assert_not_called()
            dry = Path(tmp) / "dry-run"
            self.assertEqual({p.name for p in dry.iterdir()}, DRY_FILES)
            inbound = load(tmp, "dry-run", "inbound_acxd_flow.json")
            st = load(tmp, "dry-run", "deploy_state.json")
            checker = __import__("subprocess").run(
                [sys.executable, os.path.join(ROOT, "tools", "check_flows.py"), str(dry)],
                capture_output=True, text=True)
        self.assertEqual((checker.returncode, checker.stdout.strip()), (0, "OK"), checker.stdout)
        agentic = action_of(inbound, config.AGENTIC_CX_ACTION_TYPE)
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"]["WorkspaceId"], deploy.DRYRUN_WORKSPACE_ID)
        queue = action_of(inbound, "UpdateContactTargetQueue")["Parameters"]["QueueId"]
        self.assertTrue(queue.startswith(
            "arn:aws:connect:us-west-2:123456789012:instance/00000000-0000-4000-8000-000000000001/queue/"), queue)
        self.assertEqual(st["region"], "us-west-2")
        self.assertEqual(st["connect"]["instanceArn"], REAL_ARN)
        self.assertEqual(st["connect"]["queueName"], "BasicQueue")
        self.assertIn("DRYRUN-WORKSPACE-ID", err)


class ApiKeyLoadingTests(unittest.TestCase):
    def _key_file(self, tmp, text, mode=0o600):
        p = Path(tmp, "key")
        p.write_text(text, encoding="utf-8")
        os.chmod(p, mode)
        return str(p)

    def _cfg_logs(self, env, *extra):
        with clean_env(**env), mock.patch("sys.stdin.isatty", return_value=False):
            with self.assertLogs("deploy", "INFO") as logs:
                cfg = parse("--phase", "acxd", "--region", "us-east-1", *extra)
        self.assertFalse(any("SECRETVALUE" in m or config.API_KEY_PREFIX in m for m in logs.output))
        return cfg, logs.output

    def test_env_beats_file(self):
        cfg, logs = self._cfg_logs({"ACXD_API_KEY": KEY}, "--acxd-api-key-file", "/nonexistent/key")
        self.assertEqual((cfg.api_key, cfg.api_key_source), (KEY, "env"))
        self.assertTrue(any("ignoring --acxd-api-key-file" in m for m in logs))

    def test_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, logs = self._cfg_logs({}, "--acxd-api-key-file", self._key_file(tmp, KEY + "\n"))
            self.assertEqual((cfg.api_key, cfg.api_key_source), (KEY, "file"))
            self.assertFalse(any("chmod 600" in m for m in logs))
            self.assertTrue(any("ACXD API key source: file" in m for m in logs))
            _, logs = self._cfg_logs({}, "--acxd-api-key-file", self._key_file(tmp, KEY, 0o644))
            self.assertTrue(any("chmod 600" in m for m in logs))

    def test_file_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            for path in ("/nonexistent/key", self._key_file(tmp, "  \n"), self._key_file(tmp, "a\nb\n")):
                with clean_env(), mock.patch("urllib.request.urlopen", mock.Mock(side_effect=AssertionError)):
                    code, out, err = run_main(acxd_args(tmp, "--acxd-api-key-file", path))
                self.assertEqual(code, 2, path)
                self.assertIn("--acxd-api-key-file", err)
                self.assertNotIn("SECRETVALUE", out + err)

    def test_cli_key_warns(self):
        cfg, logs = self._cfg_logs({}, "--acxd-api-key", KEY)
        self.assertEqual((cfg.api_key, cfg.api_key_source), (KEY, "cli"))
        self.assertTrue(any("shell" in m for m in logs))
        with tempfile.TemporaryDirectory() as tmp:
            cfg, logs = self._cfg_logs({}, "--acxd-api-key", "other.value",
                                       "--acxd-api-key-file", self._key_file(tmp, KEY))
        self.assertEqual(cfg.api_key_source, "file")
        self.assertTrue(any("ignoring --acxd-api-key" in m for m in logs))

    def test_prompt_and_missing(self):
        with clean_env(), mock.patch("sys.stdin.isatty", return_value=True), \
                mock.patch("deploy.getpass.getpass", return_value=KEY) as gp:
            cfg = parse("--phase", "acxd", "--region", "us-east-1")
        gp.assert_called_once()
        self.assertEqual((cfg.api_key, cfg.api_key_source), (KEY, "prompt"))
        with clean_env(), mock.patch("sys.stdin.isatty", return_value=False):
            code, out, err = run_main(["--preflight", "--instance-arn", TARN])
        self.assertEqual(code, 2)
        self.assertIn("ACXD_API_KEY", err)
        # --phase connect alone and --dry-run never prompt
        for argv in (["--phase", "connect", "--instance-arn", TARN], ["--dry-run", "--instance-arn", TARN]):
            with clean_env(), mock.patch("sys.stdin.isatty", return_value=True), \
                    mock.patch("deploy.getpass.getpass") as gp:
                cfg = parse(*argv)
            gp.assert_not_called()
            self.assertIsNone(cfg.api_key)

    def test_mask_has_no_prefix(self):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("sys.stdin.isatty", return_value=False):
            with self.assertLogs("deploy", "INFO") as logs:
                parse("--phase", "acxd", "--region", "us-east-1")
        line = [m for m in logs.output if "API key source" in m][0]
        self.assertIn("****test.****", line)
        self.assertNotIn(config.API_KEY_PREFIX, line)


class WorkspaceDiscoveryTests(unittest.TestCase):
    def run_acxd(self, fake, tmp, *extra):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.time.sleep"):
            return run_main(["--phase", "acxd", "--region", "us-east-1", "--output-dir", tmp,
                             "--poll-interval", "1", *extra])

    def test_single_workspace(self):  # (a)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertEqual(fake.calls[0], ("GET", "/sdk/workspaces"))
        self.assertIsNone(fake.ws_headers[0])
        self.assertEqual(set(fake.ws_headers[1:]), {WS})
        a = st["acxd"]
        self.assertEqual((a["workspaceId"], a["workspaceName"], a["workspaceSource"]), (WS, "ws1", "ListWorkspaces"))
        self.assertIn(f"ws1 ({WS})", out)

    def _assert_no_writes(self, fake):
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})

    def test_zero_workspaces(self):  # (b)
        fake = FakeAcxd()
        fake.workspaces = []
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertIn("--acxd-workspace-id", err)
        self._assert_no_writes(fake)

    def test_multiple_workspaces(self):  # (c), (d)
        fake = FakeAcxd()
        fake.workspaces = [{"workspaceId": WS, "name": "ws1"}, {"workspaceId": WS2, "name": "ws2"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertIn(WS, err)
        self.assertIn(WS2, err)
        self.assertIn("--acxd-workspace-id", err)
        self._assert_no_writes(fake)

        fake = FakeAcxd()
        fake.workspaces = [{"workspaceId": WS, "name": "ws1"}, {"workspaceId": WS2, "name": "ws2"}]
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "deploy_state.json").write_text(json.dumps(
                {"schemaVersion": 1, "dryRun": False, "region": "us-east-1", "acxd": {"workspaceId": WS2}}))
            code, _, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertEqual(set(fake.ws_headers[1:]), {WS2})
        self.assertEqual((st["acxd"]["workspaceId"], st["acxd"]["workspaceSource"]), (WS2, "deploy_state.json"))

    def test_forbidden(self):  # (e)
        fake = FakeAcxd()
        fake.workspaces_error = http_error(403, "AccessDeniedException")
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err = self.run_acxd(fake, tmp)
        self.assertEqual(code, 2)
        self.assertIn("--acxd-workspace-id", err)
        self.assertNotIn("SECRETVALUE", out + err)
        self.assertEqual(fake.calls, [("GET", "/sdk/workspaces")])

    def test_connect_phase_discovery_failure_is_warning(self):  # (g)
        client, stub = connect_client()
        stub_reads(stub)
        with tempfile.TemporaryDirectory() as tmp, clean_env(ACXD_API_KEY=KEY), stub, \
                mock.patch("deploy.boto3.client", return_value=client), mock.patch("deploy.time.sleep"), \
                mock.patch("urllib.request.urlopen", mock.Mock(side_effect=urllib.error.URLError("down"))) as uo:
            with self.assertLogs("deploy", "WARNING") as logs:
                code, _, _ = run_main(connect_args(tmp))
        self.assertEqual(code, 2)
        self.assertTrue(uo.called)
        self.assertTrue(any("workspace discovery failed" in m for m in logs.output))

    def test_state_for_other_instance_ignored(self):  # (h)
        client, stub = connect_client()
        stub_reads(stub)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            Path(tmp, "deploy_state.json").write_text(json.dumps(
                {"schemaVersion": 1, "dryRun": False, "region": "us-east-1",
                 "connect": {"instanceId": "21111111-2222-4333-8444-555555555555"},
                 "acxd": {"applicationId": "APPX"}}))
            with self.assertLogs("deploy", "INFO") as logs:
                code, _, _ = run_main(connect_args(tmp))
        self.assertEqual(code, 2)
        self.assertTrue(any("ignoring deploy_state.json" in m for m in logs.output))
        self.assertFalse(any("applicationId source: deploy_state.json" in m for m in logs.output))

    def test_filter_state(self):
        with clean_env():
            cfg = parse(*dry_args("x"))
        base = {"region": "us-east-1", "connect": {"instanceId": INSTANCE}, "acxd": {"workspaceId": WS2}}
        self.assertIs(deploy.filter_state(base, cfg, check_workspace=False), base)
        self.assertIsNone(deploy.filter_state(base, cfg, check_workspace=True))
        self.assertIsNone(deploy.filter_state({**base, "region": "us-west-2"}, cfg, check_workspace=False))


class QueueDefaultTests(unittest.TestCase):
    def _resolve(self, queues, *extra):
        with clean_env():
            cfg = parse("--dry-run", "--instance-arn", TARN, *extra)
        client, stub = connect_client()
        stub_list_queues(stub, queues)
        with stub:
            return deploy.resolve_queue(client, cfg)

    def test_basic_queue_default(self):  # (a)
        self.assertEqual(self._resolve(DEFAULT_QUEUES), (BQ_ARN, "BasicQueue", "default:BasicQueue"))

    def test_first_standard_queue(self):  # (b)
        with self.assertLogs("deploy", "WARNING") as logs:
            got = self._resolve([("Zeta", BQ_ARN), ("Alpha", OTHER_Q_ARN)])
        self.assertEqual(got, (OTHER_Q_ARN, "Alpha", "default:first-standard"))
        self.assertTrue(any("first STANDARD queue" in m for m in logs.output))

    def test_queue_name_override(self):
        self.assertEqual(self._resolve(DEFAULT_QUEUES, "--queue-name", "Other"),
                         (OTHER_Q_ARN, "Other", "--queue-name"))

    def test_no_standard_queue(self):  # (c)
        with self.assertRaises(deploy.ConfigError):
            self._resolve([])
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, captured = run_two_input(fake, tmp, lambda s: (stub_describe_instance(s),
                                                                         stub_list_queues(s, [])))
        self.assertEqual(code, 2)
        self.assertIn("--queue-arn", err)
        self.assertEqual([n for n, _ in captured], ["DescribeInstance", "ListQueues"])
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})


class TwoInputRunTests(unittest.TestCase):
    def test_full_run(self):  # (d)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err, captured = run_two_input(fake, tmp, stub_create_path)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertEqual([n for n, _ in captured], FULL_RUN_CALLS)
        inbound = inbound_from(captured)
        agentic = action_of(inbound, config.AGENTIC_CX_ACTION_TYPE)
        self.assertEqual(agentic["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": WS, "ApplicationId": "APP1", "Alias": "ALIAS1"})
        am = inbound["Metadata"]["ActionMetadata"][config.AGENTIC_CX_ACTION_ID]
        self.assertEqual(am["parameters"]["AgentConfiguration"]["WorkspaceId"]["displayName"], "ws1")
        self.assertEqual(action_of(inbound, "UpdateContactTargetQueue")["Parameters"]["QueueId"], BQ_ARN)
        self.assertEqual(fake.calls[0], ("GET", "/sdk/workspaces"))
        self.assertEqual((st["connect"]["queueArn"], st["connect"]["queueName"], st["connect"]["instanceArn"]),
                         (BQ_ARN, "BasicQueue", TARN))
        self.assertEqual(st["acxd"]["language"], "zh-CN")
        self.assertEqual(st["completedSteps"], list(deploy.STEP_NAMES))
        self.assertIn("associate-phone-number-contact-flow", out)
        self.assertNotIn("SECRETVALUE", out + err)

    def test_foreign_contact_flow_stops_before_any_write(self):
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, captured = run_two_input(
                fake, tmp, lambda s: (stub_describe_instance(s), stub_list_queues(s, DEFAULT_QUEUES),
                                      stub_precheck(s, EXISTING_FLOWS[:1], tags={"team": "x"})))
            self.assertFalse(Path(tmp, "deploy_state.json").exists())
        self.assertEqual(code, 2)
        self.assertIn("Connect 流 AgenticCare ScreenPop Flow", err)
        self.assertIn("--overwrite-untagged-resources", err)
        self.assertEqual([n for n, _ in captured],
                         ["DescribeInstance", "ListQueues", "ListContactFlows", "DescribeContactFlow"])
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})  # no ACXD write either

    def test_foreign_contact_flow_with_override(self):
        fake = FakeAcxd()

        def stubs(s):
            stub_describe_instance(s)
            stub_list_queues(s, DEFAULT_QUEUES)
            stub_precheck(s, EXISTING_FLOWS[:1], tags={})
            stub_list(s, EXISTING_FLOWS[:1])
            s.add_response("update_contact_flow_content", {}, {"InstanceId": INSTANCE, "ContactFlowId": "sp",
                                                               "Content": mock.ANY})
            stub_list(s)
            s.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, captured = run_two_input(fake, tmp, stubs, "--overwrite-untagged-resources")
        self.assertEqual(code, 0, err)
        self.assertIn("overwriting it because of --overwrite-untagged-resources", err)
        self.assertIn("UpdateContactFlowContent", [n for n, _ in captured])

    def test_legacy_contact_flow_without_tag_is_owned(self):
        """Flows created before the tag existed are recognised by the StartAction of our template."""
        client, stub = connect_client()
        cfg = mock.Mock(instance_id=INSTANCE, overwrite_untagged=False)
        for template in (flowlib.INBOUND_TEMPLATE, flowlib.SCREENPOP_TEMPLATE):
            start = flowlib.load_template(template)["StartAction"]
            stub.add_response("describe_contact_flow",
                              {"ContactFlow": {"Content": json.dumps({"StartAction": start}), "Tags": {}}},
                              {"InstanceId": INSTANCE, "ContactFlowId": "f1"})
        with stub, self.assertLogs("deploy", "INFO"):
            for _ in range(2):
                self.assertEqual(deploy.check_contact_flow_owned(client, cfg, {"Id": "f1", "Name": "n"}),
                                 deploy.APP_OWNED)

    def test_unpublished_contact_flow_uses_saved_version(self):
        client, stub = connect_client()
        cfg = mock.Mock(instance_id=INSTANCE, overwrite_untagged=False)
        stub.add_client_error("describe_contact_flow", "ContactFlowNotPublishedException", "saved only", 400,
                              expected_params={"InstanceId": INSTANCE, "ContactFlowId": "f1"})
        stub.add_response("describe_contact_flow", {"ContactFlow": {"Tags": dict(config.CONNECT_OWNER_TAGS)}},
                          {"InstanceId": INSTANCE, "ContactFlowId": "f1:$SAVED"})
        with stub, self.assertLogs("deploy", "INFO"):
            self.assertEqual(deploy.check_contact_flow_owned(client, cfg, {"Id": "f1", "Name": "n"}),
                             deploy.APP_OWNED)
            stub.assert_no_pending_responses()

    def test_contact_flow_created_after_precheck_is_checked(self):
        """A same-name flow that appears between the precheck and the write is checked before UPDATE."""
        client, stub = connect_client()
        cfg = mock.Mock(instance_id=INSTANCE, overwrite_untagged=False)
        stub_list(stub, EXISTING_FLOWS[:1])
        stub.add_response("describe_contact_flow", {"ContactFlow": {"Content": "{}", "Tags": {}}},
                          {"InstanceId": INSTANCE, "ContactFlowId": "sp"})
        with stub, self.assertLogs("deploy", "ERROR"), self.assertRaises(deploy.ConfigError):
            deploy.upsert_contact_flow(client, cfg, "AgenticCare ScreenPop Flow", {}, "d", Path("x"))
        stub.assert_no_pending_responses()

    def test_no_connect_write_when_build_fails(self):  # (e)
        fake = FakeAcxd()
        fake.build_statuses = ["FAILED"]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _, captured = run_two_input(fake, tmp, lambda s: (stub_describe_instance(s),
                                                                       stub_list_queues(s, DEFAULT_QUEUES),
                                                                       stub_precheck(s)))
            st = load(tmp, "deploy_state.json")
        self.assertEqual(code, 1)
        self.assertEqual([n for n, _ in captured], ["DescribeInstance", "ListQueues", "ListContactFlows"])
        self.assertIsNone(st["connect"]["screenpopFlowId"])
        self.assertIsNone(st["connect"]["inboundFlowId"])
        self.assertEqual(fake.count("POST", "/sdk/applications/APP1/builds"), 1)

    def test_bad_queue_stops_before_acxd_writes(self):  # (f)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _, captured = run_two_input(
                fake, tmp, lambda s: (stub_describe_instance(s), stub_list_queues(s, DEFAULT_QUEUES)),
                "--queue-name", "Missing")
            self.assertFalse(Path(tmp, "deploy_state.json").exists())
        self.assertEqual(code, 2)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})

    def test_instance_arn_mismatch(self):
        fake = FakeAcxd()
        other = "arn:aws:connect:us-east-1:210987654321:instance/" + INSTANCE
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, _ = run_two_input(fake, tmp, lambda s: stub_describe_instance(s, {"Arn": other}))
        self.assertEqual(code, 2)
        self.assertIn("不一致", err)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})

    def test_missing_alias_guard(self):  # (g)
        fake = FakeAcxd()
        fake.alias_missing = True
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, captured = run_two_input(fake, tmp, lambda s: (stub_describe_instance(s),
                                                                         stub_list_queues(s, DEFAULT_QUEUES),
                                                                         stub_precheck(s)))
        self.assertEqual(code, 1)
        self.assertIn("没有写入任何 Connect 流", err)
        self.assertEqual([n for n, _ in captured], ["DescribeInstance", "ListQueues", "ListContactFlows"])

    def test_two_input_preflight(self):  # (h)
        fake = FakeAcxd()

        def stubs(s):
            stub_describe_instance(s, {"InstanceAccessUrl": "https://inst1.my.connect.aws"})
            stub_list_queues(s, DEFAULT_QUEUES)
            stub_precheck(s)
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err, captured = run_two_input(fake, tmp, stubs, "--preflight", "--verbose")
            self.assertEqual(code, 0, err)
            report = load(tmp, "preflight", "preflight_report.json")
            self.assertFalse(Path(tmp, "deploy_state.json").exists())
            self.assertFalse(Path(tmp, "preflight", "deploy_state.json").exists())
            for p in Path(tmp).rglob("*"):
                if p.is_file():
                    self.assertNotIn("SECRETVALUE", p.read_text(encoding="utf-8"), p)
        self.assertEqual(fake.calls[0], ("GET", "/sdk/workspaces"))
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})
        self.assertEqual([n for n, _ in captured],
                         ["DescribeInstance", "ListQueues", "ListContactFlows"])
        r = report["resolved"]
        self.assertEqual((r["workspaceId"], r["workspaceName"], r["workspaceSource"]), (WS, "ws1", "ListWorkspaces"))
        self.assertEqual((r["queueArn"], r["queueName"], r["queueSource"]), (BQ_ARN, "BasicQueue",
                                                                             "default:BasicQueue"))
        self.assertEqual((r["instanceArn"], r["instanceAccessUrl"], r["language"]),
                         (TARN, "https://inst1.my.connect.aws", "zh-CN"))
        self.assertEqual([w["operation"] for w in report["plannedWrites"]], [
            "CreateContextVariable", "CreateContextVariable", "CreateContextVariable", "CreateFlow",
            "CreateApplication", "CreateApplicationBuild", "CreateApplicationDeployment",
            "CreateContactFlow", "CreateContactFlow"])
        self.assertTrue(any("A11" in w for w in report["warnings"]))
        self.assertIn("预检摘要", out)
        self.assertNotIn("SECRETVALUE", out + err)


class LanguageFallbackTests(unittest.TestCase):
    def test_is_language_failure(self):  # (a)
        echo = {"buildId": "B1", "status": "FAILED", "languageSettings": [{"languageCode": "zh-CN"}]}
        self.assertFalse(deploy.is_language_failure({**echo, "details": "node 13 invalid"}))
        self.assertTrue(deploy.is_language_failure({**echo, "details": "Unsupported language zh-CN"}))
        self.assertTrue(deploy.is_language_failure("Invalid locale"))
        self.assertFalse(deploy.is_language_failure("Missing required field nodes"))

    def run_acxd(self, fake, tmp, *extra):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.time.sleep"):
            return run_main(acxd_args(tmp, "--poll-interval", "1", *extra))

    def _lang_failed_fake(self):
        fake = FakeAcxd()
        fake.build_statuses = ["FAILED"]
        fake.build_details = {"B1": "Language zh-CN is not supported"}
        return fake

    def test_build_fallback(self):  # (b)
        fake = self._lang_failed_fake()
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        flow_writes = [b for m, p, b in fake.bodies if p.startswith("/sdk/flows") and m in ("POST", "PATCH")]
        self.assertEqual([b["mainLanguageCode"] for b in flow_writes], ["zh-CN", "en-US"])
        self.assertEqual(fake.count("PATCH", "/sdk/flows/AgenticCareIntake"), 1)
        self.assertEqual(fake.app_patches[-1]["settings"]["languageCode"], "en-US")
        builds = [b for m, p, b in fake.bodies if m == "POST" and p.endswith("/builds")]
        self.assertEqual([b["languageSettings"][0]["languageCode"] for b in builds], ["zh-CN", "en-US"])
        deps = [b for m, p, b in fake.bodies if m == "POST" and p.endswith("/deployments")]
        self.assertEqual([d["languageCodes"] for d in deps], [["en-US"]])
        a = st["acxd"]
        self.assertEqual((a["language"], a["languageFallbackFrom"], a["buildId"]), ("en-US", "zh-CN", "B2"))
        self.assertEqual(st["completedSteps"], list(deploy.STEP_NAMES[:5]))
        self.assertIn("自动改用 en-US", out)
        self.assertIn("zh-CN → en-US", out)
        self.assertEqual(len(fake.apps), 1)

    def test_explicit_language_no_fallback(self):  # (c)
        fake = self._lang_failed_fake()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp, "--language", "zh-CN")
        self.assertEqual(code, 1)
        self.assertIn("--language en-US", err)
        self.assertEqual(fake.count("POST", "/sdk/applications/APP1/builds"), 1)
        self.assertEqual(fake.count("POST", "/sdk/applications/APP1/deployments"), 0)

    def test_non_language_failure_no_retry(self):  # (d)
        fake = FakeAcxd()
        fake.build_statuses = ["FAILED"]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, _ = self.run_acxd(fake, tmp)
        self.assertEqual(code, 1)
        self.assertEqual(fake.count("POST", "/sdk/applications/APP1/builds"), 1)
        self.assertEqual(fake.count("PATCH", "/sdk/flows/AgenticCareIntake"), 0)

    def test_create_flow_400_fallback(self):  # (e)
        fake = FakeAcxd()
        fake.flow_post_error_once = http_error(400, "ValidationException",
                                               "languageCodes contains unsupported language zh-CN")
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = self.run_acxd(fake, tmp)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        posts = [b for m, p, b in fake.bodies if m == "POST" and p == "/sdk/flows"]
        self.assertEqual([b["mainLanguageCode"] for b in posts], ["zh-CN", "en-US"])
        self.assertEqual(st["acxd"]["language"], "en-US")

    def test_reuse_mode_no_fallback(self):
        with clean_env():
            cfg = parse(*dry_args("x", "--reuse-acxd-application", "--acxd-application-id", REUSE_APP))
        self.assertEqual(deploy._language_attempts(cfg), ["zh-CN"])
        with clean_env():
            cfg = parse(*dry_args("x"))
        self.assertEqual(deploy._language_attempts(cfg), ["zh-CN", "en-US"])

    def test_all_phase_connect_uses_fallback_language(self):  # (f)
        fake = self._lang_failed_fake()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, captured = run_two_input(fake, tmp, stub_create_path)
        self.assertEqual(code, 0, err)
        inbound = inbound_from(captured)
        self.assertEqual(action_of(inbound, "UpdateContactTextToSpeechVoice")["Parameters"]["TextToSpeechVoice"],
                         "Joanna")
        en_msgs = deploy.flowlib.load_messages()["en-US"]
        text = json.dumps(inbound, ensure_ascii=False)
        self.assertIn(en_msgs["connect"]["error_message"], text)
        # Customer Profiles attributes follow the fallback language too.
        self.assertIn(en_msgs["screenpop"]["customer"]["not_found"], text)
        self.assertIn('"$.Customer.FirstName $.Customer.LastName"', text)
        self.assertNotIn("未找到", text)

    def test_connect_phase_language_from_state(self):  # (g)
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_reads(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client):
            Path(tmp, "deploy_state.json").write_text(json.dumps(
                {"schemaVersion": 1, "dryRun": False, "acxd": {"language": "en-US"}}))
            code, _, err = run_main(connect_args(tmp, "--acxd-workspace-id", WS, "--acxd-application-id", "APP1",
                                                 "--acxd-deployment-alias", "AL1"))
            stub.assert_no_pending_responses()
        self.assertEqual(code, 0, err)
        inbound = inbound_from(captured)
        self.assertEqual(action_of(inbound, "UpdateContactTextToSpeechVoice")["Parameters"]["TextToSpeechVoice"],
                         "Joanna")


class IdempotencyAndSummaryTests(unittest.TestCase):
    def test_rerun_updates_without_duplicates(self):  # 7(a)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, _ = run_two_input(fake, tmp, stub_create_path)
            self.assertEqual(code, 0, err)
            first = len(fake.calls)
            code, _, err, captured = run_two_input(fake, tmp, stub_update_path)
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        second = fake.calls[first:]
        posts = [p for m, p in second if m == "POST"]
        self.assertEqual(posts, ["/sdk/applications/APP1/builds"])
        for path in ("/sdk/flows/AgenticCareIntake", "/sdk/applications/APP1", "/sdk/applications/APP1/deployments/D1"):
            self.assertIn(("PATCH", path), second)
        writes = [n for n, _ in captured if not n.startswith(("Describe", "List"))]
        self.assertEqual(writes, ["UpdateContactFlowContent", "UpdateContactFlowContent"])
        self.assertEqual(len(fake.apps), 1)
        self.assertEqual((st["acxd"]["applicationId"], st["acxd"]["buildId"]), ("APP1", "B2"))
        self.assertEqual((st["connect"]["screenpopFlowId"], st["connect"]["inboundFlowId"]), ("sp", "in"))

    def test_summary(self):  # 7(b)
        fake = FakeAcxd()
        extra = {"InstanceAlias": "inst1", "InstanceAccessUrl": "https://inst1.my.connect.aws"}
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err, _ = run_two_input(fake, tmp, lambda s: stub_create_path(s, describe_extra=extra))
        self.assertEqual(code, 0, err)
        for needle in (TARN, "inst1", f"ws1 ({WS})", "APP1", "ALIAS1", "AgenticCareIntake", SP_ARN, IN_ARN,
                       "BasicQueue", BQ_ARN, f"https://inst1.my.connect.aws/acxd/w/{WS}/applications/APP1",
                       "contact-flows/edit?id="):
            self.assertIn(needle, out, needle)
        self.assertNotIn("SECRETVALUE", out + err)
        self.assertNotIn(config.API_KEY_PREFIX, out + err)

    def test_default_never_touches_workshop_app(self):  # 7(c)
        fake = FakeAcxd()
        fake.apps = [{"applicationId": REUSE_APP, "name": "workshop1014"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err, _ = run_two_input(fake, tmp, stub_create_path)
        self.assertEqual(code, 0, err)
        self.assertFalse([p for _, p in fake.calls if REUSE_APP in p])
        self.assertEqual(fake.count("POST", "/sdk/applications"), 1)

    def test_application_id_without_reuse_warns(self):
        with clean_env(), mock.patch("sys.stdin.isatty", return_value=False):
            with self.assertLogs("deploy", "WARNING") as logs:
                parse(*dry_args("x", "--acxd-application-id", REUSE_APP))
        self.assertTrue(any("ignores it" in m for m in logs.output))


# ---------------------------------------------------------------------------
# Interactive mode
# ---------------------------------------------------------------------------

import prompts  # noqa: E402

TARN_A = "arn:aws:connect:us-east-1:123456789012:instance/21111111-2222-4333-8444-555555555555"
TARN_FAILED = "arn:aws:connect:us-east-1:123456789012:instance/31111111-2222-4333-8444-555555555555"
TARN_WEST = f"arn:aws:connect:us-west-2:123456789012:instance/{INSTANCE}"


def _fake_input(answers, seen):
    """input() replacement: echoes the prompt to stdout like the real one, answers from the list."""
    def fn(prompt=""):
        seen.append(prompt)
        sys.stdout.write(prompt + "\n")
        if not answers:
            raise AssertionError(f"unexpected prompt {prompt!r}")
        answer = answers.pop(0)
        if isinstance(answer, BaseException) or (isinstance(answer, type) and issubclass(answer, BaseException)):
            raise answer
        return answer
    return fn


@contextlib.contextmanager
def interactive(test, answers, profile_region="us-east-1"):
    """Interactive main(): stdin/stdout count as a terminal, input() answers from the list."""
    answers, seen = list(answers), []
    with mock.patch("deploy._stdio_is_tty", return_value=True), \
            mock.patch("builtins.input", side_effect=_fake_input(answers, seen)) as inp, \
            mock.patch("deploy._profile_region", return_value=profile_region):
        yield seen, inp
    test.assertEqual(answers, [], "not every answer was used")


def prompter_for(answers):
    """Prompter for direct collect_missing_args calls; returns (prompter, out, seen)."""
    answers, seen, out = list(answers), [], io.StringIO()
    return prompts.Prompter(input_fn=_fake_input(answers, seen), out=out), out, seen, answers


def collect(test, argv, answers, profile_region="us-east-1"):
    args = deploy.build_parser().parse_args(argv)
    p, out, seen, left = prompter_for(answers)
    with mock.patch("deploy._profile_region", return_value=profile_region), \
            contextlib.redirect_stdout(io.StringIO()):
        deploy.collect_missing_args(args, p)
    test.assertEqual(left, [], "not every answer was used")
    return args, out.getvalue() + "".join(seen), seen


def stub_list_instances(stub, instances):
    stub.add_response("list_instances", {"InstanceSummaryList": [
        {"Id": arn.rsplit("/", 1)[1], "Arn": arn, "InstanceAlias": alias, "InstanceStatus": status}
        for alias, arn, status in instances]}, {})


class InteractiveModeTests(unittest.TestCase):
    def test_is_interactive(self):  # 3(a)
        args = deploy.build_parser().parse_args(["--non-interactive"])
        with mock.patch("deploy._stdio_is_tty", return_value=True):
            self.assertFalse(deploy.is_interactive(args))
        args = deploy.build_parser().parse_args([])
        for tty in (False, True):
            with mock.patch("deploy._stdio_is_tty", return_value=tty):
                self.assertIs(deploy.is_interactive(args), tty)

    def test_non_interactive_missing_key(self):  # 3(b)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("sys.stdin.isatty", return_value=True), \
                mock.patch("deploy._stdio_is_tty", return_value=True), \
                mock.patch("builtins.input", side_effect=AssertionError("prompt")), \
                mock.patch("deploy.getpass.getpass") as gp:
            code, out, err = run_main(acxd_args(tmp, "--non-interactive"))
        self.assertEqual(code, 2)
        self.assertIn("ACXD_API_KEY", err)
        self.assertIn("--non-interactive", err)
        gp.assert_not_called()
        # stdin not a terminal: exactly the message of the non-interactive mode before this change.
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("sys.stdin.isatty", return_value=False):
            code, _, err = run_main(acxd_args(tmp, "--non-interactive"))
        self.assertEqual(code, 2)
        self.assertIn("（stdin 不是终端，无法提示输入）", err)

    def test_environment_default(self):  # 3(c)
        with clean_env():
            cfg = parse(*dry_args("x"))
        self.assertEqual(cfg.environment, "production")
        self.assertFalse(cfg.interactive)

    def test_getpass_ctrl_c(self):  # 3(d)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("sys.stdin.isatty", return_value=True), \
                mock.patch("deploy.getpass.getpass", side_effect=KeyboardInterrupt):
            code, _, err = run_main(acxd_args(tmp))
            self.assertEqual(os.listdir(tmp), [])
        self.assertEqual(code, 130)
        self.assertIn("已取消", err)


class CollectArgsTests(unittest.TestCase):
    def test_dry_run_prompts_missing(self):  # 4(a)
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            urlopen = mock.Mock(side_effect=AssertionError("network"))
            boto = mock.Mock(side_effect=AssertionError("boto3"))
            with mock.patch("urllib.request.urlopen", urlopen), mock.patch("deploy.boto3.client", boto), \
                    interactive(self, ["bad", TARN, "", "", ""]) as (seen, _):
                code, out, err = run_main(["--dry-run", "--output-dir", tmp])
            self.assertEqual(code, 0, err)
            st = load(tmp, "dry-run", "deploy_state.json")
        urlopen.assert_not_called()
        boto.assert_not_called()
        self.assertIn("交互模式", out)
        self.assertIn("输入无效", out)
        self.assertTrue(seen[0].startswith("Connect 实例 ARN"))
        self.assertIn("ACXD 部署环境", out)
        self.assertIn("对话语言", out)
        self.assertNotIn("队列", "".join(seen))
        self.assertNotIn("工作区", "".join(seen))
        self.assertEqual((st["region"], st["connect"]["instanceId"]), ("us-east-1", INSTANCE))
        self.assertEqual((st["acxd"]["environment"], st["acxd"]["language"]), ("production", "zh-CN"))

    def test_supplied_values_suppress_prompts(self):  # 4(b)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), \
                interactive(self, []) as (_, inp):
            code, out, err = run_main(["--dry-run", "--instance-arn", TARN, "--acxd-environment", "qa",
                                       "--language", "en-US", "--name-prefix", "AgenticCare", "--output-dir", tmp])
            self.assertEqual(code, 0, err)
            self.assertEqual(load(tmp, "dry-run", "deploy_state.json")["acxd"]["environment"], "qa")
        inp.assert_not_called()
        self.assertNotIn("交互模式", out)

    def test_env_var_suppresses_instance_prompt(self):  # 4(c)
        with tempfile.TemporaryDirectory() as tmp, clean_env(CONNECT_INSTANCE_ARN=TARN), \
                interactive(self, ["", "", ""]) as (seen, _):
            code, _, err = run_main(["--dry-run", "--output-dir", tmp])
        self.assertEqual(code, 0, err)
        self.assertEqual(len(seen), 3)
        self.assertFalse(any("实例" in s for s in seen))

    def test_language_choice(self):  # 4(d)
        for answer, explicit in (("2", True), ("en-US", True), ("", False), ("1", False)):
            with clean_env():
                args, _, _ = collect(self, ["--dry-run", "--instance-arn", TARN, "--acxd-environment", "qa"],
                                     [answer, ""])
                cfg = deploy.validate_config(args)
            self.assertIs(cfg.language_explicit, explicit, answer)
            self.assertEqual(cfg.language, "en-US" if explicit else "zh-CN")

    def test_instance_and_queue_lists(self):  # 4(e)
        client, stub = connect_client()
        stub_list_instances(stub, [("b-target", TARN, "ACTIVE"), ("c-failed", TARN_FAILED, "CREATION_FAILED"),
                                   ("a-first", TARN_A, "ACTIVE")])
        stub_list_queues(stub, DEFAULT_QUEUES)
        with clean_env(AWS_REGION="us-east-1"), stub, mock.patch("deploy.boto3.client", return_value=client):
            args, text, _ = collect(self, ["--preflight", "--phase", "connect"], ["2", "", "", ""])
            stub.assert_no_pending_responses()
        self.assertEqual(args.instance_arn, TARN)
        self.assertEqual((args.queue_arn, args.queue_name), (BQ_ARN, None))
        self.assertEqual(args.acxd_environment, "production")
        self.assertIsNone(args.region)
        self.assertIn("1) a-first", text)
        self.assertIn("2) b-target", text)
        self.assertNotIn("c-failed", text)
        self.assertIn("BasicQueue（默认）", text)
        self.assertNotIn("对话语言", text)

    def test_list_failures_fall_back_to_text(self):  # 4(f)
        client, stub = connect_client()
        stub.add_client_error("list_instances", "AccessDeniedException", "no", 403)
        stub.add_client_error("list_queues", "AccessDeniedException", "no", 403)
        with clean_env(AWS_REGION="us-east-1"), stub, mock.patch("deploy.boto3.client", return_value=client):
            with self.assertLogs("deploy", "WARNING") as logs:
                args, _, seen = collect(self, ["--preflight", "--phase", "connect"], [TARN, "", "", ""])
        self.assertEqual(args.instance_arn, TARN)
        self.assertEqual((args.queue_arn, args.queue_name), (None, None))
        self.assertTrue(seen[0].startswith("Connect 实例 ARN"))
        self.assertTrue(seen[1].startswith("目标队列名称或队列 ARN"))
        self.assertTrue(any("ListInstances failed (AccessDeniedException)" in m for m in logs.output))
        # Free-text queue name / ARN.
        for answer, expected in (("Support", (None, "Support")), (BQ_ARN, (BQ_ARN, None))):
            client, stub = connect_client()
            stub.add_client_error("list_queues", "AccessDeniedException", "no", 403)
            with clean_env(AWS_REGION="us-east-1"), stub, mock.patch("deploy.boto3.client", return_value=client), \
                    self.assertLogs("deploy", "WARNING"):
                args, _, _ = collect(self, ["--preflight", "--phase", "connect", "--instance-arn", TARN],
                                     [answer, "", ""])
            self.assertEqual((args.queue_arn, args.queue_name), expected)

    def test_region_prompt(self):  # 4(g)
        boto = mock.Mock(side_effect=AssertionError("boto3"))
        with clean_env(), mock.patch("deploy.boto3.client", boto):
            args, text, seen = collect(self, ["--dry-run", "--phase", "acxd"], ["2", "", "", ""])
            self.assertEqual(args.region, config.ACXD_REGIONS[1])
            self.assertIn("AWS 区域", text)
            self.assertEqual(seen[0], "请输入序号或区域名 [1]: ")  # profile region us-east-1 is item 1
            args, _, _ = collect(self, ["--dry-run", "--phase", "acxd"], ["eu-west-1", "", "", ""])
            self.assertEqual(args.region, "eu-west-1")
            args, text, _ = collect(self, ["--dry-run", "--phase", "acxd"], ["bogus", "1", "", "", ""])
            self.assertEqual(args.region, "us-east-1")
            self.assertIn("输入无效", text)
            # A profile region outside the ACXD list is appended and becomes the default.
            args, _, _ = collect(self, ["--dry-run", "--phase", "acxd"], ["", "", "", ""],
                                 profile_region="sa-east-1")
            self.assertEqual(args.region, "sa-east-1")
        with clean_env(AWS_REGION="us-west-2"):
            args, _, seen = collect(self, ["--dry-run", "--phase", "acxd"], ["", "", ""])
        self.assertIsNone(args.region)
        self.assertFalse(any("区域" in s for s in seen))
        boto.assert_not_called()

    def test_queue_flag_and_region_mismatch(self):  # 4(h)
        boto = mock.Mock(side_effect=AssertionError("boto3"))
        with clean_env(), mock.patch("deploy.boto3.client", boto):
            args, _, seen = collect(self, ["--preflight", "--phase", "connect", "--instance-arn", TARN,
                                           "--queue-name", "Support"], ["", ""])
            self.assertEqual(len(seen), 2)
            self.assertEqual(args.queue_name, "Support")
            args, text, _ = collect(self, ["--dry-run", "--phase", "connect", "--region", "us-west-2"],
                                    [TARN, TARN_WEST, "", ""])
        self.assertEqual(args.instance_arn, TARN_WEST)
        self.assertIn("与 --region us-west-2 不一致", text)
        boto.assert_not_called()

    def test_eof_exits_130(self):  # 4(i)
        with tempfile.TemporaryDirectory() as tmp, clean_env(), interactive(self, [EOFError]):
            code, _, err = run_main(["--dry-run", "--output-dir", tmp])
            self.assertEqual(os.listdir(tmp), [])
        self.assertEqual(code, 130)
        self.assertIn("已取消（未执行任何写操作）", err)

    def test_too_many_invalid_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env(), interactive(self, ["bad"] * 5):
            code, _, err = run_main(["--dry-run", "--output-dir", tmp])
        self.assertEqual(code, 2)
        self.assertIn("连续 5 次输入无效", err)


TWO_INPUT_FIXED = ("--queue-name", "BasicQueue", "--acxd-environment", "production", "--language", "zh-CN")
FULL_RUN_CALLS = ["DescribeInstance", "ListQueues", "ListContactFlows", "ListContactFlows", "CreateContactFlow",
                  "ListContactFlows", "CreateContactFlow"]


class ConfirmTests(unittest.TestCase):
    def test_cancel(self):  # 5(a)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp, interactive(self, ["", "n"]) as (seen, _):
            code, out, err, captured = run_two_input(
                fake, tmp, lambda s: (stub_describe_instance(s), stub_list_queues(s, DEFAULT_QUEUES),
                                      stub_list(s)),
                *TWO_INPUT_FIXED)
            self.assertFalse(Path(tmp, "deploy_state.json").exists())
        self.assertEqual(code, 1)
        self.assertIn("已取消", err)
        self.assertEqual(seen, ["资源名前缀 [AgenticCare]: ", "确认执行真实部署？ [y/N]: "])
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})
        self.assertEqual([n for n, _ in captured], ["DescribeInstance", "ListQueues", "ListContactFlows"])
        self.assertIn("即将执行真实部署", out)
        self.assertIn(f"ACXD API key:   {deploy.mask_key(KEY)}（来源 env）", out)
        self.assertIn("BasicQueue", out)
        self.assertIn(f"ws1 ({WS})", out)
        self.assertNotIn("SECRETVALUE", out + err)

    def test_confirm_yes(self):  # 5(b)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp, interactive(self, ["", "y"]):
            code, out, err, captured = run_two_input(fake, tmp, lambda s: stub_create_path(s, confirm_list=True),
                                                     *TWO_INPUT_FIXED)
            st = load(tmp, "deploy_state.json")
        self.assertEqual(code, 0, err)
        # The confirmation reuses the precheck's ListContactFlows: no extra Connect call.
        self.assertEqual([n for n, _ in captured], FULL_RUN_CALLS)
        self.assertEqual(st["completedSteps"], list(deploy.STEP_NAMES))
        self.assertNotIn("SECRETVALUE", out + err)

    def test_yes_flag_skips_confirmation(self):  # 5(c)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp, interactive(self, [""]) as (seen, _):
            code, out, err, captured = run_two_input(fake, tmp, stub_create_path, *TWO_INPUT_FIXED, "--yes")
        self.assertEqual(code, 0, err)
        self.assertEqual(seen, ["资源名前缀 [AgenticCare]: "])  # --yes still asks for missing values
        self.assertIn("资源名前缀: AgenticCare（默认值）", out)
        self.assertNotIn("即将执行真实部署", out)
        self.assertEqual([n for n, _ in captured], FULL_RUN_CALLS)

    def test_non_interactive_no_confirmation(self):  # 5(d)
        fake = FakeAcxd()
        with tempfile.TemporaryDirectory() as tmp, mock.patch("builtins.input", side_effect=AssertionError):
            code, out, err, _ = run_two_input(fake, tmp, stub_create_path)
        self.assertEqual(code, 0, err)
        self.assertNotIn("即将执行真实部署", out)
        self.assertNotIn("确认执行真实部署", out)

    def test_connect_phase_alone(self):  # 5(g): prechecks moved into run(), same call order
        client, stub = connect_client()
        captured = []
        capture_params(client, captured)
        stub_reads(stub)  # precheck ListContactFlows (reused by the confirmation) + the first upsert's list
        stub.add_response("create_contact_flow", {"ContactFlowId": "sp", "ContactFlowArn": SP_ARN})
        stub_list(stub)
        stub.add_response("create_contact_flow", {"ContactFlowId": "in", "ContactFlowArn": IN_ARN})
        with tempfile.TemporaryDirectory() as tmp, clean_env(), stub, \
                mock.patch("deploy.boto3.client", return_value=client), \
                interactive(self, ["", "", "y"]) as (seen, _):
            action = Path(tmp, "action.json")
            action.write_text(json.dumps(SYNTH_ACTION))
            code, out, err = run_main(connect_args(tmp, "--agentic-cx-action-file", str(action)))
            stub.assert_no_pending_responses()
        self.assertEqual(code, 0, err)
        self.assertEqual([n for n, _ in captured], ["DescribeInstance", "DescribeQueue", "ListContactFlows",
                                                    "ListContactFlows", "CreateContactFlow", "ListContactFlows",
                                                    "CreateContactFlow"])
        self.assertEqual(len(seen), 3)
        self.assertIn("ACXD API key:   未提供", out)

    def test_format_run_parameters(self):  # 5(h)
        with clean_env(ACXD_API_KEY=KEY), mock.patch("sys.stdin.isatty", return_value=False):
            acxd = parse(*acxd_args("x"))
            conn = parse(*connect_args("x"))
        text = deploy.format_run_parameters(acxd, None)
        self.assertNotIn("队列", text)
        self.assertIn("ACXD 应用", text)
        self.assertIn("（ACXD 以语言原因拒绝时自动改用 en-US）", text)
        self.assertIn(deploy.mask_key(KEY), text)
        self.assertIn("  ACXD 模型:      anthropic.claude-haiku-4-5（Generative Journey / Generative Text）", text)
        text2 = deploy.format_run_parameters(conn, None)
        self.assertNotIn("ACXD 应用", text2)
        self.assertNotIn("ACXD 模型", text2)
        self.assertIn(QARN, text2)
        self.assertNotIn("SECRETVALUE", text + text2)


class InteractiveWorkspaceTests(unittest.TestCase):
    def _run(self, fake, tmp, answers):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("urllib.request.urlopen", fake), \
                mock.patch("deploy.time.sleep"), interactive(self, answers) as (seen, _):
            code, out, err = run_main(["--phase", "acxd", "--region", "us-east-1", "--output-dir", tmp,
                                       "--poll-interval", "1"])
        return code, out, err, seen

    def test_multiple_workspaces_choice(self):  # 5(e)
        fake = FakeAcxd()
        fake.workspaces = [{"workspaceId": WS2, "name": "ws2"}, {"workspaceId": WS, "name": "ws1"}]
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err, _ = self._run(fake, tmp, ["", "", "", "2", "y"])
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertIn("请选择 ACXD 工作区", out)
        self.assertEqual(set(fake.ws_headers[1:]), {WS2})
        self.assertEqual((st["acxd"]["workspaceId"], st["acxd"]["workspaceSource"]), (WS2, "interactive"))

    def test_forbidden_asks_for_id(self):  # 5(f)
        fake = FakeAcxd()
        fake.workspaces_error = http_error(403, "AccessDeniedException")
        with tempfile.TemporaryDirectory() as tmp:
            code, out, err, seen = self._run(fake, tmp, ["", "", "", "not-a-uuid", WS, "y"])
            self.assertEqual(code, 0, err)
            st = load(tmp, "deploy_state.json")
        self.assertTrue(seen[3].startswith("ACXD 工作区 ID"))
        self.assertIn("输入无效", out)
        self.assertEqual(set(fake.ws_headers[1:]), {WS})
        self.assertEqual(st["acxd"]["workspaceSource"], "interactive")
        self.assertNotIn("SECRETVALUE", out + err)


# ---------------------------------------------------------------------------
# Resource name prefix (--name-prefix / NAME_PREFIX / prompt / default)
# ---------------------------------------------------------------------------

PREFIX_FIXED = ["--dry-run", "--instance-arn", TARN, "--acxd-environment", "qa", "--language", "zh-CN"]
PREFIX_PROMPT = "资源名前缀 [AgenticCare]: "
ALL_ARGS = ["--region", "us-east-1", "--instance-id", INSTANCE, "--queue-arn", QARN, "--acxd-workspace-id", WS]


class NamePrefixTests(unittest.TestCase):
    def test_prompt_only_when_missing(self):
        with clean_env():
            args, text, seen = collect(self, PREFIX_FIXED, [""])
        self.assertEqual(seen, [PREFIX_PROMPT])
        # The default and the 5 derived names are previewed below the explanation.
        for needle in ("资源名前缀（所有资源名都由它派生", "ACXD flowId: AgenticCareIntake",
                       "Generative Journey 节点: AgenticCareJourney", "ACXD 应用名: AgenticCareSelfService",
                       "Connect screenpop 流: AgenticCare ScreenPop Flow",
                       "Connect 入站流: AgenticCare Inbound ACXD Flow"):
            self.assertIn(needle, text)
        with clean_env():
            args, _, seen = collect(self, PREFIX_FIXED + ["--name-prefix", "Cli1"], [])
        self.assertEqual(seen, [])
        with clean_env(NAME_PREFIX="EnvPfx"):
            args, _, seen = collect(self, PREFIX_FIXED, [])
            cfg = deploy.validate_config(args)
        self.assertEqual(seen, [])
        self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("EnvPfx", "env"))

    def test_enter_accepts_default(self):
        with clean_env():
            args, _, _ = collect(self, PREFIX_FIXED, [""])
            cfg = deploy.validate_config(args)
        self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("AgenticCare", "default"))

    def test_invalid_input_is_reasked(self):
        with clean_env():
            args, text, seen = collect(self, PREFIX_FIXED, ["1bad", "ab", "has-dash", "AgenticCareGJ"])
            cfg = deploy.validate_config(args)
        self.assertEqual(seen, [PREFIX_PROMPT] * 4)
        self.assertEqual(text.count("输入无效：资源名前缀必须匹配"), 3)
        self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("AgenticCareGJ", "prompt"))
        # Typing the default explicitly still counts as an interactive answer.
        with clean_env():
            args, _, _ = collect(self, PREFIX_FIXED, ["AgenticCare"])
            self.assertEqual(deploy.validate_config(args).name_prefix_source, "prompt")

    def test_precedence_and_errors(self):
        with clean_env(NAME_PREFIX="EnvPfx"):
            cfg = parse(*dry_args("x", "--name-prefix", "CliPfx"))
            self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("CliPfx", "cli"))
            cfg = parse(*dry_args("x"))
            self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("EnvPfx", "env"))
        with clean_env():
            cfg = parse(*dry_args("x"))
            self.assertEqual((cfg.name_prefix, cfg.name_prefix_source), ("AgenticCare", "default"))
            # Non-interactive errors are unchanged.
            with self.assertRaisesRegex(deploy.ConfigError, r"^--name-prefix 必须匹配 \^\[A-Za-z\]"):
                parse(*dry_args("x", "--name-prefix", "1x"))
        with clean_env(NAME_PREFIX="bad-prefix"), \
                self.assertRaisesRegex(deploy.ConfigError, "^环境变量 NAME_PREFIX 必须匹配"):
            parse(*dry_args("x"))

    def test_problem_covers_derived_names(self):
        self.assertIsNone(deploy.name_prefix_problem("AgenticCare"))
        longest = "A" * 31
        self.assertIsNone(deploy.name_prefix_problem(longest))
        self.assertTrue(deploy.FLOW_ID_RE.match(config.acxd_flow_id(longest)))
        self.assertLessEqual(len(config.acxd_app_name(longest)), config.ACXD_APP_NAME_MAX)
        self.assertRegex(config.acxd_journey_name(longest), r"^[A-Za-z0-9]+$")
        for bad in ("", "ab", "A" * 32, "1abc", "ab c", "ab_c", "设备故障"):
            self.assertIsNotNone(deploy.name_prefix_problem(bad), bad)

    def test_source_label_in_summary(self):
        cases = ((["--name-prefix", "CliPfx"], {}, "CliPfx（来源 命令行 --name-prefix）"),
                 ([], {"NAME_PREFIX": "EnvPfx"}, "EnvPfx（来源 环境变量 NAME_PREFIX）"),
                 ([], {}, "AgenticCare（来源 默认值）"))
        for extra, env, expected in cases:
            with clean_env(ACXD_API_KEY=KEY, **env), mock.patch("sys.stdin.isatty", return_value=False):
                cfg = parse(*ALL_ARGS, "--output-dir", "x", *extra)
            text = deploy.format_run_parameters(cfg, None)
            self.assertIn(f"  资源名前缀:     {expected}", text)
            self.assertIn(f"    Generative Journey 节点: {cfg.name_prefix}Journey", text)
            self.assertIn(f"    Connect 入站流: {cfg.name_prefix} Inbound ACXD Flow", text)
        with clean_env():
            args, _, _ = collect(self, PREFIX_FIXED, ["MyPfx"])
            cfg = deploy.validate_config(args)
        self.assertIn("  资源名前缀:     MyPfx（来源 交互输入）", deploy.format_run_parameters(cfg, None))
        marks = {"flow": deploy.PREFIX_MARK_UPDATE, "screenpop": deploy.PREFIX_MARK_CREATE}
        text = deploy.format_run_parameters(cfg, None, marks)
        self.assertIn("    ACXD flowId: MyPfxIntake  → 将更新（覆盖）", text)
        self.assertIn("    Connect screenpop 流: MyPfx ScreenPop Flow  → 将新建", text)
        self.assertIn("    ACXD 应用名: MyPfxSelfService\n", text)

    def test_non_interactive_dry_run_info_line(self):
        with tempfile.TemporaryDirectory() as tmp, clean_env(), \
                mock.patch("builtins.input", side_effect=AssertionError("prompt")), \
                mock.patch("deploy._stdio_is_tty", return_value=True):
            code, out, err = run_main(dry_args(tmp, "--non-interactive"))
            self.assertEqual(code, 0, err)
            body = load(tmp, "dry-run", "acxd_flow.json")
        self.assertIn("资源名前缀: AgenticCare（默认值）\n  ACXD flowId: AgenticCareIntake\n"
                      "  Generative Journey 节点: AgenticCareJourney\n", out)
        self.assertIn("  Connect 入站流: AgenticCare Inbound ACXD Flow", out)
        self.assertNotIn("将新建", out)
        gj = [n for n in body["nodes"].values() if n["type"] == "generative_journey"][0]
        self.assertEqual((body["flowId"], gj["metadata"]["name"]), ("AgenticCareIntake", "AgenticCareJourney"))
        # Only the names of the selected phase are listed.
        with tempfile.TemporaryDirectory() as tmp, clean_env():
            code, out, _ = run_main(connect_args(tmp, "--dry-run", "--name-prefix", "OnlyConn"))
        self.assertEqual(code, 0)
        self.assertIn("资源名前缀: OnlyConn（命令行 --name-prefix）\n  Connect screenpop 流:", out)
        self.assertNotIn("OnlyConnIntake", out)

    def test_custom_prefix_dry_run(self):
        urlopen = mock.Mock(side_effect=AssertionError("network"))
        boto = mock.Mock(side_effect=AssertionError("boto3"))
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("urllib.request.urlopen", urlopen), \
                mock.patch("deploy.boto3.client", boto):
            code, out, err = run_main(dry_args(tmp, "--name-prefix", "AgenticCareGJ"))
            self.assertEqual(code, 0, err)
            body = load(tmp, "dry-run", "acxd_flow.json")
            app = load(tmp, "dry-run", "acxd_application.json")
            sp = load(tmp, "dry-run", "screenpop_flow.json")
        urlopen.assert_not_called()
        boto.assert_not_called()
        gj = [n for n in body["nodes"].values() if n["type"] == "generative_journey"][0]
        self.assertEqual((body["flowId"], gj["metadata"]["name"]), ("AgenticCareGJIntake", "AgenticCareGJJourney"))
        self.assertEqual(app["name"], "AgenticCareGJSelfService")
        self.assertEqual(sp["Metadata"]["name"], "AgenticCareGJ ScreenPop Flow")
        self.assertNotIn("AgenticCareJourney", json.dumps(body))
        # Shared across prefixes on purpose.
        self.assertEqual(body["metadata"], config.FLOW_METADATA)
        self.assertIn("资源名前缀: AgenticCareGJ（命令行 --name-prefix）", out)

    def test_interactive_dry_run_prompt(self):
        boto = mock.Mock(side_effect=AssertionError("boto3"))
        with tempfile.TemporaryDirectory() as tmp, clean_env(), mock.patch("deploy.boto3.client", boto), \
                interactive(self, ["", "", "Custom1"]) as (seen, _):
            code, out, err = run_main(["--dry-run", "--instance-arn", TARN, "--output-dir", tmp])
            self.assertEqual(code, 0, err)
            body = load(tmp, "dry-run", "acxd_flow.json")
        self.assertEqual(seen[-1], PREFIX_PROMPT)
        self.assertIn("资源名前缀: Custom1（交互输入）", out)
        self.assertEqual(body["flowId"], "Custom1Intake")
        boto.assert_not_called()

    def test_existence_marks(self):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("sys.stdin.isatty", return_value=False):
            cfg = parse(*ALL_ARGS, "--output-dir", "x")
        fake = FakeAcxd()
        fake.flows = [{"flowId": "AgenticCareIntake"}]
        client, stub = connect_client()
        stub_list(stub, EXISTING_FLOWS[:1])
        with mock.patch("urllib.request.urlopen", fake), stub:
            marks = deploy.prefix_resource_marks(cfg, client)
        self.assertEqual(marks, {"flow": "将更新（覆盖）", "app": "将新建", "screenpop": "将更新（覆盖）",
                                 "inbound": "将新建"})
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})
        # A failed list call only drops its marks.
        fake = FakeAcxd()
        fake.first_call_error = http_error(403, "AccessDeniedException")
        client, stub = connect_client()
        stub.add_client_error("list_contact_flows", "AccessDeniedException", "no", 403)
        with mock.patch("urllib.request.urlopen", fake), stub, self.assertLogs("deploy", "WARNING") as logs:
            marks = deploy.prefix_resource_marks(cfg, client)
        self.assertEqual(marks, {"app": "将新建"})
        self.assertTrue(any("ListFlows / GetFlow failed" in m for m in logs.output))
        self.assertTrue(any("ListContactFlows failed (AccessDeniedException)" in m for m in logs.output))

    def test_existence_marks_foreign_application(self):
        with clean_env(ACXD_API_KEY=KEY), mock.patch("sys.stdin.isatty", return_value=False):
            cfg = parse(*ALL_ARGS, "--output-dir", "x")
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        fake.app_metadata = {"path": "", "tags": []}
        with mock.patch("urllib.request.urlopen", fake):
            self.assertEqual(deploy.prefix_resource_marks(cfg)["app"], deploy.PREFIX_MARK_FOREIGN)
            cfg.overwrite_untagged = True
            self.assertEqual(deploy.prefix_resource_marks(cfg)["app"], deploy.PREFIX_MARK_FOREIGN_FORCED)
            fake.app_metadata = copy.deepcopy(config.APPLICATION_METADATA)
            self.assertEqual(deploy.prefix_resource_marks(cfg)["app"], deploy.PREFIX_MARK_UPDATE)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})

    def test_confirmation_shows_marks(self):
        fake = FakeAcxd()
        fake.apps = [{"applicationId": "APP1", "name": "AgenticCareSelfService"}]
        with tempfile.TemporaryDirectory() as tmp, interactive(self, ["", "n"]):
            code, out, err, _ = run_two_input(
                fake, tmp, lambda s: (stub_describe_instance(s), stub_list_queues(s, DEFAULT_QUEUES),
                                      stub_precheck(s, EXISTING_FLOWS)), *TWO_INPUT_FIXED)
        self.assertEqual(code, 1)
        self.assertIn("  资源名前缀:     AgenticCare（来源 默认值）", out)
        self.assertIn("    ACXD 应用名: AgenticCareSelfService  → 将更新（覆盖）", out)
        self.assertIn("    ACXD flowId: AgenticCareIntake  → 将新建", out)
        self.assertIn("    Connect screenpop 流: AgenticCare ScreenPop Flow  → 将更新（覆盖）", out)
        self.assertEqual({m for m, _ in fake.calls}, {"GET"})


if __name__ == "__main__":
    unittest.main()
