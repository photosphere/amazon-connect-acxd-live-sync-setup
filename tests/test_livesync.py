"""Tests for the Live Sync email-link profile, its Lambdas, infra helpers and the page (no network, no AWS)."""

import base64
import contextlib
import copy
import importlib.util
import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import botocore.session
from botocore.stub import Stubber

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import config  # noqa: E402
import deploy  # noqa: E402
import deploy_livesync  # noqa: E402
import flowlib  # noqa: E402
import livesync  # noqa: E402
import livesync_infra as infra  # noqa: E402
from flowlib import FlowValidationError  # noqa: E402

INSTANCE_ARN = "arn:aws:connect:us-east-1:123456789012:instance/11111111-2222-4333-8444-555555555555"
INSTANCE_ID = "11111111-2222-4333-8444-555555555555"
CONTACT = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
LIVE_NODE = "b1c5e7a0-0000-4000-8000-000000000003"
FAKE_ACXD_KEY = "acxd" + "_live_" + "abcd1234.secretsecret"
SIGNING_KEY = "k" * 64


def load_lambda(name):
    spec = importlib.util.spec_from_file_location(f"lambda_{name}", os.path.join(ROOT, "lambda", f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


send_link = load_lambda("send_link")
session = load_lambda("session")


def client(service):
    c = botocore.session.get_session().create_client(
        service, region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    return c, Stubber(c)


class ProfileMixin:
    def setUp(self):
        self.saved = livesync.apply_profile()
        self.messages = livesync.load_messages()
        self.runtime = dict(livesync.RUNTIME)
        self.options = copy.copy(livesync.OPTIONS)

    def tearDown(self):
        livesync.restore_profile(self.saved)
        livesync.RUNTIME.clear()
        livesync.RUNTIME.update(self.runtime)
        livesync.OPTIONS.__dict__.update(self.options.__dict__)


# ---------------------------------------------------------------------------
# Catalog and ACXD flow
# ---------------------------------------------------------------------------

class CatalogTests(unittest.TestCase):
    def test_shipped_catalog_has_three_products_and_four_models(self):
        for lang, msgs in livesync.load_messages().items():
            self.assertEqual(len(msgs["catalog"]["products"]), 3, lang)
            self.assertEqual(len(msgs["catalog"]["models"]), 4, lang)

    def test_wrong_counts_and_unsafe_names_rejected(self):
        catalog = copy.deepcopy(livesync.load_messages()["en-US"]["catalog"])
        catalog["models"] = catalog["models"][:3]
        with self.assertRaises(FlowValidationError):
            livesync.validate_catalog("en-US", catalog)
        catalog = copy.deepcopy(livesync.load_messages()["en-US"]["catalog"])
        catalog["products"][0]["name"] = "A&B"
        with self.assertRaises(FlowValidationError):
            livesync.validate_catalog("en-US", catalog)

    def test_english_only_messages(self):
        msgs = livesync.load_messages()
        self.assertEqual(list(msgs), ["en-US"])
        self.assertIn("{link}", msgs["en-US"]["email"]["body"])
        cjk = [c for c in json.dumps(msgs["en-US"], ensure_ascii=False) if "\u4e00" <= c <= "\u9fff"]
        self.assertEqual(cjk, [], "Chinese text in the en-US page / prompts / email")


class AcxdFlowTests(ProfileMixin, unittest.TestCase):
    def render(self, lang="en-US"):
        return livesync.render_livesync_acxd_flow(lang, self.messages, "LiveSync")

    def test_live_sync_node_shape(self):
        for lang in ("en-US",):
            nodes = self.render(lang)
            node = nodes[LIVE_NODE]
            self.assertEqual(node["type"], "multimodal")
            mm = node["metadata"]["multimodal"]
            self.assertEqual(mm["type"], "agentic")
            self.assertEqual(mm["modelType"], config.GENERATIVE_MODEL_TYPE)
            self.assertEqual([c["name"] for c in mm["exitConditions"]], list(config.GJ_EXIT_CONDITION_NAMES))
            names = {c["conditions"][0]["left"].get("name") for c in node["childNodes"]}
            self.assertIn(livesync.VOICE_PLUS_CONDITION, names)
            self.assertNotIn("System.gjConditionIndex", names)
            self.assertFalse(any(n["type"] in ("generative_journey", "user_input") for n in nodes.values()))

    def test_prompt_has_voice_only_fallback_with_catalog(self):
        mm = self.render()[LIVE_NODE]["metadata"]["multimodal"]
        for item in self.messages["en-US"]["catalog"]["products"] + self.messages["en-US"]["catalog"]["models"]:
            self.assertIn(item["name"], mm["prompt"])
        self.assertNotIn("<<", mm["prompt"])
        self.assertIn("email", mm["prompt"])

    def test_extraction_prompts_list_the_catalog(self):
        nodes = self.render()
        prompts_ = {n["metadata"]["generativeText"]["name"]: n["metadata"]["generativeText"]["prompt"]
                    for n in nodes.values() if n["type"] == "generative_text"}
        self.assertEqual(set(prompts_), set(config.FIELDS))
        for p in self.messages["en-US"]["catalog"]["products"]:
            self.assertIn(p["name"], prompts_["DeviceName"])

    def test_validator_rejects_gj_condition(self):
        bad = copy.deepcopy(self.render())
        bad[LIVE_NODE]["childNodes"][0]["conditions"][0]["left"]["name"] = "System.gjConditionIndex"
        with self.assertRaises(FlowValidationError):
            livesync.validate_live_sync_nodes(bad)


# ---------------------------------------------------------------------------
# Connect inbound flow
# ---------------------------------------------------------------------------

class LanguageTests(ProfileMixin, unittest.TestCase):
    def test_profile_is_english_only(self):
        self.assertEqual(config.SUPPORTED_LANGUAGES, ("en-US",))
        self.assertEqual((config.DEFAULT_LANGUAGE, config.FALLBACK_LANGUAGE), ("en-US", "en-US"))
        parser = deploy.build_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["--language", "zh-CN"])
        self.assertEqual(parser.parse_args(["--language", "en-US"]).language, "en-US")
        self.assertIn("--language {en-US}", parser.format_usage())
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(deploy_livesync.main(["--help"]), 0)
        self.assertNotIn("zh-CN", buf.getvalue())
        self.assertNotIn("Zhiyu", buf.getvalue())
        self.assertIn("对话语言只支持 en-US", buf.getvalue())

    def test_no_language_prompt(self):
        args = deploy.build_parser().parse_args(["--phase", "acxd", "--region", "us-east-1",
                                                 "--acxd-environment", "production", "--name-prefix", "LiveSync"])
        prompter = mock.Mock()
        deploy.collect_missing_args(args, prompter)
        labels = [c.args[0] for c in prompter.choice.call_args_list]
        self.assertFalse(any("语言" in label for label in labels), labels)
        self.assertIsNone(args.language)

    def test_no_language_fallback(self):
        cfg = mock.Mock(language="en-US", language_explicit=False, reuse_application=False)
        self.assertEqual(deploy._language_attempts(cfg), ["en-US"])

    def test_base_still_offers_both(self):
        livesync.restore_profile(self.saved)
        try:
            self.assertEqual(config.SUPPORTED_LANGUAGES, ("zh-CN", "en-US"))
            self.assertEqual(deploy.build_parser().parse_args(["--language", "zh-CN"]).language, "zh-CN")
        finally:
            self.saved = livesync.apply_profile()


class InboundFlowTests(ProfileMixin, unittest.TestCase):
    ARN = "arn:aws:lambda:us-east-1:123456789012:function:LiveSync-livesync-send-link"

    def inbound(self, lang="en-US"):
        ctx = {
            "FLOW_NAME": "LS Inbound", "SCREENPOP_FLOW_ARN": "arn:sp", "SCREENPOP_FLOW_NAME": "LS ScreenPop",
            "TTS_VOICE": "KATIE", "TTS_ENGINE": "connect:agentic", "QUEUE_ARN": "arn:q", "QUEUE_DISPLAY_NAME": "Q",
            "ERROR_MESSAGE": "err", "IDLE_TIMEOUT_MESSAGE": "idle",
            **flowlib.customer_profile_ctx(lang, self.messages), livesync.LANGUAGE_CTX_KEY: lang,
            livesync.SEND_LINK_CTX_KEY: self.ARN, livesync.LINK_MESSAGE_CTX_KEY: "link sent",
        }
        action, _ = flowlib.rebuild_agentic_cx_transitions(
            flowlib.build_agentic_cx_action("67898477-f36f-4b06-bdca-0d9a1f2eb9cc",
                                            "00000000-0000-4000-8000-0000000000a1", "aliasX"),
            flowlib.INBOUND_IDS)
        return flowlib.render_inbound_flow(ctx, action, None)

    def test_email_block_sits_before_agentic_cx(self):
        flow = self.inbound()
        by_id = {a["Identifier"]: a for a in flow["Actions"]}
        acx = config.AGENTIC_CX_ACTION_ID
        # Every path that reached the Agentic CX block now goes through the Lambda first.
        for a in flow["Actions"]:
            if a["Identifier"] in (livesync.SEND_LINK_ACTION_ID, livesync.LINK_SENT_CHECK_ID,
                                   livesync.LINK_SENT_MESSAGE_ID):
                continue
            self.assertNotIn(acx, [t for _, t in flowlib._transition_targets(a)], a["Identifier"])
        lam = by_id[livesync.SEND_LINK_ACTION_ID]
        self.assertEqual(lam["Type"], "InvokeLambdaFunction")
        self.assertEqual(lam["Parameters"]["LambdaFunctionARN"], self.ARN)
        self.assertEqual(lam["Parameters"]["LambdaInvocationAttributes"],
                         {"CustomerEmail": "$.Attributes.CustomerEmail"})
        self.assertEqual(lam["Transitions"]["Errors"][0]["NextAction"], acx)  # a failed email never ends the call
        check = by_id[livesync.LINK_SENT_CHECK_ID]
        self.assertEqual(check["Parameters"]["ComparisonValue"], "$.External.linkSent")
        self.assertEqual(check["Transitions"]["NextAction"], acx)
        self.assertEqual(by_id[livesync.LINK_SENT_MESSAGE_ID]["Transitions"]["NextAction"], acx)

    def test_language_block_still_present(self):
        by_id = {a["Identifier"]: a for a in self.inbound("en-US")["Actions"]}
        self.assertEqual(by_id[livesync.LANGUAGE_ACTION_ID]["Parameters"], {"LanguageCode": "en-US"})

    def test_ctx_keys_required(self):
        with self.assertRaises(FlowValidationError):
            livesync.render_inbound_flow({livesync.LANGUAGE_CTX_KEY: "zh-CN"}, {}, None)

    def test_screenpop_labels(self):
        sp = flowlib.render_screenpop_flow("en-US", self.messages, "us-east-1", "LS ScreenPop")
        labels = [i["Label"] for i in sp["Actions"][1]["Parameters"]["ViewData"]["AttributeBar"]]
        for label in ("Product name", "Product model", "Fault description"):
            self.assertIn(label, labels)


# ---------------------------------------------------------------------------
# Lambdas
# ---------------------------------------------------------------------------

def connect_event(contact=CONTACT, email=""):
    return {"Details": {"ContactData": {"ContactId": contact}, "Parameters": {"CustomerEmail": email}}}


SEND_ENV = {"SECRET_ARN": "arn:secret", "PAGE_URL": "https://d1.cloudfront.net", "EMAIL_FROM": "from@example.com",
            "TEST_EMAIL": "tester@example.com", "LANGUAGE": "zh-CN", "LINK_TTL_SECONDS": "1800",
            "EMAIL_SUBJECT": "Subj", "EMAIL_BODY": "Hi\nOpen: {link}\nValid {minutes} min"}


class SendLinkLambdaTests(unittest.TestCase):
    def setUp(self):
        send_link._secret.update(value=None, at=0.0)

    def run_handler(self, env, event):
        sm, ses = mock.Mock(), mock.Mock()
        sm.get_secret_value.return_value = {"SecretString": json.dumps({"signingKey": SIGNING_KEY})}

        def factory(name, **_):
            return {"secretsmanager": sm, "sesv2": ses}[name]
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(send_link.boto3, "client", factory), \
                contextlib.redirect_stdout(io.StringIO()) as logs:
            result = send_link.handler(event, None)
        return result, ses, logs.getvalue()

    def test_sends_to_test_email_with_signed_link(self):
        result, ses, logs = self.run_handler(SEND_ENV, connect_event(email="customer@example.org"))
        self.assertEqual(result["linkSent"], "true")
        kwargs = ses.send_email.call_args.kwargs
        self.assertEqual(kwargs["Destination"], {"ToAddresses": ["tester@example.com"]})
        self.assertEqual(kwargs["FromEmailAddress"], "from@example.com")
        text = kwargs["Content"]["Simple"]["Body"]["Text"]["Data"]
        link = text.split("Open: ", 1)[1].split("\n", 1)[0]
        self.assertTrue(link.startswith("https://d1.cloudfront.net/#t="))
        payload = session.verify_token(link.split("#t=", 1)[1], SIGNING_KEY)
        self.assertEqual((payload["c"], payload["l"]), (CONTACT, "zh-CN"))
        self.assertIn("Valid 30 min", text)
        self.assertIn('href="https://d1.cloudfront.net/#t=', kwargs["Content"]["Simple"]["Body"]["Html"]["Data"])
        # Neither the token nor the full address is logged.
        self.assertNotIn(link.split("#t=", 1)[1], logs)
        self.assertNotIn("tester@example.com", logs)

    def test_profile_email_without_test_email(self):
        env = {**SEND_ENV, "TEST_EMAIL": ""}
        result, ses, _ = self.run_handler(env, connect_event(email="customer@example.org"))
        self.assertEqual((result["linkSent"], result["reason"]), ("true", "profile"))
        self.assertEqual(ses.send_email.call_args.kwargs["Destination"]["ToAddresses"], ["customer@example.org"])

    def test_no_email_is_skipped_without_failing_the_call(self):
        env = {**SEND_ENV, "TEST_EMAIL": ""}
        for email in ("", "未找到", "Not found"):
            result, ses, _ = self.run_handler(env, connect_event(email=email))
            self.assertEqual(result, {"linkSent": "false", "reason": "no_email"})
            ses.send_email.assert_not_called()

    def test_ses_failure_returns_false(self):
        sm, ses = mock.Mock(), mock.Mock()
        sm.get_secret_value.return_value = {"SecretString": json.dumps({"signingKey": SIGNING_KEY})}
        ses.send_email.side_effect = RuntimeError("MessageRejected")
        with mock.patch.dict(os.environ, SEND_ENV, clear=True), \
                mock.patch.object(send_link.boto3, "client", lambda n, **_: {"secretsmanager": sm, "sesv2": ses}[n]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(send_link.handler(connect_event(), None), {"linkSent": "false", "reason": "send_failed"})

    def test_bad_event(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(send_link.handler({}, None)["reason"], "no_contact_id")


class SessionLambdaTests(unittest.TestCase):
    def setUp(self):
        session._secret.update(value=None, at=0.0)

    def call(self, token, keys=True, disconnected=False, method="POST", describe_error=None):
        session._secret.update(value=None, at=0.0)
        doc = {"signingKey": SIGNING_KEY, "liveSyncApiKey": "ls-api" if keys else "",
               "liveSyncDeploymentKey": "ls-dep" if keys else ""}
        sm, connect = mock.Mock(), mock.Mock()
        sm.get_secret_value.return_value = {"SecretString": json.dumps(doc)}
        contact = {"Id": CONTACT}
        if disconnected:
            contact["DisconnectTimestamp"] = "2026-10-28T00:00:00Z"
        connect.describe_contact.return_value = {"Contact": contact}
        if describe_error:
            connect.describe_contact.side_effect = describe_error
        event = {"requestContext": {"http": {"method": method}}, "body": json.dumps({"token": token})}
        env = {"SECRET_ARN": "arn:secret", "CONNECT_INSTANCE_ID": INSTANCE_ID, "AWS_REGION": "us-east-1"}
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(session.boto3, "client", lambda n, **_: {"secretsmanager": sm, "connect": connect}[n]), \
                contextlib.redirect_stdout(io.StringIO()) as logs:
            resp = session.handler(event, None)
        self.assertNotIn(token or "x" * 200, logs.getvalue())
        return resp["statusCode"], json.loads(resp["body"]), resp["headers"]

    def token(self, ttl=600, key=SIGNING_KEY, contact=CONTACT):
        return send_link.sign_token(contact, ttl, "en-US", key)

    def test_valid_token_returns_contact_and_keys(self):
        status, body, headers = self.call(self.token())
        self.assertEqual(status, 200)
        self.assertEqual(body["contactId"], CONTACT)
        self.assertEqual(body["languageCode"], "en-US")
        self.assertEqual(body["liveSync"], {"apiKey": "ls-api", "deploymentKey": "ls-dep"})
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_rejections(self):
        cases = [
            (self.token(ttl=-10), {}, 401, "LINK_EXPIRED"),
            (self.token(key="z" * 64), {}, 401, "INVALID_TOKEN"),
            (self.token()[:-3] + "AAA", {}, 401, "INVALID_TOKEN"),
            ("not-a-token", {}, 400, "INVALID_TOKEN"),
            (self.token(), {"disconnected": True}, 410, "CALL_ENDED"),
            (self.token(), {"keys": False}, 503, "LIVE_SYNC_KEYS_NOT_CONFIGURED"),
            (self.token(), {"method": "GET"}, 405, "METHOD_NOT_ALLOWED"),
            (self.token(), {"describe_error": RuntimeError("boom")}, 503, "CALL_STATUS_UNAVAILABLE"),
        ]
        for token, kw, status, code in cases:
            got_status, body, _ = self.call(token, **kw)
            self.assertEqual((got_status, body), (status, {"error": code}), (token[:12], kw))
            self.assertNotIn("liveSync", body)

    def test_tampered_payload_rejected(self):
        body, sig = self.token().split(".")
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        payload["c"] = "ffffffff-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        forged = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode() + "." + sig
        self.assertEqual(self.call(forged)[0], 401)


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

def template(**kw):
    args = dict(prefix="LiveSync", instance_id=INSTANCE_ID, language="zh-CN", email_from="from@example.com",
                test_email="tester@example.com", link_ttl_minutes=30, email_subject="S", email_body="B {link}")
    args.update(kw)
    return infra.build_template(**args)


class TemplateTests(unittest.TestCase):
    def test_template_shape_and_size(self):
        t = template()
        body = infra.template_body(t)
        self.assertLess(len(body.encode()), infra.TEMPLATE_BODY_MAX)
        res = t["Resources"]
        bucket = res["PageBucket"]["Properties"]["PublicAccessBlockConfiguration"]
        self.assertTrue(all(bucket.values()))
        dist = res["Distribution"]["Properties"]["DistributionConfig"]
        self.assertEqual(dist["DefaultCacheBehavior"]["ViewerProtocolPolicy"], "redirect-to-https")
        self.assertEqual([b["PathPattern"] for b in dist["CacheBehaviors"]], ["/api/*"])
        self.assertEqual(res["ApiRoute"]["Properties"]["RouteKey"], "POST /api/session")
        self.assertEqual(res["SendLinkConnectPermission"]["Properties"]["Principal"], "connect.amazonaws.com")
        self.assertIn("def handler", res["SessionFunction"]["Properties"]["Code"]["ZipFile"])
        session_actions = json.dumps(res["SessionRole"]["Properties"]["Policies"])
        self.assertIn("connect:DescribeContact", session_actions)
        self.assertNotIn("ses:", session_actions)
        env = res["SendLinkFunction"]["Properties"]["Environment"]["Variables"]
        self.assertEqual((env["TEST_EMAIL"], env["LINK_TTL_SECONDS"], env["LANGUAGE"]),
                         ("tester@example.com", "1800", "zh-CN"))

    def test_no_live_sync_key_in_template(self):
        self.assertNotIn("ls-api", json.dumps(template()))


class StackTests(unittest.TestCase):
    NAME = "LiveSync-livesync-page"
    OUTPUTS = [{"OutputKey": k, "OutputValue": v} for k, v in (
        ("PageUrl", "https://d1.cloudfront.net"), ("BucketName", "b1"), ("DistributionId", "E1"),
        ("SendLinkFunctionArn", "arn:aws:lambda:us-east-1:1:function:f"), ("SecretArn", "arn:s"))]

    def stack(self, status="CREATE_COMPLETE", owned=True):
        tags = [{"Key": config.CONNECT_OWNER_TAG_KEY, "Value": config.OWNER_TAG}] if owned else []
        return {"StackName": self.NAME, "StackStatus": status, "CreationTime": "2026-01-01T00:00:00Z",
                "Tags": tags, "Outputs": self.OUTPUTS}

    def test_create(self):
        cfn, stub = client("cloudformation")
        stub.add_client_error("describe_stacks", "ValidationError", f"Stack with id {self.NAME} does not exist")
        stub.add_response("create_stack", {"StackId": "id1"}, {
            "StackName": self.NAME, "TemplateBody": "{}", "Capabilities": ["CAPABILITY_IAM"], "OnFailure": "ROLLBACK",
            "Tags": [{"Key": config.CONNECT_OWNER_TAG_KEY, "Value": config.OWNER_TAG}]})
        stub.add_response("describe_stacks", {"Stacks": [self.stack()]})  # waiter
        stub.add_response("describe_stacks", {"Stacks": [self.stack()]})
        with stub, self.assertLogs("livesync_infra", "INFO"):
            out = infra.deploy_stack(cfn, self.NAME, "{}")
        self.assertEqual(out["PageUrl"], "https://d1.cloudfront.net")

    def test_up_to_date(self):
        cfn, stub = client("cloudformation")
        stub.add_response("describe_stacks", {"Stacks": [self.stack()]})
        stub.add_client_error("update_stack", "ValidationError", "No updates are to be performed.")
        with stub, self.assertLogs("livesync_infra", "INFO"):
            self.assertEqual(infra.deploy_stack(cfn, self.NAME, "{}")["BucketName"], "b1")

    def test_foreign_stack_refused(self):
        cfn, stub = client("cloudformation")
        stub.add_response("describe_stacks", {"Stacks": [self.stack(owned=False)]})
        with stub, self.assertRaises(infra.StackOwnershipError):
            infra.deploy_stack(cfn, self.NAME, "{}")

    def test_busy_stack_refused(self):
        cfn, stub = client("cloudformation")
        stub.add_response("describe_stacks", {"Stacks": [self.stack(status="UPDATE_IN_PROGRESS")]})
        with stub, self.assertRaises(RuntimeError):
            infra.check_stack(cfn, self.NAME, False)


class InfraHelperTests(unittest.TestCase):
    def connect_with(self, addresses, pages=1):
        connect, stub = client("connect")
        items = [{"EmailAddressId": f"id{i}", "EmailAddressArn": f"arn:e{i}", "EmailAddress": a, "DisplayName": "D"}
                 for i, a in enumerate(addresses)]
        if pages == 2 and len(items) > 1:
            stub.add_response("search_email_addresses", {"EmailAddresses": items[:1], "NextToken": "n1"},
                              {"InstanceId": INSTANCE_ID, "MaxResults": 100})
            stub.add_response("search_email_addresses", {"EmailAddresses": items[1:]},
                              {"InstanceId": INSTANCE_ID, "MaxResults": 100, "NextToken": "n1"})
        else:
            stub.add_response("search_email_addresses", {"EmailAddresses": items},
                              {"InstanceId": INSTANCE_ID, "MaxResults": 100})
        return connect, stub

    def test_candidates_need_production_and_verified_identity(self):
        connect, cstub = self.connect_with(["b@corp.example", "a@other.example"], pages=2)
        ses, sstub = client("sesv2")
        sstub.add_response("get_account", {"ProductionAccessEnabled": True, "SendingEnabled": True,
                                           "EnforcementStatus": "HEALTHY"})
        # a@other.example: neither address nor domain verified.
        sstub.add_client_error("get_email_identity", "NotFoundException", "no", 404,
                               expected_params={"EmailIdentity": "a@other.example"})
        sstub.add_response("get_email_identity", {"VerifiedForSendingStatus": False},
                           {"EmailIdentity": "other.example"})
        # b@corp.example: domain verified.
        sstub.add_client_error("get_email_identity", "NotFoundException", "no", 404,
                               expected_params={"EmailIdentity": "b@corp.example"})
        sstub.add_response("get_email_identity", {"VerifiedForSendingStatus": True}, {"EmailIdentity": "corp.example"})
        with cstub, sstub:
            found = infra.find_sender_candidates(connect, ses, INSTANCE_ID)
        self.assertEqual([a["address"] for a in found["addresses"]], ["a@other.example", "b@corp.example"])
        self.assertEqual([(c["address"], c["verifiedBy"]) for c in found["candidates"]],
                         [("b@corp.example", "domain")])

    def test_sandbox_has_no_candidates_and_skips_identity_checks(self):
        connect, cstub = self.connect_with(["b@corp.example"])
        ses, sstub = client("sesv2")
        sstub.add_response("get_account", {"ProductionAccessEnabled": False, "SendingEnabled": True})
        with cstub, sstub:
            found = infra.find_sender_candidates(connect, ses, INSTANCE_ID)
            sstub.assert_no_pending_responses()
        self.assertFalse(found["account"]["production"])
        self.assertEqual(found["candidates"], [])

    def test_associate_lambda_once(self):
        connect, stub = client("connect")
        stub.add_response("list_lambda_functions", {"LambdaFunctions": ["arn:f"]}, {"InstanceId": INSTANCE_ID})
        with stub:
            self.assertFalse(infra.associate_lambda(connect, INSTANCE_ID, "arn:f"))
        connect, stub = client("connect")
        stub.add_response("list_lambda_functions", {"LambdaFunctions": []}, {"InstanceId": INSTANCE_ID})
        stub.add_response("associate_lambda_function", {}, {"InstanceId": INSTANCE_ID, "FunctionArn": "arn:f"})
        with stub:
            self.assertTrue(infra.associate_lambda(connect, INSTANCE_ID, "arn:f"))

    def test_put_keys_keeps_signing_key(self):
        sm = mock.Mock()
        sm.get_secret_value.return_value = {"SecretString": json.dumps(
            {"signingKey": SIGNING_KEY, "liveSyncApiKey": "", "liveSyncDeploymentKey": ""})}
        infra.put_live_sync_keys(sm, "arn:s", "a", "d")
        doc = json.loads(sm.put_secret_value.call_args.kwargs["SecretString"])
        self.assertEqual(doc, {"signingKey": SIGNING_KEY, "liveSyncApiKey": "a", "liveSyncDeploymentKey": "d"})

    def test_upload_page_content_types(self):
        s3 = mock.Mock()
        with tempfile.TemporaryDirectory() as d:
            for name in ("index.html", "app.js", "vendor/touchpoint.umd.js"):
                Path(d, name).parent.mkdir(parents=True, exist_ok=True)
                Path(d, name).write_text("x", encoding="utf-8")
            keys = infra.upload_page(s3, "b1", Path(d))
        self.assertEqual(sorted(keys), ["app.js", "index.html", "vendor/touchpoint.umd.js"])
        types = {c.kwargs["Key"]: c.kwargs["ContentType"] for c in s3.put_object.call_args_list}
        self.assertTrue(types["index.html"].startswith("text/html"))
        self.assertIn("javascript", types["app.js"])


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

class PageTests(unittest.TestCase):
    def test_page_has_no_secrets_or_contact(self):
        opts = copy.copy(livesync.OPTIONS)
        livesync.OPTIONS.live_sync_api_key, livesync.OPTIONS.live_sync_deployment_key = "ls-api-secret", "ls-dep-secret"
        try:
            with tempfile.TemporaryDirectory() as out:
                tp = Path(out, "tp.js")
                tp.write_text("window.amazonConnectTouchpoint={};", encoding="utf-8")
                with self.assertLogs("livesync", "WARNING"):
                    summary = livesync.generate_page(out, "en-US", livesync.load_messages(), touchpoint_js=tp,
                                                     acxd_api_key=FAKE_ACXD_KEY)
                page = summary["pageDir"]
                text = "".join(p.read_text(encoding="utf-8") for p in page.rglob("*") if p.suffix in (".js", ".html"))
                for secret in ("ls-api-secret", "ls-dep-secret", FAKE_ACXD_KEY):
                    self.assertNotIn(secret, text)
                cfg = json.loads((page / "config.js").read_text(encoding="utf-8")
                                 .split("window.LIVESYNC_CONFIG = ", 1)[1].rstrip().rstrip(";"))
                self.assertEqual(set(cfg), {"languageCode", "sessionPath", "catalog", "text"})
                self.assertIn('lang="en-US"', (page / "index.html").read_text(encoding="utf-8"))
                self.assertIn('input: "external"', (page / "app.js").read_text(encoding="utf-8"))
        finally:
            livesync.OPTIONS.__dict__.update(opts.__dict__)

    def test_download_integrity_checked(self):
        resp = mock.MagicMock()
        resp.__enter__.return_value.read.return_value = b"not the tarball"
        with tempfile.TemporaryDirectory() as cache, mock.patch("urllib.request.urlopen", return_value=resp):
            with self.assertLogs("livesync", "INFO"), self.assertRaises(RuntimeError):
                livesync.fetch_touchpoint(Path(cache))
            self.assertFalse(any(Path(cache).iterdir()))


# ---------------------------------------------------------------------------
# deploy_livesync.py
# ---------------------------------------------------------------------------

class EmailOptionTests(unittest.TestCase):
    def ns(self, **kw):
        base = dict(test_email=None, email_from=None, page_only=False)
        base.update(kw)
        return mock.Mock(**base)

    def test_sender_is_not_derived_from_test_email(self):
        mode = mock.Mock(dry_run=False)
        self.assertEqual(deploy_livesync.resolve_email_options(self.ns(test_email="T@example.com"), mode, False),
                         ("T@example.com", ""))
        self.assertEqual(deploy_livesync.resolve_email_options(self.ns(), mode, False), ("", ""))

    def test_invalid(self):
        mode = mock.Mock(dry_run=False)
        for kw in ({"test_email": "nope"}, {"email_from": "nope"}):
            with self.assertRaises(deploy.ConfigError):
                deploy_livesync.resolve_email_options(self.ns(**kw), mode, False)

    def test_interactive_prompt(self):
        mode = mock.Mock(dry_run=False)
        answers = iter(["bad", "me@example.com"])
        prompter = deploy_livesync.prompts.Prompter(input_fn=lambda _p: next(answers), out=io.StringIO())
        got = deploy_livesync.resolve_email_options(self.ns(), mode, True, prompter)
        self.assertEqual(got, ("me@example.com", ""))
        # Enter = no test email (Customer Profiles email).
        prompter = deploy_livesync.prompts.Prompter(input_fn=lambda _p: "", out=io.StringIO())
        self.assertEqual(deploy_livesync.resolve_email_options(self.ns(email_from="f@example.com"), mode, True,
                                                               prompter), ("", "f@example.com"))


class DryRunTests(unittest.TestCase):
    def test_dry_run_end_to_end(self):
        env = {k: v for k, v in os.environ.items()
               if k not in ("ACXD_API_KEY", "LIVE_SYNC_API_KEY", "LIVE_SYNC_DEPLOYMENT_KEY", "NAME_PREFIX",
                            deploy_livesync.TEST_EMAIL_ENV, deploy_livesync.EMAIL_FROM_ENV)}
        saved = livesync.apply_profile()
        livesync.restore_profile(saved)
        try:
            with tempfile.TemporaryDirectory() as out, mock.patch.dict(os.environ, env, clear=True), \
                    mock.patch("boto3.client", side_effect=AssertionError("no AWS in dry-run")):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
                    rc = deploy_livesync.main(["--dry-run", "--non-interactive", "--instance-arn", INSTANCE_ARN,
                                               "--output-dir", out, "--test-email", "tester@example.com"])
                self.assertEqual(rc, 0, buf.getvalue())
                dry = Path(out) / "dry-run"
                inbound = json.loads((dry / "inbound_acxd_flow.json").read_text(encoding="utf-8"))
                types = [a["Type"] for a in inbound["Actions"]]
                self.assertIn("InvokeLambdaFunction", types)
                lam = next(a for a in inbound["Actions"] if a["Type"] == "InvokeLambdaFunction")
                self.assertTrue(lam["Parameters"]["LambdaFunctionARN"].endswith(":function:LiveSync-livesync-send-link"))
                stack = json.loads((dry / livesync.STACK_ARTIFACT).read_text(encoding="utf-8"))
                env_vars = stack["Resources"]["SendLinkFunction"]["Properties"]["Environment"]["Variables"]
                self.assertEqual(env_vars["TEST_EMAIL"], "tester@example.com")
                self.assertTrue((dry / livesync.PAGE_DIR_NAME / "config.js").is_file())
                self.assertIn("CloudFormation 栈（页面 / 邮件）: LiveSync-livesync-page", buf.getvalue())
                self.assertIn(livesync.STACK_ARTIFACT, buf.getvalue())
        finally:
            livesync.restore_profile(saved)


STACK_OUTPUTS = {"PageUrl": "https://d1.cloudfront.net", "BucketName": "b1", "DistributionId": "E1",
                 "SendLinkFunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:LiveSync-livesync-send-link",
                 "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s"}


class ConnectPhaseHookTests(unittest.TestCase):
    def setUp(self):
        self.runtime = dict(livesync.RUNTIME)
        self.options = copy.copy(livesync.OPTIONS)

    def tearDown(self):
        livesync.RUNTIME.clear()
        livesync.RUNTIME.update(self.runtime)
        livesync.OPTIONS.__dict__.update(self.options.__dict__)

    def prechecks(self, stack, found=None, cfg=None):
        cfn = mock.Mock()
        cfn.describe_stacks.return_value = {"Stacks": [stack]}
        cfg = cfg or mock.Mock(region="us-east-1", name_prefix="LiveSync", overwrite_untagged=False,
                               instance_id=INSTANCE_ID, interactive=False, prompter=None, dry_run=False)
        found = found or {"account": {"production": True, "sendingEnabled": True, "enforcement": "HEALTHY"},
                          "addresses": [{"address": "help@corp.example", "displayName": "", "id": "e1"}],
                          "candidates": [{"address": "help@corp.example", "displayName": "", "id": "e1",
                                          "verifiedBy": "domain"}]}
        clients = {"cloudformation": cfn, "connect": mock.Mock(), "sesv2": mock.Mock()}
        with mock.patch.object(deploy, "connect_prechecks", lambda c, cf: "target"):
            saved = livesync.apply_profile()
            try:
                with mock.patch.object(livesync, "_clients", return_value=clients), \
                        mock.patch.object(infra, "find_sender_candidates", return_value=found), \
                        self.assertLogs("livesync", "INFO"):
                    return deploy.connect_prechecks(None, cfg)
            finally:
                livesync.restore_profile(saved)

    def test_foreign_stack_stops_connect_prechecks(self):
        cfn = mock.Mock()
        cfn.describe_stacks.return_value = {"Stacks": [{"StackStatus": "CREATE_COMPLETE", "Tags": []}]}
        cfg = mock.Mock(region="us-east-1", name_prefix="LiveSync", overwrite_untagged=False)
        with mock.patch.object(deploy, "connect_prechecks", lambda c, cf: "target"):
            saved = livesync.apply_profile()
            try:
                with mock.patch.object(livesync, "_clients", return_value={"cloudformation": cfn}), \
                        self.assertRaises(deploy.ConfigError) as cm:
                    deploy.connect_prechecks(None, cfg)
            finally:
                livesync.restore_profile(saved)
        self.assertIn("CloudFormation 栈 LiveSync-livesync-page", str(cm.exception))

    OWNED = {"StackStatus": "UPDATE_COMPLETE",
             "Tags": [{"Key": config.CONNECT_OWNER_TAG_KEY, "Value": config.OWNER_TAG}]}

    def test_single_candidate_is_used_non_interactively(self):
        self.prechecks(self.OWNED)
        self.assertEqual((livesync.OPTIONS.email_from, livesync.RUNTIME["emailFromSource"]),
                         ("help@corp.example", "only-candidate"))

    def test_no_usable_sender_stops_before_any_write(self):
        sandbox = {"account": {"production": False, "sendingEnabled": True, "enforcement": "HEALTHY"},
                   "addresses": [{"address": "help@corp.example", "displayName": "", "id": "e1"}], "candidates": []}
        unverified = {"account": {"production": True, "sendingEnabled": True, "enforcement": "HEALTHY"},
                      "addresses": [{"address": "help@corp.example", "displayName": "", "id": "e1"}], "candidates": []}
        none = {"account": {"production": True, "sendingEnabled": True, "enforcement": "HEALTHY"},
                "addresses": [], "candidates": []}
        for found, needle in ((sandbox, "沙盒"), (unverified, "help@corp.example"), (none, "没有任何邮件地址")):
            with self.assertRaises(deploy.ConfigError) as cm:
                self.prechecks(self.OWNED, found)
            self.assertIn(needle, str(cm.exception))
            self.assertIn("没有执行任何写操作", str(cm.exception))

    def test_multiple_candidates(self):
        cands = [{"address": "a@corp.example", "displayName": "Sales", "id": "e1", "verifiedBy": "address"},
                 {"address": "b@corp.example", "displayName": "", "id": "e2", "verifiedBy": "domain"}]
        many = {"account": {"production": True, "sendingEnabled": True, "enforcement": "HEALTHY"},
                "addresses": [{k: c[k] for k in ("address", "displayName", "id")} for c in cands],
                "candidates": cands}
        with self.assertRaises(deploy.ConfigError) as cm:  # non-interactive: must be told which one
            self.prechecks(self.OWNED, many)
        self.assertIn("--email-from", str(cm.exception))
        # Interactive: the user picks from the list.
        seen = []
        prompter = deploy_livesync.prompts.Prompter(input_fn=lambda p: (seen.append(p), "2")[1], out=io.StringIO())
        cfg = mock.Mock(region="us-east-1", name_prefix="LiveSync", overwrite_untagged=False, instance_id=INSTANCE_ID,
                        interactive=True, prompter=prompter, dry_run=False)
        self.prechecks(self.OWNED, many, cfg)
        self.assertEqual((livesync.OPTIONS.email_from, livesync.RUNTIME["emailFromSource"]),
                         ("b@corp.example", "interactive"))
        # --email-from must be one of the candidates.
        livesync.OPTIONS.email_from = "A@corp.example"
        self.prechecks(self.OWNED, many)
        self.assertEqual(livesync.OPTIONS.email_from, "a@corp.example")
        livesync.OPTIONS.email_from = "x@corp.example"
        with self.assertRaises(deploy.ConfigError):
            self.prechecks(self.OWNED, many)

    def test_owned_stack_provides_send_link_arn(self):
        stack = {"StackStatus": "UPDATE_COMPLETE",
                 "Tags": [{"Key": config.CONNECT_OWNER_TAG_KEY, "Value": config.OWNER_TAG}],
                 "Outputs": [{"OutputKey": k, "OutputValue": v} for k, v in STACK_OUTPUTS.items()]}
        self.assertEqual(self.prechecks(stack), "target")  # sender: the single candidate
        self.assertEqual(livesync.RUNTIME["sendLinkFunctionArn"], STACK_OUTPUTS["SendLinkFunctionArn"])

    def test_deploy_page_infra(self):
        livesync.OPTIONS.test_email = livesync.OPTIONS.email_from = "tester@example.com"
        livesync.OPTIONS.live_sync_api_key, livesync.OPTIONS.live_sync_deployment_key = "ls-a", "ls-d"
        clients = {n: mock.Mock() for n in ("cloudformation", "sesv2", "s3", "secretsmanager", "connect")}
        clients["secretsmanager"].get_secret_value.return_value = {"SecretString": json.dumps(
            {"signingKey": SIGNING_KEY, "liveSyncApiKey": "", "liveSyncDeploymentKey": ""})}
        with tempfile.TemporaryDirectory() as out:
            tp = Path(out, "tp.js")
            tp.write_text("window.amazonConnectTouchpoint={};", encoding="utf-8")
            livesync.OPTIONS.touchpoint_js = tp
            cfg = mock.Mock(region="us-east-1", instance_id=INSTANCE_ID, name_prefix="LiveSync", language="en-US",
                            out_dir=Path(out), api_key=FAKE_ACXD_KEY, overwrite_untagged=False,
                            account_id="123456789012")
            with mock.patch.object(livesync, "_clients", return_value=clients), \
                    mock.patch.object(infra, "deploy_stack", return_value=STACK_OUTPUTS) as ds, \
                    mock.patch.object(infra, "associate_lambda", return_value=True) as assoc, \
                    mock.patch.object(infra, "live_sync_keys_configured", return_value=True), \
                    self.assertLogs("livesync", "INFO"):
                livesync.deploy_page_infra(cfg, livesync.load_messages())
            self.assertEqual(ds.call_args.args[1], "LiveSync-livesync-page")
            assoc.assert_called_once_with(clients["connect"], INSTANCE_ID, STACK_OUTPUTS["SendLinkFunctionArn"])
            clients["sesv2"].assert_not_called()  # nothing is created in SES
            keys = [c.kwargs["Key"] for c in clients["s3"].put_object.call_args_list]
            self.assertIn("index.html", keys)
            self.assertIn("config.js", keys)
            secret = json.loads(clients["secretsmanager"].put_secret_value.call_args.kwargs["SecretString"])
            self.assertEqual((secret["liveSyncApiKey"], secret["signingKey"]), ("ls-a", SIGNING_KEY))
            state = livesync.load_state(Path(out))
            self.assertEqual((state["pageUrl"], state["testEmail"], state["emailFrom"]),
                             ("https://d1.cloudfront.net", "tester@example.com", "tester@example.com"))
            state_text = Path(out, livesync.STATE_NAME).read_text(encoding="utf-8")
            for secret_value in ("ls-a", "ls-d", FAKE_ACXD_KEY, SIGNING_KEY):
                self.assertNotIn(secret_value, state_text)

    def test_aws_error_becomes_deploy_error(self):
        from botocore.exceptions import ClientError
        err = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "CreateStack")
        cfg = mock.Mock(region="us-east-1", instance_id=INSTANCE_ID, name_prefix="LiveSync", language="en-US",
                        out_dir=Path(tempfile.gettempdir()), api_key=None, overwrite_untagged=False)
        livesync.OPTIONS.email_from = "f@example.com"
        with mock.patch.object(livesync, "_clients", return_value={"cloudformation": mock.Mock()}), \
                mock.patch.object(infra, "deploy_stack", side_effect=err), \
                mock.patch.object(deploy, "write_artifact"), self.assertLogs("livesync", "ERROR"), \
                self.assertRaises(deploy.DeployError) as cm:
            livesync.deploy_page_infra(cfg, livesync.load_messages())
        self.assertIn("CreateStack: AccessDenied", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
