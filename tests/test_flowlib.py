import copy
import json
import os
import re
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import config  # noqa: E402
import flowlib  # noqa: E402
from flowlib import FlowValidationError, SpliceSpec  # noqa: E402

MESSAGES = flowlib.load_messages()
PREFIX = config.DEFAULT_NAME_PREFIX
ID05 = "d1a9e0c0-0000-4000-8000-000000000005"
ID08 = "d1a9e0c0-0000-4000-8000-000000000008"
ID09 = "d1a9e0c0-0000-4000-8000-000000000009"


def inbound_values():
    return {
        "FLOW_NAME": "X Inbound", "SCREENPOP_FLOW_ARN": "arn:sp", "SCREENPOP_FLOW_NAME": "X ScreenPop",
        "TTS_VOICE": "Zhiyu", "TTS_ENGINE": "Neural", "QUEUE_ARN": "arn:q", "QUEUE_DISPLAY_NAME": "Q",
        "ERROR_MESSAGE": "err", "IDLE_TIMEOUT_MESSAGE": "idle",
        **flowlib.customer_profile_ctx("zh-CN", MESSAGES),
    }


def N(nn):
    """ACXD template node id a0d0f1a0-0000-4000-8000-0000000000<nn>."""
    return f"a0d0f1a0-0000-4000-8000-0000000000{nn}"


def gj_cond(i):
    return {"left": {"type": "system", "name": "System.gjConditionIndex"}, "operator": "eq",
            "right": {"type": "constant", "value": i}}


def status_cond(value):
    return {"left": {"type": "node_status"}, "operator": "eq", "right": {"type": "constant", "value": value}}


def fallback_action():
    action, _ = flowlib.rebuild_agentic_cx_transitions(flowlib.load_fallback_action())
    return action


def real_action():
    """The real Agentic CX action of live Connect flow 'AI Agent - ACXD' (no secrets)."""
    with open(os.path.join(ROOT, "tests", "fixtures", "agentic_cx_action_real.json"), encoding="utf-8") as fh:
        return json.load(fh)


class RenderTemplateTests(unittest.TestCase):
    def test_whole_value_keeps_type(self):
        tpl = {"a": "{{D}}", "b": ["{{L}}"], "c": "{{B}}"}
        out = flowlib.render_template(tpl, {"D": {"x": 1}, "L": [1, 2], "B": True})
        self.assertEqual(out, {"a": {"x": 1}, "b": [[1, 2]], "c": True})

    def test_whole_value_is_deep_copied(self):
        val = {"x": [1]}
        out = flowlib.render_template({"a": "{{D}}"}, {"D": val})
        out["a"]["x"].append(2)
        self.assertEqual(val, {"x": [1]})

    def test_inline_requires_str(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template({"a": "arn:{{REGION}}:x"}, {"REGION": 5})
        self.assertIn("REGION", str(cm.exception))
        out = flowlib.render_template({"a": "arn:{{REGION}}:x"}, {"REGION": "us-east-1"})
        self.assertEqual(out["a"], "arn:us-east-1:x")

    def test_missing_and_extra_keys(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template({"a": "{{A}}", "b": "{{B}}"}, {"A": "x"})
        self.assertIn("B", str(cm.exception))
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template({"a": "{{A}}"}, {"A": "x", "EXTRA": "y"})
        self.assertIn("EXTRA", str(cm.exception))

    def test_dict_keys_not_replaced_and_caught_by_residual_check(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template({"{{K}}": "{{A}}"}, {"A": "x"})
        self.assertIn("key", str(cm.exception))

    def test_no_second_expansion(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template({"a": "{{A}}", "b": "{{B}}"}, {"A": "{{B}}", "B": "y"})
        self.assertIn("residual", str(cm.exception))

    def test_malformed_braces_caught(self):
        with self.assertRaises(FlowValidationError):
            flowlib.render_template({"a": "{{ x }}"}, {})

    def _marker_tpl(self, n):
        actions = [{"Identifier": f"id{i}", "Type": flowlib.MARKER, "Parameters": {}, "Transitions": {}}
                   for i in range(n)]
        actions.append({"Identifier": "z", "Type": "Disconnect", "Parameters": {"t": "{{A}}"}})
        return {"Actions": actions, "Metadata": {"ActionMetadata": {"id0": {"position": {"x": 1}}}}}

    def test_marker_count(self):
        for n in (0, 2):
            with self.assertRaises(FlowValidationError):
                flowlib.render_template(self._marker_tpl(n), {"A": "x"},
                                        splice=SpliceSpec(action={"Type": "T"}))

    def test_marker_reserved_in_ctx(self):
        with self.assertRaises(FlowValidationError):
            flowlib.render_template(self._marker_tpl(1), {"A": "x", "AGENTIC_CX_ACTION": "y"},
                                    splice=SpliceSpec(action={"Type": "T"}))

    def test_marker_without_splice_is_missing_placeholder(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template(self._marker_tpl(1), {"A": "x"})
        self.assertIn("AGENTIC_CX_ACTION", str(cm.exception))

    def test_splice_keeps_identifier_and_metadata_position(self):
        out = flowlib.render_template(
            self._marker_tpl(1), {"A": "x"},
            splice=SpliceSpec(action={"Identifier": "other", "Type": "T", "Parameters": {"p": 1}},
                              action_metadata={"position": {"x": 99}, "parameters": {"d": 1}}))
        self.assertEqual(out["Actions"][0], {"Identifier": "id0", "Type": "T", "Parameters": {"p": 1}})
        self.assertEqual(out["Metadata"]["ActionMetadata"]["id0"], {"position": {"x": 1}, "parameters": {"d": 1}})

    def test_spliced_action_not_substituted(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.render_template(self._marker_tpl(1), {"A": "x"},
                                    splice=SpliceSpec(action={"Type": "T", "Parameters": {"q": "{{A}}"}}))
        self.assertIn("residual", str(cm.exception))

    def test_template_placeholder_sets(self):
        for rel, expected in flowlib.TEMPLATE_PLACEHOLDERS.items():
            with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
                text = fh.read()
            found = set(re.findall(r"\{\{([A-Z][A-Z0-9_]*)\}\}", text)) - {"AGENTIC_CX_ACTION"}
            self.assertEqual(found, set(expected), rel)


class MessageTests(unittest.TestCase):
    def test_render_message(self):
        out = flowlib.render_message("a {{FIELD_DEVICE_NAME}} b")
        self.assertEqual(out, "a {DeviceName:NLX.Context} b")

    def test_render_message_patched(self):
        with mock.patch.object(config, "CONTEXT_VAR_MESSAGE_PLACEHOLDER", "{{Context.{name}}}"):
            self.assertEqual(flowlib.render_message("{{FIELD_DEVICE_MODEL}}"), "{Context.DeviceModel}")

    def test_unknown_token(self):
        for text in ("{{FIELD_UNKNOWN}}", "{{OTHER}}"):
            with self.assertRaises(FlowValidationError):
                flowlib.render_message(text)

    def test_messages_both_languages(self):
        zh, en = MESSAGES["zh-CN"]["acxd"], MESSAGES["en-US"]["acxd"]
        self.assertEqual(set(zh), set(en))
        self.assertEqual(set(zh), {"welcome", "journey_prompt", "exit_details_collected",
                                   "exit_human_handoff", "retry", "transfer", "extract"})
        for lang, msgs in (("zh-CN", zh), ("en-US", en)):
            self.assertEqual(set(msgs["extract"]), set(config.FIELDS), lang)
            texts = [v for k, v in msgs.items() if k != "extract"] + list(msgs["extract"].values())
            for text in texts:
                self.assertIsInstance(text, str)
                self.assertTrue(text.strip(), lang)
                self.assertNotIn("{{", text, lang)
            for f, text in msgs["extract"].items():
                self.assertIn("{System.transcript:NLX.System}", text, (lang, f))


class ValidatorTests(unittest.TestCase):
    def good_connect(self):
        return {"StartAction": "a", "Actions": [
            {"Identifier": "a", "Type": "X", "Transitions": {"NextAction": "b",
                                                             "Errors": [{"NextAction": "b", "ErrorType": "E"}],
                                                             "Conditions": [{"NextAction": "b"}]}},
            {"Identifier": "b", "Type": "Y", "Transitions": {}}],
            "Metadata": {"ActionMetadata": {"a": {}, "b": {}}}}

    def test_connect_ok(self):
        flowlib.validate_connect_flow(self.good_connect())

    def test_connect_negative(self):
        cases = []
        f = self.good_connect(); f["Actions"][0]["Transitions"]["Errors"][0]["NextAction"] = "zz"; cases.append(f)
        f = self.good_connect(); f["Actions"][0]["Transitions"]["Conditions"][0]["NextAction"] = "zz"; cases.append(f)
        f = self.good_connect(); f["Actions"][1]["Identifier"] = "a"; cases.append(f)
        f = self.good_connect(); f["Metadata"]["ActionMetadata"]["c"] = {}; cases.append(f)
        f = self.good_connect(); f["StartAction"] = "zz"; cases.append(f)
        f = self.good_connect(); f["Actions"][1]["Type"] = "{{X}}"; cases.append(f)
        for f in cases:
            with self.assertRaises(FlowValidationError):
                flowlib.validate_connect_flow(f)

    def good_acxd(self):
        a, b = "a0d0f1a0-0000-4000-8000-000000000001", "a0d0f1a0-0000-4000-8000-000000000002"
        return {a: {"nodeId": a, "type": "start", "childNodes": [{"nodeId": b}]},
                b: {"nodeId": b, "type": "escalate"}}

    def test_acxd_ok(self):
        flowlib.validate_acxd_flow(self.good_acxd())
        flowlib.validate_acxd_flow({"nodes": self.good_acxd()})

    def test_acxd_negative(self):
        a, b = list(self.good_acxd())
        n = self.good_acxd(); n[b]["type"] = "bogus"
        m = self.good_acxd(); m[b]["type"] = "start"
        k = self.good_acxd(); k[b]["nodeId"] = a
        c = self.good_acxd(); c[a]["childNodes"] = [{"nodeId": "a0d0f1a0-0000-4000-8000-000000000099"}]
        u = {"not-a-uuid": {"nodeId": "not-a-uuid", "type": "start"}}
        for bad in (n, m, k, c, u):
            with self.assertRaises(FlowValidationError):
                flowlib.validate_acxd_flow(bad)

    CONTINUATION = {"conditions": [{"left": {"type": "node_status"}, "operator": "eq",
                                    "right": {"type": "constant", "value": "continuation"}}]}

    def test_acxd_continuation_child(self):
        a, b = list(self.good_acxd())
        ok = self.good_acxd(); ok[b]["childNodes"] = [copy.deepcopy(self.CONTINUATION)]
        flowlib.validate_acxd_flow(ok)
        basic = self.good_acxd(); basic[b]["type"] = "basic"
        basic[b]["childNodes"] = [copy.deepcopy(self.CONTINUATION)]
        plain = self.good_acxd(); plain[b]["childNodes"] = [{"conditions": []}]
        for bad in (basic, plain):
            with self.assertRaises(FlowValidationError) as cm:
                flowlib.validate_acxd_flow(bad)
            self.assertIn("childNodes[0] -> unknown None", str(cm.exception))

    def test_acxd_terminate_type(self):
        a, b = list(self.good_acxd())
        n = self.good_acxd(); n[b]["type"] = "terminate"
        flowlib.validate_acxd_flow(n)

    def test_acxd_duplicate_message_id(self):
        a, b = list(self.good_acxd())
        n = self.good_acxd()
        n[b]["messages"] = [{"messageId": "m1", "body": "x"}, {"messageId": "m1", "body": "y"}]
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.validate_acxd_flow(n)
        self.assertIn("duplicate messageId m1", str(cm.exception))

    def test_assert_no_secret(self):
        flowlib.assert_no_secret("abc", None)
        flowlib.assert_no_secret("abc", "")
        with self.assertRaises(FlowValidationError) as cm:
            fake = config.API_KEY_PREFIX + "a.SECRET"
            flowlib.assert_no_secret(f"x {fake} y", fake)
        self.assertNotIn("SECRET", str(cm.exception))


class RenderFlowTests(unittest.TestCase):
    def test_inbound_attributes_dict(self):  # AC6
        flow = flowlib.render_inbound_flow(inbound_values(), fallback_action(), None)
        act = [a for a in flow["Actions"] if a["Identifier"] == ID05][0]
        self.assertEqual(act["Type"], "UpdateContactAttributes")
        self.assertEqual(act["Parameters"]["Attributes"], {
            "DeviceName": "$.AgenticCX.ContextVariables.DeviceName",
            "DeviceModel": "$.AgenticCX.ContextVariables.DeviceModel",
            "FaultDescription": "$.AgenticCX.ContextVariables.FaultDescription",
        })
        hook = [a for a in flow["Actions"] if a["Type"] == "UpdateContactEventHooks"][0]
        self.assertEqual(hook["Parameters"]["EventHooks"]["DefaultAgentUI"], "arn:sp")
        flowlib.validate_connect_flow(flow)

    def _screenpop(self):
        return flowlib.render_screenpop_flow("zh-CN", MESSAGES, "us-east-1", "X ScreenPop")

    def _view(self, flow):
        return [a for a in flow["Actions"] if a["Type"] == "ShowView"][0]["Parameters"]["ViewData"]

    def test_screenpop_show_values(self):  # AC14
        vd = self._view(self._screenpop())
        ts = vd["Sections"][0]["TemplateString"]
        self.assertEqual(ts.count("$.Attributes."), 6)
        refs = re.findall(r"\$\.Attributes\.([A-Za-z_]+)", ts)
        self.assertEqual(refs, list(config.SCREENPOP_FIELDS))
        self.assertEqual(refs[:3], list(config.CUSTOMER_FIELDS))
        self.assertEqual([b["Value"] for b in vd["AttributeBar"]],
                         [f"$.Attributes.{f}" for f in config.SCREENPOP_FIELDS])

    def test_screenpop_hide_values(self):  # AC14
        with mock.patch.object(config, "SCREENPOP_SECTIONS_SHOW_VALUES", False):
            vd = self._view(self._screenpop())
        self.assertTrue(vd["Sections"])
        for s in vd["Sections"]:
            self.assertNotIn("$.Attributes.", s["TemplateString"])
        self.assertEqual([b["Value"] for b in vd["AttributeBar"]],
                         [f"$.Attributes.{f}" for f in config.SCREENPOP_FIELDS])

    def test_screenpop_attribute_bar_customer_first(self):
        expected_fields = ["CustomerName", "CustomerPhone", "CustomerEmail",
                           "DeviceName", "DeviceModel", "FaultDescription"]
        expected_labels = {
            "zh-CN": ["客户姓名", "来电号码", "邮箱", "设备名称", "设备型号", "故障描述"],
            "en-US": ["Customer name", "Phone number", "Email", "Device name", "Device model",
                      "Fault description"],
        }
        for lang in ("zh-CN", "en-US"):
            bar = self._view(flowlib.render_screenpop_flow(lang, MESSAGES, "us-east-1", "X"))["AttributeBar"]
            self.assertEqual(len(bar), 6, lang)
            self.assertEqual([b["Value"] for b in bar], [f"$.Attributes.{f}" for f in expected_fields], lang)
            self.assertEqual([b["Label"] for b in bar], expected_labels[lang], lang)
            labels = MESSAGES[lang]["screenpop"]["labels"]
            self.assertEqual([b["Label"] for b in bar], [labels[f] for f in expected_fields], lang)
            self.assertTrue(all(b["Copyable"] is True for b in bar), lang)
        # The order follows config.SCREENPOP_FIELDS (one tuple to reorder).
        reordered = config.FIELDS + config.CUSTOMER_FIELDS
        with mock.patch.object(config, "SCREENPOP_FIELDS", reordered):
            vd = self._view(self._screenpop())
        self.assertEqual([b["Value"] for b in vd["AttributeBar"]], [f"$.Attributes.{f}" for f in reordered])
        self.assertEqual(re.findall(r"\$\.Attributes\.([A-Za-z_]+)", vd["Sections"][0]["TemplateString"]),
                         list(reordered))

    def test_screenpop_under_byte_limit(self):
        for show in (True, False):
            for lang in ("zh-CN", "en-US"):
                with mock.patch.object(config, "SCREENPOP_SECTIONS_SHOW_VALUES", show):
                    flow = flowlib.render_screenpop_flow(lang, MESSAGES, "us-east-1", "X")
                view = [a for a in flow["Actions"] if a["Type"] == "ShowView"][0]
                size = len(json.dumps(view["Parameters"], ensure_ascii=False).encode("utf-8"))
                self.assertLess(size, config.SHOWVIEW_PARAMETERS_MAX_BYTES, (lang, show))
        with mock.patch.object(config, "SHOWVIEW_PARAMETERS_MAX_BYTES", 500):
            with self.assertRaises(FlowValidationError) as cm:
                self._screenpop()
        self.assertIn("NFR5", str(cm.exception))

    def test_screenpop_labels_cover_fields(self):
        for lang in ("zh-CN", "en-US"):
            sp = MESSAGES[lang]["screenpop"]
            self.assertEqual(set(sp["labels"]), set(config.SCREENPOP_FIELDS), lang)
            self.assertEqual(set(sp["customer"]), {"name_format", "not_found", "no_name", "no_email"}, lang)
            for key in ("not_found", "no_name", "no_email"):
                text = sp["customer"][key]
                self.assertTrue(text.strip(), (lang, key))
                self.assertNotIn("$.", text, (lang, key))
                self.assertNotIn("{", text, (lang, key))
        self.assertEqual(MESSAGES["zh-CN"]["screenpop"]["customer"]["not_found"], "未找到")
        self.assertEqual(MESSAGES["en-US"]["screenpop"]["customer"]["not_found"], "Not found")

    def test_screenpop_labels_escaped(self):
        msgs = copy.deepcopy(MESSAGES)
        msgs["zh-CN"]["screenpop"]["labels"]["DeviceName"] = "<b>x</b>"
        flow = flowlib.render_screenpop_flow("zh-CN", msgs, "us-east-1", "X")
        ts = self._view(flow)["Sections"][0]["TemplateString"]
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", ts)
        self.assertNotIn("<b>x</b>", ts)

    def test_no_user_input_or_split(self):
        for lang in ("zh-CN", "en-US"):
            nodes = flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)
            types = {n["type"] for n in nodes.values()}
            self.assertFalse(types & {"user_input", "split"}, lang)
            self.assertFalse(types & config.FORBIDDEN_NODE_TYPES, lang)
            self.assertEqual(types, {"start", "basic", "generative_journey", "generative_text", "loop",
                                     "escalate"}, lang)

    def test_generative_journey_node(self):
        self.assertEqual(config.GENERATIVE_MODEL_TYPE, "anthropic.claude-haiku-4-5")
        for lang in ("zh-CN", "en-US"):
            nodes = flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)
            gjs = [k for k, n in nodes.items() if n["type"] == "generative_journey"]
            self.assertEqual(gjs, [N("03")], lang)
            node = nodes[N("03")]
            gj = node["metadata"]["generativeJourney"]
            self.assertEqual(gj["modelType"], config.GENERATIVE_MODEL_TYPE)
            self.assertEqual([e["name"] for e in gj["exitConditions"]], ["detailsCollected", "humanHandoff"])
            acxd = MESSAGES[lang]["acxd"]
            self.assertEqual([e["prompt"] for e in gj["exitConditions"]],
                             [acxd["exit_details_collected"], acxd["exit_human_handoff"]])
            self.assertEqual(gj["prompt"], acxd["journey_prompt"])
            self.assertEqual(gj["tools"], [])
            for k, v in config.GENERATIVE_JOURNEY_SETTINGS.items():
                self.assertEqual(gj[k], v)
            self.assertEqual(node["messages"], [])
            edges = [(c["conditions"][0], c["nodeId"]) for c in node["childNodes"]]
            self.assertEqual(edges, [
                (gj_cond(0), N("06")), (gj_cond(1), N("06")),
                (status_cond("timeout"), N("04")), (status_cond("failure"), N("04")),
            ])
            self.assertIs(type(edges[0][0]["right"]["value"]), int)

    def test_journey_name_follows_prefix(self):
        self.assertEqual(config.acxd_journey_name("AgenticCare"), "AgenticCareJourney")
        for prefix, expected in (("AgenticCare", "AgenticCareJourney"), ("AgenticCareGJ", "AgenticCareGJJourney")):
            for lang in ("zh-CN", "en-US"):
                nodes = flowlib.render_acxd_flow(lang, MESSAGES, prefix)
                self.assertEqual(nodes[N("03")]["metadata"]["name"], expected, (prefix, lang))
                others = [n["metadata"]["name"] for k, n in nodes.items() if k != N("03")]
                self.assertNotIn(expected, others)
        self.assertIn("JOURNEY_NAME", flowlib.TEMPLATE_PLACEHOLDERS[flowlib.ACXD_TEMPLATE])

    def test_template_has_no_literal_journey_name(self):
        with open(os.path.join(ROOT, flowlib.ACXD_TEMPLATE), encoding="utf-8") as fh:
            text = fh.read()
        self.assertNotIn("AgenticCareJourney", text)
        gj = [n for n in json.loads(text)["nodes"].values() if n["type"] == "generative_journey"]
        self.assertEqual([n["metadata"]["name"] for n in gj], ["{{JOURNEY_NAME}}"])

    def test_generative_text_nodes(self):
        for lang in ("zh-CN", "en-US"):
            nodes = flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)
            gts = [k for k, n in nodes.items() if n["type"] == "generative_text"]
            self.assertEqual(gts, [N("06"), N("07"), N("08")], lang)
            self.assertEqual([nodes[k]["metadata"]["generativeText"]["name"] for k in gts], list(config.FIELDS))
            for k, nxt, f in zip(gts, [N("07"), N("08"), N("09")], config.FIELDS):
                node = nodes[k]
                gt = node["metadata"]["generativeText"]
                self.assertEqual(gt["modelType"], config.GENERATIVE_MODEL_TYPE)
                self.assertIn("{System.transcript:NLX.System}", gt["prompt"])
                self.assertEqual(gt["prompt"], MESSAGES[lang]["acxd"]["extract"][f])
                for kk, v in config.GENERATIVE_TEXT_SETTINGS.items():
                    self.assertEqual(gt[kk], v)
                self.assertEqual([(c["conditions"][0], c["nodeId"]) for c in node["childNodes"]],
                                 [(status_cond(s), nxt) for s in ("success", "timeout", "failure")])

    def test_data_chain_to_escalate(self):
        for lang in ("zh-CN", "en-US"):
            nodes = flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)
            esc = nodes[N("09")]
            self.assertEqual(esc["type"], "escalate")
            self.assertEqual(esc["metadata"]["stateModifications"], [
                {"type": "context", "name": f, "modification": "set",
                 "value": {"type": "constant", "value": "{" + f + ":NLX.Local}"}} for f in config.FIELDS])
            self.assertEqual(esc["messages"][0]["body"], MESSAGES[lang]["acxd"]["transfer"])
        self.assertEqual(flowlib.field_attributes(),
                         {f: "$.AgenticCX.ContextVariables." + f for f in config.FIELDS})

    def test_connect_consumed_context_variables_are_set(self):
        """Every $.AgenticCX.ContextVariables.<v> / $.Attributes.<v> read by the Connect flows is
        produced by a generative_text and set on the escalate node (4-stage chain a/b/d)."""
        inbound = json.dumps(flowlib.render_inbound_flow(inbound_values(), fallback_action(), None))
        consumed = set(re.findall(r"\$\.AgenticCX\.ContextVariables\.([A-Za-z_]+)", inbound))
        for lang in ("zh-CN", "en-US"):
            sp = json.dumps(flowlib.render_screenpop_flow(lang, MESSAGES, "us-east-1", "X"))
            shown = set(re.findall(r"\$\.Attributes\.([A-Za-z_]+)", sp))
            self.assertEqual(consumed, set(config.FIELDS))
            self.assertEqual(shown, consumed | set(config.CUSTOMER_FIELDS), lang)
            nodes = flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)
            produced = {n["metadata"]["generativeText"]["name"] for n in nodes.values()
                        if n["type"] == "generative_text"}
            esc = [n for n in nodes.values() if n["type"] == "escalate"]
            self.assertEqual(len(esc), 1)
            set_ctx = {m["name"]: m["value"]["value"] for m in esc[0]["metadata"]["stateModifications"]
                       if m["type"] == "context" and m["modification"] == "set"}
            for v in consumed:
                self.assertIn(v, produced, (lang, v))
                self.assertEqual(set_ctx.get(v), "{" + v + ":NLX.Local}", (lang, v))

    def test_all_model_types_haiku(self):
        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == "modelType":
                        yield v
                    yield from walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    yield from walk(v)
        for lang in ("zh-CN", "en-US"):
            models = list(walk(flowlib.render_acxd_flow(lang, MESSAGES, PREFIX)))
            self.assertEqual(len(models), 4, lang)  # 1 Generative Journey + 3 Generative Text
            self.assertEqual(set(models), {config.GENERATIVE_MODEL_TYPE}, lang)

    def test_loop_retry(self):
        nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        loop = nodes[N("04")]
        self.assertEqual(loop["type"], "loop")
        self.assertEqual(loop["metadata"]["loop"]["type"], "range")
        self.assertEqual((loop["metadata"]["loop"]["start"], loop["metadata"]["loop"]["end"]), (1, 2))
        self.assertEqual(loop["childNodes"][0], {"nodeId": N("05"), "name": "", "conditions": []})
        self.assertEqual(loop["childNodes"][1]["nodeId"], N("06"))
        self.assertTrue(flowlib._is_continuation_child(loop["childNodes"][1]))
        retry = nodes[N("05")]
        self.assertEqual(retry["messages"][0]["body"], MESSAGES["zh-CN"]["acxd"]["retry"])
        self.assertEqual([c["nodeId"] for c in retry["childNodes"]], [N("03")])

    def test_validate_rejects_user_input_wrong_model_unrouted_exit(self):
        base = flowlib.render_acxd_flow("en-US", MESSAGES, PREFIX)
        flowlib.validate_acxd_flow(base)
        ui = copy.deepcopy(base)
        uid = "a0d0f1a0-0000-4000-8000-000000000099"
        ui[uid] = {"nodeId": uid, "type": "user_input", "childNodes": [{"nodeId": N("03"), "conditions": []}]}
        ui[N("02")]["childNodes"] = [{"nodeId": uid, "conditions": []}]
        model_gj = copy.deepcopy(base)
        model_gj[N("03")]["metadata"]["generativeJourney"]["modelType"] = "anthropic.claude-sonnet-5"
        model_gt = copy.deepcopy(base)
        model_gt[N("08")]["metadata"]["generativeText"]["modelType"] = "anthropic.claude-sonnet-5"
        unrouted = copy.deepcopy(base)
        unrouted[N("03")]["childNodes"] = [c for c in unrouted[N("03")]["childNodes"]
                                           if c["conditions"][0] != gj_cond(1)]
        for bad, needle in ((ui, "forbidden type 'user_input'"), (model_gj, "modelType"),
                            (model_gt, "modelType"), (unrouted, "indexes [1]")):
            with self.assertRaises(FlowValidationError) as cm:
                flowlib.validate_acxd_flow(bad)
            self.assertIn(needle, str(cm.exception))

    def test_escalate_continuation_child(self):  # A7
        nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        children = nodes[N("09")]["childNodes"]
        self.assertEqual(len(children), 1)
        self.assertNotIn("nodeId", children[0])
        self.assertTrue(flowlib._is_continuation_child(children[0]))
        flowlib.validate_acxd_flow(nodes)

    def test_message_ids_unique_uuid4(self):
        nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        ids = [m["messageId"] for n in nodes.values() for m in n.get("messages") or []]
        self.assertEqual(len(ids), 3)
        self.assertEqual(len(set(ids)), len(ids))
        for mid in ids:
            self.assertRegex(mid, flowlib.UUID4_RE)

    def test_escalate_metadata(self):
        with mock.patch.object(config, "ESCALATE_NODE_METADATA", {"name": "E", "sendContext": True}):
            nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        self.assertEqual(nodes[N("09")]["metadata"], {
            "name": "E", "sendContext": True,
            "stateModifications": [config.state_mod_set_context(f) for f in config.FIELDS]})

    def test_voice_settings(self):
        nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        self.assertFalse(any("voiceSettings" in (n.get("metadata") or {}) for n in nodes.values()))
        with mock.patch.object(config, "ACXD_NODE_VOICE_SETTINGS", {"voice": "V"}):
            nodes = flowlib.render_acxd_flow("zh-CN", MESSAGES, PREFIX)
        with_voice = sorted(k[-2:] for k, n in nodes.items() if "voiceSettings" in (n.get("metadata") or {}))
        self.assertEqual(with_voice, ["02", "05"])


CID = flowlib.INBOUND_CUSTOMER_IDS
ID03 = "d1a9e0c0-0000-4000-8000-000000000003"


def inbound_for(lang):
    values = {**inbound_values(), **flowlib.customer_profile_ctx(lang, MESSAGES)}
    flow = flowlib.render_inbound_flow(values, fallback_action(), None)
    return flow, {a["Identifier"]: a for a in flow["Actions"]}


def targets(action):
    return [t for _, t in flowlib._transition_targets(action)]


class CustomerProfileTests(unittest.TestCase):
    def test_inbound_get_customer_profile_action(self):
        flow, by_id = inbound_for("zh-CN")
        lookups = [a for a in flow["Actions"] if a["Type"] == "GetCustomerProfile"]
        self.assertEqual(len(lookups), 1)
        lookup = lookups[0]
        self.assertEqual(lookup["Identifier"], CID["lookup"])
        self.assertEqual(lookup["Parameters"]["ProfileRequestData"],
                         {"IdentifierName": "_phone", "IdentifierValue": "$.CustomerEndpoint.Address"})
        self.assertTrue({"FirstName", "LastName", "EmailAddress", "BusinessEmailAddress", "PhoneNumber"}
                        <= set(lookup["Parameters"]["ProfileResponseData"]))
        self.assertNotIn("PersonalEmailAddress", lookup["Parameters"]["ProfileResponseData"])
        tr = lookup["Transitions"]
        self.assertEqual(tr["NextAction"], CID["found"])
        self.assertNotIn("Conditions", tr)
        self.assertEqual({e["ErrorType"]: e["NextAction"] for e in tr["Errors"]},
                         {"NoneFoundError": CID["not_found"], "MultipleFoundError": CID["not_found"],
                          "NoMatchingError": CID["not_found"]})
        # Looked up early: TTS (003) -> lookup on both its outcomes, before the Agentic CX action.
        tts = by_id[ID03]["Transitions"]
        self.assertEqual(tts["NextAction"], CID["lookup"])
        self.assertEqual([e["NextAction"] for e in tts["Errors"]], [CID["lookup"]])
        meta = flow["Metadata"]["ActionMetadata"][CID["lookup"]]
        self.assertEqual(meta["customerProfilesAction"], "GetCustomerProfile")
        self.assertEqual(meta["useDynamic"], {"IdentifierName": True, "IdentifierValue": True})
        flowlib.validate_connect_flow(flow)

    def test_customer_lookup_all_paths_reach_agentic_cx(self):
        _, by_id = inbound_for("en-US")
        chain = set(CID.values())
        seen, stack, ends = set(), [CID["lookup"]], set()
        while stack:
            ident = stack.pop()
            if ident in seen:
                continue
            seen.add(ident)
            outs = targets(by_id[ident])
            self.assertTrue(outs, ident)  # every chain action has an outgoing edge
            for t in outs:
                self.assertIn(t, chain | {config.AGENTIC_CX_ACTION_ID}, (ident, t))
                if t == config.AGENTIC_CX_ACTION_ID:
                    ends.add(ident)
                else:
                    stack.append(t)
        self.assertEqual(seen, chain)  # every chain action is reachable
        self.assertNotIn(ID08, seen)
        self.assertNotIn("d1a9e0c0-0000-4000-8000-00000000000a", seen)
        # Leaf actions (no edge back into the chain) all end at the Agentic CX action.
        for ident in seen:
            if not set(targets(by_id[ident])) & chain:
                self.assertEqual(set(targets(by_id[ident])), {config.AGENTIC_CX_ACTION_ID}, ident)
        self.assertEqual(ends, {CID["check_email"], CID["business_email"], CID["no_email"], CID["not_found"]})

    def test_customer_attributes_per_language(self):
        for lang, name in (("zh-CN", "$.Customer.LastName $.Customer.FirstName"),
                           ("en-US", "$.Customer.FirstName $.Customer.LastName")):
            _, by_id = inbound_for(lang)
            cust = MESSAGES[lang]["screenpop"]["customer"]
            attrs = {k: by_id[CID[k]]["Parameters"]["Attributes"]
                     for k in ("found", "business_email", "no_email", "not_found")}
            for k in attrs:
                self.assertEqual(by_id[CID[k]]["Type"], "UpdateContactAttributes", (lang, k))
                self.assertEqual(by_id[CID[k]]["Parameters"]["TargetContact"], "Current", (lang, k))
            self.assertEqual(attrs["found"], {"CustomerName": name,
                                              "CustomerPhone": "$.CustomerEndpoint.Address",
                                              "CustomerEmail": "$.Customer.EmailAddress"}, lang)
            self.assertEqual(attrs["not_found"], {"CustomerName": cust["not_found"],
                                                  "CustomerPhone": "$.CustomerEndpoint.Address",
                                                  "CustomerEmail": cust["not_found"]}, lang)
            self.assertEqual(attrs["no_email"], {"CustomerEmail": cust["no_email"]}, lang)
            self.assertEqual(attrs["business_email"], {"CustomerEmail": "$.Customer.BusinessEmailAddress"}, lang)
            for key, value in (("check_email", "$.Customer.EmailAddress"),
                               ("check_business_email", "$.Customer.BusinessEmailAddress")):
                cmp_ = by_id[CID[key]]
                self.assertEqual(cmp_["Type"], "Compare")
                self.assertEqual(cmp_["Parameters"], {"ComparisonValue": value})
                self.assertEqual([c["Condition"] for c in cmp_["Transitions"]["Conditions"]],
                                 [{"Operator": "TextContains", "Operands": ["@"]}])
                self.assertEqual([e["ErrorType"] for e in cmp_["Transitions"]["Errors"]], ["NoMatchingCondition"])
            self.assertEqual(by_id[CID["check_email"]]["Transitions"]["Conditions"][0]["NextAction"],
                             config.AGENTIC_CX_ACTION_ID)
            self.assertEqual(by_id[CID["check_business_email"]]["Transitions"]["Conditions"][0]["NextAction"],
                             CID["business_email"])
            self.assertEqual(by_id[CID["check_business_email"]]["Transitions"]["NextAction"], CID["no_email"])
        self.assertEqual(flowlib.customer_profile_ctx("zh-CN", MESSAGES)["CUSTOMER_NOT_FOUND_ATTRIBUTES"]
                         ["CustomerName"], "未找到")
        self.assertEqual(flowlib.customer_profile_ctx("en-US", MESSAGES)["CUSTOMER_NO_EMAIL_ATTRIBUTES"]
                         ["CustomerEmail"], "Not provided")

    def test_customer_blank_name_fallback(self):
        # Found profile without FirstName / LastName: CustomerName resolves to the format's separator
        # (" "), so a Compare after the found action replaces it with the localized no_name text.
        for lang, no_name in (("zh-CN", "未提供"), ("en-US", "Not provided")):
            _, by_id = inbound_for(lang)
            self.assertEqual(MESSAGES[lang]["screenpop"]["customer"]["no_name"], no_name)
            found = by_id[CID["found"]]["Transitions"]
            self.assertEqual(found["NextAction"], CID["check_name"], lang)
            self.assertEqual([e["NextAction"] for e in found["Errors"]], [CID["check_name"]], lang)
            cmp_ = by_id[CID["check_name"]]
            self.assertEqual(cmp_["Type"], "Compare")
            self.assertEqual(cmp_["Parameters"], {"ComparisonValue": "$.Attributes.CustomerName"}, lang)
            tr = cmp_["Transitions"]
            self.assertEqual(tr["Conditions"], [{"NextAction": CID["no_name"],
                                                 "Condition": {"Operator": "Equals", "Operands": [" "]}}], lang)
            # No match / error: keep the found name and continue to the email checks.
            self.assertEqual(tr["NextAction"], CID["check_email"], lang)
            self.assertEqual(tr["Errors"], [{"NextAction": CID["check_email"], "ErrorType": "NoMatchingCondition"}])
            no_name_action = by_id[CID["no_name"]]
            self.assertEqual(no_name_action["Type"], "UpdateContactAttributes")
            self.assertEqual(no_name_action["Parameters"], {"Attributes": {"CustomerName": no_name},
                                                            "TargetContact": "Current"}, lang)
            self.assertEqual(set(targets(no_name_action)), {CID["check_email"]}, lang)

    def test_customer_blank_name(self):
        self.assertEqual(flowlib.customer_blank_name("{LastName} {FirstName}"), " ")
        self.assertEqual(flowlib.customer_blank_name("{LastName}, {FirstName}"), ", ")
        for fmt in ("{LastName}", "{LastName}{FirstName}", "{Nick} x"):
            with self.assertRaises(FlowValidationError, msg=fmt):
                flowlib.customer_blank_name(fmt)

    def test_customer_fields_not_in_acxd_fields(self):
        self.assertEqual(config.FIELDS, ("DeviceName", "DeviceModel", "FaultDescription"))
        self.assertFalse(set(config.CUSTOMER_FIELDS) & set(config.FIELDS))
        self.assertEqual(config.SCREENPOP_FIELDS, config.CUSTOMER_FIELDS + config.FIELDS)
        self.assertEqual(flowlib.field_attributes(),
                         {f: "$.AgenticCX.ContextVariables." + f for f in config.FIELDS})

    def test_customer_name_expression(self):
        self.assertEqual(flowlib.customer_name_expression("{FirstName} {LastName}"),
                         "$.Customer.FirstName $.Customer.LastName")
        self.assertEqual(flowlib.customer_name_expression("{LastName}"), "$.Customer.LastName")

    def test_customer_name_expression_rejects(self):
        for fmt, needle in (("{LastName}{FirstName}", "adjacent"), ("{Nick}", "unknown"),
                            ("abc", "no {FirstName}"), ("{FirstName", "no {FirstName}"),
                            ("{FirstName} {Last", "unbalanced")):
            with self.assertRaises(FlowValidationError, msg=fmt) as cm:
                flowlib.customer_name_expression(fmt)
            self.assertIn(needle, str(cm.exception), fmt)


class AgenticCxTests(unittest.TestCase):
    def test_rebuild_classification(self):
        action = {"Identifier": "x", "Type": "AgenticCX", "Parameters": {},
                  "Transitions": {"NextAction": "old",
                                  "Errors": [{"ErrorType": "EscalationRequested", "NextAction": "o"},
                                             {"ErrorType": "IdleChat", "NextAction": "o"},
                                             {"ErrorType": "ChatTimeout", "NextAction": "o"},
                                             {"ErrorType": "NoMatchingError", "NextAction": "o"}],
                                  "Conditions": [{"NextAction": "o",
                                                  "Condition": {"Operator": "Equals", "Operands": ["Done"]}}]}}
        new, decisions = flowlib.rebuild_agentic_cx_transitions(action)
        tr = new["Transitions"]
        self.assertEqual(new["Identifier"], config.AGENTIC_CX_ACTION_ID)
        self.assertEqual(tr["NextAction"], ID05)
        self.assertEqual([e["NextAction"] for e in tr["Errors"]], [ID05, ID09, ID09, ID08])
        self.assertEqual(tr["Conditions"][0]["NextAction"], ID05)
        self.assertEqual(tr["Errors"][0]["ErrorType"], "EscalationRequested")
        self.assertTrue(any("kept NextAction" in d for d in decisions))
        self.assertEqual(action["Transitions"]["NextAction"], "old")  # input untouched

    def test_default_as_condition(self):
        action = {"Type": "A", "Transitions": {"Conditions": [
            {"NextAction": "o", "Condition": {"Operator": "Equals", "Operands": ["Default"]}}]}}
        new, decisions = flowlib.rebuild_agentic_cx_transitions(action)
        self.assertNotIn("NextAction", new["Transitions"])
        self.assertEqual(new["Transitions"]["Conditions"][0]["NextAction"], ID05)
        self.assertTrue(any("NextAction not added" in d for d in decisions))

    def test_no_next_no_conditions(self):
        new, decisions = flowlib.rebuild_agentic_cx_transitions({"Type": "A", "Transitions": {}})
        self.assertEqual(new["Transitions"]["NextAction"], ID05)
        self.assertTrue(any("added NextAction" in d for d in decisions))

    def flow(self):
        return {"Actions": [
            {"Identifier": "a1", "Type": "MessageParticipant", "Parameters": {"Text": "hi"}},
            {"Identifier": "a2", "Type": "ConnectAgenticCX", "Parameters": {"ApplicationId": "APP1"}},
            {"Identifier": "a3", "Type": "Other", "Parameters": {"X": "APP2"}},
        ], "Metadata": {"ActionMetadata": {"a2": {"position": {"x": 1}, "parameters": {"d": 1}}}}}

    def test_select_single_action(self):
        action, meta = flowlib.select_agentic_cx_action({"Type": "T", "Parameters": {}})
        self.assertEqual(action["Type"], "T")
        self.assertIsNone(meta)

    def test_select_rule_a(self):
        action, _ = flowlib.select_agentic_cx_action(self.flow(), application_id="APP2")
        self.assertEqual(action["Identifier"], "a3")

    def test_select_rule_b_and_metadata(self):
        action, meta = flowlib.select_agentic_cx_action(self.flow())
        self.assertEqual(action["Identifier"], "a2")
        self.assertEqual(meta, {"parameters": {"d": 1}})

    def test_select_rule_c(self):
        f = self.flow()
        f["Actions"][2]["Type"] = "AnotherAgentic"
        with self.assertRaises(flowlib.AgenticCxSelectionError) as cm:
            flowlib.select_agentic_cx_action(f)
        self.assertEqual(len(cm.exception.candidates), 3)
        f["Actions"] = [f["Actions"][0]]
        with self.assertRaises(flowlib.AgenticCxSelectionError):
            flowlib.select_agentic_cx_action(f)

    def test_select_identifier(self):
        action, _ = flowlib.select_agentic_cx_action(self.flow(), identifier="a1")
        self.assertEqual(action["Identifier"], "a1")
        with self.assertRaises(flowlib.AgenticCxSelectionError):
            flowlib.select_agentic_cx_action(self.flow(), identifier="nope")

    def test_check_captured_action(self):
        a = {"Parameters": {"App": "APP1", "Alias": "LIVE-ALIAS"}}
        self.assertEqual(flowlib.check_captured_action(a, "APP1", ["production", "LIVE-ALIAS"]), [])
        b = {"Parameters": {"App": "APP1", "Env": "Production"}}
        self.assertEqual(flowlib.check_captured_action(b, "APP1", ["production", None]), [])
        c = {"Parameters": {"App": "APP1"}}
        self.assertEqual(len(flowlib.check_captured_action(c, "APP1", ["production", "LIVE"])), 1)
        self.assertEqual(len(flowlib.check_captured_action(b, "OTHER", ["production"])), 1)

    def test_rebuild_real_action(self):  # live flow 'AI Agent - ACXD'
        new, _ = flowlib.rebuild_agentic_cx_transitions(real_action())
        tr = new["Transitions"]
        self.assertEqual(tr["NextAction"], ID05)
        self.assertEqual(tr["Conditions"][0]["NextAction"], ID05)
        errors = {e["ErrorType"]: e["NextAction"] for e in tr["Errors"]}
        self.assertEqual(errors, {"NoMatchingError": ID08, "NoMatchingCondition": ID05,
                                  "InputTimeLimitExceeded": ID09})

    def test_build_action_matches_real_shape(self):
        real = real_action()
        built = flowlib.build_agentic_cx_action("WS", "APP", "ALIAS")
        self.assertEqual(built["Identifier"], config.AGENTIC_CX_ACTION_ID)
        self.assertEqual(built["Type"], real["Type"])
        self.assertEqual(set(built["Parameters"]), set(real["Parameters"]))
        self.assertEqual(set(built["Parameters"]["AgentConfiguration"]),
                         set(real["Parameters"]["AgentConfiguration"]))
        self.assertEqual(built["Parameters"]["AgentConfiguration"],
                         {"WorkspaceId": "WS", "ApplicationId": "APP", "Alias": "ALIAS"})
        self.assertEqual({k: v for k, v in built["Parameters"].items() if k != "AgentConfiguration"},
                         {k: v for k, v in real["Parameters"].items() if k != "AgentConfiguration"})
        self.assertEqual({e["ErrorType"] for e in built["Transitions"]["Errors"]},
                         {e["ErrorType"] for e in real["Transitions"]["Errors"]})
        self.assertEqual([c["Condition"] for c in built["Transitions"]["Conditions"]],
                         [c["Condition"] for c in real["Transitions"]["Conditions"]])
        built["Parameters"]["AudioFillerConfiguration"]["Enabled"] = False  # deep-copied
        self.assertTrue(config.AGENTIC_CX_STATIC_PARAMETERS["AudioFillerConfiguration"]["Enabled"])

    def test_build_action_empty_argument(self):
        with self.assertRaises(FlowValidationError) as cm:
            flowlib.build_agentic_cx_action("WS", "APP", "")
        self.assertIn("alias", str(cm.exception))
        with self.assertRaises(FlowValidationError):
            flowlib.build_agentic_cx_action(None, "APP", "ALIAS")

    def test_action_metadata(self):
        meta = flowlib.agentic_cx_action_metadata("WS", "AgenticCareSelfService", "production")
        ac = meta["parameters"]["AgentConfiguration"]
        self.assertIs(meta["isFriendlyName"], False)
        self.assertEqual(ac["WorkspaceId"], {"displayName": "WS", "useDropdown": "true"})
        self.assertEqual(ac["ApplicationId"], {"displayName": "AgenticCareSelfService"})
        self.assertEqual(ac["Alias"], {"displayName": "Production"})
        self.assertIn("SpeechRecognitionConfiguration", meta["parameters"])

    def test_fallback_inbound_validates(self):
        flow = flowlib.render_inbound_flow(inbound_values(), fallback_action(), None)
        act = [a for a in flow["Actions"] if a["Identifier"] == config.AGENTIC_CX_ACTION_ID][0]
        self.assertEqual(act["Type"], config.UNVERIFIED_ACTION_TYPE)
        self.assertEqual(act["Transitions"]["NextAction"], ID05)
        self.assertEqual(act["Transitions"]["Errors"][0]["NextAction"], ID08)
        json.dumps(flow)


if __name__ == "__main__":
    unittest.main()
