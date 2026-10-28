import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "check_flows.py")
FIELDS = ("DeviceName", "DeviceModel", "FaultDescription")
CUSTOMER_FIELDS = ("CustomerName", "CustomerPhone", "CustomerEmail")

INBOUND = {
    "StartAction": "a",
    "Actions": [
        {"Identifier": "a", "Type": "UpdateContactEventHooks",
         "Parameters": {"EventHooks": {"DefaultAgentUI": "arn:sp"}},
         "Transitions": {"NextAction": "p", "Errors": [{"NextAction": "p", "ErrorType": "NoMatchingError"}]}},
        {"Identifier": "b", "Type": "UpdateContactAttributes",
         "Parameters": {"Attributes": {f: f"$.AgenticCX.ContextVariables.{f}" for f in FIELDS}},
         "Transitions": {"NextAction": "c", "Conditions": [{"NextAction": "c"}]}},
        {"Identifier": "c", "Type": "DisconnectParticipant", "Transitions": {}},
        {"Identifier": "d", "Type": "ConnectParticipantWithAgenticCX",
         "Parameters": {"AgentConfiguration": {"WorkspaceId": "WS", "ApplicationId": "APP", "Alias": "AL"}},
         "Transitions": {"NextAction": "b", "Errors": [{"NextAction": "b", "ErrorType": "NoMatchingCondition"}]}},
        # Customer Profiles lookup (a -> p -> q -> d); appended so the indexes above stay stable.
        {"Identifier": "p", "Type": "GetCustomerProfile",
         "Parameters": {"ProfileRequestData": {"IdentifierName": "_phone",
                                               "IdentifierValue": "$.CustomerEndpoint.Address"},
                        "ProfileResponseData": ["FirstName", "LastName", "EmailAddress"]},
         "Transitions": {"NextAction": "q", "Errors": [
             {"NextAction": "q", "ErrorType": "NoneFoundError"},
             {"NextAction": "q", "ErrorType": "MultipleFoundError"},
             {"NextAction": "q", "ErrorType": "NoMatchingError"}]}},
        {"Identifier": "q", "Type": "UpdateContactAttributes",
         "Parameters": {"Attributes": {"CustomerName": "$.Customer.FirstName $.Customer.LastName",
                                       "CustomerPhone": "$.CustomerEndpoint.Address",
                                       "CustomerEmail": "$.Customer.EmailAddress"}},
         "Transitions": {"NextAction": "r", "Errors": [{"NextAction": "r", "ErrorType": "NoMatchingError"}]}},
        # Blank-name fallback (q -> r -> s -> d).
        {"Identifier": "r", "Type": "Compare", "Parameters": {"ComparisonValue": "$.Attributes.CustomerName"},
         "Transitions": {"NextAction": "d",
                         "Conditions": [{"NextAction": "s",
                                         "Condition": {"Operator": "Equals", "Operands": [" "]}}],
                         "Errors": [{"NextAction": "d", "ErrorType": "NoMatchingCondition"}]}},
        {"Identifier": "s", "Type": "UpdateContactAttributes",
         "Parameters": {"Attributes": {"CustomerName": "Not provided"}},
         "Transitions": {"NextAction": "d", "Errors": [{"NextAction": "d", "ErrorType": "NoMatchingError"}]}},
    ],
    "Metadata": {"ActionMetadata": {"a": {}, "p": {}, "q": {}, "r": {}, "s": {}, "b": {}, "c": {}, "d": {}}},
}
SCREENPOP = {
    "StartAction": "x",
    "Actions": [
        {"Identifier": "x", "Type": "ShowView",
         "Parameters": {"ViewData": {"AttributeBar": [{"Value": f"$.Attributes.{f}"}
                                                      for f in CUSTOMER_FIELDS + FIELDS]}},
         "Transitions": {"NextAction": "y"}},
        {"Identifier": "y", "Type": "DisconnectParticipant", "Transitions": {}},
    ],
    "Metadata": {"ActionMetadata": {"x": {}, "y": {}}},
}
HAIKU = "anthropic.claude-haiku-4-5"
CONT = {"conditions": [{"left": {"type": "node_status"}, "operator": "eq",
                        "right": {"type": "constant", "value": "continuation"}}]}


def gj_edge(i, target):
    return {"nodeId": target, "conditions": [{"left": {"type": "system", "name": "System.gjConditionIndex"},
                                              "operator": "eq", "right": {"type": "constant", "value": i}}]}


# Minimal valid chain: start -> generative_journey (2 exits) -> 3 generative_text -> escalate.
ACXD = {"nodes": {
    "n1": {"nodeId": "n1", "type": "start", "childNodes": [{"nodeId": "n2"}]},
    "n2": {"nodeId": "n2", "type": "generative_journey",
           "metadata": {"generativeJourney": {"modelType": HAIKU, "prompt": "p", "tools": [],
                                              "exitConditions": [{"name": "a", "prompt": "x"},
                                                                 {"name": "b", "prompt": "y"}]}},
           "childNodes": [gj_edge(0, "n3"), gj_edge(1, "n3")]},
    "n3": {"nodeId": "n3", "type": "generative_text",
           "metadata": {"generativeText": {"name": FIELDS[0], "modelType": HAIKU, "prompt": "p"}},
           "childNodes": [{"nodeId": "n4"}]},
    "n4": {"nodeId": "n4", "type": "generative_text",
           "metadata": {"generativeText": {"name": FIELDS[1], "modelType": HAIKU, "prompt": "p"}},
           "childNodes": [{"nodeId": "n5"}]},
    "n5": {"nodeId": "n5", "type": "generative_text",
           "metadata": {"generativeText": {"name": FIELDS[2], "modelType": HAIKU, "prompt": "p"}},
           "childNodes": [{"nodeId": "n6"}]},
    "n6": {"nodeId": "n6", "type": "escalate",
           "metadata": {"stateModifications": [
               {"type": "context", "name": f, "modification": "set",
                "value": {"type": "constant", "value": "{" + f + ":NLX.Local}"}} for f in FIELDS]},
           "childNodes": [CONT]},
}}


class CheckFlowsTests(unittest.TestCase):
    def run_checker(self, inbound=INBOUND, screenpop=SCREENPOP, acxd=ACXD, extra=None):
        with tempfile.TemporaryDirectory() as tmp:
            for name, obj in (("inbound_acxd_flow.json", inbound), ("screenpop_flow.json", screenpop),
                              ("acxd_flow.json", acxd)):
                with open(os.path.join(tmp, name), "w", encoding="utf-8") as fh:
                    json.dump(obj, fh)
            for name, text in (extra or {}).items():
                with open(os.path.join(tmp, name), "w", encoding="utf-8") as fh:
                    fh.write(text)
            return subprocess.run([sys.executable, SCRIPT, tmp], capture_output=True, text=True)

    def test_ok(self):
        r = self.run_checker()
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("OK", r.stdout)

    def test_dangling_next_action(self):
        f = copy.deepcopy(INBOUND)
        f["Actions"][0]["Transitions"]["Errors"][0]["NextAction"] = "zz"
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)
        f = copy.deepcopy(INBOUND)
        f["Actions"][1]["Transitions"]["Conditions"][0]["NextAction"] = "zz"
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)

    def test_extra_action_metadata(self):
        f = copy.deepcopy(SCREENPOP)
        f["Metadata"]["ActionMetadata"]["q"] = {}
        self.assertEqual(self.run_checker(screenpop=f).returncode, 1)

    def test_two_start_nodes(self):
        a = copy.deepcopy(ACXD)
        a["nodes"]["n9"] = {"nodeId": "n9", "type": "start", "childNodes": [{"nodeId": "n2"}]}
        self.assertEqual(self.run_checker(acxd=a).returncode, 1)

    def test_residual_placeholder(self):
        f = copy.deepcopy(INBOUND)
        f["Actions"][2]["Type"] = "{{X}}"
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)

    def test_wrong_attributes(self):
        f = copy.deepcopy(INBOUND)
        f["Actions"][1]["Parameters"]["Attributes"] = "{}"
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)

    def test_customer_profile_lookup_required(self):
        no_lookup = copy.deepcopy(INBOUND)
        no_lookup["Actions"] = [a for a in no_lookup["Actions"] if a["Identifier"] != "p"]
        no_lookup["Actions"][0]["Transitions"] = {"NextAction": "q"}
        del no_lookup["Metadata"]["ActionMetadata"]["p"]
        r = self.run_checker(inbound=no_lookup)
        self.assertEqual(r.returncode, 1)
        self.assertIn("GetCustomerProfile", r.stdout)
        unrouted = copy.deepcopy(INBOUND)
        lookup = [a for a in unrouted["Actions"] if a["Identifier"] == "p"][0]
        lookup["Transitions"]["Errors"] = [e for e in lookup["Transitions"]["Errors"]
                                           if e["ErrorType"] != "NoneFoundError"]
        r = self.run_checker(inbound=unrouted)
        self.assertEqual(r.returncode, 1)
        self.assertIn("NoneFoundError", r.stdout)
        wrong_id = copy.deepcopy(INBOUND)
        [a for a in wrong_id["Actions"] if a["Identifier"] == "p"][0]["Parameters"]["ProfileRequestData"] = {
            "IdentifierName": "_email", "IdentifierValue": "$.CustomerEndpoint.Address"}
        self.assertEqual(self.run_checker(inbound=wrong_id).returncode, 1)

    def test_customer_attributes_required(self):
        f = copy.deepcopy(INBOUND)
        q = [a for a in f["Actions"] if a["Identifier"] == "q"][0]
        del q["Parameters"]["Attributes"]["CustomerEmail"]
        r = self.run_checker(inbound=f)
        self.assertEqual(r.returncode, 1)
        self.assertIn("CustomerEmail", r.stdout)

    def test_blank_name_fallback_required(self):
        no_compare = copy.deepcopy(INBOUND)
        no_compare["Actions"] = [a for a in no_compare["Actions"] if a["Identifier"] not in ("r", "s")]
        [a for a in no_compare["Actions"] if a["Identifier"] == "q"][0]["Transitions"] = {"NextAction": "d"}
        for k in ("r", "s"):
            del no_compare["Metadata"]["ActionMetadata"][k]
        r = self.run_checker(inbound=no_compare)
        self.assertEqual(r.returncode, 1)
        self.assertIn("blank-name fallback", r.stdout)
        raw = copy.deepcopy(INBOUND)  # fallback writes a JSONPath, not a literal
        [a for a in raw["Actions"] if a["Identifier"] == "s"][0]["Parameters"]["Attributes"] = {
            "CustomerName": "$.Customer.FirstName"}
        self.assertEqual(self.run_checker(inbound=raw).returncode, 1)

    def test_screenpop_customer_fields_first(self):
        missing = copy.deepcopy(SCREENPOP)
        missing["Actions"][0]["Parameters"]["ViewData"]["AttributeBar"] = [
            {"Value": f"$.Attributes.{f}"} for f in ("CustomerName", "CustomerPhone") + FIELDS]
        self.assertEqual(self.run_checker(screenpop=missing).returncode, 1)
        device_first = copy.deepcopy(SCREENPOP)
        device_first["Actions"][0]["Parameters"]["ViewData"]["AttributeBar"] = [
            {"Value": f"$.Attributes.{f}"} for f in FIELDS + CUSTOMER_FIELDS]
        self.assertEqual(self.run_checker(screenpop=device_first).returncode, 1)
        device_only = copy.deepcopy(SCREENPOP)
        device_only["Actions"][0]["Parameters"]["ViewData"]["AttributeBar"] = [
            {"Value": f"$.Attributes.{f}"} for f in FIELDS]
        self.assertEqual(self.run_checker(screenpop=device_only).returncode, 1)

    def test_agentic_cx_action_required(self):
        f = copy.deepcopy(INBOUND)
        del f["Actions"][3]["Parameters"]["AgentConfiguration"]["Alias"]
        r = self.run_checker(inbound=f)
        self.assertEqual(r.returncode, 1)
        self.assertIn("Alias", r.stdout)
        f = copy.deepcopy(INBOUND)
        f["Actions"][3]["Parameters"]["AgentConfiguration"]["ApplicationId"] = ""
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)
        f = copy.deepcopy(INBOUND)
        f["Actions"][3]["Type"] = "UNVERIFIED_AGENTIC_CX_ACTION"
        self.assertEqual(self.run_checker(inbound=f).returncode, 1)

    def test_continuation_child_without_node_id(self):
        r = self.run_checker(acxd=ACXD)  # escalate n6 has a nodeId-less continuation child
        self.assertEqual(r.returncode, 0, r.stdout)
        b = copy.deepcopy(ACXD)
        b["nodes"]["n7"] = {"nodeId": "n7", "type": "basic", "childNodes": [copy.deepcopy(CONT)]}
        self.assertEqual(self.run_checker(acxd=b).returncode, 1)
        c = copy.deepcopy(ACXD)
        c["nodes"]["n7"] = {"nodeId": "n7", "type": "basic", "childNodes": [{"conditions": []}]}
        self.assertEqual(self.run_checker(acxd=c).returncode, 1)
        d = copy.deepcopy(ACXD)
        d["nodes"]["n6"]["childNodes"] = [{"conditions": []}]
        self.assertEqual(self.run_checker(acxd=d).returncode, 1)

    def test_terminate_type(self):
        a = copy.deepcopy(ACXD)
        a["nodes"]["n7"] = {"nodeId": "n7", "type": "terminate"}
        self.assertEqual(self.run_checker(acxd=a).returncode, 0)

    def test_user_input_node(self):
        a = copy.deepcopy(ACXD)
        a["nodes"]["n7"] = {"nodeId": "n7", "type": "user_input", "childNodes": [{"nodeId": "n2"}]}
        a["nodes"]["n1"]["childNodes"] = [{"nodeId": "n7"}]
        r = self.run_checker(acxd=a)
        self.assertEqual(r.returncode, 1)
        self.assertIn("user_input", r.stdout)

    def test_non_haiku_model(self):
        for node, key in (("n2", "generativeJourney"), ("n4", "generativeText")):
            a = copy.deepcopy(ACXD)
            a["nodes"][node]["metadata"][key]["modelType"] = "anthropic.claude-sonnet-5"
            r = self.run_checker(acxd=a)
            self.assertEqual(r.returncode, 1, node)
            self.assertIn("anthropic.claude-sonnet-5", r.stdout)

    def test_missing_state_modification(self):
        a = copy.deepcopy(ACXD)
        mods = a["nodes"]["n6"]["metadata"]["stateModifications"]
        a["nodes"]["n6"]["metadata"]["stateModifications"] = [m for m in mods if m["name"] != "FaultDescription"]
        r = self.run_checker(acxd=a)
        self.assertEqual(r.returncode, 1)
        self.assertIn("FaultDescription", r.stdout)
        b = copy.deepcopy(ACXD)
        b["nodes"]["n5"]["metadata"]["generativeText"]["name"] = "Other"
        r = self.run_checker(acxd=b)
        self.assertEqual(r.returncode, 1)
        self.assertIn("generativeText.name FaultDescription", r.stdout)

    def test_missing_gj_condition_edge(self):
        a = copy.deepcopy(ACXD)
        a["nodes"]["n2"]["childNodes"] = [gj_edge(0, "n3")]
        r = self.run_checker(acxd=a)
        self.assertEqual(r.returncode, 1)
        self.assertIn("exit condition 1", r.stdout)

    def test_invalid_json_file(self):
        self.assertEqual(self.run_checker(extra={"broken.json": "{"}).returncode, 1)


if __name__ == "__main__":
    unittest.main()
