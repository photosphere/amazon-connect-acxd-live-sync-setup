#!/usr/bin/env python3
"""Independent referential-integrity checker for generated flow JSON (design §7.1 step 5).

The inbound flow must look up the caller in Customer Profiles (every outcome routed) and write
CustomerName / CustomerPhone / CustomerEmail; the screen pop shows those first, then the device fields.
The ACXD flow must also use Generative Journey / Generative Text with Claude Haiku only,
contain no User input node, and carry the DeviceName / DeviceModel / FaultDescription chain
(generative_text output -> escalate set-context) that the Connect flows consume.

Deliberately does NOT import flowlib or config, so it cannot share their bugs.

Usage: python3 tools/check_flows.py [dir]     (default: output/dry-run)
Exit 0 and print OK when every check passes; otherwise print each problem and exit 1.
"""

import json
import sys
from pathlib import Path

# Independent copies of the expected values (do not import from the project).
FIELDS = ("DeviceName", "DeviceModel", "FaultDescription")
EXPECTED_ATTRIBUTES = {f: f"$.AgenticCX.ContextVariables.{f}" for f in FIELDS}
# Caller details from Customer Profiles, shown first in the screen pop.
CUSTOMER_FIELDS = ("CustomerName", "CustomerPhone", "CustomerEmail")
SCREENPOP_FIELDS = CUSTOMER_FIELDS + FIELDS
PROFILE_REQUEST = {"IdentifierName": "_phone", "IdentifierValue": "$.CustomerEndpoint.Address"}
PROFILE_ERRORS = {"NoneFoundError", "MultipleFoundError", "NoMatchingError"}
NODE_TYPES = {
    "basic", "choice", "data_request", "define", "end", "escalate", "generative_journey",
    "generative_task", "generative_text", "intent_capture", "knowledge_base", "loop", "multimodal",
    "note", "redirect", "split", "start", "transform", "user_choice", "user_input", "wait",
    "terminate",  # live API type of the Exit application node
}
CONTINUATION_PARENTS = ("escalate", "redirect")
# Claude Haiku model id of the reference app (flow Maintplink); every modelType must be this.
HAIKU_MODEL = "anthropic.claude-haiku-4-5"
# The caller's turns are taken by the Generative Journey: no User input node may exist.
FORBIDDEN_TYPES = {"user_input"}
# Real Agentic CX block action type (live Connect flows) and its required ids.
AGENTIC_CX_TYPE = "ConnectParticipantWithAgenticCX"
AGENT_CONFIGURATION_KEYS = ("WorkspaceId", "ApplicationId", "Alias")


def is_continuation(child):
    """Child edge conditioned on node_status == continuation (may have no nodeId)."""
    for c in child.get("conditions") or []:
        if not isinstance(c, dict):
            continue
        left, right = c.get("left") or {}, c.get("right") or {}
        if left.get("type") == "node_status" and right.get("value") == "continuation":
            return True
    return False


def check_connect_flow(name, flow, problems):
    actions = flow.get("Actions")
    if not isinstance(actions, list) or not actions:
        problems.append(f"{name}: no Actions")
        return
    ids = [a.get("Identifier") for a in actions]
    if len(ids) != len(set(ids)):
        dups = sorted({i for i in ids if ids.count(i) > 1}, key=str)
        problems.append(f"{name}: duplicate Identifiers {dups}")
    idset = set(ids)
    if flow.get("StartAction") not in idset:
        problems.append(f"{name}: StartAction {flow.get('StartAction')} not in Actions")
    for a in actions:
        tr = a.get("Transitions") or {}
        targets = []
        if "NextAction" in tr:
            targets.append(("NextAction", tr["NextAction"]))
        for kind in ("Conditions", "Errors"):
            for i, e in enumerate(tr.get(kind) or []):
                if "NextAction" in e:
                    targets.append((f"{kind}[{i}].NextAction", e["NextAction"]))
        for where, t in targets:
            if t not in idset:
                problems.append(f"{name}: {a.get('Identifier')} {where} -> missing {t}")
    for key in ((flow.get("Metadata") or {}).get("ActionMetadata") or {}):
        if key not in idset:
            problems.append(f"{name}: ActionMetadata key {key} is not an Identifier")
    if "{{" in json.dumps(flow, ensure_ascii=False):
        problems.append(f"{name}: residual '{{{{' placeholder")


def check_inbound(name, flow, problems):
    agentic = [a for a in flow.get("Actions", []) if a.get("Type") == AGENTIC_CX_TYPE]
    if len(agentic) != 1:
        problems.append(f"{name}: expected 1 {AGENTIC_CX_TYPE} action, found {len(agentic)}")
    else:
        conf = (agentic[0].get("Parameters") or {}).get("AgentConfiguration")
        conf = conf if isinstance(conf, dict) else {}
        for key in AGENT_CONFIGURATION_KEYS:
            if not isinstance(conf.get(key), str) or not conf.get(key):
                problems.append(f"{name}: {AGENTIC_CX_TYPE} AgentConfiguration.{key} is missing or empty")
    updates = [a for a in flow.get("Actions", []) if a.get("Type") == "UpdateContactAttributes"]
    attrs = [a for a in updates
             if "$.AgenticCX.ContextVariables." in json.dumps(a.get("Parameters", {}).get("Attributes"))]
    if len(attrs) != 1:
        problems.append(f"{name}: expected 1 UpdateContactAttributes action reading "
                        f"$.AgenticCX.ContextVariables, found {len(attrs)}")
    elif attrs[0].get("Parameters", {}).get("Attributes") != EXPECTED_ATTRIBUTES:
        problems.append(f"{name}: UpdateContactAttributes.Attributes != {EXPECTED_ATTRIBUTES}")
    check_customer_profile(name, flow, updates, problems)
    hooks = [a for a in flow.get("Actions", []) if a.get("Type") == "UpdateContactEventHooks"]
    if not any("DefaultAgentUI" in (h.get("Parameters", {}).get("EventHooks") or {}) for h in hooks):
        problems.append(f"{name}: no UpdateContactEventHooks with DefaultAgentUI")


def check_customer_profile(name, flow, updates, problems):
    """One GetCustomerProfile by caller number, every outcome routed, customer attributes written."""
    lookups = [a for a in flow.get("Actions", []) if a.get("Type") == "GetCustomerProfile"]
    if len(lookups) != 1:
        problems.append(f"{name}: expected 1 GetCustomerProfile action, found {len(lookups)}")
    else:
        lookup = lookups[0]
        if (lookup.get("Parameters") or {}).get("ProfileRequestData") != PROFILE_REQUEST:
            problems.append(f"{name}: GetCustomerProfile.ProfileRequestData != {PROFILE_REQUEST}")
        tr = lookup.get("Transitions") or {}
        if not tr.get("NextAction"):
            problems.append(f"{name}: GetCustomerProfile has no NextAction")
        routed = {e.get("ErrorType") for e in tr.get("Errors") or [] if e.get("NextAction")}
        missing = sorted(PROFILE_ERRORS - routed)
        if missing:
            problems.append(f"{name}: GetCustomerProfile errors {missing} are not routed")
    written = set()
    for a in updates:
        attributes = (a.get("Parameters") or {}).get("Attributes")
        if isinstance(attributes, dict):
            written.update(attributes)
    missing = [f for f in CUSTOMER_FIELDS if f not in written]
    if missing:
        problems.append(f"{name}: no UpdateContactAttributes writes {missing}")
    # A found profile without names must not show a blank name: a Compare on the written
    # CustomerName routes an Equals match to an action setting a literal (non-$.) CustomerName.
    by_id = {a.get("Identifier"): a for a in flow.get("Actions", [])}
    name_ref = f"$.Attributes.{CUSTOMER_FIELDS[0]}"
    fallback = False
    for a in flow.get("Actions", []):
        if a.get("Type") != "Compare" or (a.get("Parameters") or {}).get("ComparisonValue") != name_ref:
            continue
        for c in (a.get("Transitions") or {}).get("Conditions") or []:
            target = by_id.get(c.get("NextAction")) or {}
            value = ((target.get("Parameters") or {}).get("Attributes") or {}).get(CUSTOMER_FIELDS[0])
            if ((c.get("Condition") or {}).get("Operator") == "Equals"
                    and target.get("Type") == "UpdateContactAttributes"
                    and isinstance(value, str) and value.strip() and not value.startswith("$.")):
                fallback = True
    if not fallback:
        problems.append(f"{name}: no blank-name fallback (Compare {name_ref} Equals -> literal "
                        f"{CUSTOMER_FIELDS[0]})")


def check_screenpop(name, flow, problems):
    views = [a for a in flow.get("Actions", []) if a.get("Type") == "ShowView"]
    if len(views) != 1:
        problems.append(f"{name}: expected 1 ShowView action, found {len(views)}")
        return
    bar = views[0].get("Parameters", {}).get("ViewData", {}).get("AttributeBar")
    values = [b.get("Value") for b in bar] if isinstance(bar, list) else None
    if values != [f"$.Attributes.{f}" for f in SCREENPOP_FIELDS]:
        problems.append(f"{name}: ShowView AttributeBar values {values} != customer fields then device fields "
                        f"{list(SCREENPOP_FIELDS)}")


def check_acxd_flow(name, body, problems):
    nodes = body.get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        problems.append(f"{name}: no nodes")
        return
    starts = 0
    for key, node in nodes.items():
        if node.get("nodeId") != key:
            problems.append(f"{name}: key {key} != nodeId {node.get('nodeId')}")
        if node.get("type") not in NODE_TYPES:
            problems.append(f"{name}: node {key} unknown type {node.get('type')}")
        if node.get("type") == "start":
            starts += 1
        for c in node.get("childNodes") or []:
            if (c.get("nodeId") is None and node.get("type") in CONTINUATION_PARENTS
                    and is_continuation(c)):
                continue
            if c.get("nodeId") not in nodes:
                problems.append(f"{name}: node {key} child -> missing {c.get('nodeId')}")
    if starts != 1:
        problems.append(f"{name}: expected exactly 1 start node, found {starts}")
    if "{{" in json.dumps(body, ensure_ascii=False):
        problems.append(f"{name}: residual '{{{{' placeholder")
    check_generative(name, nodes, problems)


def model_types(obj):
    """Every value of a 'modelType' key anywhere in obj."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "modelType":
                yield v
            yield from model_types(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from model_types(v)


def check_generative(name, nodes, problems):
    """No User input, Haiku everywhere, routed GJ exits, and the data chain of every FIELD."""
    for key, node in nodes.items():
        if node.get("type") in FORBIDDEN_TYPES:
            problems.append(f"{name}: node {key} has forbidden type {node.get('type')}")
    for model in model_types(nodes):
        if model != HAIKU_MODEL:
            problems.append(f"{name}: modelType {model!r} is not {HAIKU_MODEL}")
    gjs = [(k, n) for k, n in nodes.items() if n.get("type") == "generative_journey"]
    if len(gjs) != 1:
        problems.append(f"{name}: expected exactly 1 generative_journey node, found {len(gjs)}")
    for key, node in gjs:
        gj = (node.get("metadata") or {}).get("generativeJourney") or {}
        if gj.get("modelType") != HAIKU_MODEL:
            problems.append(f"{name}: generative_journey {key} has no {HAIKU_MODEL} modelType")
        routed = set()
        for c in node.get("childNodes") or []:
            for cond in c.get("conditions") or []:
                if (cond.get("left") or {}).get("name") == "System.gjConditionIndex":
                    routed.add((cond.get("right") or {}).get("value"))
        for i in range(len(gj.get("exitConditions") or [])):
            if i not in routed:
                problems.append(f"{name}: generative_journey {key} exit condition {i} has no "
                                "System.gjConditionIndex edge")
    gt_names = {((n.get("metadata") or {}).get("generativeText") or {}).get("name")
                for n in nodes.values() if n.get("type") == "generative_text"}
    set_ctx = {}
    for n in nodes.values():
        if n.get("type") == "escalate":
            for m in (n.get("metadata") or {}).get("stateModifications") or []:
                if m.get("type") == "context" and m.get("modification") == "set":
                    set_ctx[m.get("name")] = (m.get("value") or {}).get("value")
    for f in FIELDS:
        if f not in gt_names:
            problems.append(f"{name}: no generative_text node with generativeText.name {f}")
        if set_ctx.get(f) != "{" + f + ":NLX.Local}":
            problems.append(f"{name}: no escalate state modification setting context {f} to {{{f}:NLX.Local}}")


def main(argv):
    d = Path(argv[1] if len(argv) > 1 else "output/dry-run")
    problems = []
    loaded = {}
    files = sorted(d.glob("*.json"))
    if not files:
        problems.append(f"no *.json files in {d}")
    for p in files:
        try:
            loaded[p.name] = json.loads(p.read_text(encoding="utf-8"))
        except ValueError as exc:
            problems.append(f"{p.name}: invalid JSON: {exc}")
    for name, extra in (("screenpop_flow.json", check_screenpop), ("inbound_acxd_flow.json", check_inbound)):
        if name in loaded:
            check_connect_flow(name, loaded[name], problems)
            extra(name, loaded[name], problems)
        else:
            problems.append(f"{name}: missing")
    if "acxd_flow.json" in loaded:
        check_acxd_flow("acxd_flow.json", loaded["acxd_flow.json"], problems)
    else:
        problems.append("acxd_flow.json: missing")
    if problems:
        for prob in problems:
            print(prob)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
