"""Template loading, placeholder substitution, validators and Agentic CX action handling.

Placeholder rules (design §6.2.1):
  1. whole-value placeholder: a JSON string value that is exactly "{{NAME}}" is
     replaced by copy.deepcopy(ctx[NAME]) (type preserved);
  2. inline placeholder: "{{NAME}}" inside a longer string, ctx[NAME] must be str;
  3. marker: "Type": "{{AGENTIC_CX_ACTION}}" in the inbound template is spliced,
     never substituted; AGENTIC_CX_ACTION is a reserved ctx key.
Order: splice -> collect & check keys -> single-pass substitution -> residual
"{{" check -> post-processing / validators.
"""

from __future__ import annotations

import copy
import html
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import config

logger = logging.getLogger("flowlib")

BASE_DIR = Path(__file__).resolve().parent

INBOUND_TEMPLATE = "flows/inbound_acxd_flow.json"
SCREENPOP_TEMPLATE = "flows/screenpop_flow.json"
ACXD_TEMPLATE = "acxd/acxd_flow.json"
MESSAGES_FILE = "acxd/messages.json"
FALLBACK_FRAGMENT = "flows/fragments/agentic_cx_action.unverified.json"

MARKER_NAME = "AGENTIC_CX_ACTION"
MARKER = "{{" + MARKER_NAME + "}}"
PLACEHOLDER_RE = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")
UUID4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")

# Inbound flow Identifiers used by the Agentic CX transition rebuild (§4).
INBOUND_IDS = {
    "attrs": "d1a9e0c0-0000-4000-8000-000000000005",
    "error": "d1a9e0c0-0000-4000-8000-000000000008",
    "idle": "d1a9e0c0-0000-4000-8000-000000000009",
}
_TARGET_NAMES = {"attrs": "SetContactAttributes", "error": "ErrorMessage", "idle": "IdleTimeoutMessage"}

# Inbound flow Identifiers of the Customer Profiles lookup chain (between TTS 003 and Agentic CX 004).
INBOUND_CUSTOMER_IDS = {
    "lookup": "d1a9e0c0-0000-4000-8000-00000000000b",
    "found": "d1a9e0c0-0000-4000-8000-00000000000c",
    "check_name": "d1a9e0c0-0000-4000-8000-000000000012",
    "no_name": "d1a9e0c0-0000-4000-8000-000000000013",
    "check_email": "d1a9e0c0-0000-4000-8000-00000000000d",
    "check_business_email": "d1a9e0c0-0000-4000-8000-00000000000e",
    "business_email": "d1a9e0c0-0000-4000-8000-00000000000f",
    "no_email": "d1a9e0c0-0000-4000-8000-000000000010",
    "not_found": "d1a9e0c0-0000-4000-8000-000000000011",
}
# Whole-value placeholders of the customer UpdateContactAttributes actions (customer_profile_ctx).
CUSTOMER_PLACEHOLDERS = frozenset({
    "CUSTOMER_FOUND_ATTRIBUTES", "CUSTOMER_BUSINESS_EMAIL_ATTRIBUTES",
    "CUSTOMER_NO_EMAIL_ATTRIBUTES", "CUSTOMER_NOT_FOUND_ATTRIBUTES",
    "CUSTOMER_NAME_ATTRIBUTE", "CUSTOMER_BLANK_NAME", "CUSTOMER_NO_NAME_ATTRIBUTES",
})

# §6.2.1: the exact placeholder set of each template (the marker is not a placeholder).
TEMPLATE_PLACEHOLDERS = {
    INBOUND_TEMPLATE: frozenset({
        "FLOW_NAME", "SCREENPOP_FLOW_ARN", "SCREENPOP_FLOW_NAME", "TTS_VOICE", "TTS_ENGINE",
        "FIELD_ATTRIBUTES", "QUEUE_ARN", "QUEUE_DISPLAY_NAME", "ERROR_MESSAGE",
        "IDLE_TIMEOUT_MESSAGE", *CUSTOMER_PLACEHOLDERS,
    }),
    SCREENPOP_TEMPLATE: frozenset({
        "FLOW_NAME", "REGION", "LABEL_HEADING", "SCREENPOP_ATTRIBUTE_BAR", "SCREENPOP_SECTIONS",
    }),
    ACXD_TEMPLATE: frozenset({
        "MSG_WELCOME", "MSG_RETRY", "MSG_TRANSFER", "JOURNEY_NAME", "GENERATIVE_JOURNEY",
        *config.GT_PLACEHOLDERS, "ESCALATE_NODE_METADATA",
    }),
}

# messages.json[lang]["acxd"] key -> placeholder name (spoken basic / escalate messages).
ACXD_MESSAGE_KEYS = {
    "welcome": "MSG_WELCOME",
    "retry": "MSG_RETRY",
    "transfer": "MSG_TRANSFER",
}
# messages.json[lang]["acxd"] key of each Generative Journey exit condition prompt,
# in config.GJ_EXIT_CONDITION_NAMES order (System.gjConditionIndex 0, 1).
GJ_EXIT_PROMPT_KEYS = {
    "detailsCollected": "exit_details_collected",
    "humanHandoff": "exit_human_handoff",
}
assert tuple(GJ_EXIT_PROMPT_KEYS) == config.GJ_EXIT_CONDITION_NAMES


class FlowValidationError(Exception):
    """Template / flow defect.  Carries the full list of problems."""

    def __init__(self, problems):
        if isinstance(problems, str):
            problems = [problems]
        self.problems = list(problems)
        super().__init__("; ".join(self.problems))


class AgenticCxSelectionError(FlowValidationError):
    """No / ambiguous Agentic CX action in a reference flow (maps to exit code 2)."""

    def __init__(self, problems, candidates=None):
        super().__init__(problems)
        self.candidates = list(candidates or [])


@dataclass
class SpliceSpec:
    action: dict
    action_metadata: dict | None = None


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_json(path):
    p = Path(path)
    if not p.is_absolute():
        p = BASE_DIR / p
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise FlowValidationError(f"JSON parse error in {p}: {exc}") from exc


def load_messages() -> dict:
    return load_json(MESSAGES_FILE)


def load_template(relpath: str) -> dict:
    return load_json(relpath)


# ---------------------------------------------------------------------------
# Template engine
# ---------------------------------------------------------------------------

def _fmt_path(path) -> str:
    out = "$"
    for p in path:
        out += f"[{p}]" if isinstance(p, int) else f".{p}"
    return out


def _walk_strings(obj, path, skip):
    """Yield (path, string) for every string value, skipping subtrees in `skip`."""
    if tuple(path) in skip:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_strings(v, path + [k], skip)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk_strings(v, path + [i], skip)
    elif isinstance(obj, str):
        yield path, obj


def _splice(tpl: dict, spec: SpliceSpec) -> set:
    actions = tpl.get("Actions") or []
    idx = [i for i, a in enumerate(actions) if isinstance(a, dict) and a.get("Type") == MARKER]
    if len(idx) != 1:
        raise FlowValidationError(f"expected exactly 1 {MARKER} marker action, found {len(idx)}")
    i = idx[0]
    ident = actions[i].get("Identifier")
    new_action = copy.deepcopy(spec.action)
    new_action["Identifier"] = ident
    actions[i] = new_action
    skip = {("Actions", i)}
    if spec.action_metadata:
        am = tpl.setdefault("Metadata", {}).setdefault("ActionMetadata", {})
        entry = am.setdefault(ident, {})
        position = entry.get("position")
        merged = copy.deepcopy(spec.action_metadata)
        merged.pop("position", None)
        entry.update(merged)
        if position is not None:
            entry["position"] = position
    if ident in ((tpl.get("Metadata") or {}).get("ActionMetadata") or {}):
        skip.add(("Metadata", "ActionMetadata", ident))
    return skip


def _substitute(obj, path, ctx, skip, problems):
    if tuple(path) in skip:
        return obj
    if isinstance(obj, dict):
        return {k: _substitute(v, path + [k], ctx, skip, problems) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute(v, path + [i], ctx, skip, problems) for i, v in enumerate(obj)]
    if isinstance(obj, str):
        m = PLACEHOLDER_RE.fullmatch(obj)
        if m:
            return copy.deepcopy(ctx[m.group(1)])

        def repl(mo):
            name = mo.group(1)
            val = ctx[name]
            if not isinstance(val, str):
                problems.append(
                    f"inline placeholder {name} requires str, got {type(val).__name__} at {_fmt_path(path)}"
                )
                return mo.group(0)
            return val

        return PLACEHOLDER_RE.sub(repl, obj)
    return obj


def find_residuals(obj, path=None) -> list:
    """Paths of every key or string value that still contains '{{'."""
    path = path or []
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and "{{" in k:
                found.append(f"{_fmt_path(path + [k])} (key)")
            found.extend(find_residuals(v, path + [k]))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(find_residuals(v, path + [i]))
    elif isinstance(obj, str) and "{{" in obj:
        found.append(_fmt_path(path))
    return found


def render_template(template: dict, ctx: dict, *, splice: SpliceSpec | None = None) -> dict:
    if MARKER_NAME in ctx:
        raise FlowValidationError(f"{MARKER_NAME} is a reserved marker and must not be in ctx")
    tpl = copy.deepcopy(template)
    skip = _splice(tpl, splice) if splice is not None else set()

    found = set()
    for _, s in _walk_strings(tpl, [], skip):
        found.update(PLACEHOLDER_RE.findall(s))
    problems = []
    missing = sorted(found - set(ctx))
    extra = sorted(set(ctx) - found)
    if missing:
        problems.append("placeholders missing from ctx: " + ", ".join(missing))
    if extra:
        problems.append("ctx keys not used by template: " + ", ".join(extra))
    if problems:
        raise FlowValidationError(problems)

    result = _substitute(tpl, [], ctx, skip, problems)
    if problems:
        raise FlowValidationError(problems)

    if "{{" in json.dumps(result, ensure_ascii=False):
        residual = find_residuals(result)
        raise FlowValidationError(["residual '{{' after substitution at " + ", ".join(residual)])
    return result


def template_placeholders(template: dict) -> set:
    """Placeholder names found in string values (marker excluded)."""
    names = set()
    for _, s in _walk_strings(template, [], set()):
        names.update(PLACEHOLDER_RE.findall(s))
    names.discard(MARKER_NAME)
    return names


# ---------------------------------------------------------------------------
# ACXD message tokens (A8)
# ---------------------------------------------------------------------------

def render_message(text: str) -> str:
    problems = []

    def repl(mo):
        token = mo.group(1)
        if token in config.FIELD_TOKENS:
            return config.CONTEXT_VAR_MESSAGE_PLACEHOLDER.format(name=config.FIELD_TOKENS[token])
        problems.append(f"unknown message token {{{{{token}}}}}")
        return mo.group(0)

    out = re.sub(r"\{\{([^{}]*)\}\}", repl, text)
    if problems:
        raise FlowValidationError(problems)
    if "{{" in out:
        raise FlowValidationError(f"residual '{{{{' in message: {text!r}")
    return out


# ---------------------------------------------------------------------------
# Validators (§6.7, AC4, AC5)
# ---------------------------------------------------------------------------

def _transition_targets(action: dict):
    tr = action.get("Transitions") or {}
    if "NextAction" in tr:
        yield "Transitions.NextAction", tr["NextAction"]
    for kind in ("Conditions", "Errors"):
        for i, entry in enumerate(tr.get(kind) or []):
            if isinstance(entry, dict) and "NextAction" in entry:
                yield f"Transitions.{kind}[{i}].NextAction", entry["NextAction"]


def validate_connect_flow(flow: dict) -> None:
    problems = []
    actions = flow.get("Actions")
    if not isinstance(actions, list) or not actions:
        raise FlowValidationError("Connect flow has no Actions")
    ids = [a.get("Identifier") for a in actions]
    seen = set()
    for ident in ids:
        if ident in seen:
            problems.append(f"duplicate Identifier {ident}")
        seen.add(ident)
    if flow.get("StartAction") not in seen:
        problems.append(f"StartAction {flow.get('StartAction')} is not an Identifier")
    for a in actions:
        for where, target in _transition_targets(a):
            if target not in seen:
                problems.append(f"action {a.get('Identifier')} {where} -> unknown {target}")
    am = (flow.get("Metadata") or {}).get("ActionMetadata") or {}
    for key in am:
        if key not in seen:
            problems.append(f"Metadata.ActionMetadata key {key} is not an Identifier")
    if "{{" in json.dumps(flow, ensure_ascii=False):
        problems.append("residual '{{' at " + ", ".join(find_residuals(flow)))
    if problems:
        raise FlowValidationError(problems)


_CONTINUATION_PARENT_TYPES = ("escalate", "redirect")


def _is_continuation_child(child) -> bool:
    """True for a child edge conditioned on node_status == continuation (A7, live data)."""
    if not isinstance(child, dict):
        return False
    for cond in child.get("conditions") or []:
        if (isinstance(cond, dict)
                and (cond.get("left") or {}).get("type") == "node_status"
                and (cond.get("right") or {}).get("value") == "continuation"):
            return True
    return False


_GENERATIVE_METADATA_KEYS = ("generativeJourney", "generativeText", "split")


def _gj_condition_indexes(node: dict) -> set:
    """Values i of every child edge conditioned on System.gjConditionIndex eq i."""
    found = set()
    for child in node.get("childNodes") or []:
        for cond in (child.get("conditions") or []) if isinstance(child, dict) else []:
            if (isinstance(cond, dict)
                    and (cond.get("left") or {}).get("name") == "System.gjConditionIndex"
                    and cond.get("operator") == "eq"):
                found.add((cond.get("right") or {}).get("value"))
    return found


def _check_generative(key: str, node: dict, problems: list) -> None:
    """No forbidden node type, every model is config.GENERATIVE_MODEL_TYPE, every GJ exit is routed."""
    ntype = node.get("type")
    if ntype in config.FORBIDDEN_NODE_TYPES:
        problems.append(f"node {key} has forbidden type {ntype!r}")
    md = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    for mkey in _GENERATIVE_METADATA_KEYS:
        sub = md.get(mkey)
        if isinstance(sub, dict) and sub.get("modelType") != config.GENERATIVE_MODEL_TYPE:
            problems.append(f"node {key} metadata.{mkey}.modelType {sub.get('modelType')!r} != "
                            f"{config.GENERATIVE_MODEL_TYPE!r}")
    if ntype == "generative_journey":
        gj = md.get("generativeJourney")
        if not isinstance(gj, dict):
            problems.append(f"node {key} generative_journey has no metadata.generativeJourney")
            return
        missing = sorted(set(range(len(gj.get("exitConditions") or []))) - _gj_condition_indexes(node))
        if missing:
            problems.append(f"node {key} exit condition indexes {missing} have no "
                            "System.gjConditionIndex child edge")


def validate_acxd_flow(body_or_nodes: dict) -> None:
    nodes = body_or_nodes.get("nodes") if "nodes" in body_or_nodes else body_or_nodes
    problems = []
    if not isinstance(nodes, dict) or not nodes:
        raise FlowValidationError("ACXD flow has no nodes")
    starts = 0
    for key, node in nodes.items():
        if not isinstance(node, dict):
            problems.append(f"node {key} is not an object")
            continue
        if node.get("nodeId") != key:
            problems.append(f"node key {key} != nodeId {node.get('nodeId')}")
        if not UUID4_RE.match(key):
            problems.append(f"nodeId {key} is not a UUIDv4")
        if node.get("type") not in config.FLOW_NODE_TYPES:
            problems.append(f"node {key} has unknown type {node.get('type')!r}")
        if node.get("type") == "start":
            starts += 1
        _check_generative(key, node, problems)
        for i, child in enumerate(node.get("childNodes") or []):
            target = child.get("nodeId") if isinstance(child, dict) else None
            if (target is None and node.get("type") in _CONTINUATION_PARENT_TYPES
                    and _is_continuation_child(child)):
                continue  # A7: nodeId-less continuation edge of escalate/redirect
            if target not in nodes:
                problems.append(f"node {key} childNodes[{i}] -> unknown {target}")
        message_ids = [m.get("messageId") for m in node.get("messages") or []
                       if isinstance(m, dict) and "messageId" in m]
        for mid in sorted({m for m in message_ids if message_ids.count(m) > 1}, key=str):
            problems.append(f"node {key} has duplicate messageId {mid}")
    if starts != 1:
        problems.append(f"expected exactly 1 start node, found {starts}")
    if "{{" in json.dumps(nodes, ensure_ascii=False):
        problems.append("residual '{{' at " + ", ".join(find_residuals(nodes)))
    if problems:
        raise FlowValidationError(problems)


def assert_no_secret(text: str, secret: str | None) -> None:
    if not secret:
        return
    if secret in text:
        raise FlowValidationError("refusing to write output: it contains the ACXD API key")


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def render_acxd_flow(lang: str, messages: dict, prefix: str) -> dict:
    """Return the substituted `nodes` map of the ACXD flow (§3.3).  `prefix` is the resource name
    prefix (--name-prefix); the Generative Journey node is named config.acxd_journey_name(prefix)."""
    msgs = messages[lang]["acxd"]
    ctx = {ph: render_message(msgs[key]) for key, ph in ACXD_MESSAGE_KEYS.items()}
    ctx["JOURNEY_NAME"] = config.acxd_journey_name(prefix)
    # A5 / A6: one Generative Journey takes every caller turn (no user_input node).
    ctx["GENERATIVE_JOURNEY"] = {
        **copy.deepcopy(config.GENERATIVE_JOURNEY_SETTINGS),
        "modelType": config.GENERATIVE_MODEL_TYPE,
        "prompt": render_message(msgs["journey_prompt"]),
        "exitConditions": [{"name": name, "prompt": render_message(msgs[GJ_EXIT_PROMPT_KEYS[name]])}
                           for name in config.GJ_EXIT_CONDITION_NAMES],
        "tools": [],
    }
    # A3 / A6: one generative_text per field; its output is the local variable <field>.
    for ph, field in config.GT_PLACEHOLDERS.items():
        ctx[ph] = {
            "name": field,
            "modelType": config.GENERATIVE_MODEL_TYPE,
            "prompt": render_message(msgs["extract"][field]),
            **copy.deepcopy(config.GENERATIVE_TEXT_SETTINGS),
        }
    # A4 / A20: the escalate node copies the locals into the workspace context variables.
    ctx["ESCALATE_NODE_METADATA"] = {
        **copy.deepcopy(config.ESCALATE_NODE_METADATA),
        "stateModifications": [config.state_mod_set_context(f) for f in config.FIELDS],
    }
    nodes = render_template(load_template(ACXD_TEMPLATE), ctx)["nodes"]

    voice = config.ACXD_NODE_VOICE_SETTINGS  # A17
    if voice is not None:
        for node in nodes.values():
            if node.get("type") == "basic" and node.get("messages"):
                node.setdefault("metadata", {})["voiceSettings"] = copy.deepcopy(voice)
    validate_acxd_flow(nodes)
    return nodes


# Sections table cells that may hold long unbroken text.
_BREAK_ALL_FIELDS = frozenset({"FaultDescription", config.CUSTOMER_EMAIL_FIELD})


def _sections_html(labels: dict, heading: str) -> str:
    rows = []
    for f in config.SCREENPOP_FIELDS:
        extra = " word-break: break-all;" if f in _BREAK_ALL_FIELDS else ""
        rows.append(
            "<tr style='height: 50px; background-color: #f9f9f9;'>"
            f" <th style='text-align: left; width: 30%; background-color: #eee;'>{html.escape(labels[f])}</th>"
            f" <td style='vertical-align: middle;{extra}'>$.Attributes.{f}</td> </tr>"
        )
    return (
        "<table cellpadding='10' cellspacing='0' style='border-collapse: collapse; width: 100%;"
        " font-family: Arial, sans-serif; box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);'>"
        " <thead> <tr style='background-color: #327194; color: #fff;'>"
        f" <th colspan='2' style='text-align: left; padding: 10px;'>{html.escape(heading)}</th>"
        " </tr> </thead> <tbody> " + " ".join(rows) + " </tbody> </table>"
    )


def render_screenpop_flow(lang: str, messages: dict, region: str, flow_name: str) -> dict:
    sp = messages[lang]["screenpop"]
    labels = sp["labels"]
    attribute_bar = [
        {"Label": labels[f], "Value": f"$.Attributes.{f}", "Copyable": True}
        for f in config.SCREENPOP_FIELDS  # customer fields first (config.SCREENPOP_FIELDS)
    ]
    if config.SCREENPOP_SECTIONS_SHOW_VALUES:  # A19
        sections = [{"TemplateString": _sections_html(labels, sp["heading"])}]
    else:
        sections = [{"TemplateString": "<p>" + html.escape(sp["sections_hint"]) + "</p>"}]
    ctx = {
        "FLOW_NAME": flow_name,
        "REGION": region,
        "LABEL_HEADING": sp["heading"],
        "SCREENPOP_ATTRIBUTE_BAR": attribute_bar,
        "SCREENPOP_SECTIONS": sections,
    }
    flow = render_template(load_template(SCREENPOP_TEMPLATE), ctx)
    validate_connect_flow(flow)
    for a in flow["Actions"]:
        if a.get("Type") == "ShowView":
            size = len(json.dumps(a["Parameters"], ensure_ascii=False).encode("utf-8"))
            if size > config.SHOWVIEW_PARAMETERS_MAX_BYTES:
                raise FlowValidationError(
                    f"ShowView Parameters are {size} bytes, limit {config.SHOWVIEW_PARAMETERS_MAX_BYTES} (NFR5)"
                )
    return flow


def field_attributes() -> dict:
    return {f: f"$.AgenticCX.ContextVariables.{f}" for f in config.FIELDS}


_NAME_TOKEN_RE = re.compile(r"\{([^{}]*)\}")


def customer_name_expression(fmt: str) -> str:
    """messages.json screenpop.customer.name_format -> Set contact attributes value (A22).

    "{LastName} {FirstName}" -> "$.Customer.LastName $.Customer.FirstName".  Two tokens with
    nothing between them are rejected: adjacent JSONPath references are not runtime-verified.
    """
    problems = []
    tokens = _NAME_TOKEN_RE.findall(fmt)
    for tok in tokens:
        if tok not in config.CUSTOMER_PROFILE_NAME_TOKENS:
            problems.append(f"unknown name_format token {{{tok}}} in {fmt!r}")
    if not tokens:
        problems.append(f"name_format {fmt!r} has no {{FirstName}} / {{LastName}} token")
    if "}{" in fmt:
        problems.append(f"name_format {fmt!r} has adjacent tokens; adjacent JSONPath references "
                        "are not runtime-verified, keep a separator such as a space")
    if problems:
        raise FlowValidationError(problems)
    out = _NAME_TOKEN_RE.sub(lambda mo: config.CUSTOMER_PROFILE_NAME_TOKENS[mo.group(1)], fmt)
    if "{" in out or "}" in out:
        raise FlowValidationError(f"name_format {fmt!r} has unbalanced braces")
    return out


def customer_blank_name(fmt: str) -> str:
    """CustomerName of a found profile with neither FirstName nor LastName.

    Unresolved $.Customer.* references become "" at runtime (EVIDENCE.md: no-profile contacts
    hold CustomerFullName == " "), so the blank value is the format with its tokens removed.
    """
    customer_name_expression(fmt)  # same validation
    blank = _NAME_TOKEN_RE.sub("", fmt)
    if not blank:
        raise FlowValidationError(f"name_format {fmt!r} leaves an empty blank name; an empty Compare "
                                  "operand is not verified, keep a separator such as a space")
    return blank


def customer_profile_ctx(lang: str, messages: dict) -> dict:
    """Attributes of the customer UpdateContactAttributes actions of the inbound flow (D2..D6).

    CUSTOMER_NAME_ATTRIBUTE / CUSTOMER_BLANK_NAME feed the Compare after the found action: a
    profile without names (CustomerName == blank) gets the localized no_name text instead.
    """
    cust = messages[lang]["screenpop"]["customer"]
    name, phone, email = config.CUSTOMER_NAME_FIELD, config.CUSTOMER_PHONE_FIELD, config.CUSTOMER_EMAIL_FIELD
    return {
        "CUSTOMER_NAME_ATTRIBUTE": f"$.Attributes.{name}",
        "CUSTOMER_BLANK_NAME": customer_blank_name(cust["name_format"]),
        "CUSTOMER_NO_NAME_ATTRIBUTES": {name: cust["no_name"]},
        "CUSTOMER_FOUND_ATTRIBUTES": {
            name: customer_name_expression(cust["name_format"]),
            phone: config.CUSTOMER_PHONE_SOURCE,
            email: config.CUSTOMER_EMAIL_SOURCE,
        },
        "CUSTOMER_BUSINESS_EMAIL_ATTRIBUTES": {email: config.CUSTOMER_BUSINESS_EMAIL_SOURCE},
        "CUSTOMER_NO_EMAIL_ATTRIBUTES": {email: cust["no_email"]},
        "CUSTOMER_NOT_FOUND_ATTRIBUTES": {
            name: cust["not_found"],
            phone: config.CUSTOMER_PHONE_SOURCE,
            email: cust["not_found"],
        },
    }


def render_inbound_flow(ctx_values: dict, captured_action: dict, captured_metadata: dict | None) -> dict:
    ctx = {**ctx_values, "FIELD_ATTRIBUTES": field_attributes()}
    flow = render_template(
        load_template(INBOUND_TEMPLATE), ctx,
        splice=SpliceSpec(action=captured_action, action_metadata=captured_metadata),
    )
    validate_connect_flow(flow)
    return flow


# ---------------------------------------------------------------------------
# Agentic CX action (§4.1)
# ---------------------------------------------------------------------------

def load_fallback_action() -> dict:
    return load_json(FALLBACK_FRAGMENT)


def build_agentic_cx_action(workspace_id: str, application_id: str, alias: str) -> dict:
    """Real Agentic CX block action (A1, live flow 'AI Agent - ACXD').  Alias is the deploymentAlias."""
    args = {"workspace_id": workspace_id, "application_id": application_id, "alias": alias}
    empty = [name for name, value in args.items() if not value]
    if empty:
        raise FlowValidationError(
            "cannot build the Agentic CX action: empty " + ", ".join(empty))
    return {
        "Identifier": config.AGENTIC_CX_ACTION_ID,
        "Type": config.AGENTIC_CX_ACTION_TYPE,
        "Parameters": {
            "AgentConfiguration": {"WorkspaceId": workspace_id, "ApplicationId": application_id, "Alias": alias},
            **copy.deepcopy(config.AGENTIC_CX_STATIC_PARAMETERS),
        },
        "Transitions": copy.deepcopy(config.AGENTIC_CX_TRANSITIONS),
    }


def agentic_cx_action_metadata(workspace_label: str, application_label: str, environment: str) -> dict:
    """Metadata.ActionMetadata entry (without position) of the generated action."""
    return {
        "isFriendlyName": False,
        "parameters": {
            "AgentConfiguration": {
                "WorkspaceId": {"displayName": workspace_label, "useDropdown": "true"},
                "ApplicationId": {"displayName": application_label},
                "Alias": {"displayName": environment.capitalize()},
            },
            **copy.deepcopy(config.AGENTIC_CX_METADATA_DISPLAY),
        },
    }


def _candidates(actions) -> list:
    return [f"{a.get('Identifier')} ({a.get('Type')})" for a in actions if isinstance(a, dict)]


def select_agentic_cx_action(source_obj: dict, application_id: str | None = None,
                             identifier: str | None = None):
    """Return (action, action_metadata) from a single action or a full flow JSON."""
    if not isinstance(source_obj, dict):
        raise AgenticCxSelectionError("Agentic CX source is not a JSON object")
    if "Type" in source_obj and "Actions" not in source_obj:
        return copy.deepcopy(source_obj), None
    actions = source_obj.get("Actions")
    if not isinstance(actions, list):
        raise AgenticCxSelectionError("Agentic CX source is neither an action nor a flow with Actions")
    cands = _candidates(actions)

    if identifier:
        chosen = [a for a in actions if isinstance(a, dict) and a.get("Identifier") == identifier]
        if not chosen:
            raise AgenticCxSelectionError(f"action Identifier {identifier} not found", cands)
        action = chosen[0]
        logger.info("Agentic CX action selected by Identifier %s", identifier)
    else:
        action = None
        if application_id:
            rule_a = [a for a in actions if isinstance(a, dict)
                      and application_id in json.dumps(a.get("Parameters", {}), ensure_ascii=False)]
            if len(rule_a) == 1:
                action = rule_a[0]
                logger.info("Agentic CX action selected by applicationId (rule a): %s", action.get("Identifier"))
            elif len(rule_a) > 1:
                raise AgenticCxSelectionError(
                    "multiple actions reference the applicationId; use --agentic-cx-action-identifier",
                    _candidates(rule_a))
        if action is None:
            rule_b = [a for a in actions if isinstance(a, dict)
                      and "agentic" in str(a.get("Type", "")).lower()]
            if len(rule_b) != 1:
                raise AgenticCxSelectionError(
                    f"cannot identify the Agentic CX action ({len(rule_b)} candidates by Type); "
                    "use --agentic-cx-action-identifier", cands)
            action = rule_b[0]
            logger.info("Agentic CX action selected by Type (rule b): %s", action.get("Identifier"))

    am = ((source_obj.get("Metadata") or {}).get("ActionMetadata") or {}).get(action.get("Identifier"))
    meta = None
    if isinstance(am, dict):
        meta = {k: copy.deepcopy(v) for k, v in am.items() if k != "position"} or None
    return copy.deepcopy(action), meta


def _classify(entry: dict, kind: str) -> str:
    # Live TP-ACXD flow: NoMatchingCondition routes to the branch that reads
    # $.AgenticCX.ContextVariables (-> attrs); InputTimeLimitExceeded is the
    # 'Idle chat timeout' branch (-> idle).
    text = json.dumps({k: v for k, v in entry.items() if k != "NextAction"}, ensure_ascii=False).lower()
    kw = config.AGENTIC_CX_BRANCH_KEYWORDS
    if any(k in text for k in kw["escalation"]):
        return "attrs"
    if any(k in text for k in kw["idle"]):
        return "idle"
    if any(k in text for k in kw["exit"]):
        return "attrs"
    return "attrs" if kind == "Conditions" else "error"


def rebuild_agentic_cx_transitions(action: dict, ids: dict | None = None):
    """Rewrite NextAction targets of the captured action (§4.1).  Returns (action, decisions)."""
    ids = ids or INBOUND_IDS
    new = copy.deepcopy(action)
    new["Identifier"] = config.AGENTIC_CX_ACTION_ID
    tr = new.get("Transitions")
    if not isinstance(tr, dict):
        tr = {}
    new["Transitions"] = tr
    conds = tr.get("Conditions") or []
    errs = tr.get("Errors") or []
    decisions = []

    if "NextAction" in tr:
        tr["NextAction"] = ids["attrs"]
        decisions.append(f"Agentic CX default branch: kept NextAction -> {_TARGET_NAMES['attrs']}")
    elif not conds:
        tr["NextAction"] = ids["attrs"]
        decisions.append(f"Agentic CX default branch: added NextAction -> {_TARGET_NAMES['attrs']}")
    else:
        decisions.append("Agentic CX default branch: encoded as Condition, NextAction not added")

    for kind, entries in (("Conditions", conds), ("Errors", errs)):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            target = _classify(entry, kind)
            entry["NextAction"] = ids[target]
            if kind == "Errors":
                label = f"ErrorType={entry.get('ErrorType')}"
            else:
                label = f"Condition={json.dumps(entry.get('Condition'), ensure_ascii=False)}"
            decisions.append(f"Agentic CX branch {label} -> {_TARGET_NAMES[target]}")

    for d in decisions:
        logger.info(d)
    return new, decisions


def check_captured_action(action: dict, application_id: str | None, alias_candidates) -> list:
    """A18: warnings only, never raises."""
    warns = []
    params = json.dumps(action.get("Parameters", {}), ensure_ascii=False)
    if application_id and application_id not in params:
        warns.append(
            f"captured Agentic CX action Parameters do not contain applicationId {application_id}; "
            "it may point at a different ACXD application"
        )
    cands = [c for c in (alias_candidates or []) if c]
    if cands and not any(c.lower() in params.lower() for c in cands):
        warns.append(
            "captured Agentic CX action alias may not match this deployment ("
            + ", ".join(cands) + "); Connect could invoke an environment without a deployment"
        )
    return warns
