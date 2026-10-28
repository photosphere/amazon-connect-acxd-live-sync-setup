"""Live Sync profile on top of the device-fault deployment (deploy.py / flowlib.py / config.py).

Demo scenario: the customer DIALS IN (any phone, landlines included).  The Connect inbound flow
emails a link to a CloudFront page; the customer opens it and the voice assistant on the phone call
drives that page through Live Sync:

  * product name  - three dummy products shown on the page, selected by voice or by click
  * product model - four dummy models shown on the page, selected by voice or by click
  * fault         - described by voice only, no page support

Everything after the conversation is the base project (three Generative Text extractions, Escalate
with set-context, Customer Profiles lookup, queue transfer, Detail-view screen pop).  This module
swaps the conversation for a Live Sync node and adds the email-link step to the inbound flow; the
AWS side (CloudFront page, session endpoint, send-link Lambda, SES) is livesync_infra.py.

apply_profile() points the base modules at the Live Sync templates, messages and hooks.  It is called
by deploy_livesync.py only; deploy.py on its own keeps deploying the base scenario.

Evidence for the Live Sync shapes (workshop "Build, Test, Optimize: Hands-On with ACXD", Part 8,
and the @amazon-connect-touchpoint/web 1.0.4 sources):
  * LS1 Live Sync node = ACXD node type "multimodal" with metadata.multimodal {type: "agentic", ...}.
  * LS2 exit conditions are matched on System.voicePlusConditionIndex, NOT System.gjConditionIndex.
  * LS3 node_status edges continuation / timeout / failure / escalation exist on the node.
  * LS4 the Connect flow sets the contact language next to the TTS voice; the Live Sync channel key
        is connect-<languageCode> and must match the published application language.
  * LS5 Touchpoint input "external" opens no contact of its own and binds the page to an existing
        contact through liveSync.contactId ("a common case is a phone call", Touchpoint README); it
        needs only the region and the Live Sync API key / deployment key (ACXD application Settings ->
        Access, not available through the ACXD SDK API).
"""

from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import logging
import os
import re
import tarfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import config
import flowlib
import livesync_infra as infra

logger = logging.getLogger("livesync")

BASE_DIR = Path(__file__).resolve().parent
LIVE_SYNC_TEMPLATE = "acxd/livesync_flow.json"
LIVE_SYNC_MESSAGES_FILE = "acxd/livesync_messages.json"
WEB_DIR = BASE_DIR / "web"
WEB_FILES = ("index.html", "app.js", "styles.css")

DEFAULT_NAME_PREFIX = "LiveSync"
LANGUAGE = "en-US"  # the only language of the Live Sync profile (ACXD app, Connect flow, page, email)
DEFAULT_OUTPUT_DIR = "output-livesync"
PAGE_DIR_NAME = "livesync-page"
STATE_NAME = "livesync_state.json"
STACK_ARTIFACT = "livesync_stack.json"
SESSION_PATH = "/api/session"

# LS1: Live Sync node settings besides prompt / exitConditions / modelType (workshop flow values).
LIVE_SYNC_NODE_SETTINGS = {
    "type": "agentic",
    # Actions are advertised by the page through the Touchpoint SDK (sendContext), as in the workshop.
    "actions": [],
    "tools": [],
    "maxTokens": 8192,
    "maxSteps": 10,
    "temperature": 0.7,
    "scopeTagColors": {},
    "scopeTags": [],
}
VOICE_PLUS_CONDITION = "System.voicePlusConditionIndex"  # LS2
REQUIRED_NODE_STATUS_EDGES = ("continuation", "timeout", "failure", "escalation")  # LS3

# Exact number of dummy entries on the page (requirement: 3 products, 4 models).
PRODUCT_COUNT = 3
MODEL_COUNT = 4
CATALOG_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")

# Inbound flow additions.  LS4: UpdateContactData (language) after the TTS voice action.
LANGUAGE_ACTION_ID = "d1a9e0c0-0000-4000-8000-000000000014"
TTS_ACTION_ID = "d1a9e0c0-0000-4000-8000-000000000003"
# Email link: InvokeLambdaFunction -> Compare $.External.linkSent -> MessageParticipant -> Agentic CX.
SEND_LINK_ACTION_ID = "d1a9e0c0-0000-4000-8000-000000000015"
LINK_SENT_CHECK_ID = "d1a9e0c0-0000-4000-8000-000000000016"
LINK_SENT_MESSAGE_ID = "d1a9e0c0-0000-4000-8000-000000000017"
LANGUAGE_CTX_KEY = "LIVE_SYNC_LANGUAGE"
SEND_LINK_CTX_KEY = "LIVE_SYNC_SEND_LINK_ARN"
LINK_MESSAGE_CTX_KEY = "LIVE_SYNC_LINK_SENT_MESSAGE"
LAMBDA_TIMEOUT_SECONDS = "8"  # InvokeLambdaFunction maximum for synchronous invocations

# Touchpoint Web SDK, pinned (npm dist.integrity of @amazon-connect-touchpoint/web@1.0.4).
TOUCHPOINT_PACKAGE = "@amazon-connect-touchpoint/web"
TOUCHPOINT_VERSION = "1.0.4"
TOUCHPOINT_TARBALL_URL = "https://registry.npmjs.org/@amazon-connect-touchpoint/web/-/web-1.0.4.tgz"
TOUCHPOINT_INTEGRITY = "sha512-j7Fsx5keb1Xud7KDlvHtDHY1dvw4jne8t7vkwWETos3ib83R/jXa5AKdydnNeTMADxd3GlxfptilkLDzTv80Fg=="
TOUCHPOINT_UMD_MEMBER = "package/lib/index.umd.js"
TOUCHPOINT_LICENSE_MEMBER = "package/LICENSE"
TOUCHPOINT_VENDOR_NAME = "vendor/touchpoint.umd.js"

LIVE_SYNC_ASSUMPTIONS = {
    # A5 / A6 of the base list describe the Generative Journey; in this profile they read:
    "A5": "不使用 user_input 节点：Live Sync 节点接管客户的每一轮对话，退出条件 detailsCollected / humanHandoff 按 "
          "System.voicePlusConditionIndex 0 / 1 路由（acxd/livesync_flow.json）",
    "A6": "Live Sync 节点与 3 个 Generative Text 节点都使用 config.GENERATIVE_MODEL_TYPE = anthropic.claude-haiku-4-5"
          "（workshop 推荐 Haiku 4.5）",
    "A11": "只支持 en-US：ACXD 应用、Connect 流（语言块与 KATIE agentic 声音）、页面和邮件都使用 en-US"
           "（zh-CN 部署在实网中失败且没有返回原因）",
    "A21": "en-US 是唯一语言，不再有语言回退",
    "L1": "Live Sync 节点写作 ACXD 节点 type multimodal + metadata.multimodal.type agentic，出口条件按 "
          "System.voicePlusConditionIndex 路由（取自 workshop Part 8 的 MainLiveSyncEnabled.flow.json）",
    "L2": "Live Sync 节点结束后，Generative Text 的 {System.transcript:NLX.System} 包含 Live Sync 对话，"
          "因此沿用基础项目的三个提取节点；页面点选只通过助手的口头复述进入对话记录",
    "L3": "Touchpoint 的 Live Sync deployment key / API key 只能在 ACXD 应用 Settings → Access 复制"
          "（ACXD SDK API 不提供），workshop 说明 deployment key 会随部署重新生成",
    "L4": "入站流在 TTS 声音块后用 UpdateContactData 设置联系语言 en-US（workshop LiveSyncFlow.json 的写法），"
          "使用 agentic 声音 KATIE（connect:agentic）",
    "L5": "页面与 Touchpoint 使用 languageCode en-US（Live Sync channel connect-en-US），与 ACXD 应用语言一致",
    "L6": "电话联系（PSTN）可以与页面上的 Touchpoint input external + liveSync.contactId（入站流中的 ContactId）"
          "同步（Touchpoint README 与 workshop 的说明，本项目未拨测）",
    "L7": "Live Sync 节点在客户打开页面之前没有页面操作可用；提示词要求先请客户打开邮件链接，打不开时改为纯语音收集",
}


# ---------------------------------------------------------------------------
# Messages and catalog
# ---------------------------------------------------------------------------

def load_messages() -> dict:
    messages = flowlib.load_json(LIVE_SYNC_MESSAGES_FILE)
    for lang, msgs in messages.items():
        validate_catalog(lang, msgs.get("catalog"))
    return messages


def validate_catalog(lang: str, catalog) -> None:
    """3 products and 4 models with unique ids and names that are safe as Connect attribute values."""
    problems = []
    if not isinstance(catalog, dict):
        raise flowlib.FlowValidationError(f"{lang}: catalog missing")
    for key, count in (("products", PRODUCT_COUNT), ("models", MODEL_COUNT)):
        items = catalog.get(key)
        if not isinstance(items, list) or len(items) != count:
            problems.append(f"{lang}: catalog.{key} must have exactly {count} entries")
            continue
        ids = [i.get("id") for i in items if isinstance(i, dict)]
        names = [i.get("name") for i in items if isinstance(i, dict)]
        if len(set(ids)) != count or not all(isinstance(i, str) and CATALOG_ID_RE.match(i) for i in ids):
            problems.append(f"{lang}: catalog.{key} ids must be unique and match {CATALOG_ID_RE.pattern}")
        if len(set(names)) != count or not all(isinstance(n, str) and n.strip() for n in names):
            problems.append(f"{lang}: catalog.{key} names must be unique and non-empty")
        for n in names:
            # The extracted value travels as a Connect attribute (key=value): & and = would truncate it.
            if isinstance(n, str) and ("&" in n or "=" in n or "\n" in n):
                problems.append(f"{lang}: catalog.{key} name {n!r} contains '&', '=' or a line break")
    if problems:
        raise flowlib.FlowValidationError(problems)


def catalog_list(items: list) -> str:
    return "\n".join(f"- {i['name']}" for i in items)


def _fill_catalog(text: str, catalog: dict) -> str:
    return (text.replace("<<PRODUCT_LIST>>", catalog_list(catalog["products"]))
                .replace("<<MODEL_LIST>>", catalog_list(catalog["models"])))


# ---------------------------------------------------------------------------
# ACXD flow
# ---------------------------------------------------------------------------

def live_sync_node_name(prefix: str) -> str:
    return f"{prefix}Node"


def render_livesync_acxd_flow(lang: str, messages: dict, prefix: str) -> dict:
    """Substituted `nodes` map: Welcome -> Live Sync -> 3 x Generative Text -> Escalate."""
    msgs = messages[lang]["acxd"]
    catalog = messages[lang]["catalog"]
    ctx = {
        "MSG_WELCOME": flowlib.render_message(msgs["welcome"]),
        "MSG_RETRY": flowlib.render_message(msgs["retry"]),
        "MSG_TRANSFER": flowlib.render_message(msgs["transfer"]),
        "LIVE_SYNC_NODE_NAME": live_sync_node_name(prefix),
        "LIVE_SYNC": {
            **copy.deepcopy(LIVE_SYNC_NODE_SETTINGS),
            # The catalog is in the prompt too: without the page (link not opened) the voice-only
            # fallback still has to pick one of the dummy products / models.
            "prompt": flowlib.render_message(_fill_catalog(msgs["live_sync_prompt"], catalog)),
            "exitConditions": [
                {"name": name, "prompt": flowlib.render_message(msgs[flowlib.GJ_EXIT_PROMPT_KEYS[name]])}
                for name in config.GJ_EXIT_CONDITION_NAMES
            ],
            "modelType": config.GENERATIVE_MODEL_TYPE,
        },
        # Same three extraction nodes and Escalate as the base flow (A3, A4, A20).
        "ESCALATE_NODE_METADATA": {
            **copy.deepcopy(config.ESCALATE_NODE_METADATA),
            "stateModifications": [config.state_mod_set_context(f) for f in config.FIELDS],
        },
    }
    for ph, fld in config.GT_PLACEHOLDERS.items():
        ctx[ph] = {
            "name": fld,
            "modelType": config.GENERATIVE_MODEL_TYPE,
            "prompt": flowlib.render_message(_fill_catalog(msgs["extract"][fld], catalog)),
            **copy.deepcopy(config.GENERATIVE_TEXT_SETTINGS),
        }
    nodes = flowlib.render_template(flowlib.load_template(LIVE_SYNC_TEMPLATE), ctx)["nodes"]
    voice = config.ACXD_NODE_VOICE_SETTINGS  # A17
    if voice is not None:
        for node in nodes.values():
            if node.get("type") == "basic" and node.get("messages"):
                node.setdefault("metadata", {})["voiceSettings"] = copy.deepcopy(voice)
    flowlib.validate_acxd_flow(nodes)
    validate_live_sync_nodes(nodes)
    return nodes


def _edge_conditions(node: dict):
    for child in node.get("childNodes") or []:
        for cond in (child.get("conditions") or []) if isinstance(child, dict) else []:
            if isinstance(cond, dict):
                yield child, cond


def validate_live_sync_nodes(nodes: dict) -> None:
    """LS1-LS3 plus: no Generative Journey / User input node, Haiku model, every exit routed."""
    problems = []
    live = [(k, n) for k, n in nodes.items() if n.get("type") == "multimodal"]
    if len(live) != 1:
        problems.append(f"expected exactly 1 Live Sync (multimodal) node, found {len(live)}")
    for key, node in nodes.items():
        if node.get("type") in ("generative_journey", *config.FORBIDDEN_NODE_TYPES):
            problems.append(f"node {key} has type {node.get('type')!r}, not allowed in the Live Sync flow")
    for key, node in live:
        mm = (node.get("metadata") or {}).get("multimodal")
        if not isinstance(mm, dict):
            problems.append(f"node {key} has no metadata.multimodal")
            continue
        if mm.get("type") != "agentic":
            problems.append(f"node {key} metadata.multimodal.type {mm.get('type')!r} != 'agentic'")
        if mm.get("modelType") != config.GENERATIVE_MODEL_TYPE:
            problems.append(f"node {key} metadata.multimodal.modelType {mm.get('modelType')!r} != "
                            f"{config.GENERATIVE_MODEL_TYPE!r}")
        if not (mm.get("prompt") or "").strip():
            problems.append(f"node {key} has an empty Live Sync prompt")
        if "<<" in (mm.get("prompt") or ""):
            problems.append(f"node {key} Live Sync prompt has an unfilled <<...>> placeholder")
        names = [c.get("name") for c in mm.get("exitConditions") or []]
        if tuple(names) != config.GJ_EXIT_CONDITION_NAMES:
            problems.append(f"node {key} exit conditions {names} != {list(config.GJ_EXIT_CONDITION_NAMES)}")
        routed, statuses = set(), set()
        for _, cond in _edge_conditions(node):
            left = cond.get("left") or {}
            value = (cond.get("right") or {}).get("value")
            if left.get("name") == "System.gjConditionIndex":
                problems.append(f"node {key} routes an exit on System.gjConditionIndex; Live Sync uses "
                                f"{VOICE_PLUS_CONDITION} (LS2)")
            if left.get("name") == VOICE_PLUS_CONDITION and cond.get("operator") == "eq":
                routed.add(value)
            if left.get("type") == "node_status":
                statuses.add(value)
        missing = sorted(set(range(len(names))) - routed)
        if missing:
            problems.append(f"node {key} exit condition indexes {missing} have no {VOICE_PLUS_CONDITION} edge")
        for status in REQUIRED_NODE_STATUS_EDGES:
            if status not in statuses:
                problems.append(f"node {key} has no node_status == {status} edge (an unwired edge ends the "
                                "flow silently)")
        for child in node.get("childNodes") or []:
            if not child.get("nodeId"):
                problems.append(f"node {key} has an edge without a target")
    if problems:
        raise flowlib.FlowValidationError(problems)


# ---------------------------------------------------------------------------
# Connect inbound flow additions
# ---------------------------------------------------------------------------

def add_language_block(flow: dict, lang: str) -> dict:
    """LS4: insert UpdateContactData(LanguageCode) between the TTS voice action and its successor."""
    flow = copy.deepcopy(flow)
    actions = flow["Actions"]
    tts = next((a for a in actions if a.get("Identifier") == TTS_ACTION_ID), None)
    if tts is None or tts.get("Type") != "UpdateContactTextToSpeechVoice":
        raise flowlib.FlowValidationError(f"inbound flow has no UpdateContactTextToSpeechVoice {TTS_ACTION_ID}")
    if any(a.get("Identifier") == LANGUAGE_ACTION_ID for a in actions):
        raise flowlib.FlowValidationError(f"inbound flow already has action {LANGUAGE_ACTION_ID}")
    successor = (tts.get("Transitions") or {}).get("NextAction")
    language_action = {
        "Parameters": {"LanguageCode": lang},
        "Identifier": LANGUAGE_ACTION_ID,
        "Type": "UpdateContactData",
        "Transitions": {"NextAction": successor,
                        "Errors": [{"NextAction": successor, "ErrorType": "NoMatchingError"}]},
    }
    tts["Transitions"]["NextAction"] = LANGUAGE_ACTION_ID
    for err in tts["Transitions"].get("Errors") or []:
        err["NextAction"] = LANGUAGE_ACTION_ID
    actions.insert(actions.index(tts) + 1, language_action)
    am = flow.setdefault("Metadata", {}).setdefault("ActionMetadata", {})
    tts_meta = am.setdefault(TTS_ACTION_ID, {})
    position = copy.deepcopy(tts_meta.get("position") or {"x": 0, "y": 0})
    # Same metadata shape as the Set voice block with "Set language" in LiveSyncFlow.json.
    tts_meta.update({
        "children": [LANGUAGE_ACTION_ID],
        "fragments": {"SetContactData": LANGUAGE_ACTION_ID},
        "overrideLanguageAttribute": True,
    })
    tts_meta.setdefault("parameters", {})["TextToSpeechVoice"] = {"languageCode": lang}
    am[LANGUAGE_ACTION_ID] = {"position": position, "dynamicParams": []}
    flowlib.validate_connect_flow(flow)
    return flow


def _retarget(action: dict, old: str, new: str) -> None:
    tr = action.get("Transitions") or {}
    if tr.get("NextAction") == old:
        tr["NextAction"] = new
    for kind in ("Conditions", "Errors"):
        for entry in tr.get(kind) or []:
            if entry.get("NextAction") == old:
                entry["NextAction"] = new


def add_email_link_block(flow: dict, function_arn: str, message: str) -> dict:
    """Insert, right before the Agentic CX block: invoke the send-link Lambda (with the Customer Profiles
    email as CustomerEmail) -> if $.External.linkSent == "true" play `message` -> Agentic CX.

    Every path that reached the Agentic CX block now reaches the Lambda first; every Lambda / Compare
    outcome (including errors) still ends in the Agentic CX block, so a failed email never ends the call.
    """
    flow = copy.deepcopy(flow)
    actions = flow["Actions"]
    acx = config.AGENTIC_CX_ACTION_ID
    if not any(a.get("Identifier") == acx for a in actions):
        raise flowlib.FlowValidationError(f"inbound flow has no Agentic CX action {acx}")
    new_ids = {SEND_LINK_ACTION_ID, LINK_SENT_CHECK_ID, LINK_SENT_MESSAGE_ID}
    if any(a.get("Identifier") in new_ids for a in actions):
        raise flowlib.FlowValidationError("inbound flow already has the email link actions")
    if not message.strip():
        raise flowlib.FlowValidationError("empty link_sent_message")
    for a in actions:
        _retarget(a, acx, SEND_LINK_ACTION_ID)
    if flow.get("StartAction") == acx:
        flow["StartAction"] = SEND_LINK_ACTION_ID
    block = [
        {"Parameters": {"LambdaFunctionARN": function_arn,
                        "InvocationTimeLimitSeconds": LAMBDA_TIMEOUT_SECONDS,
                        "InvocationType": "SYNCHRONOUS",
                        "LambdaInvocationAttributes": {"CustomerEmail": f"$.Attributes.{config.CUSTOMER_EMAIL_FIELD}"},
                        "ResponseValidation": {"ResponseType": "STRING_MAP"}},
         "Identifier": SEND_LINK_ACTION_ID, "Type": "InvokeLambdaFunction",
         "Transitions": {"NextAction": LINK_SENT_CHECK_ID,
                         "Errors": [{"NextAction": acx, "ErrorType": "NoMatchingError"}]}},
        {"Parameters": {"ComparisonValue": "$.External.linkSent"},
         "Identifier": LINK_SENT_CHECK_ID, "Type": "Compare",
         "Transitions": {"NextAction": acx,
                         "Conditions": [{"NextAction": LINK_SENT_MESSAGE_ID,
                                         "Condition": {"Operator": "Equals", "Operands": ["true"]}}],
                         "Errors": [{"NextAction": acx, "ErrorType": "NoMatchingCondition"}]}},
        {"Parameters": {"Text": message},
         "Identifier": LINK_SENT_MESSAGE_ID, "Type": "MessageParticipant",
         "Transitions": {"NextAction": acx, "Errors": [{"NextAction": acx, "ErrorType": "NoMatchingError"}]}},
    ]
    idx = next(i for i, a in enumerate(actions) if a.get("Identifier") == acx)
    actions[idx:idx] = block
    am = flow.setdefault("Metadata", {}).setdefault("ActionMetadata", {})
    base = (am.get(acx) or {}).get("position") or {"x": 880, "y": 40}
    am[SEND_LINK_ACTION_ID] = {"position": {"x": base["x"], "y": base["y"] + 900},
                               "parameters": {"LambdaFunctionARN": {"displayName": function_arn.rsplit(":", 1)[-1]}},
                               "dynamicMetadata": {"CustomerEmail": True}}
    am[LINK_SENT_CHECK_ID] = {"position": {"x": base["x"] + 240, "y": base["y"] + 900},
                              "conditions": [], "conditionMetadata": [
                                  {"operator": {"name": "Equals", "value": "Equals", "shortDisplay": "="},
                                   "value": "true"}]}
    am[LINK_SENT_MESSAGE_ID] = {"position": {"x": base["x"] + 480, "y": base["y"] + 900}}
    flowlib.validate_connect_flow(flow)
    return flow


_base_render_inbound_flow = flowlib.render_inbound_flow


def render_inbound_flow(ctx_values: dict, captured_action: dict, captured_metadata: dict | None) -> dict:
    """flowlib.render_inbound_flow + language block + email link block (values travel in the ctx)."""
    ctx = dict(ctx_values)
    lang = ctx.pop(LANGUAGE_CTX_KEY, None)
    arn = ctx.pop(SEND_LINK_CTX_KEY, None)
    message = ctx.pop(LINK_MESSAGE_CTX_KEY, None)
    missing = [k for k, v in ((LANGUAGE_CTX_KEY, lang), (SEND_LINK_CTX_KEY, arn), (LINK_MESSAGE_CTX_KEY, message))
               if not v]
    if missing:
        raise flowlib.FlowValidationError(f"{', '.join(missing)} missing from the inbound flow context")
    flow = _base_render_inbound_flow(ctx, captured_action, captured_metadata)
    return add_email_link_block(add_language_block(flow, lang), arn, message)


# ---------------------------------------------------------------------------
# Run options and runtime values shared by the hooks
# ---------------------------------------------------------------------------

@dataclass
class LiveSyncOptions:
    """Set by deploy_livesync.py before deploy.main runs."""
    test_email: str = ""                 # every caller's link goes here (demo); "" = Customer Profiles email
    email_from: str = ""                 # requested SES sender (--email-from); else chosen in the precheck
    link_ttl_minutes: int = infra.DEFAULT_LINK_TTL_MINUTES
    live_sync_api_key: str | None = field(default=None, repr=False)
    live_sync_deployment_key: str | None = field(default=None, repr=False)
    touchpoint_js: Path | None = None
    acxd_api_key: str | None = field(default=None, repr=False)


OPTIONS = LiveSyncOptions()
# Values discovered during the run: stack outputs, send-link ARN, page / SES results.
RUNTIME: dict = {}


def _clients(region: str):
    import boto3
    import botocore.config
    cfg = botocore.config.Config(retries={"mode": "standard", "max_attempts": 5})
    return {name: boto3.client(name, region_name=region, config=cfg)
            for name in ("cloudformation", "sesv2", "s3", "secretsmanager", "connect")}


def stack_template(cfg, messages: dict) -> dict:
    msgs = messages[cfg.language]["email"]
    return infra.build_template(
        prefix=cfg.name_prefix, instance_id=cfg.instance_id, language=cfg.language,
        email_from=OPTIONS.email_from or DRYRUN_SENDER, test_email=OPTIONS.test_email,
        link_ttl_minutes=OPTIONS.link_ttl_minutes,
        email_subject=msgs["subject"], email_body=msgs["body"])


DRYRUN_SENDER = "dry-run-sender@example.com"


def _sender_problem(found: dict, region: str, instance_id: str) -> str | None:
    """Why no sender can be used (None = at least one candidate)."""
    acc = found["account"]
    if not acc["production"]:
        return (f"区域 {region} 的 SES 账号仍在沙盒中，只能发给已验证的地址，无法把链接发给来电客户。"
                "请先在 SES 控制台申请 production access（Request production access），获批后重新部署。")
    if not acc["sendingEnabled"] or acc["enforcement"] == "SHUTDOWN":
        return (f"区域 {region} 的 SES 账号当前不能发信（SendingEnabled={acc['sendingEnabled']}，"
                f"EnforcementStatus={acc['enforcement']}），请先在 SES 控制台处理。")
    if not found["addresses"]:
        return (f"Connect 实例 {instance_id} 没有任何邮件地址。请先在 Connect 控制台启用 Email 渠道并创建邮件地址"
                "（Channels → Email addresses），确保其域名已在 SES 中验证，然后重新部署。")
    if not found["candidates"]:
        listed = ", ".join(a["address"] for a in found["addresses"])
        return (f"Connect 实例 {instance_id} 的邮件地址（{listed}）都没有在区域 {region} 的 SES 中验证为可发信"
                "（地址本身或其域名的 VerifiedForSendingStatus 都不是 true），Lambda 无法用它们发信。"
                "请在 SES 中验证对应域名后重新部署。")
    return None


def select_sender(cfg, clients: dict | None = None) -> str:
    """Read-only, before any write: pick the SES sender among the Connect instance's email addresses that
    can mail any recipient (SES production access + verified for sending).  None usable -> ConfigError."""
    import deploy
    clients = clients or _clients(cfg.region)
    found = infra.find_sender_candidates(clients["connect"], clients["sesv2"], cfg.instance_id)
    problem = _sender_problem(found, cfg.region, cfg.instance_id)
    if problem:
        raise deploy.ConfigError(f"{problem}本次没有执行任何写操作。")
    candidates = found["candidates"]
    by_address = {c["address"].lower(): c for c in candidates}
    if OPTIONS.email_from:
        chosen = by_address.get(OPTIONS.email_from.lower())
        if chosen is None:
            raise deploy.ConfigError(
                f"--email-from {OPTIONS.email_from} 不是 Connect 实例 {cfg.instance_id} 中可用于发信的邮件地址；"
                "可选：" + ", ".join(c["address"] for c in candidates))
        source = "--email-from"
    elif deploy._can_prompt(cfg):
        options = [(c["address"], f"{c['address']}" + (f"（{c['displayName']}）" if c["displayName"] else "")
                    + ("" if c["verifiedBy"] == "address" else "，域名已验证"))
                   for c in candidates]
        picked = cfg.prompter.choice("请选择发送邮件链接的地址（Connect 实例中已出 SES 沙盒、可发信的邮件地址）：",
                                     options, default=candidates[0]["address"])
        chosen, source = by_address[picked.lower()], "interactive"
    elif len(candidates) == 1:
        chosen, source = candidates[0], "only-candidate"
    else:
        raise deploy.ConfigError("Connect 实例中有多个可用于发信的邮件地址，请用 --email-from 指定其中一个："
                                 + ", ".join(c["address"] for c in candidates))
    OPTIONS.email_from = chosen["address"]
    RUNTIME.update(emailFrom=chosen["address"], emailFromSource=source, sesProduction=True)
    logger.info("email sender %s (source: %s; verified by SES %s identity)", chosen["address"], source,
                chosen["verifiedBy"])
    return chosen["address"]


def send_link_arn(cfg) -> str:
    return RUNTIME.get("sendLinkFunctionArn") or infra.placeholder_send_link_arn(
        cfg.region, cfg.account_id, cfg.name_prefix)


def write_state(out_dir: Path, values: dict, secret: str | None) -> Path:
    path = Path(out_dir) / STATE_NAME
    old = load_state(out_dir) or {}
    old.update({k: v for k, v in values.items() if v is not None})
    text = json.dumps(old, ensure_ascii=False, indent=2) + "\n"
    flowlib.assert_no_secret(text, secret)
    for key in (OPTIONS.live_sync_api_key, OPTIONS.live_sync_deployment_key):
        flowlib.assert_no_secret(text, key)
    _write_private(path, text.encode("utf-8"))
    return path


def load_state(out_dir: Path) -> dict | None:
    try:
        return json.loads((Path(out_dir) / STATE_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Real-run step: page infrastructure (runs inside the Connect phase, after the ACXD phase)
# ---------------------------------------------------------------------------

def deploy_page_infra(cfg, messages: dict) -> dict:
    """CloudFormation stack -> Connect AssociateLambdaFunction -> SES identity -> page upload ->
    Live Sync keys.  Returns the RUNTIME values (also written to livesync_state.json).

    AWS / stack failures become DeployError (exit 1) with the failing service named, so deploy.main
    reports them instead of mislabelling them as Connect errors."""
    from botocore.exceptions import BotoCoreError, ClientError
    deploy_mod = __import__("deploy")
    try:
        return _deploy_page_infra(cfg, messages)
    except ClientError as exc:
        err = exc.response.get("Error", {})
        op = getattr(exc, "operation_name", "?")
        logger.error("page infrastructure call %s failed: %s %s", op, err.get("Code"), err.get("Message"))
        raise deploy_mod.DeployError(f"页面 / 邮件基础设施部署失败（{op}: {err.get('Code')}）", 1) from exc
    except (BotoCoreError, RuntimeError, ValueError) as exc:
        logger.error("page infrastructure failed: %s", exc)
        raise deploy_mod.DeployError(f"页面 / 邮件基础设施部署失败: {exc}", 1) from exc


def _deploy_page_infra(cfg, messages: dict) -> dict:
    clients = _clients(cfg.region)
    name = infra.stack_name(cfg.name_prefix)
    template = stack_template(cfg, messages)
    deploy_mod = __import__("deploy")
    deploy_mod.write_artifact(cfg.out_dir, STACK_ARTIFACT, template, cfg.api_key)
    try:
        outputs = infra.deploy_stack(clients["cloudformation"], name, infra.template_body(template),
                                     allow_foreign=cfg.overwrite_untagged)
    except infra.StackOwnershipError as exc:
        raise deploy_mod.ConfigError(_foreign_stack_message(name)) from exc
    RUNTIME.update(stackName=name, pageUrl=outputs["PageUrl"], bucket=outputs["BucketName"],
                   distributionId=outputs["DistributionId"], sendLinkFunctionArn=outputs["SendLinkFunctionArn"],
                   secretArn=outputs["SecretArn"])
    added = infra.associate_lambda(clients["connect"], cfg.instance_id, outputs["SendLinkFunctionArn"])
    logger.info("send-link Lambda %s the Connect instance", "associated with" if added else "already associated with")
    if not OPTIONS.email_from:  # select_sender runs in the Connect prechecks of every real run
        raise RuntimeError("no email sender selected")
    publish_page(cfg.out_dir, cfg.language, messages, clients, cfg.api_key)
    write_state(cfg.out_dir, {**RUNTIME, "language": cfg.language, "region": cfg.region,
                              "instanceId": cfg.instance_id, "namePrefix": cfg.name_prefix,
                              "testEmail": OPTIONS.test_email or None, "emailFrom": OPTIONS.email_from},
                cfg.api_key)
    return RUNTIME


def publish_page(out_dir: Path, lang: str, messages: dict, clients: dict, acxd_api_key: str | None) -> None:
    """Render the page, upload it to the stack bucket and store the Live Sync keys when given."""
    summary = generate_page(out_dir, lang, messages, acxd_api_key=acxd_api_key,
                            touchpoint_js=OPTIONS.touchpoint_js, cache_dir=Path(out_dir) / ".cache")
    if not summary["touchpoint"]:
        raise RuntimeError("Touchpoint SDK could not be vendored; the page cannot work without it")
    keys = infra.upload_page(clients["s3"], RUNTIME["bucket"], summary["pageDir"])
    logger.info("uploaded %d page files to s3://%s", len(keys), RUNTIME["bucket"])
    if OPTIONS.live_sync_api_key and OPTIONS.live_sync_deployment_key:
        infra.put_live_sync_keys(clients["secretsmanager"], RUNTIME["secretArn"], OPTIONS.live_sync_api_key,
                                 OPTIONS.live_sync_deployment_key)
        logger.info("Live Sync keys stored in the stack secret")
    elif OPTIONS.live_sync_api_key or OPTIONS.live_sync_deployment_key:
        logger.warning("only one of the two Live Sync keys was given; the secret was not changed")
    RUNTIME["liveSyncKeysConfigured"] = infra.live_sync_keys_configured(clients["secretsmanager"],
                                                                        RUNTIME["secretArn"])


def _foreign_stack_message(name: str) -> str:
    return (f"已存在同名 CloudFormation 栈 {name}，但它不是本脚本创建的（没有标签 "
            f"{config.CONNECT_OWNER_TAG_KEY}={config.OWNER_TAG}），为避免覆盖已停止，本次没有执行任何写操作。"
            "可选做法：用 --name-prefix <新前缀> 另建一套资源；确认要覆盖时加 --overwrite-untagged-resources")


# ---------------------------------------------------------------------------
# Profile installation
# ---------------------------------------------------------------------------

LIVE_SYNC_VERIFY_STEPS = (
    "1. 确认 ACXD build 为 BUILT、deployment 为 deployed；在 ACXD 画布打开 flow，确认 Live Sync 节点的六条出边"
    "（两个出口条件、Continuation、Timeout、Failure、Escalation）都有连线（L1）。",
    "2. 在 ACXD 应用 Settings → Access 复制 Live Sync API key 和 Deployment key，设置 LIVE_SYNC_API_KEY / "
    "LIVE_SYNC_DEPLOYMENT_KEY 后运行 python3 deploy_livesync.py --page-only（写入 Secrets Manager，L3）。",
    "3. 发件地址是部署时从 Connect 实例邮件地址中选出的（SES 已出沙盒、地址或域名已验证），收件地址无需验证。",
    "4. 把电话号码关联到入站流后拨打：听到“链接已发送到您的邮箱”，在测试邮箱中打开链接（CloudFront 页面），"
    "页面显示“已连接到您的通话”。",
    "5. 在电话里说出产品（例如“洗衣机”）和型号（例如“X200 Pro”），确认页面跟随选择；也可以直接点选，确认助手跟随页面。",
    "6. 口述故障并确认复述后，座席接起电话，Detail 视图显示产品名称 / 产品型号 / 故障描述。产品或型号为空时按 L2 检查对话记录。",
    "7. 页面一直不动但对话正常：检查 Live Sync key、页面语言与 ACXD 应用语言是否一致（channel connect-<语言>，L4 / L5），"
    "以及 CloudWatch 日志 /aws/lambda/<前缀>-livesync-session。",
    "8. 没有收到邮件：查看 /aws/lambda/<前缀>-livesync-send-link 的 outcome（skipped / failed），以及收件箱的垃圾邮件目录。",
)


def apply_profile() -> dict:
    """Point the base modules at the Live Sync flow, messages and hooks.

    Returns the replaced attributes so tests can restore them with restore_profile().
    """
    import deploy  # local import: deploy imports config / flowlib at module level

    names = (
        (config, "DEFAULT_NAME_PREFIX"), (config, "ASSUMPTIONS"), (config, "REQUIRED_CONNECT_PERMISSIONS"),
        (flowlib, "load_messages"), (flowlib, "render_acxd_flow"), (flowlib, "render_inbound_flow"),
        (deploy, "inbound_ctx"), (deploy, "build_flow_body"), (deploy, "prefix_resource_names"),
        (deploy, "VERIFY_STEPS"), (deploy, "ACXD_MODEL_LINE"), (deploy, "connect_prechecks"),
        (deploy, "run_connect_phase"), (deploy, "_preflight_connect"), (deploy, "format_run_parameters"),
        (deploy, "CONNECT_PREFIX_KEYS"), (config, "DEFAULT_LANGUAGE"), (config, "FALLBACK_LANGUAGE"),
        (config, "SUPPORTED_LANGUAGES"),
    )
    saved = {(m, n): getattr(m, n) for m, n in names}
    base = {n: v for (_, n), v in saved.items()}

    def inbound_ctx(cfg, messages, screenpop_arn, queue_arn, queue_display):
        return {**base["inbound_ctx"](cfg, messages, screenpop_arn, queue_arn, queue_display),
                LANGUAGE_CTX_KEY: cfg.language, SEND_LINK_CTX_KEY: send_link_arn(cfg),
                LINK_MESSAGE_CTX_KEY: messages[cfg.language]["connect"]["link_sent_message"]}

    def build_flow_body(cfg, nodes):
        body = base["build_flow_body"](cfg, nodes)
        body["description"] = "Live Sync product repair intake: collects " + ", ".join(config.FIELDS)
        return body

    def prefix_resource_names(prefix):
        return [
            ("flow", "ACXD flowId", config.acxd_flow_id(prefix)),
            ("journey", "Live Sync 节点", live_sync_node_name(prefix)),
            ("app", "ACXD 应用名", config.acxd_app_name(prefix)),
            ("screenpop", "Connect screenpop 流", config.screenpop_flow_name(prefix)),
            ("inbound", "Connect 入站流", config.inbound_flow_name(prefix)),
            ("stack", "CloudFormation 栈（页面 / 邮件）", infra.stack_name(prefix)),
        ]

    def connect_prechecks(client, cfg):
        """Base prechecks + read-only ownership check of the page stack (before any write)."""
        target = base["connect_prechecks"](client, cfg)
        cfn = _clients(cfg.region)["cloudformation"]
        try:
            stack = infra.check_stack(cfn, infra.stack_name(cfg.name_prefix), cfg.overwrite_untagged)
        except infra.StackOwnershipError as exc:
            raise deploy.ConfigError(_foreign_stack_message(infra.stack_name(cfg.name_prefix))) from exc
        outputs = infra.stack_outputs(stack)
        RUNTIME["existingStack"] = bool(stack)
        if outputs.get("SendLinkFunctionArn"):
            RUNTIME["sendLinkFunctionArn"] = outputs["SendLinkFunctionArn"]
            RUNTIME["pageUrl"] = outputs.get("PageUrl")
        # No sender that can mail callers -> stop here, before the confirmation and any write.
        select_sender(cfg)
        return target

    def run_connect_phase(cfg, writer, messages, *args, **kwargs):
        deploy_page_infra(cfg, messages)  # the inbound flow needs the send-link Lambda ARN
        writer.update("connect", livesyncStack=RUNTIME.get("stackName"), livesyncPageUrl=RUNTIME.get("pageUrl"))
        return base["run_connect_phase"](cfg, writer, messages, *args, **kwargs)

    def preflight_connect(cfg, messages, plan, resolved, *args, **kwargs):
        name = infra.stack_name(cfg.name_prefix)
        plan.write("cloudformation", "UpdateStack" if RUNTIME.get("existingStack") else "CreateStack", name)
        plan.write("connect", "AssociateLambdaFunction", infra.send_link_function_name(cfg.name_prefix))
        plan.write("s3", "PutObject", f"{name} page bucket")
        if OPTIONS.live_sync_api_key and OPTIONS.live_sync_deployment_key:
            plan.write("secretsmanager", "PutSecretValue", f"{name} secret (Live Sync keys)")
        if not RUNTIME.get("sendLinkFunctionArn"):
            plan.warn(f"stack {name} does not exist yet; the inbound flow is rendered with the placeholder "
                      f"send-link ARN {send_link_arn(cfg)}")
        deploy.write_artifact(cfg.out_dir, STACK_ARTIFACT, stack_template(cfg, messages), cfg.api_key)
        resolved.update(livesyncStack=name, livesyncPageUrl=RUNTIME.get("pageUrl"),
                        testEmail=OPTIONS.test_email or None, emailFrom=OPTIONS.email_from)
        return base["_preflight_connect"](cfg, messages, plan, resolved, *args, **kwargs)

    def format_run_parameters(cfg, target, marks=None):
        text = base["format_run_parameters"](cfg, target, marks)
        if not cfg.connect_phase:
            return text
        extra = [f"  测试邮件地址:   {OPTIONS.test_email or '（未指定：发往客户资料中的邮箱）'}",
                 f"  发件地址:       {OPTIONS.email_from}（Connect 实例邮件地址，SES 生产环境，已验证）",
                 f"  链接有效期:     {OPTIONS.link_ttl_minutes} 分钟（通话结束后立即失效）"]
        return text + "\n" + "\n".join(extra)

    assumptions = copy.copy(config.ASSUMPTIONS)
    assumptions.update(LIVE_SYNC_ASSUMPTIONS)
    config.DEFAULT_NAME_PREFIX = DEFAULT_NAME_PREFIX
    # English only: the workshop publishes Live Sync in en-US, and the zh-CN deployment failed in the
    # field without a reason.  No language prompt, no --language zh-CN, no fallback.
    config.SUPPORTED_LANGUAGES = (LANGUAGE,)
    config.DEFAULT_LANGUAGE = config.FALLBACK_LANGUAGE = LANGUAGE
    config.ASSUMPTIONS = assumptions
    config.REQUIRED_CONNECT_PERMISSIONS = config.REQUIRED_CONNECT_PERMISSIONS + (
        "connect:AssociateLambdaFunction", "connect:ListLambdaFunctions", "connect:SearchEmailAddresses",
        "ses:GetAccount", "ses:GetEmailIdentity")
    flowlib.load_messages = load_messages
    flowlib.render_acxd_flow = render_livesync_acxd_flow
    flowlib.render_inbound_flow = render_inbound_flow
    deploy.inbound_ctx = inbound_ctx
    deploy.build_flow_body = build_flow_body
    deploy.prefix_resource_names = prefix_resource_names
    deploy.VERIFY_STEPS = LIVE_SYNC_VERIFY_STEPS
    deploy.ACXD_MODEL_LINE = (f"  ACXD 模型:      {config.GENERATIVE_MODEL_TYPE}"
                              "（Live Sync 节点 / Generative Text）")
    deploy.connect_prechecks = connect_prechecks
    deploy.run_connect_phase = run_connect_phase
    deploy._preflight_connect = preflight_connect
    deploy.format_run_parameters = format_run_parameters
    deploy.CONNECT_PREFIX_KEYS = deploy.CONNECT_PREFIX_KEYS + ("stack",)  # the stack is part of the Connect phase
    return saved


def restore_profile(saved: dict) -> None:
    for (module, name), value in saved.items():
        setattr(module, name, value)


# ---------------------------------------------------------------------------
# Touchpoint SDK (pinned, integrity-checked)
# ---------------------------------------------------------------------------

def _integrity(data: bytes) -> str:
    return "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode("ascii")


def _read_member(tgz: bytes, member: str) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(tgz), mode="r:gz") as tar:
        info = tar.getmember(member)  # exact name only, never extractall
        if not info.isfile():
            raise ValueError(f"{member} is not a regular file")
        fh = tar.extractfile(info)
        return fh.read()


def fetch_touchpoint(cache_dir: Path, offline: bool = False) -> tuple[bytes, bytes] | None:
    """(index.umd.js, LICENSE) of the pinned Touchpoint tarball; downloads once into cache_dir.

    The tarball must match TOUCHPOINT_INTEGRITY (npm dist.integrity).  offline=True (dry-run) never
    touches the network and returns None when nothing is cached.
    """
    cache = Path(cache_dir) / f"touchpoint-web-{TOUCHPOINT_VERSION}.tgz"
    data = None
    if cache.exists():
        data = cache.read_bytes()
        if _integrity(data) != TOUCHPOINT_INTEGRITY:
            logger.warning("cached %s fails the integrity check; ignoring it", cache)
            data = None
    if data is None:
        if offline:
            return None
        logger.info("downloading %s@%s from the npm registry", TOUCHPOINT_PACKAGE, TOUCHPOINT_VERSION)
        req = urllib.request.Request(TOUCHPOINT_TARBALL_URL, headers={"User-Agent": "acxd-livesync-deploy/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        if _integrity(data) != TOUCHPOINT_INTEGRITY:
            raise RuntimeError(f"{TOUCHPOINT_TARBALL_URL} failed the pinned integrity check")
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(data)
    return _read_member(data, TOUCHPOINT_UMD_MEMBER), _read_member(data, TOUCHPOINT_LICENSE_MEMBER)


# ---------------------------------------------------------------------------
# Page (static, no secrets: the Live Sync keys come from /api/session with a valid link)
# ---------------------------------------------------------------------------

def page_config(lang: str, messages: dict) -> dict:
    """window.LIVESYNC_CONFIG: catalog, UI text and the session path.  Holds no key and no contact id."""
    if lang not in messages:
        raise flowlib.FlowValidationError(f"no Live Sync messages for language {lang}")
    msgs = messages[lang]
    return {"languageCode": lang, "sessionPath": SESSION_PATH,
            "catalog": copy.deepcopy(msgs["catalog"]), "text": copy.deepcopy(msgs["page"])}


def _write_private(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def generate_page(out_dir: Path, lang: str, messages: dict, *, acxd_api_key: str | None = None,
                  touchpoint_js: Path | None = None, cache_dir: Path | None = None,
                  offline: bool = False) -> dict:
    """Write <out_dir>/livesync-page/ (index.html, app.js, styles.css, config.js, vendor/)."""
    out_dir = Path(out_dir)
    page_dir = out_dir / PAGE_DIR_NAME
    cfg = page_config(lang, messages)
    secrets = [acxd_api_key, OPTIONS.live_sync_api_key, OPTIONS.live_sync_deployment_key]
    for name in WEB_FILES:
        text = (WEB_DIR / name).read_text(encoding="utf-8")
        if name == "index.html":
            text = text.replace("{{LANG}}", lang).replace("{{TITLE}}", _html_escape(cfg["text"]["title"]))
        for s in secrets:
            flowlib.assert_no_secret(text, s)
        _write_private(page_dir / name, text.encode("utf-8"))
    config_js = ("// Generated by deploy_livesync.py. Public: no keys, no contact ids.\n"
                 "window.LIVESYNC_CONFIG = " + json.dumps(cfg, ensure_ascii=False, indent=2) + ";\n")
    for s in secrets:
        flowlib.assert_no_secret(config_js, s)
    _write_private(page_dir / "config.js", config_js.encode("utf-8"))

    vendored = None
    if touchpoint_js is not None:
        logger.warning("using the local Touchpoint bundle %s (not integrity-checked)", touchpoint_js)
        _write_private(page_dir / TOUCHPOINT_VENDOR_NAME, Path(touchpoint_js).read_bytes())
        vendored = str(touchpoint_js)
    else:
        fetched = fetch_touchpoint(cache_dir or (out_dir / ".cache"), offline=offline)
        if fetched is None:
            logger.warning("Touchpoint SDK not vendored (offline); a real run downloads it")
        else:
            umd, license_text = fetched
            _write_private(page_dir / TOUCHPOINT_VENDOR_NAME, umd)
            _write_private(page_dir / "vendor" / "TOUCHPOINT-LICENSE.txt", license_text)
            vendored = f"{TOUCHPOINT_PACKAGE}@{TOUCHPOINT_VERSION}"
    return {"pageDir": page_dir, "language": lang, "touchpoint": vendored}


def _html_escape(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;").replace("'", "&#x27;"))
