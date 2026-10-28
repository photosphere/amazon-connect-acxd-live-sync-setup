"""Constants and every unverified assumption (A1..A22) of the deployment.

Each assumption from design.md §2.3 lives in exactly one constant or function
below, so it can be adjusted without touching any other module.  Source tags
(S1..S9) refer to design.md §0.
"""

from __future__ import annotations

import copy
import re
from collections import OrderedDict

# ---------------------------------------------------------------------------
# Fields: single source of truth (design §3.1, §6.7)
# ---------------------------------------------------------------------------

# The three values collected by ACXD and shown in the agent workspace.  Used for
# ACXD context variable names, generative_text output names, state modifications,
# $.AgenticCX.ContextVariables.<f>, contact attribute keys and $.Attributes.<f>.
FIELDS = ("DeviceName", "DeviceModel", "FaultDescription")

# Caller details looked up in Amazon Connect Customer Profiles by the inbound flow (NOT
# collected by ACXD, so never part of FIELDS).  Written as user-defined contact attributes
# and read by the screen pop as $.Attributes.<f>.
CUSTOMER_NAME_FIELD = "CustomerName"
CUSTOMER_PHONE_FIELD = "CustomerPhone"
CUSTOMER_EMAIL_FIELD = "CustomerEmail"
CUSTOMER_FIELDS = (CUSTOMER_NAME_FIELD, CUSTOMER_PHONE_FIELD, CUSTOMER_EMAIL_FIELD)
# Screen pop order (attribute bar and Sections table): customer first, then the device
# fields.  Reorder this tuple freely; FIELDS stays the ACXD-only set.
SCREENPOP_FIELDS = CUSTOMER_FIELDS + FIELDS


def _upper_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).upper()


# {"FIELD_DEVICE_NAME": "DeviceName", ...} derived from FIELDS (never hand-written).
FIELD_TOKENS = {f"FIELD_{_upper_snake(f)}": f for f in FIELDS}
# {"GT_DEVICE_NAME": "DeviceName", ...}: whole-value placeholders of the generative_text
# extraction nodes in acxd/acxd_flow.json (one node per field).
GT_PLACEHOLDERS = {f"GT_{_upper_snake(f)}": f for f in FIELDS}

# ACXD context variable definition (S3 acxd-context-variables).
# Real workspace variables carry isSensitive (live GET /sdk/context-variables);
# deploy.ensure_context_variables only compares schema.type.
CONTEXT_VARIABLE_SCHEMA = {"type": "string", "isSensitive": False}
CONTEXT_VARIABLE_METADATA = {"path": "/device-fault", "tags": ["device-fault-selfservice"]}
# Application and flow share the same metadata block.
APPLICATION_METADATA = CONTEXT_VARIABLE_METADATA
FLOW_METADATA = CONTEXT_VARIABLE_METADATA
# Ownership marker of every resource whose name is derived from the prefix.  An existing resource
# with a derived name is only updated (overwritten) when it carries the marker; any other one stops
# the run before the first write (override: --overwrite-untagged-resources).
#   * ACXD application <prefix>SelfService and ACXD flow <prefix>Intake: OWNER_TAG in metadata.tags
#     (APPLICATION_METADATA / FLOW_METADATA are sent on every create and update).
#   * Connect flows '<prefix> ScreenPop Flow' / '<prefix> Inbound ACXD Flow': resource tag
#     CONNECT_OWNER_TAG_KEY = OWNER_TAG (set by CreateContactFlow), or - for flows created before the
#     tag existed - a published / saved content whose StartAction is the StartAction of our template.
OWNER_TAG = "device-fault-selfservice"
APPLICATION_OWNER_TAG = OWNER_TAG
CONNECT_OWNER_TAG_KEY = "acxd-setup"
CONNECT_OWNER_TAGS = {CONNECT_OWNER_TAG_KEY: OWNER_TAG}
assert OWNER_TAG in APPLICATION_METADATA["tags"] and OWNER_TAG in FLOW_METADATA["tags"]

DEFAULT_NAME_PREFIX = "AgenticCare"
# Environment variable of --name-prefix (precedence: CLI > NAME_PREFIX > interactive prompt > default).
NAME_PREFIX_ENV = "NAME_PREFIX"
# Length limit of the ACXD application name (S3 CreateApplication: name 1-100).  Connect documents only
# a minimum length of 1 for a contact flow Name (API_CreateContactFlow).
ACXD_APP_NAME_MAX = 100


def acxd_flow_id(prefix: str) -> str:
    return f"{prefix}Intake"


def acxd_journey_name(prefix: str) -> str:
    """metadata.name of the Generative Journey node ({{JOURNEY_NAME}} in acxd/acxd_flow.json)."""
    return f"{prefix}Journey"


def acxd_app_name(prefix: str) -> str:
    return f"{prefix}SelfService"


def screenpop_flow_name(prefix: str) -> str:
    return f"{prefix} ScreenPop Flow"


def inbound_flow_name(prefix: str) -> str:
    return f"{prefix} Inbound ACXD Flow"


# ---------------------------------------------------------------------------
# ACXD endpoint and wire protocol (S2, S4, S6)
# ---------------------------------------------------------------------------

# Regions where ACXD and the Agentic CX block are available (S6 regions).
ACXD_REGIONS = (
    "us-east-1", "us-west-2", "eu-central-1", "eu-west-2", "ap-southeast-2",
    "ap-northeast-2", "ap-northeast-1", "ap-southeast-1", "ca-central-1",
)
# S4 endpoint/bdd.js.  A2: ACXD region defaults to the Connect region (taken from --instance-arn).
ACXD_ENDPOINT_TEMPLATE = "https://api.acxd.connect.{region}.amazonaws.com"

# Fixed prefix of Admin Hub API keys.  Split on purpose: a literal search for the
# joined prefix then only finds leaked keys, never this source file.
API_KEY_PREFIX = "_".join(("acxd", "live", ""))  # join() is not constant-folded into the .pyc

# ---------------------------------------------------------------------------
# Two-input defaults (--instance-arn + ACXD API key)
# ---------------------------------------------------------------------------

# A11: default conversation language; without an explicit --language a build or
# request rejected for language reasons is retried once with FALLBACK_LANGUAGE.
DEFAULT_LANGUAGE = "zh-CN"
FALLBACK_LANGUAGE = "en-US"
# Languages offered by --language and the interactive prompt (a profile may narrow this, e.g. the
# Live Sync profile only supports en-US: then there is no prompt and no fallback).
SUPPORTED_LANGUAGES = ("zh-CN", "en-US")
# Queue used when neither --queue-arn nor --queue-name is given (else first STANDARD queue by name).
DEFAULT_QUEUE_NAME = "BasicQueue"
# Workspace discovery source 3 (ListWorkspaces 401/403 only): at most this many published contact
# flows are read with DescribeContactFlow to find a ConnectParticipantWithAgenticCX WorkspaceId.
WORKSPACE_DISCOVERY_MAX_FLOWS = 100
# A21: an ACXD rejection counts as a language failure when the error message or the
# FAILED build object - with the LANGUAGE_ECHO_KEYS fields removed, because every
# build echoes its languageSettings - contains one of these lower-case keywords.
LANGUAGE_FAILURE_KEYWORDS = ("language", "locale", "zh-cn")
LANGUAGE_ECHO_KEYS = frozenset({"languageSettings", "languageCodes", "languageCode", "mainLanguageCode"})
# Console links printed in the final summary.  The ACXD URL matches the
# application URL format of the admin website; the flow editor URL is a
# convenience link that no API can verify.
ACXD_CONSOLE_APP_URL = "{access_url}/acxd/w/{workspace_id}/applications/{application_id}"
CONNECT_FLOW_EDIT_URL = "{access_url}/contact-flows/edit?id={flow_arn}"

# S4 schemas_0.js: settings.defaultFlows uses jsonName "ACXD.<Name>" on the wire.
DEFAULT_FLOWS_WIRE_NAMES = {
    "welcome": "ACXD.Welcome",
    "fallback": "ACXD.Fallback",
    "unknown": "ACXD.Unknown",
    "escalation": "ACXD.Escalation",
    "frustration": "ACXD.Frustration",
    "help": "ACXD.Help",
    "repeat": "ACXD.Repeat",
    "resume": "ACXD.Resume",
}

ACXD_ENVIRONMENTS = ("development", "qa", "staging", "production")
DEFAULT_ACXD_ENVIRONMENT = "production"

# S4 FlowNodeType enum (21 values).
FLOW_NODE_TYPES = frozenset({
    "basic", "choice", "data_request", "define", "end", "escalate",
    "generative_journey", "generative_task", "generative_text", "intent_capture",
    "knowledge_base", "loop", "multimodal", "note", "redirect", "split", "start",
    "transform", "user_choice", "user_input", "wait",
    # Not in the S4 enum, but the live API uses it for the Exit application node.
    "terminate",
})

# ---------------------------------------------------------------------------
# Generative Journey / Generative Text (reference app workshop1014, flow Maintplink;
# .agents/tasks/genjourney-evidence/ref_flow_Maintplink.json)
# ---------------------------------------------------------------------------

# A6: the ONE model of every generative node (Generative Journey and Generative Text).
# Live value of $.nodes["4b13bb2c-..."].metadata.generativeJourney.modelType and
# $.nodes["5ffc4b5f-..."].metadata.generativeText.modelType in flow Maintplink
# (Claude Haiku 4.5; SDK enum GenerativeModelType).
GENERATIVE_MODEL_TYPE = "anthropic.claude-haiku-4-5"

# metadata.generativeJourney settings besides modelType / prompt / exitConditions / tools
# (Maintplink shape; maxTokens 8192 and temperature 0.3 from flow Main).
GENERATIVE_JOURNEY_SETTINGS = {
    "enableZeroTurnMode": False,
    "maxSteps": 10,
    "maxTokens": 8192,
    "temperature": 0.3,
    "promptVersion": "latest",
}
# metadata.generativeText settings besides name / modelType / prompt (Maintplink shape).
GENERATIVE_TEXT_SETTINGS = {"maxTokens": 4000, "temperature": 0.3}

# A5: exit condition names of the Generative Journey, in System.gjConditionIndex order
# (index 0, 1).  Both route to the extraction chain and the escalate node.
GJ_EXIT_CONDITION_NAMES = ("detailsCollected", "humanHandoff")

# Node types the generated ACXD flow must never contain (the caller's turns are taken
# by the Generative Journey, so no User input node is used).
FORBIDDEN_NODE_TYPES = frozenset({"user_input"})

# A3: how a generative_text output (metadata.generativeText.name) is read back (Python
# format string; renders as "{DeviceName:NLX.Local}").  Live: escalate state modification
# value "{modelNumber:NLX.Local}" in flow Maintplink.
LOCAL_VAR_TEMPLATE = "{{{name}:NLX.Local}}"


def state_mod_set_context(name: str) -> dict:
    """A4: state modification (escalate node) that copies local `name` into context variable `name`."""
    return {
        "type": "context",
        "name": name,
        "modification": "set",
        "value": {"type": "constant", "value": LOCAL_VAR_TEMPLATE.format(name=name)},
    }


# A9: flow-scoped context variables stay empty; workspace variables are used.
FLOW_SCOPED_CONTEXT_VARIABLES: list = []

# A13: prevent build-triggered auto deploy racing the explicit deployment.
APPLICATION_DEPLOYMENT_SETTINGS = {"oneClickDeployEnabled": False}

# A14: LanguageSetting entry shape.  Confirmed from live data: the real app
# settings.languageSettings and build languageSettings are
# [{"languageCode": "en-US", "useLex3pAsr": false}] (SDK shape); the docs shape
# (useNativeLanguage/region) does not exist on the live service.
LANGUAGE_SETTING_EXTRA: dict = {"useLex3pAsr": False}


def language_settings(lang: str) -> list:
    """A14: the single source for application and build languageSettings."""
    # Read the module attribute at call time so tests can patch it.
    return [{"languageCode": lang, **copy.deepcopy(globals()["LANGUAGE_SETTING_EXTRA"])}]


# A12: deployment terminal states.
DEPLOYMENT_SUCCESS_STATUSES = ("deployed",)
DEPLOYMENT_FAILURE_MARKER = "fail"

# ---------------------------------------------------------------------------
# Connect and screenpop
# ---------------------------------------------------------------------------

# A1: keywords used to classify Agentic CX branches in the captured action
# (lower-cased JSON text of each Errors[] / Conditions[] entry).  Confirmed from
# the live flows 'AI Agent - ACXD' and 'TP - ACXD': InputTimeLimitExceeded is the
# 'Idle chat timeout' branch, NoMatchingCondition is the branch that reads
# $.AgenticCX.ContextVariables (-> set attributes), Escalation is a Condition.
AGENTIC_CX_BRANCH_KEYWORDS = {
    "escalation": ("escalat",),
    "idle": ("idle", "timeout", "timelimit"),
    "exit": ("nomatchingcondition",),
    "default": ("default",),
}
# Fixed Identifier of the Agentic CX action in the inbound flow (§4).
AGENTIC_CX_ACTION_ID = "d1a9e0c0-0000-4000-8000-000000000004"
# A1: Type of the real Agentic CX block action (live flow 'AI Agent - ACXD',
# evidence/connect_agentic_cx_action__AI_Agent_ACXD.json).
AGENTIC_CX_ACTION_TYPE = "ConnectParticipantWithAgenticCX"
# A1: Parameters other than AgentConfiguration, copied from the real action in
# flow 'AI Agent - ACXD' (console defaults: agentic voice + Pondering Pong filler).
AGENTIC_CX_STATIC_PARAMETERS = {
    "SpeechRecognitionConfiguration": {"SpeechRecognitionEngine": "AMAZON_AGENTIC_VOICE"},
    "AudioFillerConfiguration": {
        "Enabled": True,
        "AudioType": "MELODY_PONDERING_PONG",
        "StartDelayInMilliseconds": 2500,
        "MinimumPlayDurationInMilliseconds": 3000,
        "ResponseDeliveryDelayInMilliseconds": 500,
    },
}
# A1: branch set of the real action in flow 'AI Agent - ACXD' (NextAction targets
# are filled in by flowlib.rebuild_agentic_cx_transitions).
AGENTIC_CX_TRANSITIONS = {
    "NextAction": "",
    "Errors": [
        {"ErrorType": "NoMatchingError"},
        {"ErrorType": "NoMatchingCondition"},
        {"ErrorType": "InputTimeLimitExceeded"},
    ],
    "Conditions": [{"Condition": {"Operator": "Equals", "Operands": ["Escalation"]}}],
}
# A1: console display names in Metadata.ActionMetadata of the real action in flow
# 'AI Agent - ACXD' (AgentConfiguration display names are added per run).
AGENTIC_CX_METADATA_DISPLAY = {
    "SpeechRecognitionConfiguration": {
        "SpeechRecognitionEngine": {"displayName": "Amazon Connect agentic voice (recommended)"},
    },
    "AudioFillerConfiguration": {"AudioType": {"displayName": "Melody - Pondering Pong"}},
}
# A1: obvious placeholder Type of the unverified fallback fragment.
UNVERIFIED_ACTION_TYPE = "UNVERIFIED_AGENTIC_CX_ACTION"

# A8: how an ACXD message body references a context variable (Python format
# string; renders as "{DeviceName:NLX.Context}").  Corrected from live data:
# real message body 'Thank you, customer {customerId:NLX.Context}.'.
CONTEXT_VAR_MESSAGE_PLACEHOLDER = "{{{name}:NLX.Context}}"

# A17: voiceSettings injected into basic nodes with messages; None = not sent.
ACXD_NODE_VOICE_SETTINGS = None

# A20: metadata of the escalate node.  Add "sendContext": True if context
# variables do not come back to Connect.
ESCALATE_NODE_METADATA = {"name": "EscalateToAgent"}

# A19: show the three values in the HTML Sections table (True) or only in the
# plain-text AttributeBar (False).
SCREENPOP_SECTIONS_SHOW_VALUES = True

# NFR5: limit for the static ShowView parameters.
SHOWVIEW_PARAMETERS_MAX_BYTES = 4096

# ---------------------------------------------------------------------------
# Customer Profiles (A22; .agents/tasks/screenpop-profile-evidence/EVIDENCE.md)
# ---------------------------------------------------------------------------

# A22: GetCustomerProfile results live under $.Customer (live flow 'cn ScreenPop Flow' and
# devguide interactions-getcustomerprofile).  messages.json screenpop.customer.name_format
# uses {FirstName} / {LastName}; tokens must be separated (adjacent $.A$.B is not runtime-verified).
CUSTOMER_PROFILE_NAME_TOKENS = {"FirstName": "$.Customer.FirstName", "LastName": "$.Customer.LastName"}
# The caller's number is shown on every path, also when no profile is found.
CUSTOMER_PHONE_SOURCE = "$.CustomerEndpoint.Address"
CUSTOMER_EMAIL_SOURCE = "$.Customer.EmailAddress"
# Shown when EmailAddress is empty (PersonalEmailAddress cannot be requested by the block).
CUSTOMER_BUSINESS_EMAIL_SOURCE = "$.Customer.BusinessEmailAddress"

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

DEFAULT_BUILD_TIMEOUT = 600
DEFAULT_POLL_INTERVAL = 5

REQUIRED_CONNECT_PERMISSIONS = (
    "connect:DescribeInstance",
    "connect:DescribeQueue",
    "connect:ListQueues",
    "connect:ListContactFlows",
    "connect:DescribeContactFlow",
    "connect:CreateContactFlow",
    "connect:UpdateContactFlowContent",
    # CreateContactFlow with Tags (ownership marker) also needs TagResource.
    "connect:TagResource",
)

# Printed by --dry-run (FR6).
ASSUMPTIONS = OrderedDict([
    ("A1", "已按实网流确认：Agentic CX 块 Type 为 ConnectParticipantWithAgenticCX，脚本按 workspaceId / applicationId / deploymentAlias 生成（config.AGENTIC_CX_ACTION_TYPE / AGENTIC_CX_BRANCH_KEYWORDS）"),
    ("A2", "ACXD API 区域默认与 Connect 实例区域相同，即 --instance-arn 中的区域（--acxd-region / --acxd-endpoint 可覆盖）"),
    ("A3", "已按参考应用确认：generative_text 的输出（generativeText.name）以 {DeviceName:NLX.Local} 读取（config.LOCAL_VAR_TEMPLATE，参考流 Maintplink）"),
    ("A4", "escalate 节点的 stateModifications 把三个本地变量写入同名工作区上下文变量（config.state_mod_set_context）"),
    ("A5", "不使用 user_input 节点：Generative Journey 接管客户的每一轮对话，退出条件按 System.gjConditionIndex 路由（config.GJ_EXIT_CONDITION_NAMES / acxd/acxd_flow.json）"),
    ("A6", "所有 Generative Journey / Generative Text 节点使用 config.GENERATIVE_MODEL_TYPE = anthropic.claude-haiku-4-5（参考流 Maintplink）；zh-CN 下 Haiku 的对话效果未经实测"),
    ("A7", "已按实网数据修正：escalate 节点带一个无 nodeId 的 node_status == continuation 子节点（acxd/acxd_flow.json）"),
    ("A8", "已按实网数据修正：消息中以 {DeviceName:NLX.Context} 形式引用上下文变量（config.CONTEXT_VAR_MESSAGE_PLACEHOLDER）"),
    ("A9", "工作区级上下文变量即 $.AgenticCX.ContextVariables 的来源，flow 级变量留空（config.FLOW_SCOPED_CONTEXT_VARIABLES）"),
    ("A10", "DefaultAgentUI 事件流可通过 $.Attributes.<key> 读到原联系的属性"),
    ("A11", "zh-CN 在 Connect agentic voice 下端到端可用；默认 zh-CN，未显式指定 --language 时 ACXD 以语言原因拒绝会自动改用 en-US（config.DEFAULT_LANGUAGE / FALLBACK_LANGUAGE）"),
    ("A12", "Deployment 成功状态为 deployed，含 fail 视为失败（config.DEPLOYMENT_SUCCESS_STATUSES）"),
    ("A13", "oneClickDeployEnabled=false 可阻止 build 后自动部署（config.APPLICATION_DEPLOYMENT_SETTINGS）"),
    ("A14", "已按实网数据修正：LanguageSetting 写作 {languageCode, useLex3pAsr: false}（config.LANGUAGE_SETTING_EXTRA / language_settings）"),
    ("A15", "客户端提供的 UUIDv4 nodeId 被服务端接受（不一致时只 WARN，见 flowNodeIdsRemapped）"),
    ("A16", "ACXD 错误类型可能只在 x-amzn-ErrorType 头中给出"),
    ("A17", "ACXD 对话使用应用默认 TTS 声音（config.ACXD_NODE_VOICE_SETTINGS / LANGUAGE_SETTING_EXTRA.voice）"),
    ("A18", "已按实网流确认：AgentConfiguration.Alias 即 deploymentAlias（随机字符串），环境名只出现在 ActionMetadata 的 displayName 中（采集的 action 只用于挑选与 WARN）"),
    ("A19", "Detail view 会转义 Sections HTML 中代入的属性值（config.SCREENPOP_SECTIONS_SHOW_VALUES）"),
    ("A20", "Escalate 节点不设 sendContext 时上下文变量仍会回传 Connect（config.ESCALATE_NODE_METADATA）"),
    ("A21", "ACXD 以语言原因拒绝时，错误信息或 build 失败详情中（去掉 language* 回显字段后）含 language / locale / zh-cn（config.LANGUAGE_FAILURE_KEYWORDS / LANGUAGE_ECHO_KEYS）"),
    ("A22", "已按实网流确认 GetCustomerProfile 的写法（_phone = $.CustomerEndpoint.Address，结果在 $.Customer.*）；zh-CN 姓名写作“姓 名”中间保留一个空格，因为两个引用紧挨着（无分隔符）在运行时是否解析未经验证（config.CUSTOMER_PROFILE_NAME_TOKENS / messages.json screenpop.customer.name_format）"),
])

# ---------------------------------------------------------------------------
# Load-time invariants (§6.7, §6.2.1)
# ---------------------------------------------------------------------------

for _f in FIELDS:
    assert re.fullmatch(r"[A-Za-z_]{1,64}", _f), f"invalid field name {_f!r}"
    assert not _f.startswith("nlx_context"), f"reserved field name {_f!r}"
assert len(set(FIELDS)) == len(FIELDS), "duplicate field names"
for _f in CUSTOMER_FIELDS:
    assert re.fullmatch(r"[A-Za-z_]{1,64}", _f), f"invalid customer field name {_f!r}"
assert not set(CUSTOMER_FIELDS) & set(FIELDS), "customer fields overlap FIELDS"
assert len(set(SCREENPOP_FIELDS)) == len(SCREENPOP_FIELDS), "duplicate screen pop fields"
assert sorted(SCREENPOP_FIELDS) == sorted(CUSTOMER_FIELDS + FIELDS), "SCREENPOP_FIELDS must list every field once"
_rendered = CONTEXT_VAR_MESSAGE_PLACEHOLDER.format(name="X")
assert "X" in _rendered and "{{" not in _rendered, "bad CONTEXT_VAR_MESSAGE_PLACEHOLDER"
_rendered = LOCAL_VAR_TEMPLATE.format(name="X")
assert _rendered == "{X:NLX.Local}", "bad LOCAL_VAR_TEMPLATE"
del _f, _rendered
