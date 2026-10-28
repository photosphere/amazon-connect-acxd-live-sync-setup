"""ACXD 设备故障自助报修 + Agent Workspace 弹屏 部署脚本。

功能：
  1. ACXD 阶段（--phase acxd）：在 ACXD 工作区中创建 / 更新上下文变量
     DeviceName、DeviceModel、FaultDescription，ACXD flow（依次询问设备名称、
     型号、故障描述并让客户确认，最后 Escalate 回 Connect），ACXD application，
     然后新建 build、轮询至 BUILT，再部署到指定环境并轮询至 deployed。
  2. Connect 阶段（--phase connect）：创建 / 更新 screenpop 流（ShowView + Detail
     视图显示客户与设备六项）与入站流（日志 → DefaultAgentUI 事件流 → TTS 声音 →
     按来电号码查询客户资料（Customer Profiles）→ Agentic CX 块 → 写联系人属性 →
     设置队列 → 转队列）。
  3. --phase all（默认）依次执行以上两个阶段：先完成 Connect 只读预检，再把 ACXD
     部署到 deployed，最后才写 Connect 流。

只需两个输入（其余全部自动推导或使用默认值，现有参数都保留为可选覆盖）：
  export ACXD_API_KEY=<ACXD API key>     # 或 --acxd-api-key-file <文件>，或不设置、运行时提示输入
  python3 deploy.py --instance-arn arn:aws:connect:<区域>:<账号>:instance/<实例ID>

  自动推导：区域 / 实例 ID / 账号取自 --instance-arn；ACXD endpoint 按区域生成；
  ACXD 工作区通过只读 ListWorkspaces 发现（多个时取 deploy_state.json 中记录的，
  否则要求 --acxd-workspace-id）；队列默认 BasicQueue，没有时取名称排序后的第一个
  STANDARD 队列；应用默认新建专用应用 AgenticCareSelfService；部署环境 production，
  deploymentAlias 取自本次部署；语言默认 zh-CN，未显式指定 --language 时 ACXD 以
  语言原因拒绝会自动改用 en-US。

要求：Python >= 3.10（boto3==1.43.49 的要求），pip install -r requirements.txt。

密钥：ACXD API key 的来源优先级为 环境变量 ACXD_API_KEY > --acxd-api-key-file <文件>
> --acxd-api-key <key>（会进入 shell 历史与进程列表，不推荐）> 交互终端中的 getpass
提示。key 不会写入任何输出文件，日志中只显示 ****<key ID 末 4 位>.**** 形式的掩码。

交互模式：在终端（stdin 与 stdout 都是终端）中运行时，缺少的参数会逐项询问（实例 / 队列 /
  区域 / 部署环境 / 语言用编号列表，资源名前缀显示默认值 AgenticCare 与派生的资源名，直接回车
  接受 [ ] 中的默认值，Ctrl-C 取消）；参数优先级为
  命令行参数 > 环境变量 > 交互提示 > 默认值。真实运行写入前先显示参数摘要（key 只显示掩码）
  并要求确认，--yes 跳过确认；--non-interactive 完全关闭提示与确认（CI / 管道本来就不交互）。
  python3 deploy.py

示例：
  # 预演（不发起任何网络调用，产物写入 output/dry-run/，工作区 / 队列用占位值）
  python3 deploy.py --dry-run --instance-arn <实例ARN>

  # 只读预检（只调用 GET/List/Describe，产物与计划写操作写入 output/preflight/）
  ACXD_API_KEY=<ACXD API key> python3 deploy.py --preflight --instance-arn <实例ARN>

  # 旧写法仍然可用：分别给出区域 / 实例 / 队列 / 工作区
  python3 deploy.py --dry-run --region us-east-1 \\
      --instance-id <实例ID> --queue-arn <队列ARN> --acxd-workspace-id <工作区ID>

  # 只运行 ACXD 阶段
  ACXD_API_KEY=<ACXD API key> python3 deploy.py --phase acxd \\
      --region us-east-1 --acxd-workspace-id <工作区ID>

  # Connect 阶段单独运行（按 workspaceId / applicationId / deploymentAlias 生成 Agentic CX 块，
  # 无需参考流；applicationId / deploymentAlias 也可用 ACXD_API_KEY 自动查询或取自 deploy_state.json）
  python3 deploy.py --phase connect --region us-east-1 --instance-id <实例ID> \\
      --queue-arn <队列ARN> --acxd-workspace-id <工作区ID> \\
      --acxd-application-id <应用ID> --acxd-deployment-alias <deploymentAlias>

退出码：0 成功；1 运行 / 服务端错误（含在确认提示处取消）；2 配置错误；130 在交互提示处按 Ctrl-C / EOF 取消。
"""

from __future__ import annotations

import argparse
import copy
import datetime as _dt
import getpass
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import botocore.config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

import config
import flowlib
import prompts
from acxd_client import (AcxdAmbiguousWriteError, AcxdApiError, AcxdClient, endpoint_for_region,
                         mask_key, redact)
from flowlib import AgenticCxSelectionError, FlowValidationError

logger = logging.getLogger("deploy")

STATE_SCHEMA_VERSION = 1
DRYRUN_APPLICATION_ID = "DRYRUN-APPLICATION-ID"
DRYRUN_BUILD_ID = "DRYRUN-BUILD-ID"
DRYRUN_DEPLOYMENT_ID = "DRYRUN-DEPLOYMENT-ID"
DRYRUN_QUEUE_NAME = "DRYRUN-QUEUE"
DRYRUN_DEPLOYMENT_ALIAS = "DRYRUN-DEPLOYMENT-ALIAS"
PREFLIGHT_APPLICATION_PENDING = "PREFLIGHT-APPLICATION-PENDING"
PREFLIGHT_ALIAS_PENDING = "PREFLIGHT-ALIAS-PENDING"
PREFLIGHT_BUILD_PENDING = "PREFLIGHT-BUILD-PENDING"
PREFLIGHT_SCREENPOP_ID = "PREFLIGHT-SCREENPOP"
DRYRUN_WORKSPACE_ID = "DRYRUN-WORKSPACE-ID"
CONNECT_READ_PREFIXES = ("Describe", "List", "Get", "Search")

REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$")
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
INSTANCE_ARN_RE = re.compile(
    r"^arn:(aws[a-z-]*):connect:([a-z0-9-]+):(\d{12}):instance/([0-9a-fA-F-]{36})$")
QUEUE_ARN_RE = re.compile(
    r"^arn:aws[a-z-]*:connect:[a-z0-9-]+:\d{12}:instance/([0-9a-f-]{36})/queue/([0-9a-f-]{36})$")
FLOW_ARN_RE = re.compile(
    r"^arn:aws[a-z-]*:connect:[a-z0-9-]+:\d{12}:instance/[0-9a-f-]{36}/contact-flow/([0-9a-f-]{36})$")
API_KEY_RE = re.compile("^" + re.escape(config.API_KEY_PREFIX) + r"[^.]+\..+$")
PREFIX_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{2,30}$")
FLOW_ID_RE = re.compile(r"^[A-Za-z0-9]{3,64}$")
DEPLOYMENT_ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

STEP_NAMES = ("ensure_context_variables", "upsert_acxd_flow", "upsert_application", "create_build",
              "upsert_deployment", "upsert_screenpop_flow", "upsert_inbound_flow")


class ConfigError(Exception):
    """Configuration error -> exit code 2."""


class DeployError(Exception):
    def __init__(self, message, code=1):
        super().__init__(message)
        self.code = code


class LanguageRejectedError(DeployError):
    """ACXD rejected a request or build for language reasons (A21) -> en-US retry when allowed."""


@dataclass(frozen=True)
class InstanceRef:
    partition: str
    region: str
    account_id: str
    instance_id: str

    @property
    def arn(self) -> str:
        return f"arn:{self.partition}:connect:{self.region}:{self.account_id}:instance/{self.instance_id}"


@dataclass
class ConnectTarget:
    """Result of the read-only Connect prechecks (DescribeInstance + queue + same-name flows)."""
    instance_arn: str | None
    instance_alias: str | None
    access_url: str | None
    queue_arn: str
    queue_name: str
    queue_source: str
    # Contact flows with a derived name that already exist: name -> (ListContactFlows summary, owner
    # status APP_OWNED / APP_FORCED).  A foreign flow never gets here (ConfigError in the precheck).
    existing_flows: dict = field(default_factory=dict)


@dataclass
class Config:
    phase: str
    dry_run: bool
    region: str
    instance_id: str | None
    queue_arn: str | None
    queue_name: str | None
    workspace_id: str | None
    acxd_region: str
    acxd_endpoint: str
    environment: str
    application_id: str | None
    deployment_alias: str | None
    reuse_application: bool
    language: str
    name_prefix: str
    tts_voice: str
    tts_engine: str
    action_file: str | None
    action_obj: dict | None
    reference_flow_id: str | None
    action_identifier: str | None
    allow_unverified: bool
    build_timeout: int
    poll_interval: int
    output_dir: Path
    verbose: bool
    preflight: bool = False
    overwrite_untagged: bool = False
    api_key: str | None = field(default=None, repr=False)
    api_key_source: str | None = None
    instance_arn: str | None = None
    account_id: str | None = None
    workspace_name: str | None = None
    workspace_source: str | None = None
    language_explicit: bool = True
    language_fallback_from: str | None = None
    tts_voice_override: str | None = None
    tts_engine_override: str | None = None
    # Where name_prefix came from: cli / env / prompt / default (NAME_PREFIX_SOURCE_LABELS).
    name_prefix_source: str = "default"
    # Set by main() after validate_config: interactive prompts (workspace choice, confirmation).
    interactive: bool = False
    assume_yes: bool = False
    prompter: object = field(default=None, repr=False, compare=False)

    @property
    def acxd_phase(self) -> bool:
        return self.phase in ("acxd", "all")

    @property
    def connect_phase(self) -> bool:
        return self.phase in ("connect", "all")

    @property
    def out_dir(self) -> Path:
        if self.dry_run:
            return self.output_dir / "dry-run"
        if self.preflight:
            return self.output_dir / "preflight"
        return self.output_dir


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _env(name):
    val = os.environ.get(name)
    return val if val else None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="deploy.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--phase", choices=("acxd", "connect", "all"), default="all",
                   help="执行阶段，默认 all")
    p.add_argument("--dry-run", action="store_true",
                   help="不发起任何 AWS / ACXD 网络调用，只把请求体与流 JSON 写入 <output-dir>/dry-run/")
    p.add_argument("--preflight", action="store_true",
                   help="只读预检：只调用 GET/List/Describe，把将要发送的请求体写入 <output-dir>/preflight/ "
                        "并列出计划中的写操作")
    p.add_argument("--instance-arn", default=_env("CONNECT_INSTANCE_ARN"),
                   help="Connect 实例 ARN（环境变量 CONNECT_INSTANCE_ARN）；区域、账号与实例 ID 都从中推导")
    p.add_argument("--acxd-api-key-file", default=None,
                   help="从文件读取 ACXD API key（文件只含 key 一行，建议 chmod 600）；环境变量 ACXD_API_KEY 优先")
    p.add_argument("--acxd-api-key", default=None,
                   help="直接传入 ACXD API key（不推荐：会进入 shell 历史与进程列表）；优先级低于 ACXD_API_KEY "
                        "与 --acxd-api-key-file")
    p.add_argument("--region", default=None,
                   help="可选覆盖：Connect 实例区域；默认取自 --instance-arn，其次环境变量 AWS_REGION")
    p.add_argument("--instance-id", default=None,
                   help="可选覆盖：Connect 实例 ID；默认取自 --instance-arn，其次环境变量 CONNECT_INSTANCE_ID")
    p.add_argument("--queue-arn", default=_env("CONNECT_QUEUE_ARN"),
                   help="可选覆盖：目标队列 ARN（环境变量 CONNECT_QUEUE_ARN），与 --queue-name 最多提供一个；"
                        f"都不给时使用 {config.DEFAULT_QUEUE_NAME}，没有则取名称排序后的第一个 STANDARD 队列")
    p.add_argument("--queue-name", default=_env("CONNECT_QUEUE_NAME"),
                   help="可选覆盖：目标队列名称（环境变量 CONNECT_QUEUE_NAME），与 --queue-arn 最多提供一个")
    p.add_argument("--acxd-workspace-id", default=_env("ACXD_WORKSPACE_ID"),
                   help="可选覆盖：ACXD 工作区 ID（环境变量 ACXD_WORKSPACE_ID）；默认通过只读 ListWorkspaces 自动发现")
    p.add_argument("--acxd-region", default=_env("ACXD_REGION"),
                   help="ACXD API 区域（环境变量 ACXD_REGION），默认等于 --region（假设 A2）")
    p.add_argument("--acxd-endpoint", default=_env("ACXD_ENDPOINT"),
                   help="覆盖 ACXD API endpoint（环境变量 ACXD_ENDPOINT），必须为 https://")
    p.add_argument("--acxd-environment", choices=config.ACXD_ENVIRONMENTS, default=None,
                   help=f"ACXD 部署环境，默认 {config.DEFAULT_ACXD_ENVIRONMENT}（交互模式下未指定时会询问）")
    p.add_argument("--acxd-application-id", default=None,
                   help="ACXD applicationId（connect 阶段单独运行时用于生成 / 校验 Agentic CX action；"
                        "配合 --reuse-acxd-application 时指定要复用的现有应用）")
    p.add_argument("--acxd-deployment-alias", default=_env("ACXD_DEPLOYMENT_ALIAS"),
                   help="已部署环境的 deploymentAlias（Agentic CX 块 Alias），connect 阶段单独运行时使用"
                        "（环境变量 ACXD_DEPLOYMENT_ALIAS）")
    p.add_argument("--reuse-acxd-application", action="store_true",
                   help="把 ACXD flow 部署进 --acxd-application-id 指定的现有应用：追加 flow 并把 Welcome "
                        "默认流改为本 flow，会替换该应用当前环境的部署")
    p.add_argument("--overwrite-untagged-resources", "--overwrite-untagged-application", action="store_true",
                   dest="overwrite_untagged",
                   help="允许覆盖不是本脚本创建的同名资源（ACXD 应用 / ACXD flow 的 metadata.tags 中没有 "
                        + config.OWNER_TAG + "；Connect 流没有标签 " + config.CONNECT_OWNER_TAG_KEY + "="
                        + config.OWNER_TAG + "，内容也不是本脚本的模板）。默认遇到这样的同名资源时，在任何写操作之前"
                        "退出 2（不推荐，优先改用 --name-prefix）")
    if len(config.SUPPORTED_LANGUAGES) > 1:
        lang_help = (f"对话语言，默认 {config.DEFAULT_LANGUAGE}；未显式指定时，ACXD 以语言原因拒绝会自动改用 "
                     f"{config.FALLBACK_LANGUAGE}（显式指定则不回退）")
    else:
        lang_help = f"对话语言，只支持 {config.DEFAULT_LANGUAGE}"
    p.add_argument("--language", choices=config.SUPPORTED_LANGUAGES, default=None, help=lang_help)
    p.add_argument("--name-prefix", default=None,
                   help=f"资源名前缀（环境变量 {config.NAME_PREFIX_ENV}），默认 {config.DEFAULT_NAME_PREFIX}；"
                        "派生 ACXD flowId <前缀>Intake、Journey 节点 <前缀>Journey、ACXD 应用 <前缀>SelfService、"
                        "Connect 流 '<前缀> ScreenPop Flow' / '<前缀> Inbound ACXD Flow'。同一前缀重复运行会更新"
                        "（覆盖）这些同名资源，新前缀会新建一套（交互模式下未指定时会询问）")
    try:
        conn = {lang: flowlib.load_messages()[lang]["connect"] for lang in config.SUPPORTED_LANGUAGES}
        voices = "，".join(f"{lang} {c['tts_voice']}" for lang, c in conn.items())
        engines = "，".join(f"{lang} {c['tts_engine']}" for lang, c in conn.items())
    except (KeyError, FlowValidationError):
        voices = engines = "按语言"
    p.add_argument("--tts-voice", default=None, help=f"Connect 提示语的 TTS 声音，默认按语言（{voices}）")
    p.add_argument("--tts-engine", default=None, help=f"Connect 提示语的 TTS 引擎，默认按语言（{engines}）")
    p.add_argument("--agentic-cx-action-file", default=None,
                   help="Agentic CX action JSON 文件（单个 action 或完整流 JSON）")
    p.add_argument("--agentic-cx-reference-flow-id", default=None,
                   help="在控制台手工配置了 Agentic CX 块的参考流 ID 或 ARN")
    p.add_argument("--agentic-cx-action-identifier", default=None,
                   help="在参考流 / 文件中按 Identifier 指定 Agentic CX action")
    p.add_argument("--allow-unverified-agentic-cx-action", action="store_true",
                   help="允许在非 dry-run 中使用未验证的兜底 Agentic CX 片段（Connect 很可能拒绝）")
    p.add_argument("--build-timeout", type=int, default=config.DEFAULT_BUILD_TIMEOUT,
                   help=f"build / deployment 轮询超时秒数，默认 {config.DEFAULT_BUILD_TIMEOUT}")
    p.add_argument("--poll-interval", type=int, default=config.DEFAULT_POLL_INTERVAL,
                   help=f"轮询间隔秒数，默认 {config.DEFAULT_POLL_INTERVAL}")
    p.add_argument("--output-dir", default="output", help="输出目录，默认 ./output")
    p.add_argument("--verbose", action="store_true", help="DEBUG 日志（不含请求头与密钥）")
    p.add_argument("--non-interactive", action="store_true",
                   help="关闭交互模式：不询问任何参数（包括 API key）、不要求确认；缺少必需参数时直接报错退出")
    p.add_argument("--yes", "-y", action="store_true",
                   help="交互模式下跳过真实运行前的参数确认（缺少的参数仍会询问）")
    return p


def parse_instance_arn(arn) -> InstanceRef:
    """arn:<partition>:connect:<region>:<account>:instance/<id> -> InstanceRef (no network)."""
    value = (arn or "").strip()
    m = INSTANCE_ARN_RE.match(value)
    if not m or not REGION_RE.match(m.group(2)) or not UUID_RE.match(m.group(4)):
        hint = ""
        if "/queue/" in value:
            hint = "；这是队列 ARN，请提供实例 ARN"
        elif "/contact-flow/" in value:
            hint = "；这是流 ARN，请提供实例 ARN"
        raise ConfigError(f"--instance-arn 格式错误: {value or '<空>'}"
                          f"（应为 arn:aws:connect:<区域>:<12 位账号>:instance/<实例ID>）{hint}")
    return InstanceRef(partition=m.group(1), region=m.group(2), account_id=m.group(3),
                       instance_id=m.group(4).lower())


def _read_api_key_file(path: str) -> str:
    """Read the key from a file.  Error messages name the path only, never the content."""
    try:
        mode = os.stat(path).st_mode
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(f"无法读取 --acxd-api-key-file {path}") from exc
    key = text.strip()
    if not key or "\n" in key or "\r" in key:
        raise ConfigError(f"--acxd-api-key-file {path} 为空或包含多行，文件中只能有 key 一行")
    if mode & 0o077:
        logger.warning("API key file %s is readable by group/others; run chmod 600 %s", path, path)
    return key


def load_api_key(args, required: bool, allow_prompt: bool):
    """Precedence: env ACXD_API_KEY > --acxd-api-key-file > --acxd-api-key > getpass prompt.

    Returns (key, source).  Lower-precedence sources are not read once a higher one is set.
    """
    env_key = _env("ACXD_API_KEY")
    file_path = args.acxd_api_key_file
    cli_key = args.acxd_api_key
    if cli_key:
        logger.warning("--acxd-api-key puts the key into the shell history and the process list; "
                       "prefer ACXD_API_KEY or --acxd-api-key-file")
    candidates = [("env", "ACXD_API_KEY", env_key), ("file", "--acxd-api-key-file", file_path),
                  ("cli", "--acxd-api-key", cli_key)]
    given = [(source, name, value) for source, name, value in candidates if value]
    key = source = None
    if given:
        source, _, value = given[0]
        key = _read_api_key_file(value) if source == "file" else value
        if len(given) > 1:
            logger.warning("ACXD API key taken from %s; ignoring %s", given[0][1], ", ".join(n for _, n, _ in given[1:]))
    elif required:
        stdin_tty = sys.stdin is not None and sys.stdin.isatty()
        if allow_prompt and stdin_tty:
            try:
                key = getpass.getpass("ACXD API key: ") or None
            except (EOFError, KeyboardInterrupt) as exc:
                raise prompts.PromptAborted() from exc
            source = "prompt"
            if not key:
                raise ConfigError("输入的 ACXD API key 为空（也可设置环境变量 ACXD_API_KEY 或使用 --acxd-api-key-file）")
        else:
            reason = "--non-interactive 模式，不提示输入" if stdin_tty else "stdin 不是终端，无法提示输入"
            raise ConfigError("缺少 ACXD API key：请设置环境变量 ACXD_API_KEY 或使用 --acxd-api-key-file <文件>"
                              f"（{reason}）")
    if key:
        logger.info("ACXD API key source: %s (%s)", source, mask_key(key))
        if not API_KEY_RE.match(key):
            logger.warning("ACXD API key does not look like an Admin Hub API key (<prefix>.<secret>)")
    return key, source


def set_language(cfg: Config, lang: str, messages: dict) -> None:
    """Switch the conversation language; TTS defaults follow unless given explicitly."""
    cfg.language = lang
    conn = messages[lang]["connect"]
    cfg.tts_voice = cfg.tts_voice_override or conn["tts_voice"]
    cfg.tts_engine = cfg.tts_engine_override or conn["tts_engine"]


def _resolve_region_and_instance(args, ref: InstanceRef | None):
    """Explicit flag > --instance-arn > environment variable; explicit flags must match the ARN."""
    env_region = _env("AWS_REGION")
    if args.region:
        region = args.region
        if not REGION_RE.match(region):
            raise ConfigError(f"--region 格式错误: {region}")
        if ref and region != ref.region:
            raise ConfigError(f"--region {region} 与 --instance-arn 中的区域 {ref.region} 不一致")
    elif ref:
        region = ref.region
        if env_region and env_region != ref.region:
            logger.info("AWS_REGION=%s differs from the --instance-arn region %s; using %s",
                        env_region, ref.region, ref.region)
    else:
        region = env_region
        if not region:
            raise ConfigError("缺少 --instance-arn（或 --region / 环境变量 AWS_REGION）")
        if not REGION_RE.match(region):
            raise ConfigError(f"--region / AWS_REGION 格式错误: {region}")

    if args.instance_id:
        instance_id = args.instance_id
        if not UUID_RE.match(instance_id):
            raise ConfigError(f"--instance-id 格式错误: {instance_id}")
        if ref and instance_id.lower() != ref.instance_id:
            raise ConfigError(f"--instance-id {instance_id} 与 --instance-arn 中的实例 ID {ref.instance_id} 不一致")
    elif ref:
        instance_id = ref.instance_id
    else:
        instance_id = _env("CONNECT_INSTANCE_ID")
        if instance_id and not UUID_RE.match(instance_id):
            raise ConfigError(f"--instance-id / CONNECT_INSTANCE_ID 格式错误: {instance_id}")
    return region, instance_id


NAME_PREFIX_SOURCE_LABELS = {"cli": "命令行 --name-prefix", "env": f"环境变量 {config.NAME_PREFIX_ENV}",
                             "prompt": "交互输入", "default": "默认值"}
PREFIX_MARK_UPDATE = "将更新（覆盖）"
PREFIX_MARK_CREATE = "将新建"
PREFIX_MARK_FOREIGN = "已存在同名资源但不是本脚本创建的：部署会在任何写操作前停止"
PREFIX_MARK_FOREIGN_FORCED = "已存在同名资源且不是本脚本创建的：将被覆盖（--overwrite-untagged-resources）"


def resolve_name_prefix(args):
    """(prefix, source): CLI flag > environment variable NAME_PREFIX > interactive prompt > default.

    collect_missing_args stores a prompt answer in args.name_prefix together with
    args.name_prefix_source ("prompt", or "default" when Enter accepted the default)."""
    if args.name_prefix is not None:
        return args.name_prefix, getattr(args, "name_prefix_source", None) or "cli"
    env = _env(config.NAME_PREFIX_ENV)
    if env is not None:
        return env, "env"
    return config.DEFAULT_NAME_PREFIX, "default"


def name_prefix_problem(prefix: str) -> str | None:
    """Why `prefix` cannot be used (None = valid).  Shared by validate_config and the prompt.

    PREFIX_RE (letter + 2-30 letters / digits) also guarantees every derived name: the ACXD flowId
    <p>Intake matches FLOW_ID_RE (3-64 alphanumerics), the application name <p>SelfService has at
    most 42 of the 100 allowed characters, the Connect flow names are non-empty (Connect documents
    only a minimum length) and the Generative Journey node name <p>Journey is plain alphanumeric
    (reference flows use free-text node names, .agents/tasks/genjourney-evidence/)."""
    if not PREFIX_RE.match(prefix):
        return f"必须匹配 {PREFIX_RE.pattern}"
    if not FLOW_ID_RE.match(config.acxd_flow_id(prefix)):
        return "生成的 ACXD flowId 不合法"
    if len(config.acxd_app_name(prefix)) > config.ACXD_APP_NAME_MAX:
        return f"生成的 ACXD 应用名超过 {config.ACXD_APP_NAME_MAX} 个字符"
    return None


def prefix_resource_names(prefix: str) -> list:
    """[(key, label, name)] of every resource / node name derived from the prefix."""
    return [
        ("flow", "ACXD flowId", config.acxd_flow_id(prefix)),
        ("journey", "Generative Journey 节点", config.acxd_journey_name(prefix)),
        ("app", "ACXD 应用名", config.acxd_app_name(prefix)),
        ("screenpop", "Connect screenpop 流", config.screenpop_flow_name(prefix)),
        ("inbound", "Connect 入站流", config.inbound_flow_name(prefix)),
    ]


ACXD_PREFIX_KEYS = ("flow", "journey", "app")
CONNECT_PREFIX_KEYS = ("screenpop", "inbound")


def _phase_prefix_keys(cfg: Config) -> tuple:
    """Derived names the phase of this run creates / updates."""
    return (ACXD_PREFIX_KEYS if cfg.acxd_phase else ()) + (CONNECT_PREFIX_KEYS if cfg.connect_phase else ())


def format_prefix_names(prefix: str, marks: dict | None = None, reuse: bool = False,
                        indent: str = "    ", keys=None) -> str:
    """One line per derived name (all, or only `keys`); marks[key] (将更新（覆盖） / 将新建) is
    appended when known."""
    lines = []
    for key, label, name in prefix_resource_names(prefix):
        if keys is not None and key not in keys:
            continue
        note = "（复用模式下不使用此名称）" if key == "app" and reuse else ""
        mark = (marks or {}).get(key)
        lines.append(f"{indent}{label}: {name}{note}" + (f"  → {mark}" if mark else ""))
    return "\n".join(lines)


def format_name_prefix(cfg: Config, marks: dict | None = None) -> str:
    """'资源名前缀: <p>（<source>）' followed by the derived names (every mode prints this once)."""
    head = f"资源名前缀: {cfg.name_prefix}（{NAME_PREFIX_SOURCE_LABELS[cfg.name_prefix_source]}）"
    return head + "\n" + format_prefix_names(cfg.name_prefix, marks, cfg.reuse_application, indent="  ",
                                             keys=_phase_prefix_keys(cfg))


def validate_config(args) -> Config:
    """All validation happens here, before any network call (§6.3, §6.3.1)."""
    if args.preflight and args.dry_run:
        raise ConfigError("--preflight 与 --dry-run 不能同时使用")
    ref = parse_instance_arn(args.instance_arn) if args.instance_arn is not None else None
    region, instance_id = _resolve_region_and_instance(args, ref)
    if region not in config.ACXD_REGIONS:
        logger.warning("region %s is not in the ACXD region list %s", region, ", ".join(config.ACXD_REGIONS))
    acxd_region = args.acxd_region or region
    if not REGION_RE.match(acxd_region):
        raise ConfigError(f"--acxd-region 格式错误: {acxd_region}")

    connect_phase = args.phase in ("connect", "all")
    acxd_phase = args.phase in ("acxd", "all")
    name_prefix, prefix_source = resolve_name_prefix(args)

    if connect_phase:
        if not instance_id:
            raise ConfigError("缺少 --instance-arn（或 --instance-id / 环境变量 CONNECT_INSTANCE_ID）")
    if args.queue_arn and args.queue_name:
        raise ConfigError("--queue-arn 与 --queue-name 最多提供一个")
    if args.queue_arn:
        m = QUEUE_ARN_RE.match(args.queue_arn)
        if not m:
            raise ConfigError(f"--queue-arn 格式错误: {args.queue_arn}")
        if instance_id and m.group(1) != instance_id:
            raise ConfigError("--queue-arn 中的实例 ID 与 --instance-arn / --instance-id 不一致")
    if args.queue_name and not (1 <= len(args.queue_name) <= 127):
        raise ConfigError("--queue-name 长度必须为 1-127")

    if args.acxd_endpoint:
        if not args.acxd_endpoint.startswith("https://"):
            raise ConfigError("--acxd-endpoint / ACXD_ENDPOINT 必须以 https:// 开头")
        endpoint = args.acxd_endpoint.rstrip("/")
    else:
        endpoint = endpoint_for_region(acxd_region)

    if args.acxd_workspace_id and not UUID_RE.match(args.acxd_workspace_id):
        raise ConfigError(f"--acxd-workspace-id 格式错误: {args.acxd_workspace_id}")
    if args.acxd_deployment_alias and not DEPLOYMENT_ALIAS_RE.match(args.acxd_deployment_alias):
        raise ConfigError(f"--acxd-deployment-alias 必须匹配 {DEPLOYMENT_ALIAS_RE.pattern}")
    if args.reuse_acxd_application:
        if not args.acxd_application_id:
            raise ConfigError("--reuse-acxd-application 需要同时提供 --acxd-application-id <现有应用ID>")
        if not UUID_RE.match(args.acxd_application_id):
            raise ConfigError(f"--acxd-application-id 格式错误（复用模式需要 UUID）: {args.acxd_application_id}")
        if not acxd_phase:
            logger.warning("--reuse-acxd-application has no effect with --phase connect")
    elif args.acxd_application_id and acxd_phase:
        logger.warning("--acxd-application-id without --reuse-acxd-application: the ACXD phase ignores it and "
                       "creates / updates the dedicated application %s", config.acxd_app_name(name_prefix))

    problem = name_prefix_problem(name_prefix)
    if problem:
        raise ConfigError(f"{'环境变量 ' + config.NAME_PREFIX_ENV if prefix_source == 'env' else '--name-prefix'} "
                          f"{problem}")
    if args.build_timeout <= 0 or args.poll_interval <= 0:
        raise ConfigError("--build-timeout 与 --poll-interval 必须为正整数")

    action_obj = None
    if args.agentic_cx_action_file:
        try:
            with open(args.agentic_cx_action_file, encoding="utf-8") as fh:
                action_obj = json.load(fh)
        except (OSError, ValueError) as exc:
            raise ConfigError(f"--agentic-cx-action-file 无法读取或不是 JSON: {exc}") from exc
    ref_flow = args.agentic_cx_reference_flow_id
    if ref_flow:
        m = FLOW_ARN_RE.match(ref_flow)
        if m:
            ref_flow = m.group(1)
        elif not UUID_RE.match(ref_flow):
            raise ConfigError(f"--agentic-cx-reference-flow-id 必须是流 ID 或流 ARN: {ref_flow}")

    # Required (with a TTY prompt) only for acxd/all outside dry-run, preflight included (§6.3.1).
    # The connect phase alone never prompts but uses an env / file / CLI key for the lookups.
    required = acxd_phase and not args.dry_run
    api_key, key_source = load_api_key(args, required=required, allow_prompt=required and not args.non_interactive)

    messages = flowlib.load_messages()
    language = args.language or config.DEFAULT_LANGUAGE
    conn_msgs = messages[language]["connect"]

    return Config(
        phase=args.phase, dry_run=args.dry_run, region=region, instance_id=instance_id,
        queue_arn=args.queue_arn, queue_name=args.queue_name, workspace_id=args.acxd_workspace_id,
        acxd_region=acxd_region, acxd_endpoint=endpoint,
        environment=args.acxd_environment or config.DEFAULT_ACXD_ENVIRONMENT,
        application_id=args.acxd_application_id, deployment_alias=args.acxd_deployment_alias,
        reuse_application=args.reuse_acxd_application, language=language, name_prefix=name_prefix,
        name_prefix_source=prefix_source,
        tts_voice=args.tts_voice or conn_msgs["tts_voice"], tts_engine=args.tts_engine or conn_msgs["tts_engine"],
        action_file=args.agentic_cx_action_file, action_obj=action_obj, reference_flow_id=ref_flow,
        action_identifier=args.agentic_cx_action_identifier,
        allow_unverified=args.allow_unverified_agentic_cx_action, build_timeout=args.build_timeout,
        poll_interval=args.poll_interval, output_dir=Path(args.output_dir), verbose=args.verbose,
        preflight=args.preflight, overwrite_untagged=args.overwrite_untagged,
        api_key=api_key, api_key_source=key_source,
        instance_arn=ref.arn if ref else None, account_id=ref.account_id if ref else None,
        workspace_source="--acxd-workspace-id" if args.acxd_workspace_id else None,
        language_explicit=args.language is not None,
        tts_voice_override=args.tts_voice, tts_engine_override=args.tts_engine,
    )


# ---------------------------------------------------------------------------
# Interactive mode: ask only for what CLI flags / environment variables left missing
# ---------------------------------------------------------------------------

INTERACTIVE_INTRO = ("交互模式：将逐项询问缺少的参数（直接回车接受 [ ] 中的默认值，Ctrl-C 取消；"
                     "--non-interactive 关闭交互）")
INSTANCE_ARN_HINT = "arn:aws:connect:<区域>:<账号>:instance/<实例ID>"


def _stdio_is_tty() -> bool:
    return (sys.stdin is not None and sys.stdin.isatty()
            and sys.stdout is not None and sys.stdout.isatty())


def is_interactive(args) -> bool:
    """Prompts need a terminal on both ends: the prompt goes to stdout, the answer comes from stdin."""
    return not args.non_interactive and _stdio_is_tty()


def _profile_region() -> str | None:
    """Region of the local AWS profile (reads local config only, no network)."""
    try:
        region = boto3.session.Session().region_name
    except Exception:  # noqa: BLE001 - a broken local profile only loses the default
        return None
    return region if region and REGION_RE.match(region) else None


def _list_active_instances(region: str) -> list:
    """Read-only ListInstances; ACTIVE instances sorted by alias."""
    client = make_connect_client(region)
    items = []
    for page in client.get_paginator("list_instances").paginate():
        items.extend(i for i in page.get("InstanceSummaryList", [])
                     if i.get("Arn") and i.get("InstanceStatus") == "ACTIVE")
    return sorted(items, key=lambda i: (i.get("InstanceAlias") or "", i.get("Id") or ""))


def _list_standard_queues(region: str, instance_id: str) -> list:
    """Read-only ListQueues (STANDARD) sorted by name."""
    client = make_connect_client(region)
    items = []
    for page in client.get_paginator("list_queues").paginate(InstanceId=instance_id, QueueTypes=["STANDARD"]):
        items.extend(q for q in page.get("QueueSummaryList", []) if q.get("Arn") and q.get("Name"))
    return sorted(items, key=lambda q: q["Name"])


def _list_failed(what: str, exc) -> None:
    code = _err_code(exc) if isinstance(exc, ClientError) else type(exc).__name__
    logger.warning("%s failed (%s); falling back to free-text input", what, code)


def _region_and_instance_hint(args):
    """(region, instance_id) as validate_config will resolve them, or (None, None) if not usable yet."""
    try:
        ref = parse_instance_arn(args.instance_arn) if args.instance_arn is not None else None
    except ConfigError:
        return None, None
    region = args.region or (ref.region if ref else None) or _env("AWS_REGION")
    instance_id = args.instance_id or (ref.instance_id if ref else None) or _env("CONNECT_INSTANCE_ID")
    if not (region and REGION_RE.match(region) and instance_id and UUID_RE.match(instance_id)):
        return None, None
    return region, instance_id.lower()


def _prompt_region(prompter) -> str:
    regions = list(config.ACXD_REGIONS)
    default = _profile_region()
    if default and default not in regions:
        regions.append(default)

    def free(value):
        if not REGION_RE.match(value):
            raise ValueError(f"区域格式错误: {value}（例如 us-east-1）")
        return value
    return prompter.choice("AWS 区域（Connect 实例与 ACXD 所在区域）：", [(r, r) for r in regions],
                           default=default, free_text=free, prompt="请输入序号或区域名")


def _instance_arn_validator(args):
    def validate(value):
        try:
            ref = parse_instance_arn(value)
        except ConfigError as exc:
            raise ValueError(str(exc)) from exc
        if args.region and ref.region != args.region:
            raise ValueError(f"ARN 中的区域 {ref.region} 与 --region {args.region} 不一致")
        return ref.arn
    return validate


def _prompt_instance(args, prompter) -> str:
    validate = _instance_arn_validator(args)
    if not args.dry_run:  # dry-run never touches the network
        region = args.region or _env("AWS_REGION") or _prompt_region(prompter)
        try:
            instances = _list_active_instances(region)
        except (BotoCoreError, ClientError) as exc:
            _list_failed("ListInstances", exc)
            instances = []
        else:
            if not instances:
                logger.warning("no ACTIVE Connect instance in %s; falling back to free-text input", region)
        if instances:
            options = [(i["Arn"], f"{i.get('InstanceAlias') or '-'}  {i.get('Id')}") for i in instances]
            return prompter.choice(f"请选择 Connect 实例（区域 {region}）：", options,
                                   default=options[0][0] if len(options) == 1 else None,
                                   free_text=validate, prompt="请输入序号或实例 ARN")
    return prompter.text(f"Connect 实例 ARN（{INSTANCE_ARN_HINT}）", validate=validate)


def _prompt_queue(args, prompter, region: str, instance_id: str) -> None:
    try:
        queues = _list_standard_queues(region, instance_id)
    except (BotoCoreError, ClientError) as exc:
        _list_failed("ListQueues", exc)
        queues = None
    if queues == []:
        return  # resolve_queue reports "no STANDARD queue" as before
    if queues:
        default = next((q for q in queues if q["Name"] == config.DEFAULT_QUEUE_NAME), queues[0])
        arn = prompter.choice("请选择目标队列：", [(q["Arn"], q["Name"]) for q in queues], default=default["Arn"])
        if QUEUE_ARN_RE.match(arn):
            args.queue_arn = arn
        else:
            args.queue_name = next(q["Name"] for q in queues if q["Arn"] == arn)
        return

    def validate(value):
        if QUEUE_ARN_RE.match(value):
            return ("arn", value)
        if len(value) > 127:
            raise ValueError("队列名称长度必须为 1-127")
        return ("name", value)
    got = prompter.text(f"目标队列名称或队列 ARN [自动：{config.DEFAULT_QUEUE_NAME}]", validate=validate,
                        allow_empty=True)
    if got:
        kind, value = got
        if kind == "arn":
            args.queue_arn = value
        else:
            args.queue_name = value


def collect_missing_args(args, prompter) -> None:
    """Fill args attributes that are still None after CLI flags and environment variables.

    "Missing" is decided exactly like validate_config does, so supplied values (even invalid
    ones) are never asked again; validate_config still validates everything afterwards.
    Order: Connect instance (region first when listing) -> region -> queue -> ACXD environment
    -> language -> resource name prefix.  The API key prompt stays in validate_config (getpass).
    """
    if args.preflight and args.dry_run:
        return  # validate_config rejects the combination
    connect_phase = args.phase in ("connect", "all")
    acxd_phase = args.phase in ("acxd", "all")

    has_instance = (args.instance_arn is not None or args.instance_id or _env("CONNECT_INSTANCE_ID"))
    if connect_phase and not has_instance:
        args.instance_arn = _prompt_instance(args, prompter)
    if not args.region and args.instance_arn is None and not _env("AWS_REGION"):
        args.region = _prompt_region(prompter)

    if connect_phase and not args.dry_run and not args.queue_arn and not args.queue_name:
        region, instance_id = _region_and_instance_hint(args)
        if region:
            _prompt_queue(args, prompter, region, instance_id)

    if args.acxd_environment is None and (acxd_phase or not args.acxd_deployment_alias):
        args.acxd_environment = prompter.choice(
            "ACXD 部署环境：", [(e, e) for e in config.ACXD_ENVIRONMENTS],
            default=config.DEFAULT_ACXD_ENVIRONMENT)

    if acxd_phase and args.language is None and len(config.SUPPORTED_LANGUAGES) > 1:
        lang = prompter.choice("对话语言：", [
            (config.DEFAULT_LANGUAGE,
             f"{config.DEFAULT_LANGUAGE}（ACXD 以语言原因拒绝时自动改用 {config.FALLBACK_LANGUAGE}）"),
            (config.FALLBACK_LANGUAGE, config.FALLBACK_LANGUAGE)], default=config.DEFAULT_LANGUAGE)
        # The default keeps args.language None so the automatic en-US fallback still applies.
        if lang != config.DEFAULT_LANGUAGE:
            args.language = lang

    if args.name_prefix is None and _env(config.NAME_PREFIX_ENV) is None:
        _prompt_name_prefix(args, prompter)


def _prompt_name_prefix(args, prompter) -> None:
    """Ask for the resource name prefix; Enter accepts config.DEFAULT_NAME_PREFIX."""
    default = config.DEFAULT_NAME_PREFIX
    prompter.info("资源名前缀（所有资源名都由它派生：同一前缀重复运行会更新（覆盖）同名资源，新前缀会新建一套；"
                  "字母开头，共 3-31 位字母或数字）。默认前缀派生的资源名：")
    prompter.info(format_prefix_names(default))
    typed = []

    def validate(value):
        problem = name_prefix_problem(value)
        if problem:
            raise ValueError(f"资源名前缀{problem}（字母开头，共 3-31 位字母或数字）")
        typed.append(value)
        return value
    args.name_prefix = prompter.text("资源名前缀", default=default, validate=validate)
    args.name_prefix_source = "prompt" if typed else "default"


# ---------------------------------------------------------------------------
# State file (§6.4.1) and artifacts
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def empty_state(cfg: Config) -> dict:
    return {
        "schemaVersion": STATE_SCHEMA_VERSION,
        "dryRun": cfg.dry_run,
        "updatedAt": _utc_now(),
        "region": cfg.region,
        "acxd": {
            "workspaceId": cfg.workspace_id, "workspaceName": cfg.workspace_name,
            "workspaceSource": cfg.workspace_source, "endpoint": cfg.acxd_endpoint,
            "contextVariables": None, "flowId": None, "applicationId": None,
            "flowNodeIdsRemapped": None, "buildId": None, "buildVersion": None,
            "deploymentId": None, "environment": cfg.environment, "deploymentAlias": None,
            "applicationReused": None, "applicationName": None, "previousWelcomeFlowId": None,
            "language": None, "languageFallbackFrom": None,
        },
        "connect": {
            "instanceId": cfg.instance_id, "instanceArn": cfg.instance_arn, "instanceAlias": None,
            "instanceAccessUrl": None, "queueArn": None, "queueName": None,
            "screenpopFlowId": None, "screenpopFlowArn": None,
            "inboundFlowId": None, "inboundFlowArn": None, "agenticCxActionSource": None,
        },
        "completedSteps": [],
    }


def load_state(path) -> dict | None:
    """Return a real (dryRun == false, schemaVersion == 1) state or None."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        logger.warning("ignoring unreadable state file %s: %s", p, exc)
        return None
    if not isinstance(data, dict) or data.get("schemaVersion") != STATE_SCHEMA_VERSION:
        logger.warning("ignoring state file %s: unsupported schemaVersion", p)
        return None
    if data.get("dryRun") is not False:
        return None
    return data


def filter_state(base: dict | None, cfg: Config, check_workspace: bool) -> dict | None:
    """Drop a state file written for another region / instance / workspace (never merged or used for lookups)."""
    if not base:
        return base
    mismatches = []
    if base.get("region") and base["region"] != cfg.region:
        mismatches.append(f"region {base['region']} != {cfg.region}")
    inst = (base.get("connect") or {}).get("instanceId")
    if inst and cfg.instance_id and inst.lower() != cfg.instance_id.lower():
        mismatches.append(f"instanceId {inst} != {cfg.instance_id}")
    ws = (base.get("acxd") or {}).get("workspaceId")
    if check_workspace and ws and cfg.workspace_id and ws != cfg.workspace_id:
        mismatches.append(f"workspaceId {ws} != {cfg.workspace_id}")
    if mismatches:
        logger.warning("ignoring deploy_state.json written for a different target (%s)", "; ".join(mismatches))
        return None
    return base


def _dump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2) + "\n"


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, path)


def write_artifact(out_dir: Path, name: str, obj, secret: str | None) -> Path:
    text = _dump(obj)
    flowlib.assert_no_secret(text, secret)
    path = Path(out_dir) / name
    _atomic_write(path, text)
    logger.info("wrote %s", path)
    return path


class StateWriter:
    def __init__(self, path: Path, cfg: Config, secret: str | None, base: dict | None = None):
        self.path = Path(path)
        self.secret = secret
        self.state = empty_state(cfg)
        if base and not cfg.dry_run:
            for section in ("acxd", "connect"):
                for k, v in (base.get(section) or {}).items():
                    if v is not None and k in self.state[section]:
                        self.state[section][k] = v
            # values from this run's configuration win
            acxd, connect = self.state["acxd"], self.state["connect"]
            if cfg.workspace_id:
                acxd["workspaceId"] = cfg.workspace_id
                acxd["workspaceName"] = cfg.workspace_name or (
                    acxd["workspaceName"] if (base.get("acxd") or {}).get("workspaceId") == cfg.workspace_id
                    else None)
                acxd["workspaceSource"] = cfg.workspace_source
            acxd["endpoint"] = cfg.acxd_endpoint
            acxd["environment"] = cfg.environment
            connect["instanceId"] = cfg.instance_id or connect["instanceId"]
            connect["instanceArn"] = cfg.instance_arn or connect["instanceArn"]

    def update(self, section: str, **values) -> None:
        self.state[section].update(values)

    def step(self, name: str) -> None:
        if name not in self.state["completedSteps"]:  # the language retry repeats steps
            self.state["completedSteps"].append(name)
        self.write()

    def write(self) -> None:
        self.state["updatedAt"] = _utc_now()
        text = _dump(self.state)
        flowlib.assert_no_secret(text, self.secret)
        _atomic_write(self.path, text)


# ---------------------------------------------------------------------------
# Request body builders (pure)
# ---------------------------------------------------------------------------

def build_context_variable_bodies() -> list:
    return [
        {
            "name": f,
            "schema": copy.deepcopy(config.CONTEXT_VARIABLE_SCHEMA),
            "disallowExternalModification": False,
            "metadata": copy.deepcopy(config.CONTEXT_VARIABLE_METADATA),
        }
        for f in config.FIELDS
    ]


def build_flow_body(cfg: Config, nodes: dict) -> dict:
    return {
        "flowId": config.acxd_flow_id(cfg.name_prefix),
        "description": "Device fault intake: collects " + ", ".join(config.FIELDS),
        "mainLanguageCode": cfg.language,
        "languageCodes": [cfg.language],
        "slotTypes": [],
        "contextVariables": copy.deepcopy(config.FLOW_SCOPED_CONTEXT_VARIABLES),
        "nodes": nodes,
        "metadata": copy.deepcopy(config.FLOW_METADATA),
    }


def update_flow_body(body: dict) -> dict:
    return {k: v for k, v in body.items() if k != "flowId"}


def build_application_body(cfg: Config, messages: dict) -> dict:
    flow_ref = {"flowId": config.acxd_flow_id(cfg.name_prefix)}
    default_flows_sdk = {"welcome": flow_ref}  # SDK member names
    default_flows = {config.DEFAULT_FLOWS_WIRE_NAMES[k]: copy.deepcopy(v) for k, v in default_flows_sdk.items()}
    return {
        "name": config.acxd_app_name(cfg.name_prefix),
        "description": messages[cfg.language]["application_description"],
        "flows": [copy.deepcopy(flow_ref)],
        "settings": {
            "languageCode": cfg.language,
            "languageCodes": [cfg.language],
            "languageSettings": config.language_settings(cfg.language),
            "defaultFlows": default_flows,
        },
        "deploymentSettings": copy.deepcopy(config.APPLICATION_DEPLOYMENT_SETTINGS),
        "metadata": copy.deepcopy(config.APPLICATION_METADATA),
    }


def build_reuse_application_patch(existing: dict, flow_id: str) -> dict:
    """UpdateApplication body for --reuse-acxd-application: append our flow, point Welcome at it.

    Everything else in settings is kept verbatim; name, description, metadata, deploymentSettings
    and the read-only timestamps are deliberately not sent.
    """
    flows = [copy.deepcopy(f) for f in existing.get("flows") or []]
    if not any(isinstance(f, dict) and f.get("flowId") == flow_id for f in flows):
        flows.append({"flowId": flow_id})
    settings = copy.deepcopy(existing.get("settings") or {})
    settings.setdefault("defaultFlows", {})[config.DEFAULT_FLOWS_WIRE_NAMES["welcome"]] = {"flowId": flow_id}
    return {"flows": flows, "settings": settings}


def build_build_body(cfg: Config) -> dict:
    return {
        "version": "v" + _dt.datetime.now(_dt.timezone.utc).strftime("%y%m%d%H%M%S"),
        "description": "deploy.py build",
        "languageSettings": config.language_settings(cfg.language),
    }


def build_deployment_body(cfg: Config, build_id: str) -> dict:
    return {
        "buildIdentifier": build_id,
        "environment": cfg.environment,
        "languageCodes": [cfg.language],
        "description": "deploy.py deployment",
    }


def dryrun_screenpop_arn(cfg: Config) -> str:
    return (f"arn:aws:connect:{cfg.region}:{cfg.account_id or '000000000000'}:instance/{cfg.instance_id}"
            "/contact-flow/DRYRUN-SCREENPOP")


# ---------------------------------------------------------------------------
# Agentic CX helpers shared by dry-run and real run
# ---------------------------------------------------------------------------

def alias_text(cfg: Config, alias: str | None) -> str:
    if alias:
        return f"{alias}（environment={cfg.environment}）"
    return cfg.environment


def reference_flow_instructions(cfg: Config, alias: str | None) -> str:
    app = config.acxd_app_name(cfg.name_prefix)
    return "\n".join([
        "无法生成 Agentic CX action（ConnectParticipantWithAgenticCX 块需要 workspaceId、applicationId 与 "
        "deploymentAlias）。获取方法：",
        "  默认方式（脚本自动生成该块，无需参考流）：",
        "  1. 提供 --acxd-workspace-id <工作区ID>；",
        "  2. applicationId 与 deploymentAlias 按以下任一方式获得：",
        "     a. 设置 ACXD_API_KEY，脚本通过 ListApplications 按名称 " + app
        + " 查询 applicationId，再通过 ListApplicationDeployments 取 environment=" + cfg.environment
        + " 的 deploymentAlias；",
        "     b. 自动读取上次真实运行写入的 output/deploy_state.json；",
        "     c. 显式传入 --acxd-application-id <应用ID> --acxd-deployment-alias <deploymentAlias>；",
        "  3. 重新运行：python3 deploy.py --phase connect --acxd-workspace-id <工作区ID> ...",
        "  可选覆盖方式：",
        "  - 在 Connect 控制台新建流，拖入 Agentic CX 块，选择工作区 / 应用 " + app + " / alias："
        + alias_text(cfg, alias) + "，发布后用 --agentic-cx-reference-flow-id <流ID> 重新运行；",
        "  - 或用 --agentic-cx-action-file 提供 action JSON；仅测试时可加 --allow-unverified-agentic-cx-action",
    ])


def _prepare_action(action, meta, app_id, alias_candidates, source):
    rebuilt, _ = flowlib.rebuild_agentic_cx_transitions(action, flowlib.INBOUND_IDS)
    # The generated action carries this run's ids by construction; the fallback has
    # placeholder Parameters by definition.
    if source not in ("generated", "unverified-fallback"):
        for w in flowlib.check_captured_action(rebuilt, app_id, alias_candidates):
            logger.warning(w)
    return rebuilt, meta


def _generated_action(cfg: Config, app_id: str, alias: str, app_name: str | None = None):
    """A1: the real Agentic CX block built from this run's ids (source 'generated')."""
    action = flowlib.build_agentic_cx_action(cfg.workspace_id, app_id, alias)
    meta = flowlib.agentic_cx_action_metadata(cfg.workspace_name or cfg.workspace_id,
                                              app_name or config.acxd_app_name(cfg.name_prefix), cfg.environment)
    return action, meta


def _select(obj, app_id, identifier):
    try:
        return flowlib.select_agentic_cx_action(obj, app_id, identifier)
    except AgenticCxSelectionError as exc:
        lines = [str(exc)] + [f"  candidate: {c}" for c in exc.candidates]
        raise ConfigError("\n".join(lines)) from exc


def inbound_ctx(cfg: Config, messages: dict, screenpop_arn: str, queue_arn: str, queue_display: str) -> dict:
    conn = messages[cfg.language]["connect"]
    return {
        "FLOW_NAME": config.inbound_flow_name(cfg.name_prefix),
        "SCREENPOP_FLOW_ARN": screenpop_arn,
        "SCREENPOP_FLOW_NAME": config.screenpop_flow_name(cfg.name_prefix),
        "TTS_VOICE": cfg.tts_voice,
        "TTS_ENGINE": cfg.tts_engine,
        "QUEUE_ARN": queue_arn,
        "QUEUE_DISPLAY_NAME": queue_display,
        "ERROR_MESSAGE": conn["error_message"],
        "IDLE_TIMEOUT_MESSAGE": conn["idle_timeout_message"],
        # Customer Profiles lookup -> CustomerName / CustomerPhone / CustomerEmail (A22).
        **flowlib.customer_profile_ctx(cfg.language, messages),
    }


def print_assumptions() -> None:
    print("\n未解析的假设清单（详见 README 与 config.py）：")
    for k, v in config.ASSUMPTIONS.items():
        print(f"  {k}: {v}")


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def run_dry(cfg: Config) -> int:
    out = cfg.out_dir
    try:
        out.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"无法创建输出目录 {out}: {exc}") from exc
    secret = cfg.api_key
    messages = flowlib.load_messages()
    if not cfg.workspace_id:
        cfg.workspace_id, cfg.workspace_source = DRYRUN_WORKSPACE_ID, "dry-run placeholder"
        logger.warning("dry-run: the ACXD workspace cannot be discovered offline (ListWorkspaces); using the "
                       "placeholder %s", DRYRUN_WORKSPACE_ID)
    writer = StateWriter(out / "deploy_state.json", cfg, secret)
    writer.update("acxd", language=cfg.language)

    if cfg.acxd_phase:
        bodies = build_context_variable_bodies()
        write_artifact(out, "acxd_context_variables.json", bodies, secret)
        nodes = flowlib.render_acxd_flow(cfg.language, messages, cfg.name_prefix)
        write_artifact(out, "acxd_flow.json", build_flow_body(cfg, nodes), secret)
        if cfg.reuse_application:
            lang = cfg.language
            patch = build_reuse_application_patch(
                {"flows": [], "settings": {"languageCode": lang, "languageCodes": [lang]}},
                config.acxd_flow_id(cfg.name_prefix))
            write_artifact(out, "acxd_application.json", patch, secret)
            logger.warning("dry-run: reuse mode merges with the live application settings at run time; "
                           "use --preflight to render the real merged body")
            dry_app_id, dry_app_name = cfg.application_id, None
        else:
            write_artifact(out, "acxd_application.json", build_application_body(cfg, messages), secret)
            dry_app_id, dry_app_name = DRYRUN_APPLICATION_ID, config.acxd_app_name(cfg.name_prefix)
        build_body = build_build_body(cfg)
        write_artifact(out, "acxd_build.json", build_body, secret)
        write_artifact(out, "acxd_deployment.json", build_deployment_body(cfg, DRYRUN_BUILD_ID), secret)
        writer.update("acxd", contextVariables=list(config.FIELDS), flowId=config.acxd_flow_id(cfg.name_prefix),
                      applicationId=dry_app_id, flowNodeIdsRemapped=None, buildId=DRYRUN_BUILD_ID,
                      buildVersion=build_body["version"], deploymentId=DRYRUN_DEPLOYMENT_ID,
                      applicationReused=cfg.reuse_application, applicationName=dry_app_name)

    if cfg.connect_phase:
        # The dry-run applicationId is fake, so only a user-supplied id is used for rule a / WARN checks.
        app_id = cfg.application_id
        if cfg.queue_arn:
            queue_arn, queue_display = cfg.queue_arn, cfg.queue_name or DRYRUN_QUEUE_NAME
        else:
            queue_arn = (f"arn:aws:connect:{cfg.region}:{cfg.account_id or '000000000000'}:instance/"
                         f"{cfg.instance_id}/queue/DRYRUN-QUEUE")
            queue_display = cfg.queue_name or config.DEFAULT_QUEUE_NAME
            logger.warning("dry-run: queue %s cannot be resolved offline, using placeholder ARN %s",
                           queue_display, queue_arn)

        gen_app_id = cfg.application_id or (DRYRUN_APPLICATION_ID if cfg.acxd_phase else None)
        alias = cfg.deployment_alias
        if cfg.action_obj is not None:
            action, meta = _select(cfg.action_obj, app_id, cfg.action_identifier)
            source = "file"
        elif cfg.workspace_id and gen_app_id:
            alias = alias or DRYRUN_DEPLOYMENT_ALIAS
            action, meta = _generated_action(cfg, gen_app_id, alias)
            source = "generated"
        else:
            if cfg.reference_flow_id:
                logger.warning("dry-run cannot call describe_contact_flow; using the unverified fallback fragment")
            else:
                logger.warning("no Agentic CX action source given; using the unverified fallback fragment (A1)")
            action, meta = flowlib.load_fallback_action(), None
            source = "unverified-fallback"
        logger.info("Agentic CX action source: %s", source)
        action, meta = _prepare_action(action, meta, app_id, [alias], source)
        writer.update("acxd", deploymentAlias=alias)

        sp_arn = dryrun_screenpop_arn(cfg)
        screenpop = flowlib.render_screenpop_flow(cfg.language, messages, cfg.region,
                                                  config.screenpop_flow_name(cfg.name_prefix))
        write_artifact(out, "screenpop_flow.json", screenpop, secret)
        inbound = flowlib.render_inbound_flow(inbound_ctx(cfg, messages, sp_arn, queue_arn, queue_display),
                                              action, meta)
        write_artifact(out, "inbound_acxd_flow.json", inbound, secret)
        writer.update("connect", queueArn=queue_arn, queueName=queue_display,
                      screenpopFlowId="DRYRUN-SCREENPOP", screenpopFlowArn=sp_arn,
                      inboundFlowId="DRYRUN-INBOUND",
                      inboundFlowArn=sp_arn.replace("DRYRUN-SCREENPOP", "DRYRUN-INBOUND"),
                      agenticCxActionSource=source)
        if source == "unverified-fallback":
            print(reference_flow_instructions(cfg, None))

    writer.write()
    print(f"\ndry-run 完成：产物已写入 {out}/（未发起任何网络调用）")
    if cfg.acxd_phase:
        print(ACXD_MODEL_LINE)
    print_assumptions()
    return 0


# ---------------------------------------------------------------------------
# ACXD phase (§6.4 steps 1-5)
# ---------------------------------------------------------------------------

def _strip_language_echo(obj):
    if isinstance(obj, dict):
        return {k: _strip_language_echo(v) for k, v in obj.items() if k not in config.LANGUAGE_ECHO_KEYS}
    if isinstance(obj, list):
        return [_strip_language_echo(v) for v in obj]
    return obj


def is_language_failure(obj) -> bool:
    """A21: True when an error message / FAILED build object points at the language."""
    text = obj if isinstance(obj, str) else json.dumps(_strip_language_echo(obj), ensure_ascii=False)
    text = text.lower()
    return any(k in text for k in config.LANGUAGE_FAILURE_KEYWORDS)


def _acxd_fail(exc: AcxdApiError, op: str, body_path: Path | None = None):
    where = f"；请求体: {body_path}" if body_path else ""
    if exc.status == 400 and is_language_failure(exc.message or ""):
        logger.error("%s rejected for language reasons: status=%s type=%s message=%s%s", op, exc.status,
                     exc.error_type, exc.message, where)
        raise LanguageRejectedError(f"{op}: ACXD 以语言原因拒绝了请求（{exc.message}）", 1) from exc
    if exc.status == 401:
        hint = "API key 无效或已吊销，请在 Admin Hub 重新生成"
    elif exc.status == 403:
        hint = "请检查 API key 所属 programmatic user 在该工作区的角色"
    elif exc.status == 400:
        hint = "请求被拒绝（校验错误），请查看 message"
    else:
        hint = "ACXD 请求失败"
    logger.error("%s failed: status=%s type=%s message=%s%s", op, exc.status, exc.error_type, exc.message, where)
    raise DeployError(f"{op}: {hint}", 1) from exc


def _create_with_recovery(op, create_fn, find_fn):
    """Create-type POST with "result unknown" recovery (§6.5).

    Returns ("created", response) | ("found", existing) | ("conflict", None).
    """
    try:
        return "created", create_fn()
    except AcxdAmbiguousWriteError as exc:
        logger.warning("%s result unknown (%s); re-listing before retrying", op, exc)
    except AcxdApiError as exc:
        if exc.status == 409:
            logger.info("%s returned 409 Conflict; treating as existing", op)
            return "conflict", None
        raise
    found = find_fn()
    if found is not None:
        logger.info("%s: resource exists after ambiguous create", op)
        return "found", found
    try:
        return "created", create_fn()
    except AcxdAmbiguousWriteError as exc:
        logger.error("%s failed twice with unknown result: %s", op, exc)
        raise DeployError(f"{op}: 两次创建结果均不明，请检查 ACXD 后重跑", 1) from exc
    except AcxdApiError as exc:
        if exc.status == 409:
            return "conflict", None
        raise


def _list_context_variables(client: AcxdClient, path: Path) -> list:
    """First ACXD call of the phase: 403 / 404 means a wrong workspace or role (exit 2)."""
    try:
        return client.list_context_variables()
    except AcxdApiError as exc:
        if exc.status in (403, 404):
            logger.error("ListContextVariables failed: status=%s type=%s", exc.status, exc.error_type)
            raise ConfigError("首个 ACXD 调用失败：请检查 --acxd-workspace-id 是否正确，以及 API key 所属 "
                              "programmatic user 在该工作区的角色") from exc
        _acxd_fail(exc, "ListContextVariables", path)


def ensure_context_variables(client: AcxdClient, cfg: Config, writer: StateWriter) -> None:
    bodies = build_context_variable_bodies()
    path = write_artifact(cfg.out_dir, "acxd_context_variables.json", bodies, cfg.api_key)
    existing = _list_context_variables(client, path)

    def find(name):
        for item in client.list_context_variables():
            if item.get("name") == name:
                return item
        return None

    by_name = {i.get("name"): i for i in existing}
    for body in bodies:
        name = body["name"]
        if name in by_name:
            got = (by_name[name].get("schema") or {}).get("type")
            if got != body["schema"]["type"]:
                logger.warning("context variable %s exists with schema.type=%s (expected %s); not modified",
                               name, got, body["schema"]["type"])
            else:
                logger.info("context variable %s exists", name)
            continue
        try:
            kind, _ = _create_with_recovery("CreateContextVariable",
                                            lambda b=body: client.create_context_variable(b),
                                            lambda n=name: find(n))
        except AcxdApiError as exc:
            _acxd_fail(exc, "CreateContextVariable", path)
        logger.info("context variable %s: %s", name, kind)
    writer.update("acxd", contextVariables=list(config.FIELDS))
    writer.step("ensure_context_variables")


def upsert_acxd_flow(client: AcxdClient, cfg: Config, body: dict, writer: StateWriter) -> None:
    path = write_artifact(cfg.out_dir, "acxd_flow.json", body, cfg.api_key)
    flow_id = body["flowId"]

    def find():
        for item in client.list_flows():
            if item.get("flowId") == flow_id:
                return item
        return None

    def update():
        try:
            return client.update_flow(flow_id, update_flow_body(body))
        except AcxdApiError as exc:
            if exc.status == 404:
                logger.warning("UpdateFlow 404 (deleted meanwhile); creating once")
                return client.create_flow(body)
            if exc.status == 409:
                logger.error("UpdateFlow 409: the flow is being edited concurrently")
                raise DeployError("UpdateFlow 冲突（画布中有人同时编辑），请稍后重试", 1) from exc
            raise

    try:
        if find() is not None:
            logger.info("ACXD flow %s exists; updating", flow_id)
            # Checked again right before the PATCH (precheck_acxd_names ran before any write).
            check_acxd_flow_owned(client, cfg, flow_id)
            resp = update()
        else:
            kind, resp = _create_with_recovery("CreateFlow", lambda: client.create_flow(body), find)
            if kind != "created":
                # A 409 / found flow may be a foreign flow created meanwhile.
                check_acxd_flow_owned(client, cfg, flow_id)
                resp = update()
    except AcxdApiError as exc:
        _acxd_fail(exc, "UpsertFlow", path)

    sent = set(body["nodes"])
    got = resp.get("nodes") if isinstance(resp, dict) else None
    if isinstance(got, dict):
        remapped = set(got) != sent
        if remapped:
            logger.warning("ACXD flow node IDs differ from the ones sent (A15): sent=%s returned=%s",
                           sorted(sent), sorted(got))
    else:
        remapped = None
        logger.debug("flow response has no nodes object; flowNodeIdsRemapped=null")
    writer.update("acxd", flowId=flow_id, flowNodeIdsRemapped=remapped)
    writer.step("upsert_acxd_flow")


# ---------------------------------------------------------------------------
# Ownership guard: only resources created by this script are overwritten (config.OWNER_TAG)
# ---------------------------------------------------------------------------

APP_OWNED, APP_FORCED, APP_MISSING = "owned", "forced", "missing"
OVERWRITE_FLAG = "--overwrite-untagged-resources"


def metadata_owned(doc: dict | None) -> bool:
    """ACXD application / flow: True when metadata.tags contains config.OWNER_TAG."""
    tags = ((doc or {}).get("metadata") or {}).get("tags") if isinstance(doc, dict) else None
    return isinstance(tags, list) and config.OWNER_TAG in tags


application_owned = metadata_owned


def _ownership_verdict(cfg: Config, kind: str, name: str, ident: str, owned: bool, evidence: str,
                       alternatives: str) -> str:
    """APP_OWNED / APP_FORCED, or ConfigError (exit 2) for a foreign resource without the override flag."""
    if owned:
        logger.info("%s %s (%s) was created by this script (%s); it will be updated", kind, name, ident, evidence)
        return APP_OWNED
    if cfg.overwrite_untagged:
        logger.warning("%s %s (%s) was not created by this script (%s); overwriting it because of %s",
                       kind, name, ident, evidence, OVERWRITE_FLAG)
        return APP_FORCED
    logger.error("%s %s (%s) was not created by this script (%s)", kind, name, ident, evidence)
    raise ConfigError(
        f"已存在同名 {kind} {name}（{ident}），但它不是本脚本创建的（{evidence}），为避免覆盖已停止，"
        f"本次没有执行任何写操作。可选做法：用 --name-prefix <新前缀> 另建一套资源{alternatives}；"
        f"确认要覆盖时加 {OVERWRITE_FLAG}")


def _find_application_by_name(client: AcxdClient, name: str):
    """The single application named `name`, or None.  More than one -> DeployError (exit 1)."""
    matches = [a for a in client.list_applications() if a.get("name") == name]
    if len(matches) > 1:
        ids = ", ".join(str(a.get("applicationId")) for a in matches)
        logger.error("multiple ACXD applications named %s: %s", name, ids)
        raise DeployError(f"存在多个同名应用 {name}（{ids}），请改用 --name-prefix", 1)
    return matches[0] if matches else None


def check_application_owned(client: AcxdClient, cfg: Config, app_id: str, name: str) -> str:
    """GetApplication (read-only) of an existing same-name application before it is overwritten.

    Returns APP_OWNED, APP_FORCED (foreign, --overwrite-untagged-resources; WARN) or APP_MISSING
    (404: the caller creates it).  A foreign application otherwise raises ConfigError (exit 2).
    """
    try:
        app = client.get_application(app_id)
    except AcxdApiError as exc:
        if exc.status == 404:
            logger.warning("ACXD application %s (%s) disappeared before the ownership check", name, app_id)
            return APP_MISSING
        _acxd_fail(exc, "GetApplication")
    tags = ((app.get("metadata") or {}).get("tags")) if isinstance(app, dict) else None
    return _ownership_verdict(
        cfg, "ACXD 应用", name, app_id, metadata_owned(app), f"metadata.tags={tags}",
        f"；或用 --reuse-acxd-application --acxd-application-id {app_id} 把 flow 追加进该应用")


def check_acxd_flow_owned(client: AcxdClient, cfg: Config, flow_id: str) -> str:
    """GetFlow (read-only) of an existing ACXD flow with the derived flowId.  Same results as above."""
    try:
        flow = client.get_flow(flow_id)
    except AcxdApiError as exc:
        if exc.status == 404:
            logger.warning("ACXD flow %s disappeared before the ownership check", flow_id)
            return APP_MISSING
        _acxd_fail(exc, "GetFlow")
    tags = ((flow.get("metadata") or {}).get("tags")) if isinstance(flow, dict) else None
    return _ownership_verdict(cfg, "ACXD flow", flow_id, flow_id, metadata_owned(flow), f"metadata.tags={tags}", "")


def _flow_listed(client: AcxdClient, flow_id: str) -> bool:
    return any(item.get("flowId") == flow_id for item in client.list_flows())


def precheck_acxd_names(client: AcxdClient, cfg: Config) -> None:
    """Read-only, before the first ACXD write: duplicate / foreign same-name application and a foreign
    flow with the derived flowId stop the run here (the reused application is checked separately)."""
    try:
        if not cfg.reuse_application:
            name = config.acxd_app_name(cfg.name_prefix)
            existing = _find_application_by_name(client, name)
            if existing is not None and existing.get("applicationId"):
                check_application_owned(client, cfg, existing["applicationId"], name)
        flow_id = config.acxd_flow_id(cfg.name_prefix)
        if _flow_listed(client, flow_id):
            check_acxd_flow_owned(client, cfg, flow_id)
    except AcxdApiError as exc:
        # First ACXD calls of the phase: 403 / 404 means a wrong workspace or role (exit 2).
        if exc.status in (403, 404):
            logger.error("ACXD precheck failed: status=%s type=%s", exc.status, exc.error_type)
            raise ConfigError("首个 ACXD 调用失败：请检查 --acxd-workspace-id 是否正确，以及 API key 所属 "
                              "programmatic user 在该工作区的角色") from exc
        _acxd_fail(exc, "ListApplications / ListFlows")


def _template_start_actions() -> frozenset:
    """StartAction of both Connect templates: marks flows created before the resource tag existed."""
    return frozenset(flowlib.load_template(t)["StartAction"]
                     for t in (flowlib.INBOUND_TEMPLATE, flowlib.SCREENPOP_TEMPLATE))


def _describe_flow_for_owner(client, cfg: Config, flow_id: str):
    """DescribeContactFlow of the published version, else of the saved one ('<id>:$SAVED')."""
    try:
        return client.describe_contact_flow(InstanceId=cfg.instance_id, ContactFlowId=flow_id)["ContactFlow"]
    except ClientError as exc:
        if _err_code(exc) != "ContactFlowNotPublishedException":
            raise
    return client.describe_contact_flow(InstanceId=cfg.instance_id, ContactFlowId=f"{flow_id}:$SAVED")["ContactFlow"]


def check_contact_flow_owned(client, cfg: Config, summary: dict) -> str:
    """Owned when the flow carries tag CONNECT_OWNER_TAG_KEY=OWNER_TAG, or (flows created before the
    tag existed) its content starts at the StartAction of our template.  Read-only (Describe)."""
    name, flow_id = summary.get("Name"), summary["Id"]
    try:
        flow = _describe_flow_for_owner(client, cfg, flow_id)
    except ClientError as exc:
        if _err_code(exc) == "ResourceNotFoundException":
            logger.warning("contact flow %s (%s) disappeared before the ownership check", name, flow_id)
            return APP_MISSING
        raise
    tags = flow.get("Tags") or {}
    try:
        start = (json.loads(flow.get("Content") or "{}") or {}).get("StartAction")
    except (TypeError, ValueError):
        start = None
    tagged = tags.get(config.CONNECT_OWNER_TAG_KEY) == config.OWNER_TAG
    legacy = start in _template_start_actions()
    evidence = (f"标签 {config.CONNECT_OWNER_TAG_KEY}={tags.get(config.CONNECT_OWNER_TAG_KEY)}，"
                f"StartAction={start}")
    return _ownership_verdict(cfg, "Connect 流", name, flow_id, tagged or legacy, evidence, "")


def precheck_contact_flows(client, cfg: Config) -> dict:
    """Read-only, before any write: one ListContactFlows, ownership of every derived-name flow.

    Returns {name: (summary, status)} of the flows that exist (foreign ones raise ConfigError)."""
    wanted = {config.screenpop_flow_name(cfg.name_prefix), config.inbound_flow_name(cfg.name_prefix)}
    found = {}
    for page in client.get_paginator("list_contact_flows").paginate(
            InstanceId=cfg.instance_id, ContactFlowTypes=["CONTACT_FLOW"]):
        for s in page.get("ContactFlowSummaryList", []):
            if s.get("Name") in wanted and s.get("Name") not in found:
                found[s["Name"]] = s
    existing = {}
    for name in sorted(found):
        status = check_contact_flow_owned(client, cfg, found[name])
        if status != APP_MISSING:
            existing[name] = (found[name], status)
    return existing


def upsert_application(client: AcxdClient, cfg: Config, body: dict, writer: StateWriter) -> str:
    path = write_artifact(cfg.out_dir, "acxd_application.json", body, cfg.api_key)
    name = body["name"]

    def find():
        return _find_application_by_name(client, name)

    def update(app_id):
        try:
            client.update_application(app_id, body)
            return app_id
        except AcxdApiError as exc:
            if exc.status == 404:
                logger.warning("UpdateApplication 404 (deleted meanwhile); creating once")
                return client.create_application(body)["applicationId"]
            raise

    try:
        existing = find()
        if existing is not None:
            logger.info("ACXD application %s exists; updating", name)
            # Checked again right before the PATCH (precheck_acxd_names ran before any write).
            check_application_owned(client, cfg, existing["applicationId"], name)
            app_id = update(existing["applicationId"])
        else:
            kind, resp = _create_with_recovery("CreateApplication", lambda: client.create_application(body), find)
            if kind == "created":
                app_id = resp["applicationId"]
            else:
                found = resp if kind == "found" else find()
                if found is None:
                    raise DeployError("CreateApplication 409 但列表中找不到同名应用", 1)
                # A 409 may come from a foreign application created meanwhile.
                check_application_owned(client, cfg, found["applicationId"], name)
                app_id = update(found["applicationId"])
    except AcxdApiError as exc:
        _acxd_fail(exc, "UpsertApplication", path)
    logger.info("ACXD applicationId=%s", app_id)
    writer.update("acxd", applicationId=app_id)
    writer.step("upsert_application")
    return app_id


def _get_reuse_application(client: AcxdClient, cfg: Config) -> dict:
    """GetApplication for --reuse-acxd-application plus the language check (read-only)."""
    app_id = cfg.application_id
    try:
        existing = client.get_application(app_id)
    except AcxdApiError as exc:
        if exc.status == 404:
            logger.error("GetApplication 404 for applicationId=%s", app_id)
            raise ConfigError(f"应用不存在: {app_id}（请检查 --acxd-application-id 与 --acxd-workspace-id）") from exc
        _acxd_fail(exc, "GetApplication")
    settings = existing.get("settings") or {}
    codes = settings.get("languageCodes")
    if not codes:
        codes = [settings["languageCode"]] if settings.get("languageCode") else []
    if cfg.language not in codes:
        hint = f"--language {codes[0]}" if codes else "--language <应用语言>"
        raise ConfigError(f"--language {cfg.language} 不在应用 {existing.get('name') or app_id} 的语言 "
                          f"{codes} 中，请改用 {hint}")
    return existing


def reuse_application(client: AcxdClient, cfg: Config, flow_id: str, writer: StateWriter):
    """--reuse-acxd-application: merge our flow into an existing application.  Returns (app_id, app_name)."""
    app_id = cfg.application_id
    existing = _get_reuse_application(client, cfg)
    app_name = existing.get("name")
    write_artifact(cfg.out_dir, "acxd_application_before.json", redact(existing), cfg.api_key)
    body = build_reuse_application_patch(existing, flow_id)
    path = write_artifact(cfg.out_dir, "acxd_application.json", body, cfg.api_key)
    welcome_key = config.DEFAULT_FLOWS_WIRE_NAMES["welcome"]
    previous = (((existing.get("settings") or {}).get("defaultFlows") or {}).get(welcome_key) or {}).get("flowId")
    logger.warning("reusing ACXD application %s (%s): appending flow %s and changing %s %s -> %s; "
                   "the %s deployment will be replaced (rollback reference: %s)",
                   app_name, app_id, flow_id, welcome_key, previous, flow_id, cfg.environment,
                   cfg.out_dir / "acxd_application_before.json")
    try:
        client.update_application(app_id, body)
    except AcxdApiError as exc:
        _acxd_fail(exc, "UpdateApplication", path)
    logger.info("ACXD applicationId=%s (reused)", app_id)
    writer.update("acxd", applicationId=app_id, applicationReused=True, applicationName=app_name,
                  previousWelcomeFlowId=previous)
    writer.step("upsert_application")
    return app_id, app_name


def create_build_and_poll(client: AcxdClient, cfg: Config, app_id: str, writer: StateWriter) -> str:
    body = build_build_body(cfg)
    path = write_artifact(cfg.out_dir, "acxd_build.json", body, cfg.api_key)
    try:
        resp = client.create_build(app_id, body)
    except AcxdAmbiguousWriteError as exc:
        logger.error("CreateApplicationBuild result unknown: %s", exc)
        raise DeployError("build 可能已创建，请在 ACXD 控制台查看后重跑脚本", 1) from exc
    except AcxdApiError as exc:
        _acxd_fail(exc, "CreateApplicationBuild", path)
    build_id = resp.get("buildId")
    logger.info("ACXD build %s created (version %s)", build_id, body["version"])
    writer.update("acxd", buildId=build_id, buildVersion=body["version"])

    deadline = time.monotonic() + cfg.build_timeout
    while True:
        try:
            b = client.get_build(app_id, build_id)
        except AcxdApiError as exc:
            if exc.status == 404:
                logger.error("GetApplicationBuild 404 for buildId=%s", build_id)
                raise DeployError(f"build {build_id} 不存在", 1) from exc
            _acxd_fail(exc, "GetApplicationBuild")
        status = str(b.get("status") or "").upper()
        logger.info("build %s status=%s", build_id, status)
        if status == "BUILT":
            break
        if status == "FAILED":
            p = write_artifact(cfg.out_dir, "acxd_build_result.json", redact(b), cfg.api_key)
            logger.error("build FAILED: %s (full response: %s)", json.dumps(redact(b), ensure_ascii=False), p)
            if is_language_failure(b):
                raise LanguageRejectedError(f"build {build_id} FAILED（语言 {cfg.language} 被拒绝，详见 {p}）", 1)
            raise DeployError(f"build {build_id} FAILED", 1)
        if time.monotonic() >= deadline:
            logger.error("build %s did not finish within %ss", build_id, cfg.build_timeout)
            raise DeployError(f"build {build_id} 轮询超时", 1)
        time.sleep(cfg.poll_interval)
    writer.step("create_build")
    return build_id


def upsert_deployment_and_poll(client: AcxdClient, cfg: Config, app_id: str, build_id: str,
                               writer: StateWriter) -> str | None:
    body = build_deployment_body(cfg, build_id)
    path = write_artifact(cfg.out_dir, "acxd_deployment.json", body, cfg.api_key)

    def find():
        for d in client.list_deployments(app_id):
            if d.get("environment") == cfg.environment:
                return d
        return None

    def update(dep_id):
        try:
            return client.update_deployment(app_id, dep_id, body)
        except AcxdApiError as exc:
            if exc.status == 404:
                logger.warning("UpdateApplicationDeployment 404; creating once")
                return client.create_deployment(app_id, body)
            raise

    try:
        existing = find()
        if existing is not None:
            dep_id = existing["deploymentId"]
            logger.info("deployment %s exists for environment %s; updating", dep_id, cfg.environment)
            resp = update(dep_id)
        else:
            kind, resp = _create_with_recovery("CreateApplicationDeployment",
                                               lambda: client.create_deployment(app_id, body), find)
            if kind == "conflict":
                resp = find()
                if resp is None:
                    raise DeployError("CreateApplicationDeployment 409 但找不到该环境的 deployment", 1)
                kind = "found"
            if kind == "found":
                found_build = resp.get("buildId") or resp.get("buildIdentifier")
                if found_build != build_id:
                    resp = update(resp["deploymentId"]) or resp
        dep_id = resp.get("deploymentId") or (existing or {}).get("deploymentId")
    except AcxdApiError as exc:
        _acxd_fail(exc, "UpsertApplicationDeployment", path)
    alias = resp.get("deploymentAlias")
    writer.update("acxd", deploymentId=dep_id)

    deadline = time.monotonic() + cfg.build_timeout
    while True:
        try:
            d = client.get_deployment(app_id, dep_id)
        except AcxdApiError as exc:
            if exc.status == 404:
                logger.error("GetApplicationDeployment 404 for deploymentId=%s", dep_id)
                raise DeployError(f"deployment {dep_id} 不存在", 1) from exc
            _acxd_fail(exc, "GetApplicationDeployment")
        status = str(d.get("deploymentStatus") or "")
        alias = d.get("deploymentAlias") or alias
        logger.info("deployment %s status=%s", dep_id, status)
        if status in config.DEPLOYMENT_SUCCESS_STATUSES:
            break
        if config.DEPLOYMENT_FAILURE_MARKER in status.lower():
            # GetApplicationDeployment carries the application's runtime apiKey: never log it.
            p = write_artifact(cfg.out_dir, "acxd_deployment_result.json", redact(d), cfg.api_key)
            logger.error("deployment failed: %s (response: %s)", json.dumps(redact(d), ensure_ascii=False), p)
            raise DeployError(
                f"deployment {dep_id} 失败: {status}（ACXD 没有返回失败原因；build {build_id} 已是 BUILT）。"
                f"排查：python3 tools/acxd_diagnose_deployment.py --output-dir {cfg.output_dir}"
                "（只读，汇总工作区内各应用的部署状态与语言，并查询审计记录中的错误信息）", 1)
        if time.monotonic() >= deadline:
            logger.error("deployment %s did not finish within %ss", dep_id, cfg.build_timeout)
            raise DeployError(f"deployment {dep_id} 轮询超时", 1)
        time.sleep(cfg.poll_interval)
    writer.update("acxd", deploymentAlias=alias or None)
    writer.step("upsert_deployment")
    return alias or None


def _can_prompt(cfg: Config) -> bool:
    return cfg.interactive and cfg.prompter is not None and not cfg.dry_run


def _uuid_validator(what: str):
    def validate(value):
        if not UUID_RE.match(value):
            raise ValueError(f"{what}必须是 UUID: {value}")
        return value
    return validate


def resolve_workspace(cfg: Config, state: dict | None, read_only: bool) -> None:
    """Fill cfg.workspace_id / workspace_name / workspace_source (D2).  Read-only.

    --acxd-workspace-id wins without any request; without an API key nothing is looked up.
    Order: GET /sdk/workspaces, then (on 401/403 only) the WorkspaceId of existing published
    Agentic CX actions in the instance (ListContactFlows / DescribeContactFlow).  In interactive
    mode the two ambiguous / forbidden outcomes ask the user instead of failing (source 'interactive').
    """
    if cfg.workspace_id:
        cfg.workspace_source = cfg.workspace_source or "--acxd-workspace-id"
        logger.info("ACXD workspace %s (source: %s)", cfg.workspace_id, cfg.workspace_source)
        return
    if not cfg.api_key:
        return
    client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, None, sleep=lambda s: time.sleep(s), read_only=read_only)
    try:
        items = client.list_workspaces()
    except AcxdApiError as exc:
        if exc.status in (401, 403):
            logger.error("ListWorkspaces failed: status=%s type=%s", exc.status, exc.error_type)
            # Source 2 (boto3 Connect) has no workspace operation; source 3 is read-only.
            if _workspace_from_agentic_cx_actions(cfg, state):
                return
            if _can_prompt(cfg):
                cfg.workspace_id = cfg.prompter.text("ACXD 工作区 ID", validate=_uuid_validator("工作区 ID"))
                cfg.workspace_name, cfg.workspace_source = None, "interactive"
                logger.info("ACXD workspace %s (source: interactive)", cfg.workspace_id)
                return
            raise ConfigError(f"无法自动发现 ACXD 工作区（ListWorkspaces 返回 {exc.status}，实例中也没有可读取 "
                              "WorkspaceId 的已发布 Agentic CX 块）：API key 无效，或其 programmatic user 没有列出"
                              "工作区的权限；请用 --acxd-workspace-id <工作区ID> 指定") from exc
        _acxd_fail(exc, "ListWorkspaces")
    workspaces = [w for w in items if isinstance(w, dict) and w.get("workspaceId")]
    chosen, source = None, "ListWorkspaces"
    if len(workspaces) == 1:
        chosen = workspaces[0]
    elif not workspaces:
        raise ConfigError("该 ACXD API key 看不到任何工作区（ListWorkspaces 返回 0 个）；请检查 key 所属的 "
                          "programmatic user，或用 --acxd-workspace-id <工作区ID> 指定")
    else:
        remembered = ((state or {}).get("acxd") or {}).get("workspaceId")
        chosen = next((w for w in workspaces if w["workspaceId"] == remembered), None)
        source = "deploy_state.json"
        if chosen is None and _can_prompt(cfg):
            ordered = sorted(workspaces, key=lambda w: (w.get("name") or "", w["workspaceId"]))
            picked = cfg.prompter.choice("请选择 ACXD 工作区：",
                                         [(w["workspaceId"], f"{w.get('name')} ({w['workspaceId']})") for w in ordered])
            chosen = next(w for w in ordered if w["workspaceId"] == picked)
            source = "interactive"
        if chosen is None:
            lines = "\n".join(f"  {w.get('name')} ({w['workspaceId']})" for w in workspaces)
            raise ConfigError(f"该 ACXD API key 可以访问 {len(workspaces)} 个工作区，无法自动选择：\n{lines}\n"
                              "请用 --acxd-workspace-id <工作区ID> 指定")
    cfg.workspace_id = chosen["workspaceId"]
    cfg.workspace_name = chosen.get("name") or None
    cfg.workspace_source = source
    logger.info("ACXD workspace %s (%s) (source: %s)", cfg.workspace_name, cfg.workspace_id, source)


def _workspace_from_agentic_cx_actions(cfg: Config, state: dict | None) -> bool:
    """Discovery source 3 (read-only): WorkspaceId of ConnectParticipantWithAgenticCX actions in the
    published contact flows of the instance.  Only ListContactFlows / DescribeContactFlow, always
    behind the read-only guard.  Returns True when cfg.workspace_id was filled."""
    if not cfg.instance_id:
        return False
    found = {}  # workspaceId -> name of the first flow it was seen in
    truncated = False
    try:
        client = install_connect_read_only_guard(make_connect_client(cfg.region))
        summaries = []
        for page in client.get_paginator("list_contact_flows").paginate(
                InstanceId=cfg.instance_id, ContactFlowTypes=["CONTACT_FLOW"]):
            summaries.extend(s for s in page.get("ContactFlowSummaryList", [])
                             if s.get("Id") and s.get("ContactFlowStatus") != "SAVED"
                             and s.get("ContactFlowState", "ACTIVE") == "ACTIVE")
        preferred = {cfg.reference_flow_id, config.inbound_flow_name(cfg.name_prefix)}
        summaries.sort(key=lambda s: (s["Id"] not in preferred and s.get("Name") not in preferred,
                                      s.get("Name") or ""))
        truncated = len(summaries) > config.WORKSPACE_DISCOVERY_MAX_FLOWS
        for s in summaries[:config.WORKSPACE_DISCOVERY_MAX_FLOWS]:
            try:
                resp = client.describe_contact_flow(InstanceId=cfg.instance_id, ContactFlowId=s["Id"])
                content = json.loads(resp["ContactFlow"]["Content"])
            except ClientError as exc:
                if _err_code(exc) in ("ResourceNotFoundException", "ContactFlowNotPublishedException"):
                    continue
                raise
            except (KeyError, TypeError, ValueError):
                continue
            for a in content.get("Actions", []) if isinstance(content, dict) else []:
                if not isinstance(a, dict) or a.get("Type") != config.AGENTIC_CX_ACTION_TYPE:
                    continue
                ws = ((a.get("Parameters") or {}).get("AgentConfiguration") or {}).get("WorkspaceId")
                if isinstance(ws, str) and UUID_RE.match(ws):
                    found.setdefault(ws, s.get("Name") or s["Id"])
    except (BotoCoreError, ClientError) as exc:
        logger.warning("workspace discovery from existing Agentic CX actions failed (%s)",
                       _err_code(exc) if isinstance(exc, ClientError) else type(exc).__name__)
        return False
    if truncated:
        logger.warning("workspace discovery read only the first %d published contact flows",
                       config.WORKSPACE_DISCOVERY_MAX_FLOWS)
    if len(found) > 1:
        remembered = ((state or {}).get("acxd") or {}).get("workspaceId")
        if remembered not in found:
            lines = "; ".join(f"{ws} ({name})" for ws, name in found.items())
            logger.warning("existing Agentic CX actions reference %d workspaces (%s); not choosing one",
                           len(found), lines)
            return False
        found = {remembered: found[remembered]}
    if not found:
        logger.warning("no published contact flow of instance %s contains a %s action with a WorkspaceId",
                       cfg.instance_id, config.AGENTIC_CX_ACTION_TYPE)
        return False
    (ws, flow_name), = found.items()
    cfg.workspace_id, cfg.workspace_name = ws, None
    cfg.workspace_source = f"existing-agentic-cx-action:{flow_name}"
    logger.info("ACXD workspace %s (source: %s)", ws, cfg.workspace_source)
    return True


def _language_attempts(cfg: Config) -> list:
    """D6: the en-US retry is allowed only without an explicit --language and outside reuse mode."""
    langs = [cfg.language]
    if (not cfg.language_explicit and not cfg.reuse_application and cfg.language != config.FALLBACK_LANGUAGE
            and config.FALLBACK_LANGUAGE in config.SUPPORTED_LANGUAGES):
        langs.append(config.FALLBACK_LANGUAGE)
    return langs


def run_acxd_phase(cfg: Config, writer: StateWriter, messages: dict):
    logger.info("ACXD endpoint %s, workspace %s, key %s", cfg.acxd_endpoint, cfg.workspace_id, mask_key(cfg.api_key))
    client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, cfg.workspace_id, sleep=lambda s: time.sleep(s))
    if cfg.reuse_application:
        # Fail on a missing application / language mismatch before any ACXD write.
        _get_reuse_application(client, cfg)
    # Fail on duplicate / foreign same-name applications and a foreign flow before any ACXD write.
    precheck_acxd_names(client, cfg)
    try:
        ensure_context_variables(client, cfg, writer)  # language independent: once
    except LanguageRejectedError as exc:
        # Outside the retry loop: only flow / application / build trigger the automatic fallback.
        raise DeployError(f"{exc}{_language_hint(cfg)}", 1) from exc
    attempts = _language_attempts(cfg)
    for i, lang in enumerate(attempts):
        if lang != cfg.language:
            set_language(cfg, lang, messages)
        try:
            nodes = flowlib.render_acxd_flow(cfg.language, messages, cfg.name_prefix)
            body = build_flow_body(cfg, nodes)
            upsert_acxd_flow(client, cfg, body, writer)
            if cfg.reuse_application:
                app_id, app_name = reuse_application(client, cfg, body["flowId"], writer)
            else:
                app_id = upsert_application(client, cfg, build_application_body(cfg, messages), writer)
                app_name = config.acxd_app_name(cfg.name_prefix)
                writer.update("acxd", applicationReused=False, applicationName=app_name)
            build_id = create_build_and_poll(client, cfg, app_id, writer)
            break
        except LanguageRejectedError as exc:
            if i + 1 >= len(attempts):
                raise DeployError(f"{exc}{_language_hint(cfg)}{_fallback_draft_note(cfg, i)}", 1) from exc
            nxt = attempts[i + 1]
            logger.warning("ACXD rejected language %s (%s); retrying flow / application / build with %s",
                           cfg.language, exc, nxt)
            print(f"\nACXD 不接受 {cfg.language}，已自动改用 {nxt} 重新部署 flow / 应用 / build"
                  "（未显式指定 --language，见假设 A11 / A21）")
            cfg.language_fallback_from = cfg.language_fallback_from or cfg.language
        except DeployError as exc:
            if i == 0:
                raise
            # A non-language failure during the fallback attempt.
            raise DeployError(f"{exc}{_fallback_draft_note(cfg, i)}", exc.code) from exc
    # Only a BUILT build reaches the deployment, so no deployment uses a rejected language.
    writer.update("acxd", language=cfg.language, languageFallbackFrom=cfg.language_fallback_from)
    try:
        alias = upsert_deployment_and_poll(client, cfg, app_id, build_id, writer)
    except LanguageRejectedError as exc:
        raise DeployError(f"{exc}{_language_hint(cfg)}", 1) from exc
    return app_id, alias, app_name


def _language_hint(cfg: Config) -> str:
    return (f"；可改用 --language {config.FALLBACK_LANGUAGE}"
            if cfg.language != config.FALLBACK_LANGUAGE else "")


def _fallback_draft_note(cfg: Config, attempt: int) -> str:
    """The fallback attempt may already have PATCHed the ACXD flow / application (not in deploy_state.json)."""
    if attempt == 0:
        return ""
    return (f"；注意：自动回退已把 ACXD flow {config.acxd_flow_id(cfg.name_prefix)} / 应用草稿改为 {cfg.language}（线上 deployment "
            "未变，deploy_state.json 未记录此语言），下次运行会按所选语言重新写入")


# ---------------------------------------------------------------------------
# Connect phase (§6.4 steps 6-11)
# ---------------------------------------------------------------------------

def make_connect_client(region: str):
    return boto3.client("connect", region_name=region,
                        config=botocore.config.Config(retries={"mode": "standard", "max_attempts": 5}))


def install_connect_read_only_guard(client):
    """--preflight: make every Connect operation other than Describe*/List*/Get*/Search* raise.

    The handler runs on before-call, i.e. after parameter validation and before any request is
    sent.  It is registered first on 'before-call.*.*' (the client's emitter only serves this
    connect client) so no other before-call handler that returns a response - for example
    botocore's Stubber - can short-circuit it.
    """
    def guard(model, params, request_signer, context, **kwargs):
        if not model.name.startswith(CONNECT_READ_PREFIXES):
            raise RuntimeError(f"preflight is read-only: {model.name}")

    client.meta.events.register_first("before-call.*.*", guard, unique_id="deploy-preflight-read-only-guard")
    return client


def _err_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def resolve_queue(client, cfg: Config):
    """Returns (queue_arn, queue_name, source): --queue-arn > --queue-name > BasicQueue > first STANDARD queue."""
    if cfg.queue_arn:
        queue_id = QUEUE_ARN_RE.match(cfg.queue_arn).group(2)
        try:
            q = client.describe_queue(InstanceId=cfg.instance_id, QueueId=queue_id)["Queue"]
        except ClientError as exc:
            if _err_code(exc) == "ResourceNotFoundException":
                raise ConfigError(f"队列不存在: {cfg.queue_arn}") from exc
            raise
        return cfg.queue_arn, q.get("Name") or queue_id, "--queue-arn"
    queues = []
    paginator = client.get_paginator("list_queues")
    for page in paginator.paginate(InstanceId=cfg.instance_id, QueueTypes=["STANDARD"]):
        queues.extend(q for q in page.get("QueueSummaryList", []) if q.get("Arn") and q.get("Name"))
    if cfg.queue_name:
        matches = [q for q in queues if q["Name"] == cfg.queue_name]
        if len(matches) != 1:
            raise ConfigError(f"按名称 {cfg.queue_name!r} 找到 {len(matches)} 个 STANDARD 队列，需要恰好 1 个")
        return matches[0]["Arn"], matches[0]["Name"], "--queue-name"
    default = [q for q in queues if q["Name"] == config.DEFAULT_QUEUE_NAME]
    if default:
        return default[0]["Arn"], default[0]["Name"], f"default:{config.DEFAULT_QUEUE_NAME}"
    if not queues:
        raise ConfigError("实例中没有任何 STANDARD 队列，请用 --queue-arn / --queue-name 指定目标队列")
    # ListQueues has no guaranteed order; sorting keeps the choice deterministic.
    first = sorted(queues, key=lambda q: q["Name"])[0]
    logger.warning("queue %s not found; using the first STANDARD queue by name: %s (override with --queue-name)",
                   config.DEFAULT_QUEUE_NAME, first["Name"])
    return first["Arn"], first["Name"], "default:first-standard"


def connect_prechecks(client, cfg: Config) -> ConnectTarget:
    """Read-only Connect checks run before any ACXD or Connect write (DescribeInstance + queue)."""
    try:
        inst = client.describe_instance(InstanceId=cfg.instance_id).get("Instance") or {}
    except ClientError as exc:
        if _err_code(exc) == "ResourceNotFoundException":
            raise ConfigError(f"Connect 实例不存在: {cfg.instance_id}") from exc
        raise
    arn = inst.get("Arn")
    if cfg.instance_arn and arn and arn != cfg.instance_arn:
        raise ConfigError(f"DescribeInstance 返回的实例 ARN {arn} 与 --instance-arn {cfg.instance_arn} 不一致"
                          "（AWS 凭证所属账号 / 区域与 ARN 不符？）")
    if arn and not cfg.instance_arn:
        cfg.instance_arn = arn
        m = INSTANCE_ARN_RE.match(arn)
        cfg.account_id = cfg.account_id or (m.group(3) if m else None)
    queue_arn, queue_name, source = resolve_queue(client, cfg)
    logger.info("queue %s (%s), source: %s", queue_name, queue_arn, source)
    # Foreign same-name contact flows stop the run here, before any ACXD or Connect write.
    existing = precheck_contact_flows(client, cfg)
    return ConnectTarget(instance_arn=cfg.instance_arn, instance_alias=inst.get("InstanceAlias"),
                         access_url=(inst.get("InstanceAccessUrl") or "").rstrip("/") or None,
                         queue_arn=queue_arn, queue_name=queue_name, queue_source=source,
                         existing_flows=existing)


def resolve_application_id(cfg: Config, state: dict | None) -> str | None:
    """§6.3.1, only for --phase connect alone.  Never changes the exit code."""
    if cfg.application_id:
        logger.info("applicationId source: --acxd-application-id")
        return cfg.application_id
    name = config.acxd_app_name(cfg.name_prefix)
    if cfg.api_key and cfg.workspace_id:
        try:
            client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, cfg.workspace_id, sleep=lambda s: time.sleep(s),
                                read_only=cfg.preflight)
            matches = [a for a in client.list_applications() if a.get("name") == name]
            if len(matches) == 1 and matches[0].get("applicationId"):
                logger.info("applicationId source: ACXD ListApplications")
                return matches[0]["applicationId"]
            logger.warning("ACXD name lookup found %d applications named %s", len(matches), name)
        except Exception as exc:  # noqa: BLE001 - optional lookup must never fail the run
            logger.warning("ACXD name lookup ListApplications failed: status=%s type=%s",
                           getattr(exc, "status", None), getattr(exc, "error_type", type(exc).__name__))
    elif cfg.api_key or cfg.workspace_id:
        logger.info("跳过 ACXD 名称查询：需要同时提供 ACXD_API_KEY 与 --acxd-workspace-id")
    app_id = ((state or {}).get("acxd") or {}).get("applicationId")
    if app_id:
        logger.info("applicationId source: deploy_state.json")
        return app_id
    logger.warning("ACXD applicationId unknown; skipping applicationId checks of the Agentic CX action")
    return None


def resolve_deployment_alias(cfg: Config, app_id: str | None, state: dict | None) -> str | None:
    """A18: deploymentAlias of cfg.environment, only for --phase connect alone.  Never changes the exit code."""
    if cfg.deployment_alias:
        logger.info("deploymentAlias source: --acxd-deployment-alias")
        return cfg.deployment_alias
    if cfg.api_key and cfg.workspace_id and app_id:
        try:
            client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, cfg.workspace_id, sleep=lambda s: time.sleep(s),
                                read_only=cfg.preflight)
            for d in client.list_deployments(app_id):
                if d.get("environment") == cfg.environment and d.get("deploymentAlias"):
                    logger.info("deploymentAlias source: ACXD ListApplicationDeployments")
                    return d["deploymentAlias"]
            logger.warning("ACXD lookup found no deployment with a deploymentAlias for environment %s",
                           cfg.environment)
        except Exception as exc:  # noqa: BLE001 - optional lookup must never fail the run
            logger.warning("ACXD lookup ListApplicationDeployments failed: status=%s type=%s",
                           getattr(exc, "status", None), getattr(exc, "error_type", type(exc).__name__))
    alias = ((state or {}).get("acxd") or {}).get("deploymentAlias")
    if alias:
        logger.info("deploymentAlias source: deploy_state.json")
        return alias
    logger.warning("ACXD deploymentAlias unknown; the Agentic CX action cannot be generated")
    return None


def _list_flows_by_name(client, cfg: Config, name: str):
    paginator = client.get_paginator("list_contact_flows")
    for page in paginator.paginate(InstanceId=cfg.instance_id, ContactFlowTypes=["CONTACT_FLOW"]):
        for s in page.get("ContactFlowSummaryList", []):
            if s.get("Name") == name:
                return s
    return None


def resolve_agentic_cx_action(client, cfg: Config, app_id, alias, app_name=None):
    """Priority file > reference-flow > generated > existing-flow > unverified-fallback.

    Read-only Connect calls only.  Returns (action, meta, source).
    """
    action = meta = None
    if cfg.action_obj is not None:
        action, meta = _select(cfg.action_obj, app_id, cfg.action_identifier)
        source = "file"
    elif cfg.reference_flow_id:
        try:
            resp = client.describe_contact_flow(InstanceId=cfg.instance_id, ContactFlowId=cfg.reference_flow_id)
        except ClientError as exc:
            if _err_code(exc) in ("ResourceNotFoundException", "ContactFlowNotPublishedException"):
                raise ConfigError(f"参考流 {cfg.reference_flow_id} 不存在或未发布（{_err_code(exc)}），"
                                  "请发布参考流或改用 --agentic-cx-action-file") from exc
            raise
        try:
            content = json.loads(resp["ContactFlow"]["Content"])
        except (KeyError, ValueError) as exc:
            raise ConfigError("参考流 Content 不是合法 JSON") from exc
        action, meta = _select(content, app_id, cfg.action_identifier)
        source = "reference-flow"
    elif cfg.workspace_id and app_id and alias:
        action, meta = _generated_action(cfg, app_id, alias, app_name)
        source = "generated"
    else:
        source = None
        summary = _list_flows_by_name(client, cfg, config.inbound_flow_name(cfg.name_prefix))
        if summary is not None:
            resp = client.describe_contact_flow(InstanceId=cfg.instance_id, ContactFlowId=summary["Id"])
            try:
                content = json.loads(resp["ContactFlow"]["Content"])
            except (KeyError, ValueError):
                content = {}
            for a in content.get("Actions", []):
                if (a.get("Identifier") == config.AGENTIC_CX_ACTION_ID
                        and a.get("Type") not in (config.UNVERIFIED_ACTION_TYPE, flowlib.MARKER)):
                    action = a
                    am = ((content.get("Metadata") or {}).get("ActionMetadata") or {}).get(a["Identifier"])
                    if isinstance(am, dict):
                        meta = {k: v for k, v in am.items() if k != "position"} or None
                    source = "existing-flow"
                    break
        if action is None:
            action, meta = flowlib.load_fallback_action(), None
            source = "unverified-fallback"
    logger.info("Agentic CX action source: %s", source)

    if source == "unverified-fallback" and not cfg.allow_unverified:
        print(reference_flow_instructions(cfg, alias))
        raise ConfigError("拒绝在非 dry-run 中使用未验证的 Agentic CX action（未提供 action 来源）")
    action, meta = _prepare_action(action, meta, app_id, [alias], source)
    return action, meta, source


def upsert_contact_flow(client, cfg: Config, name: str, content: dict, description: str, path: Path,
                        unverified: bool = False, verified_ids=frozenset()):
    """verified_ids: flow ids whose ownership precheck_contact_flows already confirmed in this run;
    any other existing flow (created since the precheck) is checked before it is updated."""
    content_str = json.dumps(content, ensure_ascii=False)

    def update(summary):
        if summary["Id"] not in verified_ids and check_contact_flow_owned(client, cfg, summary) == APP_MISSING:
            raise DeployError(f"Connect 流 {name} 在检查期间被删除，请重跑", 1)
        client.update_contact_flow_content(InstanceId=cfg.instance_id, ContactFlowId=summary["Id"],
                                           Content=content_str)
        logger.info("updated contact flow %s (%s)", name, summary["Id"])
        return summary["Id"], summary["Arn"]

    try:
        found = _list_flows_by_name(client, cfg, name)
        if found is not None:
            return update(found)
        try:
            # The tag marks the flow as created by this script (ownership guard of later runs).
            resp = client.create_contact_flow(InstanceId=cfg.instance_id, Name=name, Type="CONTACT_FLOW",
                                              Description=description, Content=content_str, Status="PUBLISHED",
                                              Tags=dict(config.CONNECT_OWNER_TAGS))
            logger.info("created contact flow %s (%s)", name, resp["ContactFlowId"])
            return resp["ContactFlowId"], resp["ContactFlowArn"]
        except ClientError as exc:
            if _err_code(exc) != "DuplicateResourceException":
                raise
            logger.warning("contact flow %s was created concurrently; updating", name)
            found = _list_flows_by_name(client, cfg, name)
            if found is None:
                raise
            return update(found)
    except ClientError as exc:
        if _err_code(exc) == "InvalidContactFlowException":
            for prob in exc.response.get("problems") or []:
                logger.error("InvalidContactFlowException: %s", prob.get("message"))
            logger.error("rendered flow JSON: %s", path)
            if unverified:
                logger.error("the unverified Agentic CX fallback fragment was used (A1); provide a real action "
                             "via --agentic-cx-reference-flow-id or --agentic-cx-action-file")
            raise DeployError(f"Connect 拒绝了流 {name}", 1) from exc
        raise


def _record_target(writer: StateWriter, target: ConnectTarget) -> None:
    writer.update("connect", instanceArn=target.instance_arn, instanceAlias=target.instance_alias,
                  instanceAccessUrl=target.access_url, queueArn=target.queue_arn, queueName=target.queue_name)


def run_connect_phase(cfg: Config, writer: StateWriter, messages: dict, base_state: dict | None,
                      app_id: str | None, alias: str | None, app_name: str | None = None,
                      client=None, target: ConnectTarget | None = None) -> ConnectTarget:
    if client is None:
        client = make_connect_client(cfg.region)
    if target is None:
        target = connect_prechecks(client, cfg)
    _record_target(writer, target)
    queue_arn, queue_display = target.queue_arn, target.queue_name

    if cfg.phase == "connect":
        app_id = resolve_application_id(cfg, base_state)
        alias = resolve_deployment_alias(cfg, app_id, base_state)
        app_name = ((base_state or {}).get("acxd") or {}).get("applicationName") or config.acxd_app_name(
            cfg.name_prefix)
    action, meta, source = resolve_agentic_cx_action(client, cfg, app_id, alias, app_name)
    writer.update("connect", queueArn=queue_arn, agenticCxActionSource=source)

    # Step 9: first Connect write.
    sp_name = config.screenpop_flow_name(cfg.name_prefix)
    screenpop = flowlib.render_screenpop_flow(cfg.language, messages, cfg.region, sp_name)
    sp_path = write_artifact(cfg.out_dir, "screenpop_flow.json", screenpop, cfg.api_key)
    verified = frozenset(s["Id"] for s, _ in target.existing_flows.values())
    sp_id, sp_arn = upsert_contact_flow(client, cfg, sp_name, screenpop, screenpop["Metadata"]["description"],
                                        sp_path, verified_ids=verified)
    writer.update("connect", screenpopFlowId=sp_id, screenpopFlowArn=sp_arn)
    writer.step("upsert_screenpop_flow")

    # Step 10.
    in_name = config.inbound_flow_name(cfg.name_prefix)
    inbound = flowlib.render_inbound_flow(inbound_ctx(cfg, messages, sp_arn, queue_arn, queue_display), action, meta)
    in_path = write_artifact(cfg.out_dir, "inbound_acxd_flow.json", inbound, cfg.api_key)
    in_id, in_arn = upsert_contact_flow(client, cfg, in_name, inbound, inbound["Metadata"]["description"], in_path,
                                        unverified=(source == "unverified-fallback"), verified_ids=verified)
    writer.update("connect", inboundFlowId=in_id, inboundFlowArn=in_arn)
    writer.step("upsert_inbound_flow")
    return target


def print_next_steps(cfg: Config, state: dict) -> None:
    in_id = (state.get("connect") or {}).get("inboundFlowId") or "<入站流ID>"
    print("\n关联电话号码示例命令：")
    print(f"  aws connect associate-phone-number-contact-flow --region {cfg.region} "
          f"--instance-id {cfg.instance_id} --phone-number-id <电话号码ID> --contact-flow-id {in_id}")
    print("\n真实环境验证步骤：")
    for line in VERIFY_STEPS:
        print("  " + line)


def print_summary(cfg: Config, state: dict, target: ConnectTarget | None, header: str) -> None:
    """D10: final summary with console links (Chinese labels).  Never contains the API key."""
    a, c = state.get("acxd") or {}, state.get("connect") or {}
    access_url = (target.access_url if target else None) or c.get("instanceAccessUrl")
    lines = [f"\n{header}"]
    inst_arn = (target.instance_arn if target else None) or c.get("instanceArn") or cfg.instance_arn
    alias = (target.instance_alias if target else None) or c.get("instanceAlias")
    if cfg.instance_id or inst_arn:
        extra = "，".join(x for x in (f"alias {alias}" if alias else None, f"区域 {cfg.region}") if x)
        lines.append(f"  Connect 实例:   {inst_arn or cfg.instance_id}（{extra}）")
    if cfg.workspace_id:
        name = cfg.workspace_name or a.get("workspaceName")
        ws = f"{name} ({cfg.workspace_id})" if name else cfg.workspace_id
        lines.append(f"  ACXD 工作区:    {ws}，来源 {cfg.workspace_source or a.get('workspaceSource') or '-'}")
    if cfg.acxd_phase or a.get("applicationId"):
        app_name = a.get("applicationName") or config.acxd_app_name(cfg.name_prefix)
        lines.append(f"  ACXD 应用:      {app_name} ({a.get('applicationId') or '待创建'})")
        lines.append(f"  ACXD flow:      {a.get('flowId') or config.acxd_flow_id(cfg.name_prefix)}")
        if a.get("buildId"):
            lines.append(f"  build:          {a.get('buildId')}（version {a.get('buildVersion')}）")
        lines.append(f"  部署:           environment={cfg.environment}，deploymentAlias="
                     f"{a.get('deploymentAlias') or '待创建'}")
    lang = cfg.language
    if cfg.language_fallback_from:
        lang = f"{cfg.language_fallback_from} → {cfg.language}（自动回退）"
    lines.append(f"  语言:           {lang}")
    queue_arn = (target.queue_arn if target else None) or c.get("queueArn")
    if queue_arn:
        q_name = (target.queue_name if target else None) or c.get("queueName")
        q_src = f"，来源 {target.queue_source}" if target else ""
        lines.append(f"  队列:           {q_name}  {queue_arn}{q_src}")
    for label, key in (("screenpop 流", "screenpop"), ("入站流", "inbound")):
        name = (config.screenpop_flow_name(cfg.name_prefix) if key == "screenpop"
                else config.inbound_flow_name(cfg.name_prefix))
        fid, farn = c.get(f"{key}FlowId"), c.get(f"{key}FlowArn")
        if fid or farn:
            lines.append(f"  {label}: {name}  id={fid}  arn={farn}")
    if access_url:
        lines.append("  控制台链接：")
        lines.append(f"    Connect 管理网站: {access_url}")
        if cfg.workspace_id and a.get("applicationId"):
            lines.append("    ACXD 应用:        " + config.ACXD_CONSOLE_APP_URL.format(
                access_url=access_url, workspace_id=cfg.workspace_id, application_id=a["applicationId"]))
        for label, key in (("screenpop 流编辑", "screenpop"), ("入站流编辑", "inbound")):
            if c.get(f"{key}FlowArn"):
                lines.append(f"    {label}（控制台链接，未经 API 验证）: " + config.CONNECT_FLOW_EDIT_URL.format(
                    access_url=access_url, flow_arn=c[f"{key}FlowArn"]))
    text = "\n".join(lines)
    flowlib.assert_no_secret(text, cfg.api_key)
    print(text)


VERIFY_STEPS = (
    "1. 确认 ACXD build 为 BUILT、deployment 为 deployed（languageSettings 被拒时按 A14 调整 config.LANGUAGE_SETTING_EXTRA）。",
    "2. 在 ACXD 控制台打开 ACXD flow 画布检查连线，并用应用 Test 标签页测试，确认三个上下文变量被写入。",
    "3. 确认日志中 Agentic CX action source 为 generated，且 applicationId / deploymentAlias 来源正确"
    "（单独运行 connect 阶段时用 --acxd-workspace-id，必要时加 --acxd-application-id / --acxd-deployment-alias）。",
    "4. 在 Connect 控制台打开入站流，确认 Agentic CX 块显示所选工作区 / 应用 / 环境，并核对日志里的分支映射。",
    "5. 把电话号码关联到入站流并拨测，确认 ACXD 提示语以所选语言播报（A17）。",
    "6. 座席接起后 agent workspace 弹出 Detail 视图，显示客户姓名 / 电话 / 邮箱与设备三项（已有客户资料的号码显示资料中的姓名和邮箱，"
    "未知号码显示未找到和来电号码，A22）；若设备属性为空，按 A20 设置 sendContext 后重跑 --phase acxd。",
    "7. 用聊天渠道输入 <b>x</b> 检查弹屏是否原样显示；若被渲染，把 config.SCREENPOP_SECTIONS_SHOW_VALUES 设为 False 后重跑 --phase connect（A19）。",
    "8. 在 CloudWatch 流日志中确认 Error 分支未被触发。",
)


# ---------------------------------------------------------------------------
# Preflight (read-only against the real environment)
# ---------------------------------------------------------------------------

class _Plan:
    """Write operations a real run would perform, plus warnings (preflight_report.json)."""

    def __init__(self):
        self.writes = []
        self.warnings = []
        self.prefix_marks = {}  # prefix_resource_names key -> 将更新（覆盖） / 将新建

    def mark(self, key: str, exists: bool) -> None:
        self.prefix_marks[key] = PREFIX_MARK_UPDATE if exists else PREFIX_MARK_CREATE

    def write(self, service: str, operation: str, target: str) -> None:
        logger.info("preflight: planned %s %s %s", service, operation, target)
        self.writes.append({"service": service, "operation": operation, "target": target})

    def warn(self, message: str) -> None:
        logger.warning("preflight: %s", message)
        self.warnings.append(message)


def _acxd_read(fn, op: str):
    try:
        return fn()
    except AcxdApiError as exc:
        _acxd_fail(exc, op)


def _preflight_acxd(cfg: Config, messages: dict, plan: _Plan):
    """ACXD part of --preflight (GET only).  Returns (app_id, app_name, alias); unknown values are None."""
    out, secret = cfg.out_dir, cfg.api_key
    logger.info("ACXD endpoint %s, workspace %s, key %s (read-only)", cfg.acxd_endpoint, cfg.workspace_id,
                mask_key(cfg.api_key))
    client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, cfg.workspace_id, sleep=lambda s: time.sleep(s),
                        read_only=True)
    existing_app = _get_reuse_application(client, cfg) if cfg.reuse_application else None

    bodies = build_context_variable_bodies()
    path = write_artifact(out, "acxd_context_variables.json", bodies, secret)
    by_name = {i.get("name"): i for i in _list_context_variables(client, path)}
    for body in bodies:
        name = body["name"]
        if name not in by_name:
            plan.write("acxd", "CreateContextVariable", name)
            continue
        got = (by_name[name].get("schema") or {}).get("type")
        if got != body["schema"]["type"]:
            plan.warn(f"context variable {name} exists with schema.type={got} (expected "
                      f"{body['schema']['type']}); a real run does not modify it")

    flow_body = build_flow_body(cfg, flowlib.render_acxd_flow(cfg.language, messages, cfg.name_prefix))
    write_artifact(out, "acxd_flow.json", flow_body, secret)
    flow_id = flow_body["flowId"]
    flow_exists = any(f.get("flowId") == flow_id for f in _acxd_read(client.list_flows, "ListFlows"))
    if flow_exists:
        # Foreign flow with the derived flowId -> ConfigError here, exactly as in a real run.
        owner = check_acxd_flow_owned(client, cfg, flow_id)
        if owner == APP_FORCED:
            plan.warn(f"ACXD flow {flow_id} was not created by this script; a real run overwrites it because "
                      f"of {OVERWRITE_FLAG}")
        elif owner == APP_MISSING:
            flow_exists = False
    plan.write("acxd", "UpdateFlow" if flow_exists else "CreateFlow", flow_id)
    plan.mark("flow", flow_exists)

    if existing_app is not None:
        app_id, app_name = cfg.application_id, existing_app.get("name")
        write_artifact(out, "acxd_application_before.json", redact(existing_app), secret)
        write_artifact(out, "acxd_application.json", build_reuse_application_patch(existing_app, flow_id), secret)
        welcome_key = config.DEFAULT_FLOWS_WIRE_NAMES["welcome"]
        previous = (((existing_app.get("settings") or {}).get("defaultFlows") or {}).get(welcome_key)
                    or {}).get("flowId")
        plan.write("acxd", "UpdateApplication", f"{app_name} ({app_id})")
        plan.warn(f"reuse mode changes the existing application {app_name} ({app_id}): flow {flow_id} is "
                  f"appended, {welcome_key} {previous} -> {flow_id}, and its {cfg.environment} deployment "
                  "will be replaced")
    else:
        app_name = config.acxd_app_name(cfg.name_prefix)
        write_artifact(out, "acxd_application.json", build_application_body(cfg, messages), secret)
        matches = [a for a in _acxd_read(client.list_applications, "ListApplications") if a.get("name") == app_name]
        if len(matches) > 1:
            ids = ", ".join(str(a.get("applicationId")) for a in matches)
            logger.error("multiple ACXD applications named %s: %s", app_name, ids)
            raise DeployError(f"存在多个同名应用 {app_name}（{ids}），请改用 --name-prefix", 1)
        app_id = matches[0].get("applicationId") if matches else None
        plan.mark("app", bool(matches))
        if app_id:
            # Untagged same-name application -> ConfigError here, exactly as in a real run.
            owner = check_application_owned(client, cfg, app_id, app_name)
            if owner == APP_FORCED:
                plan.warn(f"application {app_name} ({app_id}) was not created by this script; a real run "
                          f"overwrites it because of {OVERWRITE_FLAG}")
            elif owner == APP_MISSING:
                app_id = None  # deleted between ListApplications and GetApplication
                plan.mark("app", False)
        if app_id:
            plan.write("acxd", "UpdateApplication", f"{app_name} ({app_id})")
        else:
            plan.write("acxd", "CreateApplication", app_name)

    app_target = f"{app_name} ({app_id})" if app_id else app_name
    write_artifact(out, "acxd_build.json", build_build_body(cfg), secret)
    plan.write("acxd", "CreateApplicationBuild", app_target)
    write_artifact(out, "acxd_deployment.json", build_deployment_body(cfg, PREFLIGHT_BUILD_PENDING), secret)
    dep = None
    if app_id:
        deps = _acxd_read(lambda: client.list_deployments(app_id), "ListApplicationDeployments")
        dep = next((d for d in deps if d.get("environment") == cfg.environment), None)
    alias = None
    if dep is not None:
        alias = dep.get("deploymentAlias") or None
        plan.write("acxd", "UpdateApplicationDeployment",
                   f"{cfg.environment} (deploymentId={dep.get('deploymentId')}, current deploymentAlias={alias})")
    else:
        plan.write("acxd", "CreateApplicationDeployment", f"{cfg.environment} ({app_target})")
    return app_id, app_name, alias


def _preflight_connect(cfg: Config, messages: dict, plan: _Plan, resolved: dict, acxd_result, base,
                       client, target: ConnectTarget) -> None:
    """Connect part of --preflight (Describe/List/Get/Search only, enforced by the guard)."""
    out, secret = cfg.out_dir, cfg.api_key
    queue_arn, queue_display = target.queue_arn, target.queue_name

    sp_name, in_name = config.screenpop_flow_name(cfg.name_prefix), config.inbound_flow_name(cfg.name_prefix)
    # connect_prechecks already listed the flows and checked their ownership (foreign -> exit 2).
    existing = target.existing_flows
    sp = existing[sp_name][0] if sp_name in existing else None
    inb = existing[in_name][0] if in_name in existing else None
    plan.mark("screenpop", sp is not None)
    plan.mark("inbound", inb is not None)
    for name, (summary, status) in existing.items():
        if status == APP_FORCED:
            plan.warn(f"contact flow {name} ({summary['Id']}) was not created by this script; a real run "
                      f"overwrites it because of {OVERWRITE_FLAG}")
    for name, found in ((sp_name, sp), (in_name, inb)):
        if found is not None:
            plan.write("connect", "UpdateContactFlowContent", f"{name} ({found['Id']})")
        else:
            plan.write("connect", "CreateContactFlow", name)

    if cfg.acxd_phase:
        app_id, app_name, alias = acxd_result
        if cfg.action_obj is None and not cfg.reference_flow_id:
            # The application / deployment is created by this run: render with explicit placeholders.
            pending = []
            if not app_id:
                app_id = PREFLIGHT_APPLICATION_PENDING
                pending.append("applicationId")
            if not alias:
                alias = PREFLIGHT_ALIAS_PENDING
                pending.append("deploymentAlias")
            if pending:
                plan.warn(f"{' and '.join(pending)} will only exist after the ACXD phase of a real run; the "
                          f"rendered Agentic CX action uses {app_id} / {alias} and a real run substitutes the "
                          "values returned by ACXD")
    else:
        app_id = resolve_application_id(cfg, base)
        alias = resolve_deployment_alias(cfg, app_id, base)
        app_name = ((base or {}).get("acxd") or {}).get("applicationName") or config.acxd_app_name(cfg.name_prefix)
        resolved.update(applicationId=app_id, applicationName=app_name, deploymentAlias=alias)
    action, meta, source = resolve_agentic_cx_action(client, cfg, app_id, alias, app_name)

    sp_arn = sp["Arn"] if sp is not None else queue_arn.split("/queue/")[0] + "/contact-flow/" + PREFLIGHT_SCREENPOP_ID
    if sp is None:
        plan.warn(f"screenpop flow {sp_name} does not exist yet; the inbound flow is rendered with the "
                  f"placeholder ARN {sp_arn} and a real run uses the ARN returned by CreateContactFlow")
    screenpop = flowlib.render_screenpop_flow(cfg.language, messages, cfg.region, sp_name)
    inbound = flowlib.render_inbound_flow(inbound_ctx(cfg, messages, sp_arn, queue_arn, queue_display), action, meta)
    flowlib.validate_connect_flow(screenpop)
    flowlib.validate_connect_flow(inbound)
    write_artifact(out, "screenpop_flow.json", screenpop, secret)
    write_artifact(out, "inbound_acxd_flow.json", inbound, secret)
    resolved.update(queueArn=queue_arn, queueName=queue_display, queueSource=target.queue_source,
                    instanceArn=target.instance_arn, instanceAlias=target.instance_alias,
                    instanceAccessUrl=target.access_url,
                    screenpopFlowId=sp["Id"] if sp else None, screenpopFlowArn=sp["Arn"] if sp else None,
                    inboundFlowId=inb["Id"] if inb else None, inboundFlowArn=inb["Arn"] if inb else None,
                    agenticCxActionSource=source)


def run_preflight(cfg: Config) -> int:
    """--preflight: GET/List/Describe only; never writes deploy_state.json or dry-run/."""
    out = cfg.out_dir
    try:
        out.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"无法创建输出目录 {out}: {exc}") from exc
    messages = flowlib.load_messages()
    plan = _Plan()
    base = filter_state(load_state(cfg.output_dir / "deploy_state.json"), cfg, check_workspace=False)
    _resolve_workspace_for_phase(cfg, base, read_only=True)
    base = filter_state(base, cfg, check_workspace=True)
    if cfg.phase == "connect":
        _language_from_state(cfg, base, messages)
    resolved = {k: None for k in ("instanceId", "queueArn", "workspaceId", "applicationId", "applicationName",
                                  "deploymentAlias", "screenpopFlowId", "inboundFlowId", "agenticCxActionSource")}
    resolved.update(instanceId=cfg.instance_id, workspaceId=cfg.workspace_id, workspaceName=cfg.workspace_name,
                    workspaceSource=cfg.workspace_source, language=cfg.language)
    target = None
    acxd_result = None
    if cfg.acxd_phase:
        if len(_language_attempts(cfg)) > 1:
            plan.warn(f"language {cfg.language} is not verified (A11); if ACXD rejects it for language reasons a "
                      f"real run automatically retries the flow / application / build with "
                      f"{config.FALLBACK_LANGUAGE} (A21)")
        acxd_result = _preflight_acxd(cfg, messages, plan)
        resolved.update(applicationId=acxd_result[0], applicationName=acxd_result[1], deploymentAlias=acxd_result[2])
    if cfg.connect_phase:
        # Everything here is read-only, so the ACXD checks may run first (a real run does the
        # Connect prechecks first because its ACXD part writes).
        client = install_connect_read_only_guard(make_connect_client(cfg.region))
        target = connect_prechecks(client, cfg)
        _preflight_connect(cfg, messages, plan, resolved, acxd_result, base, client, target)
    report = {
        "schemaVersion": 1,
        "preflight": True,
        "generatedAt": _utc_now(),
        "region": cfg.region,
        "resolved": resolved,
        "plannedWrites": plan.writes,
        "warnings": plan.warnings,
    }
    write_artifact(out, "preflight_report.json", report, cfg.api_key)
    print(f"\npreflight 完成（只读：只调用了 GET/List/Describe，没有执行任何写操作），产物已写入 {out}/")
    print("真实运行将执行以下写操作：")
    for i, w in enumerate(plan.writes, 1):
        print(f"  {i}. [{w['service']}] {w['operation']}  {w['target']}")
    print(format_name_prefix(cfg, plan.prefix_marks))
    if plan.warnings:
        print("警告：")
        for w in plan.warnings:
            print("  - " + w)
    summary_state = {
        "acxd": {"applicationId": resolved.get("applicationId"), "applicationName": resolved.get("applicationName"),
                 "deploymentAlias": resolved.get("deploymentAlias"), "workspaceName": cfg.workspace_name},
        "connect": {"screenpopFlowId": resolved.get("screenpopFlowId"),
                    "screenpopFlowArn": resolved.get("screenpopFlowArn"),
                    "inboundFlowId": resolved.get("inboundFlowId"), "inboundFlowArn": resolved.get("inboundFlowArn")},
    }
    print_summary(cfg, summary_state, target, "预检摘要（真实运行前的解析结果，尚未写入任何资源）：")
    return 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _connect_failure(exc) -> DeployError:
    if isinstance(exc, NoCredentialsError):
        logger.error("no AWS credentials; required permissions: %s", ", ".join(config.REQUIRED_CONNECT_PERMISSIONS))
        return DeployError("缺少 AWS 凭证", 1)
    code = _err_code(exc)
    if "AccessDenied" in code:
        logger.error("Connect access denied (%s); required permissions: %s", code,
                     ", ".join(config.REQUIRED_CONNECT_PERMISSIONS))
    else:
        logger.error("Connect API error %s: %s", code, exc)
    return DeployError(f"Connect 调用失败: {code}", 1)


ACXD_MODEL_LINE = f"  ACXD 模型:      {config.GENERATIVE_MODEL_TYPE}（Generative Journey / Generative Text）"


def format_run_parameters(cfg: Config, target: ConnectTarget | None, marks: dict | None = None) -> str:
    """Resolved parameters of a real run (Chinese labels, only the lines relevant to the phase).
    The API key appears only masked.  marks: prefix_resource_marks() result (may be empty)."""
    lines = [f"  阶段:           {cfg.phase}", f"  区域:           {cfg.region}"]
    if cfg.connect_phase:
        inst = (target.instance_arn if target else None) or cfg.instance_arn or cfg.instance_id
        alias = target.instance_alias if target else None
        lines.append(f"  Connect 实例:   {inst}" + (f"（alias {alias}）" if alias else ""))
        if target:
            lines.append(f"  队列:           {target.queue_name}  {target.queue_arn}，来源 {target.queue_source}")
        else:
            lines.append("  队列:           " + (cfg.queue_arn or cfg.queue_name
                                                 or f"自动（{config.DEFAULT_QUEUE_NAME} → 第一个 STANDARD 队列）"))
    if cfg.acxd_phase or cfg.workspace_id:
        lines.append(f"  ACXD endpoint:  {cfg.acxd_endpoint}")
        if cfg.workspace_id:
            ws = f"{cfg.workspace_name} ({cfg.workspace_id})" if cfg.workspace_name else cfg.workspace_id
            lines.append(f"  ACXD 工作区:    {ws}，来源 {cfg.workspace_source or '-'}")
        else:
            lines.append("  ACXD 工作区:    未指定")
    if cfg.acxd_phase:
        app = (f"复用 {cfg.application_id}" if cfg.reuse_application
               else f"{config.acxd_app_name(cfg.name_prefix)}（新建或按名称更新）")
        lines.append(f"  ACXD 应用:      {app}")
        lines.append(ACXD_MODEL_LINE)
    if cfg.acxd_phase or not cfg.deployment_alias:
        lines.append(f"  部署环境:       {cfg.environment}")
    if cfg.connect_phase and not cfg.acxd_phase and cfg.deployment_alias:
        lines.append(f"  deploymentAlias: {cfg.deployment_alias}")
    lang = cfg.language
    if cfg.acxd_phase and len(_language_attempts(cfg)) > 1:
        lang += f"（ACXD 以语言原因拒绝时自动改用 {config.FALLBACK_LANGUAGE}）"
    lines.append(f"  语言:           {lang}")
    lines.append(f"  资源名前缀:     {cfg.name_prefix}（来源 {NAME_PREFIX_SOURCE_LABELS[cfg.name_prefix_source]}）")
    lines.append(format_prefix_names(cfg.name_prefix, marks, cfg.reuse_application, indent="    ",
                                     keys=_phase_prefix_keys(cfg)))
    lines.append(f"  输出目录:       {cfg.out_dir}")
    key = f"{mask_key(cfg.api_key)}（来源 {cfg.api_key_source}）" if cfg.api_key else "未提供"
    lines.append(f"  ACXD API key:   {key}")
    text = "\n".join(lines)
    flowlib.assert_no_secret(text, cfg.api_key)
    return text


def prefix_resource_marks(cfg: Config, connect_client=None, target: ConnectTarget | None = None) -> dict:
    """Read-only check whether the prefix-derived resources already exist:
    {key: 将更新（覆盖） / 将新建 / PREFIX_MARK_FOREIGN(_FORCED)}.

    ACXD ListFlows / GetFlow / ListApplications / GetApplication (read-only client); the Connect flows come
    from target.existing_flows (connect_prechecks) or, without a target, one ListContactFlows.  A failed
    call only drops its marks (WARN), it never fails the run.  Never called by dry-run."""
    marks = {}

    def mark(exists: bool) -> str:
        return PREFIX_MARK_UPDATE if exists else PREFIX_MARK_CREATE

    def foreign() -> str:
        return PREFIX_MARK_FOREIGN_FORCED if cfg.overwrite_untagged else PREFIX_MARK_FOREIGN
    if cfg.acxd_phase and cfg.api_key and cfg.workspace_id:
        client = AcxdClient(cfg.acxd_endpoint, cfg.api_key, cfg.workspace_id, sleep=lambda s: time.sleep(s),
                            read_only=True)
        flow_id = config.acxd_flow_id(cfg.name_prefix)
        try:
            exists = any(f.get("flowId") == flow_id for f in client.list_flows())
            marks["flow"] = mark(exists)
            if exists and not metadata_owned(client.get_flow(flow_id)):
                marks["flow"] = foreign()
        except AcxdApiError as exc:
            logger.warning("ListFlows / GetFlow failed (%s); not showing whether %s exists", exc, flow_id)
        if not cfg.reuse_application:
            app_name = config.acxd_app_name(cfg.name_prefix)
            try:
                same = [a for a in client.list_applications() if a.get("name") == app_name]
                marks["app"] = mark(bool(same))
                if len(same) == 1 and same[0].get("applicationId"):
                    if not metadata_owned(client.get_application(same[0]["applicationId"])):
                        marks["app"] = foreign()
            except AcxdApiError as exc:
                logger.warning("ListApplications / GetApplication failed (%s); not showing whether %s exists",
                               exc, app_name)
    if cfg.connect_phase and target is not None:
        for key, name in (("screenpop", config.screenpop_flow_name(cfg.name_prefix)),
                          ("inbound", config.inbound_flow_name(cfg.name_prefix))):
            entry = target.existing_flows.get(name)
            marks[key] = (mark(False) if entry is None
                          else PREFIX_MARK_FOREIGN_FORCED if entry[1] == APP_FORCED else mark(True))
    elif cfg.connect_phase and connect_client is not None and cfg.instance_id:
        wanted = {"screenpop": config.screenpop_flow_name(cfg.name_prefix),
                  "inbound": config.inbound_flow_name(cfg.name_prefix)}
        try:
            names = set()
            for page in connect_client.get_paginator("list_contact_flows").paginate(
                    InstanceId=cfg.instance_id, ContactFlowTypes=["CONTACT_FLOW"]):
                names.update(s.get("Name") for s in page.get("ContactFlowSummaryList", []))
        except (BotoCoreError, ClientError) as exc:
            code = _err_code(exc) if isinstance(exc, ClientError) else type(exc).__name__
            logger.warning("ListContactFlows failed (%s); not showing whether the Connect flows exist", code)
        else:
            marks.update({key: mark(name in names) for key, name in wanted.items()})
    return marks


def confirm_before_write(cfg: Config, target: ConnectTarget | None, connect_client=None) -> None:
    """Interactive real runs only (dry-run / preflight never get here); --yes skips the question."""
    if not cfg.interactive or cfg.assume_yes or cfg.prompter is None:
        return
    marks = prefix_resource_marks(cfg, connect_client, target)
    print("\n即将执行真实部署（将写入 ACXD / Connect），参数摘要：")
    print(format_run_parameters(cfg, target, marks))
    if not cfg.prompter.confirm("确认执行真实部署？"):
        raise DeployError("已取消：未执行任何写操作", 1)


def run(cfg: Config) -> int:
    if cfg.dry_run:
        return run_dry(cfg)
    if cfg.preflight:
        try:
            return run_preflight(cfg)
        except (NoCredentialsError, ClientError) as exc:
            raise _connect_failure(exc) from exc
    try:
        cfg.out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"无法创建输出目录 {cfg.out_dir}: {exc}") from exc
    messages = flowlib.load_messages()
    state_path = cfg.out_dir / "deploy_state.json"
    # D5 order: state -> workspace (GET) -> Connect prechecks (Describe/List) -> ACXD writes up to
    # deployed -> alias guard -> Connect writes -> summary.
    base = filter_state(load_state(state_path), cfg, check_workspace=False)
    _resolve_workspace_for_phase(cfg, base, read_only=False)
    base = filter_state(base, cfg, check_workspace=True)
    if cfg.phase == "connect":
        _language_from_state(cfg, base, messages)
    writer = StateWriter(state_path, cfg, cfg.api_key, base)
    app_id = alias = app_name = None
    client = target = None
    try:
        if cfg.connect_phase:
            # A wrong instance / queue / credential stops here, before anything is written to ACXD
            # or Connect (for --phase connect alone these were the first calls of the phase anyway).
            client = make_connect_client(cfg.region)
            target = connect_prechecks(client, cfg)
            _record_target(writer, target)
        # Interactive real run: show the resolved parameters and ask before the first write.
        confirm_before_write(cfg, target, client)
        if cfg.acxd_phase:
            app_id, alias, app_name = run_acxd_phase(cfg, writer, messages)
            generated = cfg.action_obj is None and not cfg.reference_flow_id
            if cfg.connect_phase and generated and not (app_id and alias):
                raise DeployError("ACXD 阶段没有得到 applicationId / deploymentAlias，已停止：没有写入任何 Connect 流", 1)
        if cfg.connect_phase:
            target = run_connect_phase(cfg, writer, messages, base, app_id, alias, app_name,
                                       client=client, target=target)
    except (NoCredentialsError, ClientError) as exc:
        raise _connect_failure(exc) from exc
    finally:
        if writer.state["completedSteps"]:
            logger.info("completed steps: %s", ", ".join(writer.state["completedSteps"]))
    writer.write()
    print_summary(cfg, writer.state, target, "部署完成，摘要：")
    if cfg.connect_phase:
        print_next_steps(cfg, writer.state)
    print(f"\n部署状态已写入 {state_path}")
    return 0


def _resolve_workspace_for_phase(cfg: Config, base: dict | None, read_only: bool) -> None:
    """Workspace discovery is required for acxd / all; for --phase connect alone it is optional."""
    if cfg.phase != "connect":
        resolve_workspace(cfg, base, read_only)
        return
    try:
        resolve_workspace(cfg, base, read_only)
    except (ConfigError, DeployError) as exc:
        logger.warning("ACXD workspace discovery failed; continuing without a workspace id (%s)", exc)


def _language_from_state(cfg: Config, base: dict | None, messages: dict) -> None:
    """Without --language, start from the language of the deployed ACXD application recorded in the
    (filtered) deploy_state.json, so a re-run after an en-US fallback does not retry the rejected
    language.  In acxd / all phases the reuse mode keeps following the reused application."""
    if cfg.language_explicit:
        return
    acxd = (base or {}).get("acxd") or {}
    if cfg.acxd_phase and (cfg.reuse_application or acxd.get("applicationReused")):
        return
    lang = acxd.get("language")
    if lang in messages and lang != cfg.language:
        logger.info("language %s taken from deploy_state.json (language of the deployed ACXD application)", lang)
        set_language(cfg, lang, messages)
        cfg.language_fallback_from = cfg.language_fallback_from or acxd.get("languageFallbackFrom")


class _StderrHandler(logging.StreamHandler):
    """Always writes to the current sys.stderr (works with redirect_stderr)."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, _value):
        pass


def setup_logging(verbose: bool) -> None:
    root = logging.getLogger()
    if not any(isinstance(h, _StderrHandler) for h in root.handlers):
        h = _StderrHandler()
        h.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(h)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    setup_logging(args.verbose)
    try:
        # Precedence: CLI flag > environment variable (both via argparse / validate_config) >
        # interactive prompt > default.  Prompts only fill what is still missing.
        interactive = is_interactive(args)
        prompter = prompts.Prompter(intro=INTERACTIVE_INTRO) if interactive else None
        if interactive:
            collect_missing_args(args, prompter)
        cfg = validate_config(args)
        cfg.interactive, cfg.assume_yes, cfg.prompter = interactive, args.yes, prompter
        # Every mode (dry-run, preflight, --yes, --non-interactive) shows the effective prefix once.
        print(format_name_prefix(cfg))
        return run(cfg)
    except prompts.PromptAborted:
        print("\n已取消（未执行任何写操作）", file=sys.stderr)
        return 130
    except prompts.PromptError as exc:
        logger.error("%s", exc)
        return 2
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
    except DeployError as exc:
        logger.error("%s", exc)
        return exc.code
    except AcxdApiError as exc:
        logger.error("%s", exc)
        return 1
    except FlowValidationError as exc:
        for prob in exc.problems:
            logger.error("flow validation: %s", prob)
        return 1
    except (BotoCoreError, ClientError) as exc:
        logger.error("AWS error: %s", exc)
        return 1
    except OSError as exc:
        logger.error("cannot write output: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
