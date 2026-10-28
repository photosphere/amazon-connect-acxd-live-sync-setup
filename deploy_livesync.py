"""ACXD Live Sync 产品报修演示：来电 → 邮件链接 → CloudFront 页面与语音同步。

客户先拨打热线（座机、手机都可以）。Connect 入站流调用 Lambda，用 SES 把报修页面的链接发到邮箱；
客户在任意设备上打开链接（CloudFront 上的页面），页面通过 Touchpoint 的 external 模式绑定到这通电话，
电话里的语音助手一边说话一边驱动页面：

  * 产品名称：页面上 3 个 dummy 产品，说出或点选，助手通过 select_product 在页面上选中；
  * 产品型号：页面上 4 个 dummy 型号，说出或点选，助手通过 select_model 在页面上选中；
  * 故障描述：在电话里直接口述，页面不提供输入。

只支持英语（en-US）：ACXD 应用、Connect 流、页面和邮件都使用 en-US，不提供 --language zh-CN，也不询问对话语言。

部署内容（沿用 deploy.py 的两输入、交互模式、--dry-run、--preflight、幂等重跑、同名资源保护与状态文件）：
  ACXD：上下文变量 → flow <前缀>Intake（欢迎语 → Live Sync 节点 → 3 个 Generative Text → Escalate）
        → 应用 <前缀>SelfService → build → 部署
  AWS： CloudFormation 栈 <前缀>-livesync-page（S3 + CloudFront 页面、/api/session、发邮件 Lambda、
        Secrets Manager），Connect AssociateLambdaFunction，上传页面
  发件地址：部署前按 Connect 实例检查（只读）：实例区域的 SES 账号必须已出沙盒，实例的邮件地址中
        地址或域名在 SES 中已验证可发信的才可选；有多个时让用户选择，一个都没有时提示并结束部署
  Connect：<前缀> ScreenPop Flow 与 <前缀> Inbound ACXD Flow（客户资料查询 → 发送邮件链接 → 播报
        “链接已发送” → Agentic CX → 转人工）
  默认资源名前缀 LiveSync、默认输出目录 output-livesync，不会覆盖 deploy.py 的 AgenticCare 资源。

用法：
  export ACXD_API_KEY='<ACXD API key>'
  python3 deploy_livesync.py --instance-arn arn:aws:connect:<区域>:<账号>:instance/<实例ID> \\
      --test-email you@example.com

  # 部署后在 ACXD 应用 Settings → Access 复制 Live Sync API key 与 Deployment key，写入 Secrets Manager：
  export LIVE_SYNC_API_KEY='<Live Sync API key>' LIVE_SYNC_DEPLOYMENT_KEY='<Deployment key>'
  python3 deploy_livesync.py --page-only

  # 预演（不联网）/ 只读预检
  python3 deploy_livesync.py --dry-run --instance-arn <实例ARN> --test-email you@example.com
  python3 deploy_livesync.py --preflight --instance-arn <实例ARN> --test-email you@example.com

本脚本自己的参数见 --help 开头；其余参数原样交给 deploy.py。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import deploy
import livesync
import livesync_infra as infra
import prompts

logger = logging.getLogger("deploy_livesync")

LIVE_SYNC_API_KEY_ENV = "LIVE_SYNC_API_KEY"
LIVE_SYNC_DEPLOYMENT_KEY_ENV = "LIVE_SYNC_DEPLOYMENT_KEY"
TEST_EMAIL_ENV = "LIVE_SYNC_TEST_EMAIL"
EMAIL_FROM_ENV = "LIVE_SYNC_EMAIL_FROM"


def build_livesync_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(add_help=False, prog="deploy_livesync.py")
    g = p.add_argument_group("Live Sync 邮件链接演示参数（其余参数见下方 deploy.py 帮助）")
    g.add_argument("--test-email", default=os.environ.get(TEST_EMAIL_ENV),
                   help=f"测试邮件地址（环境变量 {TEST_EMAIL_ENV}）：任何人打进来，链接都发到这个地址。"
                        "不指定时发往来电号码在 Customer Profiles 中的邮箱。交互模式下未指定时会询问")
    g.add_argument("--email-from", default=os.environ.get(EMAIL_FROM_ENV),
                   help=f"发件地址（环境变量 {EMAIL_FROM_ENV}）：必须是 Connect 实例中可发信的邮件地址之一。"
                        "不指定时：交互模式下从可用地址中选择；非交互模式下只有一个可用地址时自动使用，多个时报错")
    g.add_argument("--link-ttl-minutes", type=int, default=infra.DEFAULT_LINK_TTL_MINUTES,
                   help=f"邮件链接有效期（分钟），默认 {infra.DEFAULT_LINK_TTL_MINUTES}；通话结束后链接也会立即失效")
    g.add_argument("--page-only", action="store_true",
                   help="不部署 ACXD / Connect：按 livesync_state.json 重新生成并上传页面，并在提供了 Live Sync key 时"
                        "写入 Secrets Manager（与 --dry-run 同用时只在本地生成页面）")
    g.add_argument("--live-sync-api-key-file", metavar="FILE",
                   help=f"Live Sync API key 文件（环境变量 {LIVE_SYNC_API_KEY_ENV} 优先）")
    g.add_argument("--live-sync-deployment-key-file", metavar="FILE",
                   help=f"Live Sync Deployment key 文件（环境变量 {LIVE_SYNC_DEPLOYMENT_KEY_ENV} 优先）")
    g.add_argument("--touchpoint-js", metavar="FILE",
                   help=f"使用本地的 Touchpoint UMD 包，而不是下载 {livesync.TOUCHPOINT_PACKAGE}@"
                        f"{livesync.TOUCHPOINT_VERSION}（下载时校验 npm integrity）")
    return p


def _base_mode(rest: list) -> argparse.Namespace:
    """The deploy.py options this wrapper needs to know (parsed, not consumed)."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--preflight", action="store_true")
    p.add_argument("--phase", default="all")
    p.add_argument("--output-dir")
    p.add_argument("--acxd-api-key-file")
    p.add_argument("--non-interactive", action="store_true")
    p.add_argument("--region")
    p.add_argument("--name-prefix")
    mode, _ = p.parse_known_args(rest)
    return mode


def _read_secret(env_name: str, path: str | None, label: str) -> str | None:
    value = os.environ.get(env_name)
    if value:
        if path:
            logger.warning("%s taken from %s; ignoring the file %s", label, env_name, path)
        return value.strip()
    if not path:
        return None
    try:
        mode = os.stat(path).st_mode
        text = Path(path).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise deploy.ConfigError(f"无法读取 {label} 文件 {path}") from exc
    if not text or "\n" in text:
        raise deploy.ConfigError(f"{label} 文件 {path} 为空或包含多行")
    if mode & 0o077:
        logger.warning("%s file %s is readable by group/others; run chmod 600 %s", label, path, path)
    return text


def _acxd_api_key(mode: argparse.Namespace) -> str | None:
    """Only used to make sure the ACXD API key never lands in a page file or the secret."""
    key = os.environ.get("ACXD_API_KEY")
    if key:
        return key
    if mode.acxd_api_key_file:
        try:
            return Path(mode.acxd_api_key_file).read_text(encoding="utf-8").strip() or None
        except OSError:
            return None
    return None


def _validate_email(value: str, flag: str) -> str:
    value = (value or "").strip()
    if not infra.EMAIL_RE.match(value):
        raise deploy.ConfigError(f"{flag} 不是有效的邮件地址: {value or '<空>'}")
    return value


def resolve_email_options(ls_args, mode, interactive: bool, prompter=None) -> tuple[str, str]:
    """(test_email, requested sender).  Test email: --test-email / env > interactive prompt > '' (profile
    email).  The sender is only validated here; it is chosen / checked against the Connect instance's
    email addresses in the Connect prechecks (livesync.select_sender)."""
    test_email = (ls_args.test_email or "").strip()
    if not test_email and interactive:
        prompter = prompter or prompts.Prompter()

        def validate(value):
            if not infra.EMAIL_RE.match(value):
                raise ValueError(f"不是有效的邮件地址: {value}")
            return value
        test_email = prompter.text(
            "测试邮件地址（任何人打进来都把链接发到这里；直接回车 = 发往客户资料中的邮箱）",
            validate=validate, allow_empty=True) or ""
    if test_email:
        test_email = _validate_email(test_email, "--test-email")
    email_from = (ls_args.email_from or "").strip()
    return test_email, (_validate_email(email_from, "--email-from") if email_from else "")


def _interactive(mode) -> bool:
    return (not mode.non_interactive and sys.stdin is not None and sys.stdin.isatty()
            and sys.stdout is not None and sys.stdout.isatty())


def _state_dir(out_root: Path, dry_run: bool) -> Path:
    return out_root / "dry-run" if dry_run else out_root




def dry_run_artifacts(state_dir: Path, mode) -> dict:
    """After deploy.py --dry-run: stack template and the page, rendered offline."""
    messages = livesync.load_messages()
    lang = livesync.LANGUAGE  # English only (an older zh-CN state file is ignored)
    try:
        state = json.loads((state_dir / "deploy_state.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise deploy.ConfigError(f"无法读取 {state_dir / 'deploy_state.json'}（先运行一次部署或 --dry-run）") from exc
    cfg = argparse.Namespace(
        name_prefix=mode.name_prefix or os.environ.get(livesync.config.NAME_PREFIX_ENV) or livesync.DEFAULT_NAME_PREFIX,
        instance_id=(state.get("connect") or {}).get("instanceId") or "00000000-0000-4000-8000-000000000000",
        language=lang)
    if not livesync.OPTIONS.email_from:
        logger.warning("dry-run cannot check the Connect instance email addresses offline; the stack template "
                       "uses the placeholder sender %s (pass --email-from to render a real one)", livesync.DRYRUN_SENDER)
    template = livesync.stack_template(cfg, messages)
    deploy.write_artifact(state_dir, livesync.STACK_ARTIFACT, template, livesync.OPTIONS.acxd_api_key)
    infra.template_body(template)  # size check
    summary = livesync.generate_page(state_dir, lang, messages, acxd_api_key=livesync.OPTIONS.acxd_api_key,
                                     touchpoint_js=livesync.OPTIONS.touchpoint_js,
                                     cache_dir=state_dir.parent / ".cache", offline=True)
    return {**summary, "template": template is not None}


def page_only(out_root: Path) -> dict:
    """Re-render and upload the page; store the Live Sync keys when given."""
    st = livesync.load_state(out_root)
    if not st or not st.get("bucket") or not st.get("secretArn"):
        raise deploy.ConfigError(f"{out_root / livesync.STATE_NAME} 中没有栈信息：请先完整运行一次 deploy_livesync.py")
    livesync.RUNTIME.update({k: st[k] for k in ("bucket", "secretArn", "pageUrl", "stackName") if st.get(k)})
    messages = livesync.load_messages()
    clients = livesync._clients(st["region"])
    livesync.publish_page(out_root, livesync.LANGUAGE, messages, clients, livesync.OPTIONS.acxd_api_key)
    livesync.write_state(out_root, {"liveSyncKeysConfigured": livesync.RUNTIME.get("liveSyncKeysConfigured")},
                         livesync.OPTIONS.acxd_api_key)
    return {**st, **livesync.RUNTIME}


def print_live_summary(st: dict) -> None:
    print("\nLive Sync 邮件链接演示：")
    print(f"  页面地址:      {st.get('pageUrl')}（只能通过邮件里的带签名链接使用）")
    print(f"  CloudFormation 栈: {st.get('stackName')}")
    print(f"  测试邮件地址:  {st.get('testEmail') or '未指定：发往来电号码在客户资料中的邮箱'}")
    source = {"--email-from": "--email-from", "interactive": "交互选择", "only-candidate": "唯一可用地址"}.get(
        st.get("emailFromSource"), st.get("emailFromSource") or "-")
    print(f"  发件地址:      {st.get('emailFrom')}（Connect 实例邮件地址，SES 已出沙盒；来源 {source}）")
    if st.get("liveSyncKeysConfigured"):
        print("  Live Sync key: 已写入 Secrets Manager")
    else:
        print("  ⚠ Live Sync key 尚未配置：页面会显示“演示环境尚未配置完成”。在 ACXD 应用 Settings → Access 复制后运行：")
        print(f"      export {LIVE_SYNC_API_KEY_ENV}='<Live Sync API key>' {LIVE_SYNC_DEPLOYMENT_KEY_ENV}='<Deployment key>'")
        print("      python3 deploy_livesync.py --page-only")
    print("  演示：把电话号码关联到入站流 → 拨打 → 在测试邮箱打开“产品报修”邮件中的链接 → 在电话里说出产品和型号")


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ls_parser = build_livesync_parser()
    if "-h" in argv or "--help" in argv:
        ls_parser.print_help()
        print("\n" + "=" * 72 + "\n")
        livesync.apply_profile()
        base = deploy.build_parser()
        base.prog = "deploy_livesync.py"
        base.description = ("以下参数原样交给 deploy.py（Live Sync 模式：默认前缀 LiveSync、输出目录 "
                            f"{livesync.DEFAULT_OUTPUT_DIR}，对话语言只支持 {livesync.LANGUAGE}）。")
        base.print_help()
        return 0
    ls_args, rest = ls_parser.parse_known_args(argv)
    if not any(a == "--output-dir" or a.startswith("--output-dir=") for a in rest):
        rest += ["--output-dir", livesync.DEFAULT_OUTPUT_DIR]
    mode = _base_mode(rest)
    out_root = Path(mode.output_dir)
    deploy.setup_logging("--verbose" in rest)
    if ls_args.link_ttl_minutes < 5 or ls_args.link_ttl_minutes > 240:
        logger.error("--link-ttl-minutes 必须在 5 到 240 之间")
        return 2
    try:
        opts = livesync.OPTIONS
        opts.acxd_api_key = _acxd_api_key(mode)
        opts.live_sync_api_key = _read_secret(LIVE_SYNC_API_KEY_ENV, ls_args.live_sync_api_key_file, "Live Sync API key")
        opts.live_sync_deployment_key = _read_secret(LIVE_SYNC_DEPLOYMENT_KEY_ENV,
                                                     ls_args.live_sync_deployment_key_file, "Live Sync Deployment key")
        for label, value in (("Live Sync API key", opts.live_sync_api_key),
                             ("Live Sync Deployment key", opts.live_sync_deployment_key)):
            if value and opts.acxd_api_key and value == opts.acxd_api_key:
                raise deploy.ConfigError(f"{label} 与 ACXD API key 相同：两者来自不同位置，请从 ACXD 应用 "
                                         "Settings → Access 复制 Live Sync 的 key")
        opts.touchpoint_js = Path(ls_args.touchpoint_js) if ls_args.touchpoint_js else None
        opts.link_ttl_minutes = ls_args.link_ttl_minutes
        needs_email = not ls_args.page_only and mode.phase != "acxd"
        if needs_email:
            opts.test_email, opts.email_from = resolve_email_options(ls_args, mode, _interactive(mode))
    except prompts.PromptAborted:
        print("\n已取消（未执行任何写操作）", file=sys.stderr)
        return 130
    except (deploy.ConfigError, prompts.PromptError) as exc:
        logger.error("%s", exc)
        return 2
    livesync.apply_profile()

    try:
        if ls_args.page_only:
            if mode.preflight:
                logger.error("--page-only cannot be combined with --preflight")
                return 2
            if mode.dry_run:
                summary = dry_run_artifacts(_state_dir(out_root, True), mode)
                print(f"\n页面已在本地生成（dry-run，未上传）：{summary['pageDir']}")
                return 0
            print_live_summary({**page_only(out_root), **(livesync.load_state(out_root) or {})})
            return 0

        rc = deploy.main(rest)
        if rc != 0:
            return rc
        if mode.preflight:
            print("\n--preflight：栈模板写入 preflight/livesync_stack.json，未部署页面、未发送任何邮件")
            return 0
        if mode.dry_run:
            summary = dry_run_artifacts(_state_dir(out_root, True), mode)
            print("\nLive Sync 邮件链接演示（dry-run）：")
            print(f"  栈模板:  {'dry-run/' + livesync.STACK_ARTIFACT if summary['template'] else '未生成（缺少发件地址）'}")
            print(f"  页面:    {summary['pageDir']}（未上传；Touchpoint "
                  f"{summary['touchpoint'] or '未下载，真实运行时下载'}）")
            return 0
        if mode.phase == "acxd":
            print("\n--phase acxd：没有部署页面与邮件（需要 connect 阶段）")
            return 0
        print_live_summary(livesync.load_state(out_root) or {})
        return 0
    except deploy.ConfigError as exc:
        logger.error("%s", exc)
        return 2
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        logger.error("Live Sync page step failed: %s", exc)
        return 1
    except livesync.flowlib.FlowValidationError as exc:
        for prob in exc.problems:
            logger.error("page: %s", prob)
        return 1
    except Exception as exc:  # noqa: BLE001 - boto3 errors of the page-only path
        logger.error("AWS error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
