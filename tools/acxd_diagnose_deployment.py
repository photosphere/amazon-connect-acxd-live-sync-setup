"""Read-only diagnosis of a failed ACXD application deployment.

  python3 tools/acxd_diagnose_deployment.py --output-dir output-livesync
  python3 tools/acxd_diagnose_deployment.py --output-dir output            # deploy.py runs

ACXD returns deploymentStatus "failed" without a reason, so this tool collects what can explain it:
  1. the failed deployment, its build and the application (languages, flows, settings);
  2. every application of the workspace with its deployments (environment, status, languages), to see
     whether any deployment in this language / with this kind of flow has ever succeeded;
  3. the workspace audit trail (StartTrailQuery / GetTrailQueryResults) for deployment events of the last
     --hours hours, with their errorCode / errorMessage.
Only GET requests plus the trail query (which reads the audit log and changes nothing).  Every response
is redacted (apiKey and other credential fields) before it is printed or written.

Reads region / workspace / application / deployment ids from <output-dir>/deploy_state.json.  The ACXD API
key comes from ACXD_API_KEY, --acxd-api-key-file or a hidden prompt.  The report is written to
<output-dir>/acxd_deployment_diagnosis.json.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from acxd_client import AcxdApiError, AcxdClient, endpoint_for_region, mask_key, redact  # noqa: E402

DEPLOYMENT_EVENTS = ("DeploymentCreate", "DeploymentUpdate")


def _key(args) -> str:
    key = os.environ.get("ACXD_API_KEY")
    if not key and args.acxd_api_key_file:
        key = Path(args.acxd_api_key_file).read_text(encoding="utf-8").strip()
    if not key and sys.stdin.isatty():
        key = getpass.getpass("ACXD API key: ").strip()
    if not key:
        raise SystemExit("缺少 ACXD API key（ACXD_API_KEY / --acxd-api-key-file / 交互输入）")
    return key


def _safe(fn, *a):
    try:
        return fn(*a), None
    except AcxdApiError as exc:
        return None, f"{exc.status} {exc.error_type}: {exc.message}"


def _trail(client: AcxdClient, hours: float) -> dict:
    end = time.time()
    out = {"events": [], "errors": []}
    for name in DEPLOYMENT_EVENTS:
        started, err = _safe(client.start_trail_query,
                             {"startTimestamp": end - hours * 3600, "endTimestamp": end, "eventName": name,
                              "size": 50})
        if err:
            out["errors"].append(f"StartTrailQuery {name}: {err}")
            continue
        result_id = (started or {}).get("resultId")
        for _ in range(20):
            res, err = _safe(client.get_trail_query_results, result_id)
            if err:
                out["errors"].append(f"GetTrailQueryResults {name}: {err}")
                break
            if str(res.get("status", "")).upper() not in ("RUNNING", "QUEUED", "PENDING", "SCHEDULED"):
                for item in res.get("items") or []:
                    for field in ("requestParameters", "responseElements"):
                        try:
                            item[field] = redact(json.loads(item[field])) if item.get(field) else item.get(field)
                        except (TypeError, ValueError):
                            item[field] = "<unparsed, omitted>"
                    out["events"].append(redact(item))
                break
            time.sleep(1.5)
    return out


def diagnose(client: AcxdClient, state: dict, hours: float) -> dict:
    acxd = state.get("acxd") or {}
    app_id, dep_id, build_id = acxd.get("applicationId"), acxd.get("deploymentId"), acxd.get("buildId")
    report = {"applicationId": app_id, "deploymentId": dep_id, "buildId": build_id,
              "language": acxd.get("language"), "environment": acxd.get("environment")}
    if app_id and dep_id:
        report["deployment"], report["deploymentError"] = _safe(client.get_deployment, app_id, dep_id)
    if app_id and build_id:
        report["build"], report["buildError"] = _safe(client.get_build, app_id, build_id)
    if app_id:
        app, report["applicationError"] = _safe(client.get_application, app_id)
        if app:
            settings = app.get("settings") or {}
            report["application"] = {"name": app.get("name"), "flows": app.get("flows"),
                                     "languageCodes": settings.get("languageCodes"),
                                     "languageSettings": settings.get("languageSettings"),
                                     "defaultFlows": settings.get("defaultFlows"),
                                     "guardrails": settings.get("guardrails")}
    apps, err = _safe(client.list_applications)
    report["workspaceApplications"], report["workspaceApplicationsError"] = [], err
    for a in apps or []:
        deps, derr = _safe(client.list_deployments, a.get("applicationId"))
        report["workspaceApplications"].append({
            "name": a.get("name"), "applicationId": a.get("applicationId"), "error": derr,
            "deployments": [{"environment": d.get("environment"), "status": d.get("deploymentStatus"),
                             "languageCodes": d.get("languageCodes"), "updatedAt": d.get("updatedAt")}
                            for d in deps or []]})
    report["trail"] = _trail(client, hours)
    return redact(report)


def print_report(r: dict) -> None:
    print(f"\n失败的部署: {r.get('deploymentId')}  状态 {((r.get('deployment') or {}).get('deploymentStatus'))}  "
          f"环境 {r.get('environment')}  语言 {r.get('language')}")
    b = r.get("build") or {}
    print(f"build: {r.get('buildId')}  状态 {b.get('status')}  languageSettings {b.get('languageSettings')}")
    app = r.get("application") or {}
    print(f"应用: {app.get('name')}  languageCodes {app.get('languageCodes')}  flows {app.get('flows')}")
    print("\n工作区内各应用的部署：")
    for a in r.get("workspaceApplications") or []:
        deps = a["deployments"] or [{"environment": "-", "status": "无部署", "languageCodes": None}]
        for d in deps:
            print(f"  {a['name']:<32} {d['environment']:<12} {str(d['status']):<12} {d.get('languageCodes')}")
    langs_ok = sorted({lang for a in r.get("workspaceApplications") or [] for d in a["deployments"]
                       if d.get("status") == "deployed" for lang in d.get("languageCodes") or []})
    print(f"\n成功部署过的语言: {langs_ok or '无'}")
    trail = r.get("trail") or {}
    print("\n审计记录（部署事件）：")
    for e in trail.get("events") or []:
        print(f"  {e.get('eventTime')} {e.get('eventName')} errorCode={e.get('errorCode')} "
              f"errorMessage={e.get('errorMessage')}")
    for err in trail.get("errors") or []:
        print(f"  （无法查询: {err}）")
    if not trail.get("events") and not trail.get("errors"):
        print("  无")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Read-only diagnosis of a failed ACXD deployment")
    p.add_argument("--output-dir", default="output-livesync")
    p.add_argument("--acxd-api-key-file")
    p.add_argument("--hours", type=float, default=6.0, help="audit trail window, default 6 hours")
    args = p.parse_args(argv)
    out = Path(args.output_dir)
    try:
        state = json.loads((out / "deploy_state.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"无法读取 {out / 'deploy_state.json'}: {exc}")
    acxd = state.get("acxd") or {}
    key = _key(args)
    endpoint = acxd.get("endpoint") or endpoint_for_region(state["region"])
    print(f"ACXD {endpoint}  工作区 {acxd.get('workspaceId')}  key {mask_key(key)}")
    client = AcxdClient(endpoint, key, acxd.get("workspaceId"))
    report = diagnose(client, state, args.hours)
    text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if key in text:
        raise SystemExit("refusing to write the report: it contains the ACXD API key")
    path = out / "acxd_deployment_diagnosis.json"
    path.write_text(text + "\n", encoding="utf-8")
    print_report(report)
    print(f"\n完整报告（已脱敏）: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
