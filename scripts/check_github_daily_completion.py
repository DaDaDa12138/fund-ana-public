#!/usr/bin/env python3
"""Cheap cloud-only catch-up guard; never equate missing receipts with success."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.shanghai_time import SHANGHAI, now_shanghai
from backend.github_storage import source_repository, state_repository

# GitHub cron is UTC; these are Shanghai 07:47 and 10:47 respectively.
DAILY_SCHEDULES = frozenset({"47 23 * * *", "47 2 * * *", "17 0,1,4 * * *"})
SCHEDULE_SLOTS = {
    "47 23 * * *": ((23,), 47, None),
    "47 2 * * *": ((2,), 47, None),
    "17 0,1,4 * * *": ((0, 1, 4), 17, None),
    "17 19 * * 6": ((19,), 17, 5),  # Python Saturday = 5; cron Saturday = 6.
}


def schedule_observation(schedule: str, observed: datetime, created: datetime | None) -> dict:
    """Infer the latest UTC slot; GitHub does not expose its original due timestamp."""
    if observed.tzinfo is None or (created is not None and created.tzinfo is None):
        raise ValueError("schedule_observation_requires_aware_time")
    result = {"cron": schedule or None, "cron_timezone": "UTC", "timezone": "Asia/Shanghai",
              "observed_at": observed.astimezone(SHANGHAI).isoformat(),
              "run_created_at": created.astimezone(SHANGHAI).isoformat() if created else None,
              "expected_at": None, "delivery_delay_seconds": None,
              "pre_guard_seconds": max(0, (observed - created).total_seconds()) if created else None,
              "expected_at_basis": "latest_matching_slot_not_original_event_timestamp"}
    if schedule in SCHEDULE_SLOTS:
        reference = (created or observed).astimezone(timezone.utc)
        hours, minute, weekday = SCHEDULE_SLOTS[schedule]
        slots = [datetime.combine(reference.date() - timedelta(days=days),
                                  time(hour, minute), tzinfo=timezone.utc)
                 for days in range(8) for hour in hours
                 if weekday is None or (reference.date() - timedelta(days=days)).weekday() == weekday]
        expected = max(slot for slot in slots if slot <= reference)
        result["expected_at"] = expected.astimezone(SHANGHAI).isoformat()
        if created:
            result["delivery_delay_seconds"] = (created - expected).total_seconds()
    return result


def workflow_created_at() -> datetime | None:
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not run_id.isdigit():
        return None
    try:
        environment = dict(os.environ)
        # The workflow token reads its own run; the scoped storage token reads
        # private receipts. Neither token needs access to both repositories.
        if os.environ.get("GITHUB_TOKEN"):
            environment["GH_TOKEN"] = os.environ["GITHUB_TOKEN"]
        result = subprocess.run(["gh", "api", "repos/" + source_repository() + "/actions/runs/" + run_id,
                                 "--jq", ".created_at"], env=environment,
                                capture_output=True, text=True, timeout=20)
        if result.returncode:
            return None
        created = datetime.fromisoformat(result.stdout.strip().replace("Z", "+00:00"))
        return created if created.tzinfo is not None else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None  # Unavailable timing evidence must not become a fabricated zero delay.


def write_summary(observation: dict, prior: str | None, diagnostic: bool) -> None:
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary:
        return
    action = ("仅检查定时配置与完成收据；未采集、未发布" if diagnostic else
              "已核验今日完整日更，跳过重复采集；未生成新报告" if prior else
              "需要继续真实任务；本检查不代表采集或发布成功")
    rows = [("实际动作", action), ("业务时区", "Asia/Shanghai"),
            ("触发 cron（UTC）", observation["cron"]),
            ("最近匹配计划时间（上海，推算）", observation["expected_at"]),
            ("GitHub 创建运行（上海）", observation["run_created_at"]),
            ("检查时刻（上海）", observation["observed_at"]),
            ("计划到创建间隔（秒）", observation["delivery_delay_seconds"]),
            ("已核验日更运行", prior)]
    lines = ["## 定时触发与日更完成检查", "", "| 项目 | 结果 |", "|---|---|"]
    lines += [f"| {label} | {value if value is not None else '未知/不适用'} |" for label, value in rows]
    lines += ["", "计划：上海07:47日更；08:17、09:17、10:47、12:17补偿；周日03:17维护。",
              "计划时间按实际cron的最近时点推算；平台未提供原始到期时间，跨多个周期的延迟无法唯一归属。",
              "GitHub定时投递为尽力执行，补偿仍依赖同一平台，不能保证准点。", ""]
    with Path(summary).open("a") as stream:
        stream.write("\n".join(lines))


def completed_daily(checks: list[dict], now: datetime) -> str | None:
    local = now.astimezone(SHANGHAI)
    boundary = datetime.combine(local.date(), time(7, 47), tzinfo=SHANGHAI)
    for check in checks:
        if (check.get("schema") != "fund-ana-github-check-v1" or check.get("job") != "daily_refresh"
                or check.get("executor") != "github_hosted" or check.get("collection_requested") is not True
                or check.get("status") not in {"published", "unchanged", "published_with_warnings", "unchanged_with_warnings"}
                or check.get("publication_sync_status") != "verified_current"
                or not re.fullmatch(r"[0-9a-f]{40}", str(check.get("cloud_state_commit") or ""))):
            continue
        if check["status"] in {"published_with_warnings", "unchanged_with_warnings"}:
            proof = check.get("nonblocking_validation") or {}
            if (proof.get("contract") not in {"github-safe-publication-completion-v1", "github-safe-publication-completion-v2"}
                    or proof.get("status") != "verified" or proof.get("formal_overlap_count") != 0
                    or proof.get("blocking_count") != 0):
                continue
            if proof["contract"].endswith("-v2"):
                superseded = proof.get("superseded_refresh_proofs")
                if (not isinstance(superseded, list) or not superseded
                        or check.get("task_warning_reason") != "period_return_refresh_superseded"
                        or any(not isinstance(item, dict) or item.get("fcode") not in proof.get("excluded_fcodes", [])
                               or item.get("status") != "superseded_refresh_rejected"
                               or item.get("returns_and_nav") != "reprojected_current_rank"
                               or item.get("nav_date") != (check.get("publication") or {}).get("nav_date")
                               or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("accepted_source_sha256") or ""))
                               for item in superseded)):
                    continue
        elif check.get("task_status") not in {"success", "skipped"}:
            continue
        try:
            started = datetime.fromisoformat(check["checked_at"].replace("Z", "+00:00"))
            ended = datetime.fromisoformat(check["finished_at"].replace("Z", "+00:00"))
            if started.tzinfo is None or ended.tzinfo is None or not boundary <= started <= ended <= local:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        return str(check["github_run_id"])
    return None


def main() -> None:
    event = os.environ.get("GITHUB_EVENT_NAME")
    schedule = os.environ.get("GITHUB_EVENT_SCHEDULE", "")
    diagnostic = event == "workflow_dispatch" and os.environ.get("SCHEDULE_CHECK_ONLY") == "true"
    daily = (event == "schedule" and schedule in DAILY_SCHEDULES) or diagnostic
    now = now_shanghai()
    prior = None
    if daily:
        repository = state_repository()
        prefix = "repos/" + repository + "/contents/checks/" + now.date().isoformat()
        def read(path, raw=False):
            command = ["gh", "api"]
            if raw:
                command += ["-H", "Accept: application/vnd.github.raw+json"]
            result = subprocess.run([*command, path + "?ref=daily-reports"], capture_output=True, text=True, timeout=60)
            if result.returncode:
                if "HTTP 404" in result.stderr:
                    return None
                raise RuntimeError("daily_receipt_lookup_unavailable")
            return json.loads(result.stdout)
        try:
            listing = read(prefix) or []
            checks = [read("repos/" + repository + "/contents/" + item["path"], True)
                      for item in listing if item.get("type") == "file" and item["name"].endswith(".json")]
            prior = completed_daily([c for c in checks if isinstance(c, dict)], now)
        except (ValueError, RuntimeError, subprocess.TimeoutExpired):
            print("::warning::无法核验今日完成收据，继续真实采集；不把缺失证据当成功。")
    run = prior is None and not diagnostic
    observation = schedule_observation(schedule, now, workflow_created_at())
    delay = observation["delivery_delay_seconds"]
    if delay is not None and delay > 900:
        print(f"::warning::定时运行创建晚于最近匹配计划时间约{delay / 60:.1f}分钟；时区换算为UTC→上海，详见运行摘要。")
    write_summary(observation, prior, diagnostic)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a") as stream:
            stream.write(f"run={'true' if run else 'false'}\n")
    print(json.dumps({"run": run, "verified_prior_daily_run": prior, "timezone": "Asia/Shanghai",
                      "diagnostic_only": diagnostic, "collection_required": prior is None if daily else None,
                      "schedule_observation": observation}))


if __name__ == "__main__":
    main()
