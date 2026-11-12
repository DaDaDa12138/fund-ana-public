from __future__ import annotations

from typing import Any


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _short_blocker(blocker: dict[str, Any] | None) -> dict[str, str]:
    payload = dict(blocker or {})
    return {
        "code": _clean_text(payload.get("code")),
        "label": _clean_text(payload.get("label")),
        "detail": _clean_text(payload.get("detail")),
        "severity": _clean_text(payload.get("severity")),
        "action_path": _clean_text(payload.get("action_path")),
    }


def _is_blocking_blocker(blocker: dict[str, Any] | None) -> bool:
    payload = dict(blocker or {})
    severity = _clean_text(payload.get("severity")).lower()
    return bool(payload.get("blocking")) or severity == "blocking"


def _extract_count_summary(record: dict[str, Any] | None) -> dict[str, int]:
    payload = dict(record or {})
    counts = dict(payload.get("counts") or {})
    return {
        "final_count": int(payload.get("final_count") or counts.get("final_count") or 0),
        "dynamic_final_count": int(payload.get("dynamic_final_count") or counts.get("dynamic_final_count") or 0),
        "project_tracker_current_active_count": int(
            payload.get("project_tracker_current_active_count")
            or counts.get("project_tracker_current_active_count")
            or 0
        ),
        "stable_bond_final_count": int(
            payload.get("stable_bond_final_count")
            or counts.get("stable_bond_final_count")
            or 0
        ),
        "enhanced_bond_final_count": int(
            payload.get("enhanced_bond_final_count")
            or counts.get("enhanced_bond_final_count")
            or 0
        ),
    }


def _extract_optional_count(record: dict[str, Any] | None, key: str) -> int | None:
    payload = dict(record or {})
    counts = dict(payload.get("counts") or {})
    value = payload.get(key)
    if value in (None, ""):
        value = counts.get(key)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _extract_snapshot_summary(record: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(record or {})
    snapshot = dict(payload.get("snapshot") or {})
    return {
        "candidate_date": _clean_text(snapshot.get("candidate_date")),
        "candidate_fund_count": _extract_optional_count({"counts": snapshot}, "candidate_fund_count"),
        "coverage_ratio_vs_total": snapshot.get("coverage_ratio_vs_total"),
        "risk_1y_effective_end_date": _clean_text(snapshot.get("risk_1y_effective_end_date")),
        "risk_1y_effective_fund_count": _extract_optional_count(
            {"counts": snapshot},
            "risk_1y_effective_fund_count",
        ),
        "risk_1y_effective_coverage_ratio_vs_candidate": snapshot.get(
            "risk_1y_effective_coverage_ratio_vs_candidate"
        ),
    }


def _record_run_semantics(payload: dict[str, Any], *, publishable: bool, published: bool) -> Any:
    explicit = payload.get("run_semantics")
    if explicit not in (None, ""):
        return explicit
    if published:
        return "formal_publication"
    if publishable:
        return "publishable_snapshot"
    return "diagnostic_only"


def _record_publication_status(payload: dict[str, Any], *, publishable: bool, published: bool) -> Any:
    explicit = payload.get("publication_status")
    if explicit not in (None, ""):
        return explicit
    if published:
        return "current_published"
    if publishable:
        return "publishable_not_current"
    return "not_publishable"


def _compact_record(record: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(record or {})
    if not payload:
        return {}
    snapshot = _extract_snapshot_summary(payload)
    blockers = [
        _short_blocker(item if isinstance(item, dict) else {})
        for item in list(payload.get("publish_blockers") or [])
    ]
    published = bool(payload.get("published"))
    publishable = bool(payload.get("publishable")) or published
    return {
        "run_id": _clean_text(payload.get("run_id")) or None,
        "generated_at": _clean_text(payload.get("generated_at")) or None,
        "group_key": _clean_text(payload.get("group_key")) or None,
        "report_path": _clean_text(payload.get("report_path")) or None,
        "final_count": _extract_optional_count(payload, "final_count"),
        "dynamic_final_count": _extract_optional_count(payload, "dynamic_final_count"),
        "project_tracker_current_active_count": _extract_optional_count(
            payload,
            "project_tracker_current_active_count",
        ),
        "stable_bond_final_count": _extract_optional_count(payload, "stable_bond_final_count"),
        "enhanced_bond_final_count": _extract_optional_count(payload, "enhanced_bond_final_count"),
        "screenable_universe_count": _extract_optional_count(payload, "screenable_universe_count"),
        "publishable_pool_count": _extract_optional_count(payload, "publishable_pool_count"),
        "snapshot": snapshot,
        "effective_nav_date": snapshot.get("candidate_date") or None,
        "risk_effective_end_date": snapshot.get("risk_1y_effective_end_date") or None,
        "publishable": publishable,
        "published": published,
        "publish_blockers": blockers,
        "run_semantics": _record_run_semantics(
            payload,
            publishable=publishable,
            published=published,
        ),
        "publication_status": _record_publication_status(
            payload,
            publishable=publishable,
            published=published,
        ),
        "publish_hold": bool(payload.get("publish_hold")),
        "publish_blocked": bool(payload.get("publish_blocked")),
        "publish_hold_reason_codes": [
            _clean_text(item)
            for item in list(payload.get("publish_hold_reason_codes") or [])
            if _clean_text(item)
        ],
        "primary_blocker": blockers[0] if blockers else {},
        "blockers_preview": blockers[:3],
    }


def build_publication_status_summary(
    publication: dict[str, Any] | None,
    *,
    tracker_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    publication_payload = dict(publication or {})
    layers = dict(publication_payload.get("automation_layers") or {})
    latest_successful = dict(layers.get("latest_successful_run") or {})
    latest_publishable = dict(layers.get("latest_publishable_run") or {})
    current_published = dict(layers.get("current_published_run") or {})
    latest_database_snapshot = dict(layers.get("latest_database_snapshot") or {})
    tracker_payload = dict(tracker_summary or {})
    current_published_record = _compact_record(
        {
            **current_published,
            "published": bool(current_published),
            "publishable": bool(current_published),
        }
    )
    latest_successful_record = _compact_record(latest_successful)
    latest_publishable_record = _compact_record(
        {
            **latest_publishable,
            "publishable": bool(latest_publishable),
        }
    )

    latest_successful_run_id = _clean_text(latest_successful.get("run_id"))
    latest_publishable_run_id = _clean_text(latest_publishable.get("run_id"))
    current_published_run_id = (
        _clean_text(current_published.get("run_id"))
        or _clean_text(publication_payload.get("current_published_run_id"))
    )
    latest_canonical_run_id = _clean_text(publication_payload.get("latest_canonical_run_id"))
    latest_successful_is_currently_published = bool(
        layers.get("latest_successful_is_currently_published")
    ) or bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_run_id == current_published_run_id
    )
    latest_successful_is_publishable = bool(
        layers.get("latest_successful_is_publishable")
    ) or bool(latest_successful.get("publishable"))
    latest_successful_published = bool(latest_successful.get("published")) or latest_successful_is_currently_published
    legacy_publish_hold = bool(latest_successful.get("publish_hold"))
    publishable_not_current = bool(
        latest_successful_run_id
        and current_published_run_id
        and latest_successful_is_publishable
        and latest_successful_run_id != current_published_run_id
    )
    blockers = list(
        latest_successful.get("publish_blockers")
        or layers.get("latest_successful_blockers")
        or []
    )
    blocking_blockers = [item for item in blockers if _is_blocking_blocker(item if isinstance(item, dict) else {})]
    publish_blocked = (
        bool(latest_successful.get("publish_blocked"))
        or bool(blocking_blockers)
        or (not latest_successful_is_publishable and not latest_successful_is_currently_published)
        or legacy_publish_hold
        or publishable_not_current
    )
    primary_blocker = _short_blocker(blockers[0] if blockers else {})
    replay_precision = dict(latest_successful.get("replay_precision") or {})
    tracker_latest_changes = _clean_text(
        tracker_payload.get("latest_changes")
        or (tracker_payload.get("narrative_summary") or {}).get("latest_changes")
    )
    replay_precision_status = _clean_text(replay_precision.get("status"))
    replay_precision_status_label = _clean_text(replay_precision.get("status_label"))
    replay_precision_detail = _clean_text(replay_precision.get("detail"))
    current_published_effective_nav_date = (
        current_published_record.get("effective_nav_date")
        or _clean_text(tracker_payload.get("latest_nav_snapshot_date"))
        or None
    )
    latest_database_snapshot_date = _clean_text(latest_database_snapshot.get("candidate_date")) or None
    database_snapshot_ahead = bool(
        latest_database_snapshot_date
        and current_published_effective_nav_date
        and latest_database_snapshot_date > current_published_effective_nav_date
    )

    if not latest_successful_run_id:
        status = "no_successful_run"
        status_label = "暂无完成运行"
        recommended_action = "wait_for_scheduler"
        headline = "当前尚未记录成功筛选运行。"
        detail = "等待自动调度完成数据更新、筛选与发布闭环。"
    elif latest_successful_is_currently_published and database_snapshot_ahead:
        status = "success_published_with_backlog"
        status_label = "已发布，存在待筛选快照"
        recommended_action = "automatic_retry"
        headline = (
            f"当前正式发布版本为 {current_published_run_id}；数据库已到 "
            f"{latest_database_snapshot_date}，等待自动筛选发布。"
        )
        detail = (
            f"已发布净值日仍为 {current_published_effective_nav_date}，"
            f"数据库最新有效净值日为 {latest_database_snapshot_date}；"
            "两者保持不同口径，未将数据库日期冒充正式发布日期。"
        )
    elif latest_successful_is_currently_published:
        status = "success_published"
        status_label = "已正式发布"
        recommended_action = "none"
        headline = f"最新运行 {latest_successful_run_id} 已是当前正式发布版本。"
        detail = tracker_latest_changes or "当前发布链已收敛，暂无额外名单变化摘要。"
    elif publish_blocked or not latest_successful_is_publishable:
        status = "publication_blocked"
        status_label = "发布阻断，未正式发布"
        recommended_action = "repair_publication_pointer" if publishable_not_current else "resolve_publish_blockers"
        headline = (
            f"最新运行 {latest_successful_run_id} 已可发布，但正式指针仍指向 {current_published_run_id or '-'}。"
            if publishable_not_current
            else f"最新运行 {latest_successful_run_id} 未通过发布门禁。"
        )
        legacy_hold_reasons = latest_successful.get("publish_hold_reason_codes") or []
        legacy_hold_text = "、".join(_clean_text(item) for item in legacy_hold_reasons if _clean_text(item))
        detail = (
            "发布指针未及时切换，需要自动重建 publication registry 并写回 latest.json。"
            if publishable_not_current
            else ""
        ) or (
            primary_blocker.get("detail")
            or primary_blocker.get("label")
            or legacy_hold_text
            or "需复核发布阻断项。"
        )
    else:
        status = "publication_pending_review"
        status_label = "发布状态待确认"
        recommended_action = "review_publication_state"
        headline = f"最新运行 {latest_successful_run_id} 已完成，发布态仍需确认。"
        detail = "当前 publication registry 未提供足够字段区分已发布、暂缓或阻断。"

    if replay_precision_detail:
        detail = f"{detail} {replay_precision_detail}".strip()

    return {
        "available": True,
        "status": status,
        "status_label": status_label,
        "recommended_action": recommended_action,
        "headline": headline,
        "detail": detail,
        "tracker_latest_changes": tracker_latest_changes,
        "latest_successful_run_id": latest_successful_run_id,
        "latest_successful_generated_at": _clean_text(latest_successful.get("generated_at")),
        "latest_publishable_run_id": latest_publishable_run_id,
        "latest_publishable_generated_at": _clean_text(latest_publishable.get("generated_at")),
        "current_published_run_id": current_published_run_id,
        "current_published_generated_at": _clean_text(current_published.get("generated_at")),
        "latest_canonical_run_id": latest_canonical_run_id,
        "latest_database_snapshot_date": _clean_text(latest_database_snapshot.get("candidate_date")),
        "current_effective_nav_date": current_published_effective_nav_date,
        "database_snapshot_ahead_of_publication": database_snapshot_ahead,
        "latest_successful_effective_nav_date": (
            latest_successful_record.get("effective_nav_date")
            or _clean_text(latest_database_snapshot.get("candidate_date"))
            or None
        ),
        "current_published": current_published_record,
        "latest_successful": latest_successful_record,
        "latest_publishable": latest_publishable_record,
        "latest_successful_is_currently_published": bool(latest_successful_is_currently_published),
        "latest_successful_is_publishable": bool(latest_successful_is_publishable),
        "latest_successful_published": bool(latest_successful_published),
        "publish_hold": bool(False),
        "legacy_publish_hold": bool(legacy_publish_hold),
        "publish_blocked": bool(publish_blocked),
        "replay_precision_status": replay_precision_status,
        "replay_precision_status_label": replay_precision_status_label,
        "replay_precision_detail": replay_precision_detail,
        "exact_replay_inputs_available": bool(replay_precision.get("exact_replay_inputs_available")),
        "publish_gate_exact_replay_available": bool(replay_precision.get("publish_gate_exact_replay_available")),
        "final_layer_exact_replay_available": bool(replay_precision.get("final_layer_exact_replay_available")),
        "blocker_count": int(len(blockers)),
        "primary_blocker": primary_blocker,
        "blockers_preview": [_short_blocker(item) for item in blockers[:3]],
        "state_components": {
            "execution_status": "success" if latest_successful_run_id else "unknown",
            "publish_gate_status": (
                "blocked" if publish_blocked or not latest_successful_is_publishable else "passed"
            ) if latest_successful_run_id else "unknown",
            "publication_state": (
                "current_published"
                if latest_successful_is_currently_published
                else ("not_current" if current_published_run_id else "no_current_publication")
            ),
        },
    }


def build_publication_summary(
    publication: dict[str, Any] | None,
    *,
    tracker_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    publication_payload = dict(publication or {})
    layers = dict(publication_payload.get("automation_layers") or {})
    status_summary = build_publication_status_summary(
        publication_payload,
        tracker_summary=tracker_summary,
    )
    current_published_run = dict(layers.get("current_published_run") or {})
    latest_successful_run = dict(layers.get("latest_successful_run") or {})
    latest_publishable_run = dict(layers.get("latest_publishable_run") or {})
    effective_record = current_published_run or latest_successful_run or latest_publishable_run
    if current_published_run:
        effective_record = {
            **current_published_run,
            "published": True,
            "publishable": True,
        }
    elif latest_publishable_run and not latest_successful_run:
        effective_record = {**latest_publishable_run, "publishable": True}
    counts = _extract_count_summary(effective_record)
    effective_compact_record = _compact_record(effective_record)
    return {
        "current_published_run_id": status_summary.get("current_published_run_id"),
        "latest_canonical_run_id": status_summary.get("latest_canonical_run_id"),
        "latest_canonical_report_path": publication_payload.get("latest_canonical_report_path"),
        "latest_successful_run_id": status_summary.get("latest_successful_run_id"),
        "latest_successful_is_currently_published": bool(
            status_summary.get("latest_successful_is_currently_published")
        ),
        "latest_successful_blockers": list(
            (layers.get("latest_successful_blockers") or latest_successful_run.get("publish_blockers") or [])
        ),
        "publishable": bool(effective_compact_record.get("publishable")),
        "publish_blockers": list(effective_compact_record.get("publish_blockers") or []),
        "run_semantics": effective_compact_record.get("run_semantics"),
        "publication_status": effective_compact_record.get("publication_status"),
        "final_count": counts["final_count"],
        "dynamic_final_count": counts["dynamic_final_count"],
        "project_tracker_current_active_count": counts["project_tracker_current_active_count"],
        "stable_bond_final_count": counts["stable_bond_final_count"],
        "enhanced_bond_final_count": counts["enhanced_bond_final_count"],
        "status_summary": status_summary,
    }
