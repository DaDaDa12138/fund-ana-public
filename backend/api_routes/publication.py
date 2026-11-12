from __future__ import annotations

from typing import Any, Callable

from flask import Flask, Response, jsonify


PublicationLoader = Callable[[], dict[str, Any]]
RequestArgTruthy = Callable[..., bool]
PublicationSummarySourceMetadataBuilder = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


def _publication_run_semantics(row: dict[str, Any], *, publishable: bool, published: bool) -> Any:
    explicit = row.get("run_semantics")
    if explicit not in (None, ""):
        return explicit
    if published:
        return "formal_publication"
    if publishable:
        return "publishable_snapshot"
    return "diagnostic_only"


def _publication_status(row: dict[str, Any], *, publishable: bool, published: bool) -> Any:
    explicit = row.get("publication_status")
    if explicit not in (None, ""):
        return explicit
    if published:
        return "current_published"
    if publishable:
        return "publishable_not_current"
    return "not_publishable"


def build_publication_registry_summary(publication: dict[str, Any] | None) -> dict[str, Any]:
    current = dict(publication or {})
    archived_superseded = current.get("archived_superseded_runs")
    canonical_runs: list[dict[str, Any]] = []
    for item in current.get("canonical_runs") or []:
        row = dict(item or {})
        snapshot = dict(row.get("snapshot") or {})
        published = bool(row.get("published"))
        publishable = bool(row.get("publishable")) or published
        canonical_runs.append(
            {
                "run_id": row.get("run_id"),
                "group_key": row.get("group_key"),
                "published": published,
                "publishable": publishable,
                "publish_hold": bool(row.get("publish_hold")),
                "publish_blocked": bool(row.get("publish_blocked")) or not publishable,
                "publish_blockers": [
                    dict(blocker)
                    for blocker in (row.get("publish_blockers") or [])
                    if isinstance(blocker, dict)
                ],
                "run_semantics": _publication_run_semantics(
                    row,
                    publishable=publishable,
                    published=published,
                ),
                "publication_status": _publication_status(
                    row,
                    publishable=publishable,
                    published=published,
                ),
                "publish_state": row.get("publish_state"),
                "generated_at": row.get("generated_at"),
                "snapshot": {
                    "candidate_date": snapshot.get("candidate_date"),
                    "candidate_fund_count": snapshot.get("candidate_fund_count"),
                    "coverage_ratio_vs_total": snapshot.get("coverage_ratio_vs_total"),
                },
                "superseded_run_ids": list(row.get("superseded_run_ids") or []),
            }
        )
    return {
        "latest_canonical_run_id": current.get("latest_canonical_run_id"),
        "current_published_run_id": current.get("current_published_run_id"),
        "latest_canonical_report_path": current.get("latest_canonical_report_path"),
        "canonical_run_count": len(canonical_runs),
        "archived_superseded_run_count": (
            len(archived_superseded) if isinstance(archived_superseded, list) else None
        ),
        "canonical_runs": canonical_runs,
    }


def register_publication_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    load_publication_registry: PublicationLoader,
    request_arg_truthy: RequestArgTruthy,
    build_publication_summary_source_metadata: PublicationSummarySourceMetadataBuilder | None = None,
) -> None:
    def _attach_source_metadata(
        publication: dict[str, Any],
        summary: dict[str, Any],
    ) -> dict[str, Any]:
        if build_publication_summary_source_metadata is None:
            return summary
        metadata = build_publication_summary_source_metadata(publication, summary)
        if metadata:
            summary.update(metadata)
        return summary

    @app.get("/api/publication", endpoint="publication_registry")
    def publication_registry() -> Response:
        ensure_scheduler()
        publication = load_publication_registry()
        if request_arg_truthy("full", "detail"):
            return jsonify(publication)
        summary = build_publication_registry_summary(publication)
        summary["payload_scope"] = {
            "mode": "summary",
            "full_endpoint": "/api/publication?full=1",
            "omitted_fields": [
                "automation_layers",
                "publication_integrity",
                "superseded_runs",
                "archived_superseded_runs",
            ],
        }
        return jsonify(_attach_source_metadata(publication, summary))

    @app.get("/api/publication/summary", endpoint="publication_registry_summary")
    def publication_registry_summary() -> Response:
        ensure_scheduler()
        publication = load_publication_registry()
        summary = build_publication_registry_summary(publication)
        return jsonify(_attach_source_metadata(publication, summary))
