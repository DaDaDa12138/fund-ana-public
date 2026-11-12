"""Exact-byte shared decision metrics, with legacy TEXT and archive compatibility.

References live beside the decision table so the canonical ten decision columns
and historical Parquet schema do not change. Transactions are owned by callers.
"""
from __future__ import annotations

import hashlib
import sqlite3
from typing import Sequence

PAYLOADS = "screening_decision_payloads"
REFS = "screening_decision_payload_refs"
FORMAT = "screening_decision_payload_format"
TABLES = {PAYLOADS, REFS, FORMAT}
BATCH_SIZE = 700


class DecisionPayloadError(ValueError):
    pass


def _ident(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise DecisionPayloadError("unsafe_payload_identifier")
    return '"' + value + '"'


def _chunks(values: Sequence, size: int = BATCH_SIZE):
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def storage_available(connection: sqlite3.Connection, schema: str = "main") -> bool:
    schema_sql = _ident(schema)
    names = {row[0] for row in connection.execute(
        f"SELECT name FROM {schema_sql}.sqlite_master WHERE type='table' AND name IN (?,?,?)", tuple(sorted(TABLES)))}
    if not names:
        return False
    if names != TABLES:
        raise DecisionPayloadError("incomplete_decision_payload_storage")
    versions = connection.execute(f"SELECT version FROM {schema_sql}.{FORMAT}").fetchall()
    if len(versions) != 1 or versions[0][0] != 1:
        raise DecisionPayloadError("unsupported_decision_payload_storage")
    return True


def begin_write(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")


def begin_read(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        connection.execute("BEGIN")


def ensure_storage(connection: sqlite3.Connection) -> None:
    """Initialize atomically, leaving commit/rollback to the caller."""
    begin_write(connection)
    available = storage_available(connection)
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='screening_decisions'").fetchone():
        raise DecisionPayloadError("screening_decisions_table_required")
    if not available:
        connection.execute(f"CREATE TABLE {PAYLOADS} (id INTEGER PRIMARY KEY, sha256 BLOB NOT NULL UNIQUE "
                           "CHECK(typeof(sha256)='blob' AND length(sha256)=32), "
                           "payload TEXT NOT NULL CHECK(typeof(payload)='text'))")
        connection.execute(f"CREATE TABLE {REFS} (decision_id INTEGER PRIMARY KEY "
                           "REFERENCES screening_decisions(id) ON DELETE CASCADE, "
                           f"payload_id INTEGER NOT NULL REFERENCES {PAYLOADS}(id))")
        connection.execute(f"CREATE INDEX idx_decision_payload_refs_payload ON {REFS}(payload_id)")
        connection.execute(f"CREATE TABLE {FORMAT} (version INTEGER NOT NULL CHECK(version=1))")
        connection.execute(f"INSERT INTO {FORMAT}(version) VALUES(1)")
    # The triggers protect raw sqlite3 clients too, including connections whose
    # foreign_keys pragma is off. No trigger changes decision INSERT row counts.
    statements = [
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_immutable BEFORE UPDATE ON {PAYLOADS}
            WHEN NEW.id IS NOT OLD.id OR NEW.sha256 IS NOT OLD.sha256 OR NEW.payload IS NOT OLD.payload
            BEGIN SELECT RAISE(ABORT,'decision_payload_is_immutable'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_referenced BEFORE DELETE ON {PAYLOADS}
            WHEN EXISTS(SELECT 1 FROM {REFS} WHERE payload_id=OLD.id)
            BEGIN SELECT RAISE(ABORT,'decision_payload_still_referenced'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_ref_valid BEFORE INSERT ON {REFS}
            WHEN NOT EXISTS(SELECT 1 FROM screening_decisions d JOIN {PAYLOADS} p ON p.id=NEW.payload_id
                WHERE d.id=NEW.decision_id AND (d.metrics_json IS NULL OR d.metrics_json=p.payload))
            BEGIN SELECT RAISE(ABORT,'decision_payload_reference_invalid'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_ref_immutable BEFORE UPDATE ON {REFS}
            WHEN NEW.decision_id IS NOT OLD.decision_id OR NEW.payload_id IS NOT OLD.payload_id
            BEGIN SELECT RAISE(ABORT,'decision_payload_reference_is_immutable'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_ref_restore_before_delete BEFORE DELETE ON {REFS}
            WHEN EXISTS(SELECT 1 FROM screening_decisions WHERE id=OLD.decision_id AND metrics_json IS NULL)
            BEGIN SELECT RAISE(ABORT,'restore_inline_payload_before_removing_reference'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_delete_refs AFTER DELETE ON screening_decisions
            BEGIN DELETE FROM {REFS} WHERE decision_id=OLD.id; END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_update_inline BEFORE UPDATE OF metrics_json ON screening_decisions
            WHEN NEW.metrics_json IS NOT NULL AND EXISTS(SELECT 1 FROM {REFS} r JOIN {PAYLOADS} p ON p.id=r.payload_id
                WHERE r.decision_id=OLD.id AND NEW.metrics_json IS NOT p.payload)
            BEGIN SELECT RAISE(ABORT,'decision_inline_payload_conflicts_with_reference'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS decision_payload_stable_id BEFORE UPDATE OF id ON screening_decisions
            WHEN NEW.id IS NOT OLD.id AND EXISTS(SELECT 1 FROM {REFS} WHERE decision_id=OLD.id)
            BEGIN SELECT RAISE(ABORT,'referenced_decision_id_is_immutable'); END""",
    ]
    for statement in statements:
        connection.execute(statement)


def _digest(value: str) -> bytes:
    if not isinstance(value, str):
        raise DecisionPayloadError("decision_payload_text_required")
    return hashlib.sha256(value.encode("utf-8")).digest()


def _verify_payload(value, digest) -> str:
    if not isinstance(value, str) or not isinstance(digest, bytes) or _digest(value) != digest:
        raise DecisionPayloadError("decision_payload_sha256_mismatch")
    return value


def intern_payloads(connection: sqlite3.Connection, values: Sequence[str | None]) -> list[int | None]:
    unique: dict[bytes, str] = {}
    for value in values:
        if value is not None:
            digest = _digest(value)
            if digest in unique and unique[digest] != value:
                raise DecisionPayloadError("decision_payload_digest_collision")
            unique[digest] = value
    connection.executemany(f"INSERT INTO {PAYLOADS}(sha256,payload) VALUES(?,?) ON CONFLICT(sha256) DO NOTHING",
                           list(unique.items()))
    found = {}
    for chunk in _chunks(list(unique)):
        marks = ",".join("?" for _ in chunk)
        for row_id, digest, value in connection.execute(f"SELECT id,sha256,payload FROM {PAYLOADS} WHERE sha256 IN ({marks})", chunk):
            _verify_payload(value, digest)
            if value != unique[digest]:
                raise DecisionPayloadError("decision_payload_digest_collision")
            found[digest] = row_id
    if set(found) != set(unique):
        raise DecisionPayloadError("decision_payload_intern_incomplete")
    return [None if value is None else found[_digest(value)] for value in values]


def resolve_metric_texts(connection: sqlite3.Connection, rows: Sequence[tuple[int, str | None]],
                         schema: str = "main") -> list[str | None]:
    """Batch-resolve while preserving NULL, empty text and original JSON bytes."""
    if not storage_available(connection, schema):
        return [value for _, value in rows]
    qualified = _ident(schema)
    found = {}
    checked = {}
    for chunk in _chunks(list(dict.fromkeys(row_id for row_id, _ in rows))):
        marks = ",".join("?" for _ in chunk)
        for decision_id, payload_id, value, digest in connection.execute(
            f"SELECT r.decision_id,r.payload_id,p.payload,p.sha256 FROM {qualified}.{REFS} r "
            f"LEFT JOIN {qualified}.{PAYLOADS} p ON p.id=r.payload_id WHERE r.decision_id IN ({marks})", chunk,
        ):
            if payload_id not in checked:
                checked[payload_id] = _verify_payload(value, digest)
            elif checked[payload_id] != value:
                raise DecisionPayloadError("decision_payload_changed_during_read")
            found[decision_id] = value
    result = []
    for row_id, inline in rows:
        if row_id in found:
            if inline is not None and inline != found[row_id]:
                raise DecisionPayloadError("decision_inline_payload_mismatch")
            result.append(found[row_id])
        else:
            result.append(inline)
    return result


def canonical_source(connection: sqlite3.Connection, table: str, columns: Sequence[str],
                     *, schema: str = "main", alias: str = "h") -> tuple[str, str]:
    """Return the complete legacy projection and FROM clause for archival."""
    table_sql, schema_sql, alias_sql = _ident(table), _ident(schema), _ident(alias)
    source = f"{schema_sql}.{table_sql} {alias_sql}"
    pick_snapshot = table == "screening_picks" and "snapshot_json" in columns
    if pick_snapshot:
        from .screening_pick_payloads import SQL_DECODER, register_sql_decoder

        register_sql_decoder(connection)
    shared = table == "screening_decisions" and storage_available(connection, schema)
    if shared:
        source += (f" LEFT JOIN {schema_sql}.{REFS} payload_ref ON payload_ref.decision_id={alias_sql}.id"
                   f" LEFT JOIN {schema_sql}.{PAYLOADS} payload_value ON payload_value.id=payload_ref.payload_id")
    expressions = []
    for column in columns:
        column_sql = _ident(column)
        if pick_snapshot and column == "snapshot_json":
            expressions.append(f"CAST({SQL_DECODER}({alias_sql}.{column_sql}) AS TEXT) AS {column_sql}")
        elif shared and column == "metrics_json":
            expressions.append(f"CAST(CASE WHEN payload_ref.decision_id IS NOT NULL THEN payload_value.payload "
                               f"ELSE {alias_sql}.{column_sql} END AS TEXT) AS {column_sql}")
        else:
            expressions.append(f"{alias_sql}.{column_sql}")
    return ",".join(expressions), source


def verify_reference_scope(connection: sqlite3.Connection, run_ids: Sequence[str], *, schema: str = "main") -> int:
    """Fail before an archive can turn a broken reference into SQL NULL."""
    begin_read(connection)
    if not storage_available(connection, schema):
        return 0
    qualified = _ident(schema)
    verified = 0
    for chunk in _chunks(list(dict.fromkeys(run_ids))):
        marks = ",".join("?" for _ in chunk)
        cursor = connection.execute(
            f"SELECT d.id,d.metrics_json FROM {qualified}.screening_decisions d "
            f"JOIN {qualified}.{REFS} r ON r.decision_id=d.id WHERE d.run_id IN ({marks})", chunk)
        while rows := cursor.fetchmany(BATCH_SIZE):
            resolve_metric_texts(connection, rows, schema)
            verified += len(rows)
    return verified


def share_existing_rows(connection: sqlite3.Connection, rows: Sequence[tuple[int, str | None]],
                        *, remove_inline: bool) -> dict:
    """Share an already selected page, verify originals, then optionally retire TEXT."""
    expected = resolve_metric_texts(connection, rows)
    active = [(row_id, inline) for row_id, inline in rows if inline is not None]
    payload_ids = intern_payloads(connection, [inline for _, inline in active])
    before = connection.total_changes
    connection.executemany(f"INSERT INTO {REFS}(decision_id,payload_id) VALUES(?,?) ON CONFLICT(decision_id) DO NOTHING",
                           [(row[0], payload_id) for row, payload_id in zip(active, payload_ids)])
    inserted = connection.total_changes - before
    if resolve_metric_texts(connection, rows) != expected:
        raise DecisionPayloadError("shared_decision_payload_values_changed")
    retired = 0
    if remove_inline:
        cursor = connection.executemany("UPDATE screening_decisions SET metrics_json=NULL WHERE id=? AND metrics_json=?", active)
        retired = cursor.rowcount
        if retired != len(active):
            raise DecisionPayloadError("decision_payload_retirement_incomplete")
    actual = {}
    for chunk in _chunks([row_id for row_id, _ in rows]):
        marks = ",".join("?" for _ in chunk)
        actual.update(connection.execute(
            f"SELECT id,metrics_json FROM screening_decisions WHERE id IN ({marks})", chunk))
    if any(row_id not in actual for row_id, _ in rows):
        raise DecisionPayloadError("shared_decision_row_missing")
    after = [(row_id, actual[row_id]) for row_id, _ in rows]
    if (remove_inline and any(value is not None for _, value in after)) or resolve_metric_texts(connection, after) != expected:
        raise DecisionPayloadError("stored_decision_payload_values_changed")
    return {"selected_rows": len(rows), "references_inserted": inserted, "inline_payloads_retired": retired,
            "all_original_payloads_match": True}


def collect_unreferenced_payloads(connection: sqlite3.Connection, limit: int = 5000) -> int:
    if not storage_available(connection):
        return 0
    if not 1 <= limit <= 5000:
        raise DecisionPayloadError("invalid_payload_collection_limit")
    return connection.execute(
        f"DELETE FROM {PAYLOADS} WHERE id IN (SELECT p.id FROM {PAYLOADS} p "
        f"WHERE NOT EXISTS(SELECT 1 FROM {REFS} r WHERE r.payload_id=p.id) ORDER BY p.id LIMIT ?)", (limit,)
    ).rowcount


def restore_inline_rows(connection: sqlite3.Connection, rows: Sequence[tuple[int, str | None]]) -> dict:
    """Restore exact legacy TEXT before removing any reference (rollback path)."""
    expected = resolve_metric_texts(connection, rows)
    if not storage_available(connection):
        return {"selected_rows": len(rows), "inline_payloads_restored": 0, "references_removed": 0}
    writes = [(value, row_id) for (row_id, inline), value in zip(rows, expected)
              if inline is None and value is not None]
    changed = connection.executemany("UPDATE screening_decisions SET metrics_json=? WHERE id=? AND metrics_json IS NULL", writes).rowcount
    if writes and changed != len(writes):
        raise DecisionPayloadError("decision_payload_restore_incomplete")
    removed = 0
    for chunk in _chunks([row_id for row_id, _ in rows]):
        marks = ",".join("?" for _ in chunk)
        removed += connection.execute(f"DELETE FROM {REFS} WHERE decision_id IN ({marks})", chunk).rowcount
    actual = []
    for chunk in _chunks([row_id for row_id, _ in rows]):
        marks = ",".join("?" for _ in chunk)
        actual.extend(connection.execute(f"SELECT id,metrics_json FROM screening_decisions WHERE id IN ({marks})", chunk))
    values = dict(actual)
    if any(row_id not in values for row_id, _ in rows) or [values[row_id] for row_id, _ in rows] != expected:
        raise DecisionPayloadError("restored_decision_payload_values_changed")
    return {"selected_rows": len(rows), "inline_payloads_restored": len(writes), "references_removed": removed,
            "all_original_payloads_match": True}
