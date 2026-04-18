from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol, Sequence

import sqlite3

from nutrition_diary.config import Settings


@dataclass(frozen=True)
class StageScope:
    since_date: str | None = None  # YYYY-MM-DD
    photo_hashes: Sequence[str] | None = None
    cluster_id: str | None = None
    entry_id: str | None = None


@dataclass(frozen=True)
class StageContext:
    db: sqlite3.Connection
    settings: Settings
    dump_dir: Path | None = None
    force: bool = False
    dry_run: bool = False


@dataclass
class StageError:
    item_key: str
    error: str


@dataclass
class StageResult:
    stage: str
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[StageError] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []


class Stage(Protocol):
    name: str

    def select_work(self, ctx: StageContext, scope: StageScope) -> Iterable[str]: ...

    def run_one(self, ctx: StageContext, item_key: str) -> dict | None: ...


def _now_ts() -> int:
    return int(time.time())


def dump_json(ctx: StageContext, stage_name: str, item_key: str, payload: dict) -> None:
    if ctx.dump_dir is None:
        return
    out_dir = ctx.dump_dir / stage_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{item_key}.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _get_stage_status(ctx: StageContext, stage_name: str, item_key: str) -> str | None:
    row = ctx.db.execute(
        "SELECT status FROM stage_runs WHERE stage_name=? AND item_key=?",
        (stage_name, item_key),
    ).fetchone()
    return None if row is None else str(row["status"])


def _set_stage_status(
    ctx: StageContext,
    stage_name: str,
    item_key: str,
    status: str,
    error: str | None = None,
) -> None:
    now = _now_ts()
    ctx.db.execute(
        """
        INSERT INTO stage_runs(stage_name, item_key, status, attempts, last_error, last_run_at)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(stage_name, item_key) DO UPDATE SET
          status=excluded.status,
          attempts=stage_runs.attempts+1,
          last_error=excluded.last_error,
          last_run_at=excluded.last_run_at
        """,
        (stage_name, item_key, status, error, now),
    )


def run_stage(stage: Stage, ctx: StageContext, scope: StageScope) -> StageResult:
    result = StageResult(stage=stage.name)
    for item_key in stage.select_work(ctx, scope):
        prior = _get_stage_status(ctx, stage.name, item_key)
        if not ctx.force and prior == "success":
            result.skipped += 1
            continue

        if ctx.dry_run:
            result.skipped += 1
            continue

        try:
            payload = stage.run_one(ctx, item_key)
            _set_stage_status(ctx, stage.name, item_key, "success", None)
            ctx.db.commit()
            if payload is not None:
                dump_json(ctx, stage.name, item_key, payload)
            result.processed += 1
        except Exception as e:  # noqa: BLE001
            ctx.db.rollback()
            _set_stage_status(ctx, stage.name, item_key, "failed", str(e))
            ctx.db.commit()
            result.failed += 1
            result.errors.append(StageError(item_key=item_key, error=str(e)))
    return result


def stage_status_summary(db: sqlite3.Connection, stage_name: str | None = None) -> list[dict]:
    if stage_name is None:
        rows = db.execute(
            """
            SELECT stage_name, status, COUNT(*) AS n
            FROM stage_runs
            GROUP BY stage_name, status
            ORDER BY stage_name, status
            """
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT stage_name, status, COUNT(*) AS n
            FROM stage_runs
            WHERE stage_name=?
            GROUP BY stage_name, status
            ORDER BY stage_name, status
            """,
            (stage_name,),
        ).fetchall()
    return [dict(r) for r in rows]

