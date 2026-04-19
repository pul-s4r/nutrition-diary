from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from nutrition_diary.config import Settings
from nutrition_diary.db import connect, migrate
from nutrition_diary.sources.local import LocalPhotoSource
from nutrition_diary.stages import (
    AssembleStage,
    ClusterStage,
    ExportStage,
    GroundStage,
    MetadataStage,
    RecognizeStage,
    SourceStage,
    StageContext,
    StageScope,
    UploadStage,
    run_stage,
    stage_status_summary,
)

app = typer.Typer(no_args_is_help=True)
console = Console()


def _ctx(
    settings: Settings,
    *,
    dump: bool,
    force: bool,
    dry_run: bool,
) -> StageContext:
    db = connect(settings.db_path)
    migrate(db)
    dump_dir = settings.stages_dump_dir if dump else None
    return StageContext(db=db, settings=settings, dump_dir=dump_dir, force=force, dry_run=dry_run)


@app.command()
def ingest(
    source: str = typer.Option("local", "--source"),
    path: Path = typer.Option(..., "--path", exists=True, file_okay=False, dir_okay=True),
    since: Optional[str] = typer.Option(None, "--since", help="YYYY-MM-DD"),
    dump: bool = typer.Option(False, "--dump"),
    force: bool = typer.Option(False, "--force"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    settings = Settings()
    ctx = _ctx(settings, dump=dump, force=force, dry_run=dry_run)
    scope = StageScope(since_date=since)

    if source != "local":
        raise typer.BadParameter("Only --source local is supported in v1 implementation.")

    source_stage = SourceStage(source=LocalPhotoSource(), root=path)
    stages = [
        source_stage,
        MetadataStage(),
        ClusterStage(),
        RecognizeStage(),
        GroundStage(),
        AssembleStage(),
        ExportStage(),
    ]

    for st in stages:
        res = run_stage(st, ctx, scope)
        console.print(
            f"[bold]{res.stage}[/bold]: processed={res.processed} skipped={res.skipped} failed={res.failed}"
        )


@app.command()
def upload(
    target: str = typer.Option("csv", "--target"),
    entry_id: Optional[str] = typer.Option(None, "--entry"),
    dump: bool = typer.Option(False, "--dump"),
    force: bool = typer.Option(False, "--force"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    settings = Settings()
    ctx = _ctx(settings, dump=dump, force=force, dry_run=dry_run)
    scope = StageScope(entry_id=entry_id)

    res = run_stage(UploadStage(target=target), ctx, scope)
    console.print(
        f"[bold]{res.stage}[/bold]: processed={res.processed} skipped={res.skipped} failed={res.failed}"
    )


@app.command("stage")
def run_one_stage(
    stage_name: str = typer.Argument(...),
    path: Optional[Path] = typer.Option(None, "--path", help="Required for stage 'source'"),
    since: Optional[str] = typer.Option(None, "--since", help="YYYY-MM-DD"),
    photo: list[str] = typer.Option([], "--photo", help="Photo hash (repeatable)"),
    cluster_id: Optional[str] = typer.Option(None, "--meal"),
    entry_id: Optional[str] = typer.Option(None, "--entry"),
    target: str = typer.Option("csv", "--target"),
    dump: bool = typer.Option(False, "--dump"),
    force: bool = typer.Option(False, "--force"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    settings = Settings()
    ctx = _ctx(settings, dump=dump, force=force, dry_run=dry_run)
    scope = StageScope(
        since_date=since, photo_hashes=photo or None, cluster_id=cluster_id, entry_id=entry_id
    )

    stage_map = {
        "metadata": MetadataStage(),
        "cluster": ClusterStage(),
        "recognize": RecognizeStage(),
        "ground": GroundStage(),
        "assemble": AssembleStage(),
        "export": ExportStage(target=target),
        "upload": UploadStage(target=target),
    }

    if stage_name == "source":
        if path is None:
            raise typer.BadParameter("--path is required for stage 'source'")
        st = SourceStage(source=LocalPhotoSource(), root=path)
    else:
        st = stage_map.get(stage_name)
        if st is None:
            raise typer.BadParameter(f"Unknown stage: {stage_name}")

    res = run_stage(st, ctx, scope)
    console.print(
        f"[bold]{res.stage}[/bold]: processed={res.processed} skipped={res.skipped} failed={res.failed}"
    )


@app.command()
def status(stage: Optional[str] = typer.Option(None, "--stage")) -> None:
    settings = Settings()
    ctx = _ctx(settings, dump=False, force=False, dry_run=False)
    summary = stage_status_summary(ctx.db, stage_name=stage)

    table = Table(title="Stage status summary")
    table.add_column("stage")
    table.add_column("status")
    table.add_column("count", justify="right")
    for row in summary:
        table.add_row(str(row["stage_name"]), str(row["status"]), str(row["n"]))
    console.print(table)
