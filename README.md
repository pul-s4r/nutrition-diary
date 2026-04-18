# Nutrition Diary

Stage-by-stage pipeline that turns meal photos into structured diary entries.

## Quickstart (local filesystem, mock recognition)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Ingest from a directory of images into local SQLite + JSONL exports
nd ingest --source local --path /path/to/photos --since 2026-01-01 --dump

# Show per-stage progress
nd status

# Re-run just one stage (example: grounding) without re-running recognition
nd stage ground --since 2026-01-01
```

## Data locations

- `data/nutrition.db`: SQLite state store (stage outputs + stage run status + caches + upload queue)
- `data/exports/`: canonical JSONL exports (Stage 7)
- `data/stages/<stage>/`: optional per-item JSON dumps (`--dump`)
