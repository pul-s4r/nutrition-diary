from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 2


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
          version INTEGER NOT NULL
        )
        """
    )
    row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
        conn.commit()
        _create_schema_v2(conn)
        return

    current = int(row["version"])
    if current == SCHEMA_VERSION:
        return
    if current == 1:
        _migrate_v1_to_v2(conn)
        return

    raise RuntimeError(
        f"Unsupported schema version {current}; expected {SCHEMA_VERSION}. "
        "Implement migrations before continuing."
    )


def _meal_cluster_column_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(meal_clusters)").fetchall()
    return {str(r[1]) for r in rows}


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    cols = _meal_cluster_column_names(conn)
    if "earliest_taken_at" not in cols:
        conn.execute("ALTER TABLE meal_clusters ADD COLUMN earliest_taken_at TEXT")
    if "latest_taken_at" not in cols:
        conn.execute("ALTER TABLE meal_clusters ADD COLUMN latest_taken_at TEXT")
    conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))
    conn.commit()


def _create_schema_v2(conn: sqlite3.Connection) -> None:
    # Stage runner status (per stage, per item_key)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stage_runs (
          stage_name   TEXT NOT NULL,
          item_key     TEXT NOT NULL,
          status       TEXT NOT NULL,  -- pending|success|failed
          attempts     INTEGER NOT NULL DEFAULT 0,
          last_error   TEXT,
          last_run_at  INTEGER,
          PRIMARY KEY(stage_name, item_key)
        )
        """
    )

    # Stage 1: source
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS photos (
          photo_hash      TEXT PRIMARY KEY,
          source_adapter  TEXT NOT NULL,
          source_ref      TEXT NOT NULL,
          local_blob_path TEXT NOT NULL,
          discovered_at   INTEGER NOT NULL
        )
        """
    )

    # Stage 2: metadata
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS photo_metadata (
          photo_hash   TEXT PRIMARY KEY REFERENCES photos(photo_hash) ON DELETE CASCADE,
          taken_at     TEXT,           -- ISO8601
          tz           TEXT,           -- IANA zone name
          gps_lat      REAL,
          gps_lon      REAL,
          orientation  INTEGER,
          make         TEXT,
          model        TEXT
        )
        """
    )

    # Stage 3: cluster
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meal_clusters (
          cluster_id         TEXT PRIMARY KEY,
          date               TEXT NOT NULL,  -- YYYY-MM-DD
          meal_type          TEXT NOT NULL,
          earliest_taken_at  TEXT,           -- ISO8601, min photo time in cluster
          latest_taken_at    TEXT            -- ISO8601, max photo time in cluster
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meal_photos (
          cluster_id  TEXT NOT NULL REFERENCES meal_clusters(cluster_id) ON DELETE CASCADE,
          photo_hash  TEXT NOT NULL REFERENCES photos(photo_hash) ON DELETE CASCADE,
          PRIMARY KEY(cluster_id, photo_hash)
        )
        """
    )

    # Stage 4: recognize (LLM output only, raw + parsed)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_results (
          photo_hash          TEXT PRIMARY KEY REFERENCES photos(photo_hash) ON DELETE CASCADE,
          model_id            TEXT NOT NULL,
          raw_json            TEXT NOT NULL,
          identification_json TEXT,      -- nullable if not_food
          confidence          REAL,
          tokens_in           INTEGER,
          tokens_out          INTEGER,
          cost_est            REAL,
          latency_ms          INTEGER,
          created_at          INTEGER NOT NULL
        )
        """
    )

    # Stage 5: ground
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS grounding_results (
          photo_hash     TEXT PRIMARY KEY REFERENCES photos(photo_hash) ON DELETE CASCADE,
          source         TEXT,
          fdc_id         TEXT,
          matched_name   TEXT,
          match_conf     REAL,
          per_100g_json  TEXT,
          scaled_json    TEXT,
          created_at     INTEGER NOT NULL
        )
        """
    )

    # Shared USDA cache (also used by grounding stage)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usda_cache (
          cache_key        TEXT PRIMARY KEY,
          fdc_id           TEXT,
          matched_name     TEXT,
          match_confidence REAL,
          data_type        TEXT,
          nutrients_json   TEXT,
          cached_at        INTEGER NOT NULL,
          ttl_seconds      INTEGER
        )
        """
    )

    # Stage 6: assemble
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS diary_entries (
          entry_id          TEXT PRIMARY KEY,
          date              TEXT NOT NULL,
          meal_type         TEXT NOT NULL,
          items_json        TEXT NOT NULL,
          overall_confidence REAL,
          source_cluster_id TEXT NOT NULL REFERENCES meal_clusters(cluster_id) ON DELETE CASCADE,
          approved          INTEGER NOT NULL DEFAULT 1
        )
        """
    )

    # Stage 7: export + Stage 8: upload queue
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS upload_queue (
          entry_id     TEXT NOT NULL REFERENCES diary_entries(entry_id) ON DELETE CASCADE,
          target       TEXT NOT NULL,
          status       TEXT NOT NULL, -- pending|success|failed|skipped
          attempts     INTEGER NOT NULL DEFAULT 0,
          last_error   TEXT,
          updated_at   INTEGER NOT NULL,
          PRIMARY KEY(entry_id, target)
        )
        """
    )

    conn.commit()

