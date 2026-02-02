#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Standalone Analytics Dashboard server (port 5555).

- Uses UI/UX from analytics_templates/analytics_dashboard.html (copied from Analytics_Dashboard.html)
- Scans Oberon L10 zip logs into SQLite cache (raw table)
- Computes fast aggregates from SQLite for UI (daily/weekly/monthly + summary matrix + SKU table)

This server is intentionally separate from app.py / daily_test_app.py.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytz
from flask import Flask, Response, jsonify, render_template, request


# -----------------------------
# Config
# -----------------------------

BASE_PATH = r"\\10.16.137.111\Oberon\L10"

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYTICS_CACHE_DIR = os.path.join(APP_DIR, "analytics_cache")
DB_PATH = os.path.join(ANALYTICS_CACHE_DIR, "analytics.db")
STATE_PATH = os.path.join(ANALYTICS_CACHE_DIR, "raw_state.json")

CA_TZ = pytz.timezone("America/Los_Angeles")
TW_TZ = pytz.timezone("Asia/Taipei")

AUTO_SCAN_EVERY_SECONDS = 5 * 60
AUTO_SCAN_OVERLAP_MINUTES = 10
RETENTION_DAYS = 90

# IMPORTANT:
# Oberon zip filenames end with "...YYYYMMDDTHHMMSSZ" but in this environment the timestamp
# should be treated as *California local time* (PST/PDT), matching the hourly report logic.
# If you change this mode, cached rows must be rebuilt because ca_date/ca_ms will change.
TIMESTAMP_MODE = "ca_local_suffix_v2"

# Final pass rules
PASS_AT_FCT_PART_NUMBERS = {
    "675-24109-0010-TS2",
}


# -----------------------------
# Flask app
# -----------------------------

app = Flask(
    __name__,
    template_folder=os.path.join(APP_DIR, "analytics_templates"),
)


# -----------------------------
# Auto scan / retention status (shared with UI)
# -----------------------------


auto_status_lock = threading.Lock()
next_auto_scan_ms: Optional[int] = None
last_retention_cleanup_ms: Optional[int] = None


# -----------------------------
# Utilities
# -----------------------------


def ensure_dirs() -> None:
    os.makedirs(ANALYTICS_CACHE_DIR, exist_ok=True)


def utc_ms(dt: datetime) -> int:
    """Convert aware datetime to epoch milliseconds."""
    if dt.tzinfo is None:
        raise ValueError("utc_ms expects tz-aware datetime")
    return int(dt.timestamp() * 1000)


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """
    Parse timestamp from suffix like: 20260107T160130Z.

    NOTE: Even though the suffix ends with "Z", for this project we treat it as
    California local time (PST/PDT), matching the hourly report implementation.

    Returns tz-aware CA datetime.
    """
    try:
        m = re.search(r"(\d{8})T(\d{6})Z", filename)
        if not m:
            return None
        dt_naive = datetime.strptime(f"{m.group(1)}T{m.group(2)}", "%Y%m%dT%H%M%S")
        return CA_TZ.localize(dt_naive)
    except Exception:
        return None


def convert_to_ca_time(dt: datetime) -> datetime:
    """
    Convert a datetime to CA tz-aware datetime.

    - If dt is naive, assume it is already CA local time and localize it.
    - If dt is tz-aware, convert it to CA timezone.
    """
    if dt.tzinfo is None:
        return CA_TZ.localize(dt)
    return dt.astimezone(CA_TZ)


def ca_fields_from_utc(utc_dt: datetime) -> Tuple[int, str, int, str, str]:
    """
    Return:
    - ca_ms (epoch ms of the instant)
    - ca_date (YYYY-MM-DD in CA)
    - ca_hour (0-23 in CA)
    - ca_week (YYYY-Www ISO week in CA)
    - ca_month (YYYY-MM in CA)
    """
    # Despite the name, accept any tz-aware datetime and normalize to CA.
    ca_dt = convert_to_ca_time(utc_dt)
    ca_ms = utc_ms(ca_dt)
    ca_date = ca_dt.strftime("%Y-%m-%d")
    ca_hour = int(ca_dt.strftime("%H"))
    iso_year, iso_week, _ = ca_dt.isocalendar()
    ca_week = f"{iso_year}-W{int(iso_week):02d}"
    ca_month = ca_dt.strftime("%Y-%m")
    return ca_ms, ca_date, ca_hour, ca_week, ca_month


def parse_source_token(filename: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Parse Bonepile marker token right after "IGSJ_".
    Examples:
    - IGSJ_NA_... => is_bonepile=0
    - IGSJ_PB-71108_... => is_bonepile=1, pb_id="PB-71108"
    """
    try:
        m = re.match(r"^IGSJ_([^_]+)_", filename)
        if not m:
            return None, None
        token = (m.group(1) or "").strip()
        token_upper = token.upper()
        if token_upper == "NA":
            return 0, None
        if token_upper.startswith("PB-"):
            return 1, token
        return None, None
    except Exception:
        return None, None


def extract_part_number_from_filename(filename: str) -> str:
    """
    Extract part number from filename (normalized). Examples:
    IGSJ_PB-71108_675-24109-0002-TS1_1830126000087_P_FLA_20260107T163248Z.zip
      => 675-24109-0002-TS1
    """
    name = filename.replace(".zip", "")
    # PB-XXXX_XXX-XXXXX-XXXX-TS<NUM>
    m = re.search(r"PB-\d+_(\d+-\d+-\d+-TS\d+)", name)
    if m:
        return m.group(1)
    # PB-XXXX_XXX-XXXXX-XXXX
    m = re.search(r"PB-\d+_(\d+-\d+-\d+)", name)
    if m:
        return m.group(1)
    # XXX-XXXXX-XXXX-TS<NUM>
    m = re.search(r"(\d+-\d+-\d+-TS\d+)", name)
    if m:
        return m.group(1)
    # XXX-XXXXX-XXXX
    m = re.search(r"(\d+-\d+-\d+)", name)
    if m:
        return m.group(1)
    return "Unknown"


def parse_test_filename(filename: str) -> Optional[Tuple[str, str, str, str]]:
    """
    Parse filename to extract:
      SN (string of digits)
      status: 'F' or 'P'
      station: e.g. RIN, FLA...
      part_number: extracted from file name
    """
    name = filename.replace(".zip", "")
    part_number = extract_part_number_from_filename(filename)

    # Pattern 1: _SN_Status_Station_
    m = re.search(r"_(\d{10,})_([FP])_([A-Z0-9]+)_", name)
    if m:
        sn, status, station = m.group(1), m.group(2), m.group(3)
        if sn.startswith("18") and len(sn) == 13:
            return sn, status, station, part_number

    # Pattern 2: find SN anywhere then find _Status_Station_
    sn_match = re.search(r"(18\d{11})", name)
    if sn_match:
        sn = sn_match.group(1)
        after_sn = name[name.find(sn) + len(sn) :]
        m2 = re.search(r"_([FP])_([A-Z0-9]+)_", after_sn)
        if m2:
            status, station = m2.group(1), m2.group(2)
            return sn, status, station, part_number

    return None


def get_pass_station_for_part_number(part_number: str) -> str:
    pn = "" if part_number is None else str(part_number).strip().upper()
    if pn in PASS_AT_FCT_PART_NUMBERS:
        return "FCT"
    if "TS2" in pn:
        return "NVL"
    return "FCT"


def is_final_pass(sn_status: str, station: str, part_number: str) -> bool:
    """Final pass for hourly-style tray logic (no legacy fallback here)."""
    if sn_status != "P":
        return False
    st = str(station or "").strip().upper()
    pn = "" if part_number is None else str(part_number).strip()
    if not pn or pn.lower() == "unknown":
        # If unknown, treat as not a final pass (keep it strict for analytics app)
        return False
    return st == get_pass_station_for_part_number(pn)


def ca_range_to_tw_dates(start_ca: datetime, end_ca: datetime) -> List[datetime.date]:
    """
    Map a CA datetime range to a list of Taiwan dates to scan, with a small safety margin.
    """
    if start_ca.tzinfo is None:
        start_ca = CA_TZ.localize(start_ca)
    if end_ca.tzinfo is None:
        end_ca = CA_TZ.localize(end_ca)
    start_tw = start_ca.astimezone(TW_TZ)
    end_tw = end_ca.astimezone(TW_TZ)
    d0 = start_tw.date()
    d1 = end_tw.date()
    # safety: scan one day before/after to avoid boundary issues
    d0 = d0 - timedelta(days=1)
    d1 = d1 + timedelta(days=1)
    days: List[datetime.date] = []
    cur = d0
    while cur <= d1:
        days.append(cur)
        cur += timedelta(days=1)
    return days


# -----------------------------
# SQLite cache
# -----------------------------


db_init_lock = threading.Lock()
db_initialized = False


def ensure_db_ready(force: bool = False) -> None:
    """
    Ensure analytics cache directory + SQLite schema exist.

    Why:
    - Users may delete analytics.db/raw_state.json to "reset".
    - SQLite will recreate an empty file, but schema will be missing -> runtime errors.
    This function makes the server self-healing.
    """
    global db_initialized
    with db_init_lock:
        if db_initialized and not force:
            return
        ensure_dirs()
        init_db()
        db_initialized = True


def connect_db() -> sqlite3.Connection:
    # IMPORTANT:
    # connect_db must NOT call ensure_db_ready() to avoid recursive init:
    # ensure_db_ready -> init_db -> connect_db -> ensure_db_ready ...
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    # IMPORTANT:
    # init_db must open SQLite directly (not via connect_db) so ensure_db_ready() can call init_db safely.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        # DB metadata (used for cache compatibility checks)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            """
        )
        conn.commit()

        # If timestamp interpretation changes, cached computed CA fields are invalid.
        row = conn.execute("SELECT value FROM meta WHERE key = 'timestamp_mode';").fetchone()
        old_mode = row["value"] if row else None

        # Detect legacy caches (no meta row yet) that were built with the old behavior.
        raw_table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='raw_entries';"
        ).fetchone()
        raw_has_rows = False
        if raw_table_exists:
            try:
                raw_has_rows = bool(
                    conn.execute("SELECT 1 FROM raw_entries LIMIT 1;").fetchone()
                )
            except Exception:
                raw_has_rows = False

        needs_reset = (old_mode is None and raw_has_rows) or (old_mode is not None and old_mode != TIMESTAMP_MODE)
        if needs_reset:
            # Hard reset raw cache tables + scan state (safe: cache only).
            conn.execute("DROP TABLE IF EXISTS raw_entries;")
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);",
                ("timestamp_mode", TIMESTAMP_MODE),
            )
            conn.commit()
            try:
                RawState().save()
            except Exception:
                pass
        elif old_mode is None:
            # Fresh DB (or empty cache): record the current mode.
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);",
                ("timestamp_mode", TIMESTAMP_MODE),
            )
            conn.commit()

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_entries (
              utc_ms INTEGER NOT NULL,
              ca_ms INTEGER NOT NULL,
              ca_date TEXT NOT NULL,
              ca_hour INTEGER NOT NULL,
              ca_week TEXT NOT NULL,
              ca_month TEXT NOT NULL,
              filename TEXT NOT NULL,
              folder_path TEXT NOT NULL,
              sn TEXT NOT NULL,
              status TEXT NOT NULL,
              station TEXT NOT NULL,
              part_number TEXT NOT NULL,
              is_bonepile INTEGER,
              pb_id TEXT,
              PRIMARY KEY (utc_ms, filename)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_ca_ms ON raw_entries (ca_ms);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_sn_ca ON raw_entries (sn, ca_ms);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_ca_date ON raw_entries (ca_date);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_ca_week ON raw_entries (ca_week);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_ca_month ON raw_entries (ca_month);")
        conn.commit()
    finally:
        conn.close()


def get_db_data_range_ca_ms() -> Tuple[Optional[int], Optional[int]]:
    """
    Return (min_ca_ms, max_ca_ms) based on actual rows in SQLite.
    This reflects the newest/oldest data we truly have (not the requested scan range).
    """
    try:
        conn = connect_db()
        try:
            row = conn.execute(
                "SELECT MIN(ca_ms) AS min_ca_ms, MAX(ca_ms) AS max_ca_ms FROM raw_entries;"
            ).fetchone()
            if not row:
                return None, None
            return row["min_ca_ms"], row["max_ca_ms"]
        finally:
            conn.close()
    except sqlite3.OperationalError:
        # Likely schema missing (db was deleted). Re-init once and retry.
        ensure_db_ready(force=True)
        conn = connect_db()
        try:
            row = conn.execute(
                "SELECT MIN(ca_ms) AS min_ca_ms, MAX(ca_ms) AS max_ca_ms FROM raw_entries;"
            ).fetchone()
            if not row:
                return None, None
            return row["min_ca_ms"], row["max_ca_ms"]
        finally:
            conn.close()


# -----------------------------
# Raw state
# -----------------------------


@dataclass
class RawState:
    min_ca_ms: Optional[int] = None
    max_ca_ms: Optional[int] = None
    min_key: Optional[Tuple[int, str]] = None  # (utc_ms, filename)
    max_key: Optional[Tuple[int, str]] = None  # (utc_ms, filename)
    min_path: Optional[str] = None
    max_path: Optional[str] = None
    last_scan_ca_ms: Optional[int] = None

    @staticmethod
    def load() -> "RawState":
        if not os.path.exists(STATE_PATH):
            return RawState()
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            return RawState(
                min_ca_ms=data.get("min_ca_ms"),
                max_ca_ms=data.get("max_ca_ms"),
                min_key=tuple(data["min_key"]) if isinstance(data.get("min_key"), list) else None,
                max_key=tuple(data["max_key"]) if isinstance(data.get("max_key"), list) else None,
                min_path=data.get("min_path"),
                max_path=data.get("max_path"),
                last_scan_ca_ms=data.get("last_scan_ca_ms"),
            )
        except Exception:
            return RawState()

    def save(self) -> None:
        tmp = STATE_PATH + ".tmp"
        data = {
            "min_ca_ms": self.min_ca_ms,
            "max_ca_ms": self.max_ca_ms,
            "min_key": list(self.min_key) if self.min_key else None,
            "max_key": list(self.max_key) if self.max_key else None,
            "min_path": self.min_path,
            "max_path": self.max_path,
            "last_scan_ca_ms": self.last_scan_ca_ms,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        if os.path.exists(STATE_PATH):
            os.remove(STATE_PATH)
        os.replace(tmp, STATE_PATH)


# -----------------------------
# Scanning
# -----------------------------


scan_lock = threading.Lock()


def iter_zip_files_for_tw_date(tw_date: datetime.date) -> Iterable[Tuple[str, str]]:
    """
    Yield (folder_path, filename) for all zip files under a Taiwan date folder.
    Structure:
      BASE_PATH/YYYY/MM/DD/<subfolder-id>/**/*.zip
    """
    dir_path = os.path.join(
        BASE_PATH,
        tw_date.strftime("%Y"),
        tw_date.strftime("%m"),
        tw_date.strftime("%d"),
    )
    if not os.path.isdir(dir_path):
        return

    try:
        # Robust: walk the whole day folder recursively (structure can change).
        for root, _dirs, files in os.walk(dir_path):
            for fn in files:
                if fn.lower().endswith(".zip"):
                    yield root, fn
    except Exception:
        return


def insert_entries(entries: List[Dict[str, Any]]) -> int:
    if not entries:
        return 0
    try:
        conn = connect_db()
        try:
            before = conn.total_changes
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO raw_entries (
                  utc_ms, ca_ms, ca_date, ca_hour, ca_week, ca_month,
                  filename, folder_path, sn, status, station, part_number,
                  is_bonepile, pb_id
                ) VALUES (
                  :utc_ms, :ca_ms, :ca_date, :ca_hour, :ca_week, :ca_month,
                  :filename, :folder_path, :sn, :status, :station, :part_number,
                  :is_bonepile, :pb_id
                );
                """,
                entries,
            )
            conn.commit()
            # Use total_changes delta because sqlite3 rowcount is unreliable for executemany+INSERT OR IGNORE.
            return int(conn.total_changes - before)
        finally:
            conn.close()
    except sqlite3.OperationalError:
        # Likely schema missing (db was deleted). Re-init once and retry.
        ensure_db_ready(force=True)
        conn = connect_db()
        try:
            before = conn.total_changes
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO raw_entries (
                  utc_ms, ca_ms, ca_date, ca_hour, ca_week, ca_month,
                  filename, folder_path, sn, status, station, part_number,
                  is_bonepile, pb_id
                ) VALUES (
                  :utc_ms, :ca_ms, :ca_date, :ca_hour, :ca_week, :ca_month,
                  :filename, :folder_path, :sn, :status, :station, :part_number,
                  :is_bonepile, :pb_id
                );
                """,
                entries,
            )
            conn.commit()
            return int(conn.total_changes - before)
        finally:
            conn.close()


def scan_range(start_ca: datetime, end_ca: datetime, state: RawState) -> Dict[str, Any]:
    """
    Scan Oberon files for a CA datetime range and store parsed entries into SQLite.
    Updates state min/max coverage and keys.
    """
    if start_ca.tzinfo is None:
        start_ca = CA_TZ.localize(start_ca)
    if end_ca.tzinfo is None:
        end_ca = CA_TZ.localize(end_ca)
    # Never scan beyond "now" in CA time.
    # Keep second precision for near-real-time updates.
    now_ca = datetime.now(CA_TZ).replace(microsecond=0)
    if end_ca > now_ca:
        end_ca = now_ca
    if start_ca > now_ca:
        return {"ok": False, "error": "start is in the future"}
    if end_ca <= start_ca:
        return {"ok": False, "error": "end must be after start"}

    tw_dates = ca_range_to_tw_dates(start_ca, end_ca)
    new_rows = 0
    seen_min_key = state.min_key
    seen_max_key = state.max_key
    seen_min_path = state.min_path
    seen_max_path = state.max_path

    start_ca_ms = utc_ms(start_ca)
    end_ca_ms = utc_ms(end_ca)

    batch: List[Dict[str, Any]] = []
    visited_zip = 0
    parsed_ok = 0
    ts_ok = 0
    in_range = 0

    def flush():
        nonlocal batch, new_rows
        if not batch:
            return
        inserted = insert_entries(batch)
        new_rows += int(inserted)
        batch = []

    for tw_date in tw_dates:
        for folder_path, fn in iter_zip_files_for_tw_date(tw_date):
            visited_zip += 1
            parsed = parse_test_filename(fn)
            if not parsed:
                continue
            parsed_ok += 1
            sn, status, station, part_number = parsed
            utc_dt = parse_timestamp_from_filename(fn)
            if not utc_dt:
                continue
            ts_ok += 1
            ca_ms, ca_date, ca_hour, ca_week, ca_month = ca_fields_from_utc(utc_dt)
            if ca_ms < start_ca_ms or ca_ms > end_ca_ms:
                continue
            in_range += 1

            is_bp, pb_id = parse_source_token(fn)
            key = (int(utc_ms(utc_dt)), fn)
            # Track min/max key/path for state
            if seen_min_key is None or key < seen_min_key:
                seen_min_key = key
                seen_min_path = folder_path
            if seen_max_key is None or key > seen_max_key:
                seen_max_key = key
                seen_max_path = folder_path

            batch.append(
                {
                    "utc_ms": int(utc_ms(utc_dt)),
                    "ca_ms": int(ca_ms),
                    "ca_date": ca_date,
                    "ca_hour": int(ca_hour),
                    "ca_week": ca_week,
                    "ca_month": ca_month,
                    "filename": fn,
                    "folder_path": folder_path,
                    "sn": sn,
                    "status": status,
                    "station": station,
                    "part_number": part_number,
                    "is_bonepile": is_bp,
                    "pb_id": pb_id,
                }
            )
            if len(batch) >= 2000:
                flush()

    flush()

    # Update coverage based on *actual data present* (prevents "scan coverage" racing ahead of data).
    data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
    if data_min_ca_ms is not None:
        state.min_ca_ms = data_min_ca_ms
    if data_max_ca_ms is not None:
        state.max_ca_ms = data_max_ca_ms
    state.min_key = seen_min_key
    state.max_key = seen_max_key
    state.min_path = seen_min_path
    state.max_path = seen_max_path
    state.last_scan_ca_ms = utc_ms(datetime.now(CA_TZ))
    state.save()

    return {
        "ok": True,
        "scanned_tw_days": len(tw_dates),
        "inserted": new_rows,
        "counters": {
            "visited_zip": visited_zip,
            "parsed_ok": parsed_ok,
            "ts_ok": ts_ok,
            "in_range": in_range,
        },
        "coverage": {"min_ca_ms": state.min_ca_ms, "max_ca_ms": state.max_ca_ms},
    }


def ensure_coverage(start_ca: datetime, end_ca: datetime) -> Dict[str, Any]:
    """
    Expand cache coverage to include the requested CA range, scanning only the missing segments.
    """
    # Clamp to "now" so we don't mark future time as covered (second precision).
    now_ca = datetime.now(CA_TZ).replace(microsecond=0)
    if end_ca.tzinfo is None:
        end_ca = CA_TZ.localize(end_ca)
    if start_ca.tzinfo is None:
        start_ca = CA_TZ.localize(start_ca)
    if end_ca > now_ca:
        end_ca = now_ca
    if start_ca > now_ca:
        start_ca = now_ca - timedelta(minutes=1)

    with scan_lock:
        state = RawState.load()

        # Always treat "covered" as actual DB coverage, not attempted coverage.
        data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
        # Keep state aligned with DB coverage (self-heal older bad states)
        changed = False
        if data_min_ca_ms is not None and state.min_ca_ms != data_min_ca_ms:
            state.min_ca_ms = data_min_ca_ms
            changed = True
        if data_max_ca_ms is not None and state.max_ca_ms != data_max_ca_ms:
            state.max_ca_ms = data_max_ca_ms
            changed = True
        if changed:
            state.save()

        result: Dict[str, Any] = {"ok": True, "actions": []}
        if state.min_ca_ms is None or state.max_ca_ms is None:
            r = scan_range(start_ca, end_ca, state)
            result["actions"].append({"type": "scan", "range": "initial", "result": r})
            return result

        start_ms = utc_ms(start_ca if start_ca.tzinfo else CA_TZ.localize(start_ca))
        end_ms = utc_ms(end_ca if end_ca.tzinfo else CA_TZ.localize(end_ca))

        if start_ms < state.min_ca_ms:
            r = scan_range(start_ca, datetime.fromtimestamp(state.min_ca_ms / 1000, CA_TZ), state)
            result["actions"].append({"type": "scan", "range": "backfill", "result": r})
        if end_ms > state.max_ca_ms:
            r = scan_range(datetime.fromtimestamp(state.max_ca_ms / 1000, CA_TZ), end_ca, state)
            result["actions"].append({"type": "scan", "range": "forward", "result": r})

        return result


# -----------------------------
# Aggregations for UI
# -----------------------------


def query_entries_in_range(start_ca: datetime, end_ca: datetime) -> List[sqlite3.Row]:
    start_ms = utc_ms(start_ca if start_ca.tzinfo else CA_TZ.localize(start_ca))
    end_ms = utc_ms(end_ca if end_ca.tzinfo else CA_TZ.localize(end_ca))
    try:
        conn = connect_db()
        try:
            cur = conn.execute(
                """
                SELECT *
                FROM raw_entries
                WHERE ca_ms BETWEEN ? AND ?
                ORDER BY sn, utc_ms, filename;
                """,
                (start_ms, end_ms),
            )
            return list(cur.fetchall())
        finally:
            conn.close()
    except sqlite3.OperationalError:
        ensure_db_ready(force=True)
        conn = connect_db()
        try:
            cur = conn.execute(
                """
                SELECT *
                FROM raw_entries
                WHERE ca_ms BETWEEN ? AND ?
                ORDER BY sn, utc_ms, filename;
                """,
                (start_ms, end_ms),
            )
            return list(cur.fetchall())
        finally:
            conn.close()


def cleanup_retention(now_ca: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Delete cached raw entries older than RETENTION_DAYS (based on CA timestamp).
    Keeps cache size bounded and prevents UI from showing ancient data forever.
    """
    global last_retention_cleanup_ms
    try:
        if now_ca is None:
            now_ca = datetime.now(CA_TZ)
        if now_ca.tzinfo is None:
            now_ca = CA_TZ.localize(now_ca)

        cutoff_ca = now_ca - timedelta(days=RETENTION_DAYS)
        cutoff_ms = utc_ms(cutoff_ca)

        conn = connect_db()
        try:
            cur = conn.execute("DELETE FROM raw_entries WHERE ca_ms < ?;", (cutoff_ms,))
            deleted = cur.rowcount if cur.rowcount is not None else 0
            conn.commit()
        finally:
            conn.close()

        # After deleting, clamp scan coverage forward so UI stays consistent.
        data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
        state = RawState.load()
        changed = False
        if data_min_ca_ms is not None and (state.min_ca_ms is None or state.min_ca_ms < data_min_ca_ms):
            state.min_ca_ms = data_min_ca_ms
            changed = True
        if data_max_ca_ms is not None and (state.max_ca_ms is None or state.max_ca_ms < data_max_ca_ms):
            state.max_ca_ms = data_max_ca_ms
            changed = True
        if changed:
            state.save()

        with auto_status_lock:
            last_retention_cleanup_ms = utc_ms(now_ca)

        return {"ok": True, "deleted": int(deleted), "cutoff_ca_ms": int(cutoff_ms)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def compute_stats(rows: List[sqlite3.Row], aggregation: str) -> Dict[str, Any]:
    """
    Compute:
      - summary matrix (tested/pass/fail) x (bp/fresh/total)
      - sku rows: part_number -> pass/fail unique SN (SN assigned to latest part_number in range)
      - breakdown rows (daily/weekly/monthly)
    """
    # Group by SN
    sn_tests: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        sn = r["sn"]
        sn_tests.setdefault(sn, []).append(r)

    sn_is_bp: Dict[str, int] = {}
    sn_pass: Dict[str, int] = {}
    sn_latest_part: Dict[str, str] = {}
    sn_latest_utc: Dict[str, int] = {}

    for sn, tests in sn_tests.items():
        is_bp = 0
        is_pass = 0
        latest_part = "Unknown"
        latest_utc = -1
        for t in tests:
            if (t["is_bonepile"] or 0) == 1:
                is_bp = 1
            if is_final_pass(t["status"], t["station"], t["part_number"]):
                is_pass = 1
            if int(t["utc_ms"]) > latest_utc:
                latest_utc = int(t["utc_ms"])
                latest_part = t["part_number"] or "Unknown"
        sn_is_bp[sn] = is_bp
        sn_pass[sn] = is_pass
        sn_latest_part[sn] = latest_part
        sn_latest_utc[sn] = latest_utc

    tested_total = len(sn_tests)
    pass_total = sum(1 for v in sn_pass.values() if v)
    fail_total = tested_total - pass_total

    tested_bp = sum(1 for sn, v in sn_is_bp.items() if v == 1)
    tested_fresh = tested_total - tested_bp
    pass_bp = sum(1 for sn in sn_tests.keys() if sn_is_bp.get(sn, 0) == 1 and sn_pass.get(sn, 0) == 1)
    pass_fresh = pass_total - pass_bp
    fail_bp = tested_bp - pass_bp
    fail_fresh = tested_fresh - pass_fresh

    summary = {
        "bp": {"tested": tested_bp, "pass": pass_bp, "fail": fail_bp},
        "fresh": {"tested": tested_fresh, "pass": pass_fresh, "fail": fail_fresh},
        "total": {"tested": tested_total, "pass": pass_total, "fail": fail_total},
    }

    # SKU rows (assign each SN to latest part_number)
    sku_stats: Dict[str, Dict[str, int]] = {}
    for sn in sn_tests.keys():
        sku = sn_latest_part.get(sn, "Unknown") or "Unknown"
        sku_stats.setdefault(sku, {"pass": 0, "fail": 0, "tested": 0})
        sku_stats[sku]["tested"] += 1
        if sn_pass.get(sn, 0) == 1:
            sku_stats[sku]["pass"] += 1
        else:
            sku_stats[sku]["fail"] += 1

    sku_rows = [
        {"sku": sku, "tested": s["tested"], "pass": s["pass"], "fail": s["fail"]}
        for sku, s in sku_stats.items()
    ]
    sku_rows.sort(key=lambda x: (-x["tested"], x["sku"]))

    # Breakdown per period (daily/weekly/monthly)
    # Count unique SN per period and compute pass/bp/fresh based on tests inside that period.
    # (A SN may appear in multiple periods; we count it in each.)
    bucket_key_field = {"daily": "ca_date", "weekly": "ca_week", "monthly": "ca_month"}.get(aggregation, "ca_date")
    bucket_sn_tests: Dict[str, Dict[str, List[sqlite3.Row]]] = {}
    for r in rows:
        bucket = r[bucket_key_field]
        sn = r["sn"]
        bucket_sn_tests.setdefault(bucket, {}).setdefault(sn, []).append(r)

    breakdown_rows: List[Dict[str, Any]] = []
    for bucket, sn_map in bucket_sn_tests.items():
        tested = len(sn_map)
        passed = 0
        bp = 0
        for sn, tests in sn_map.items():
            is_bp_bucket = any((t["is_bonepile"] or 0) == 1 for t in tests)
            if is_bp_bucket:
                bp += 1
            if any(is_final_pass(t["status"], t["station"], t["part_number"]) for t in tests):
                passed += 1
        fresh = tested - bp
        pass_rate = (passed / tested) if tested else 0.0
        breakdown_rows.append(
            {
                "period": bucket,
                "tested": tested,
                "passed": passed,
                "bonepile": bp,
                "fresh": fresh,
                "pass_rate": pass_rate,
            }
        )
    breakdown_rows.sort(key=lambda x: x["period"])

    return {
        "summary": summary,
        "sku_rows": sku_rows,
        "breakdown_rows": breakdown_rows,
    }


def _ts_group_from_part_number(part_number: str) -> str:
    pn = "" if part_number is None else str(part_number).upper()
    m = re.search(r"\bTS(\d+)\b", pn)
    if m:
        return f"TS{int(m.group(1))}"
    return "TS?"


def compute_test_flow(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    """
    Compute station flow table:
    - Stations order fixed: FLA -> FLB -> AST -> FTS -> FCT -> RIN -> NVL
    - Row 1: totals per station (unique SNs) for PASS/FAIL at that station
    - Next rows: grouped by TSx (derived from latest part_number of SN in range),
      then SKU rows sorted ascending. Counts are unique SNs.

    PASS/FAIL per station is based on raw entry status at that station:
    - PASS: SN has at least one row with status='P' at that station (within range)
    - FAIL: SN has at least one row with status='F' at that station (within range)
    A SN can be counted in both for a station if retested (same as raw behavior).
    """
    stations = ["FLA", "FLB", "AST", "FTS", "FCT", "RIN", "NVL"]

    # Group rows by SN
    sn_tests: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        sn_tests.setdefault(r["sn"], []).append(r)

    # Determine latest part_number per SN (same rule as SKU table)
    sn_latest_part: Dict[str, str] = {}
    sn_latest_key: Dict[str, Tuple[int, str]] = {}
    for sn, tests in sn_tests.items():
        best_key = (-1, "")
        best_pn = "Unknown"
        for t in tests:
            try:
                ca_ms = int(t["ca_ms"])
            except Exception:
                continue
            fn = t["filename"] or ""
            key = (ca_ms, fn)
            if key > best_key:
                best_key = key
                best_pn = t["part_number"] or "Unknown"
        sn_latest_part[sn] = best_pn
        sn_latest_key[sn] = best_key

    # Totals sets
    total_sets: Dict[str, Dict[str, set]] = {st: {"pass": set(), "fail": set()} for st in stations}

    # Per-SKU sets
    sku_sets: Dict[str, Dict[str, Dict[str, set]]] = {}
    for sn, tests in sn_tests.items():
        sku = sn_latest_part.get(sn, "Unknown") or "Unknown"
        sku_sets.setdefault(sku, {st: {"pass": set(), "fail": set()} for st in stations})
        for t in tests:
            st = str(t["station"] or "").strip().upper()
            if st not in total_sets:
                continue
            status = str(t["status"] or "").strip().upper()
            if status == "P":
                total_sets[st]["pass"].add(sn)
                sku_sets[sku][st]["pass"].add(sn)
            elif status == "F":
                total_sets[st]["fail"].add(sn)
                sku_sets[sku][st]["fail"].add(sn)

    totals = {
        st: {"pass": len(total_sets[st]["pass"]), "fail": len(total_sets[st]["fail"])}
        for st in stations
    }

    # Flat rows with TS column, sort by TS then SKU ascending
    def ts_sort_key(ts: str) -> Tuple[int, int]:
        m = re.match(r"TS(\d+)$", ts)
        if m:
            return (0, int(m.group(1)))
        return (1, 999)

    rows_out: List[Dict[str, Any]] = []
    for sku in sorted(sku_sets.keys()):
        ts = _ts_group_from_part_number(sku)
        rows_out.append(
            {
                "ts": ts,
                "sku": sku,
                "stations": {
                    st: {"pass": len(sku_sets[sku][st]["pass"]), "fail": len(sku_sets[sku][st]["fail"])}
                    for st in stations
                },
            }
        )

    rows_out.sort(key=lambda r: (ts_sort_key(r["ts"]), r["sku"]))

    return {"stations": stations, "totals": totals, "rows": rows_out}


def compute_station_sn_list(
    rows: List[sqlite3.Row],
    station: str,
    outcome: str,
    sku: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build SN list filtered by a station outcome:
    - station: e.g. FCT
    - outcome: 'pass' or 'fail' (maps to status P/F)
    - sku: optional exact match to SN latest part_number (in this slice)
    """
    st = str(station or "").strip().upper()
    want_status = "P" if outcome == "pass" else "F"

    # Group rows by SN and compute latest part_number for SKU assignment (in this slice)
    sn_rows: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        sn_rows.setdefault(r["sn"], []).append(r)

    sn_latest_part: Dict[str, str] = {}
    for sn, tests in sn_rows.items():
        best_key = (-1, "")
        best_pn = "Unknown"
        for t in tests:
            ca_ms = int(t["ca_ms"])
            fn = t["filename"] or ""
            key = (ca_ms, fn)
            if key > best_key:
                best_key = key
                best_pn = t["part_number"] or "Unknown"
        sn_latest_part[sn] = best_pn

    out: List[Dict[str, Any]] = []
    for sn, tests in sn_rows.items():
        if sku and (sn_latest_part.get(sn, "Unknown") != sku):
            continue

        matched = [t for t in tests if str(t["station"] or "").strip().upper() == st and str(t["status"] or "").strip().upper() == want_status]
        if not matched:
            continue

        # Use latest matched row as "last" context + pass time
        best_key = (-1, "")
        best_row = None
        for t in matched:
            ca_ms = int(t["ca_ms"])
            fn = t["filename"] or ""
            key = (ca_ms, fn)
            if key > best_key:
                best_key = key
                best_row = t

        last_ca_ms = int(best_key[0]) if best_key[0] >= 0 else None
        last_filename = best_row["filename"] if best_row else None
        last_station = best_row["station"] if best_row else None
        last_part_number = best_row["part_number"] if best_row else None
        last_folder_path = best_row["folder_path"] if best_row else None
        last_folder_id = os.path.basename(last_folder_path) if last_folder_path else None

        out.append(
            {
                "sn": sn,
                "result": "PASS" if outcome == "pass" else "FAIL",
                "is_pass": 1 if outcome == "pass" else 0,
                "is_bonepile": 1 if any((t["is_bonepile"] or 0) == 1 for t in tests) else 0,
                "pass_ca_ms": last_ca_ms,
                "last_filename": last_filename,
                "last_station": last_station,
                "last_part_number": last_part_number,
                "last_folder_id": last_folder_id,
                "last_folder_path": last_folder_path,
            }
        )

    out.sort(key=lambda x: (x["pass_ca_ms"] or 0, x["sn"]), reverse=True)
    return out


def compute_station_sn_list_both(
    rows: List[sqlite3.Row],
    station: str,
    sku: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return SN list for a station, including PASS and FAIL in one list (unique SN).
    If a SN has both P and F at the station in this slice, result will be "PASS/FAIL".
    """
    st = str(station or "").strip().upper()

    # Group rows by SN and compute latest part_number for SKU assignment (in this slice)
    sn_rows: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        sn_rows.setdefault(r["sn"], []).append(r)

    sn_latest_part: Dict[str, str] = {}
    for sn, tests in sn_rows.items():
        best_key = (-1, "")
        best_pn = "Unknown"
        for t in tests:
            ca_ms = int(t["ca_ms"])
            fn = t["filename"] or ""
            key = (ca_ms, fn)
            if key > best_key:
                best_key = key
                best_pn = t["part_number"] or "Unknown"
        sn_latest_part[sn] = best_pn

    out: List[Dict[str, Any]] = []
    for sn, tests in sn_rows.items():
        if sku and (sn_latest_part.get(sn, "Unknown") != sku):
            continue

        station_tests = [
            t
            for t in tests
            if str(t["station"] or "").strip().upper() == st
            and str(t["status"] or "").strip().upper() in ("P", "F")
        ]
        if not station_tests:
            continue

        # Latest per status
        best_p = (-1, "")
        best_f = (-1, "")
        best_p_row = None
        best_f_row = None
        for t in station_tests:
            ca_ms = int(t["ca_ms"])
            fn = t["filename"] or ""
            key = (ca_ms, fn)
            status = str(t["status"] or "").strip().upper()
            if status == "P" and key > best_p:
                best_p = key
                best_p_row = t
            if status == "F" and key > best_f:
                best_f = key
                best_f_row = t

        has_p = best_p_row is not None
        has_f = best_f_row is not None
        if has_p and has_f:
            result = "PASS/FAIL"
        elif has_p:
            result = "PASS"
        else:
            result = "FAIL"

        # Use the latest of (P,F) as the context row
        context_row = best_p_row if best_p >= best_f else best_f_row
        context_key = best_p if best_p >= best_f else best_f
        context_ms = int(context_key[0]) if context_key[0] >= 0 else None

        last_filename = context_row["filename"] if context_row else None
        last_station = context_row["station"] if context_row else None
        last_part_number = context_row["part_number"] if context_row else None
        last_folder_path = context_row["folder_path"] if context_row else None
        last_folder_id = os.path.basename(last_folder_path) if last_folder_path else None

        out.append(
            {
                "sn": sn,
                "result": result,
                "is_pass": 1 if result.startswith("PASS") else 0,
                "is_bonepile": 1 if any((t["is_bonepile"] or 0) == 1 for t in tests) else 0,
                # Use context time in the modal "time" column (kept as pass_ca_ms for compatibility)
                "pass_ca_ms": context_ms,
                "last_filename": last_filename,
                "last_station": last_station,
                "last_part_number": last_part_number,
                "last_folder_id": last_folder_id,
                "last_folder_path": last_folder_path,
            }
        )

    out.sort(key=lambda x: (x["pass_ca_ms"] or 0, x["sn"]), reverse=True)
    return out


def compute_sn_details(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    """
    Build per-SN details for modal drill-down.

    For each SN in the range:
    - is_bonepile: any row has is_bonepile=1
    - is_pass: any row is a final pass (based on part_number rule)
    - pass_ca_ms: latest final pass time (CA ms)
    - fail_ca_ms: latest fail time (any station/status F) (CA ms)
    - last_*: latest seen row (CA ms, filename, station, part_number, folder_path + folder_id)
    """
    sn_map: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        sn_map.setdefault(r["sn"], []).append(r)

    out: List[Dict[str, Any]] = []
    for sn, tests in sn_map.items():
        is_bp = False
        pass_ms: Optional[int] = None
        fail_ms: Optional[int] = None

        # Latest seen row (by ca_ms then filename for stability)
        last_row = None
        last_key = (-1, "")

        for t in tests:
            try:
                ca_ms = int(t["ca_ms"])
            except Exception:
                continue

            if (t["is_bonepile"] or 0) == 1:
                is_bp = True

            if t["status"] == "F":
                if fail_ms is None or ca_ms > fail_ms:
                    fail_ms = ca_ms

            if is_final_pass(t["status"], t["station"], t["part_number"]):
                if pass_ms is None or ca_ms > pass_ms:
                    pass_ms = ca_ms

            fn = t["filename"] or ""
            key = (ca_ms, fn)
            if key > last_key:
                last_key = key
                last_row = t

        last_ca_ms = int(last_key[0]) if last_key[0] >= 0 else None
        last_filename = last_row["filename"] if last_row else None
        last_station = last_row["station"] if last_row else None
        last_part_number = last_row["part_number"] if last_row else None
        last_folder_path = last_row["folder_path"] if last_row else None
        last_folder_id = os.path.basename(last_folder_path) if last_folder_path else None

        out.append(
            {
                "sn": sn,
                "result": "PASS" if pass_ms is not None else "FAIL",
                "is_pass": 1 if pass_ms is not None else 0,
                "is_bonepile": 1 if is_bp else 0,
                "pass_ca_ms": pass_ms,
                "fail_ca_ms": fail_ms,
                "last_ca_ms": last_ca_ms,
                "last_filename": last_filename,
                "last_station": last_station,
                "last_part_number": last_part_number,
                "last_folder_id": last_folder_id,
                "last_folder_path": last_folder_path,
            }
        )

    out.sort(key=lambda x: (x["last_ca_ms"] or 0, x["sn"]), reverse=True)
    return out


# -----------------------------
# Job system (in-memory)
# -----------------------------


jobs_lock = threading.Lock()
jobs: Dict[str, Dict[str, Any]] = {}


def new_job_id() -> str:
    return f"job_{int(time.time() * 1000)}_{os.getpid()}"


def set_job(job_id: str, **fields: Any) -> None:
    with jobs_lock:
        jobs.setdefault(job_id, {})
        jobs[job_id].update(fields)


def run_scan_job(job_id: str, start_ca: datetime, end_ca: datetime) -> None:
    try:
        set_job(job_id, status="running", message="Scanning...", started_at=int(time.time()))
        res = ensure_coverage(start_ca, end_ca)
        set_job(job_id, status="done", result=res, finished_at=int(time.time()))
    except Exception as e:
        set_job(job_id, status="error", error=str(e), finished_at=int(time.time()))


# -----------------------------
# Routes
# -----------------------------


@app.route("/")
def dashboard():
    return render_template("analytics_dashboard.html")


@app.route("/api/status")
def api_status():
    # Self-heal if cache files were manually deleted.
    ensure_db_ready()
    state = RawState.load()
    data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
    with auto_status_lock:
        next_ms = next_auto_scan_ms
        last_cleanup_ms = last_retention_cleanup_ms
    return jsonify(
        {
            "cache": {
                # Data coverage (actual rows present)
                "min_ca_ms": data_min_ca_ms,
                "max_ca_ms": data_max_ca_ms,
                # Scan coverage (what we have attempted to cover)
                "scan_min_ca_ms": state.min_ca_ms,
                "scan_max_ca_ms": state.max_ca_ms,
                "min_key": list(state.min_key) if state.min_key else None,
                "max_key": list(state.max_key) if state.max_key else None,
                "min_path": state.min_path,
                "max_path": state.max_path,
                "last_scan_ca_ms": state.last_scan_ca_ms,
                "scan_interval_seconds": AUTO_SCAN_EVERY_SECONDS,
                "retention_days": RETENTION_DAYS,
                "next_auto_scan_ms": next_ms,
                "last_retention_cleanup_ms": last_cleanup_ms,
            }
        }
    )


@app.route("/api/events")
def api_events():
    """
    Server-Sent Events stream for lightweight status updates.

    This avoids polling /api/status repeatedly while still updating:
    - data coverage
    - scan coverage
    - last scan time
    - next auto-scan time
    """

    def get_status_payload() -> Dict[str, Any]:
        ensure_db_ready()
        state = RawState.load()
        data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
        with auto_status_lock:
            next_ms = next_auto_scan_ms
            last_cleanup_ms = last_retention_cleanup_ms
        return {
            "cache": {
                "min_ca_ms": data_min_ca_ms,
                "max_ca_ms": data_max_ca_ms,
                "scan_min_ca_ms": state.min_ca_ms,
                "scan_max_ca_ms": state.max_ca_ms,
                "last_scan_ca_ms": state.last_scan_ca_ms,
                "scan_interval_seconds": AUTO_SCAN_EVERY_SECONDS,
                "retention_days": RETENTION_DAYS,
                "next_auto_scan_ms": next_ms,
                "last_retention_cleanup_ms": last_cleanup_ms,
            }
        }

    def gen():
        last_sent = None
        while True:
            try:
                payload = get_status_payload()
                data = json.dumps(payload, separators=(",", ":"))
                if data != last_sent:
                    last_sent = data
                    yield f"event: status\ndata: {data}\n\n"
            except Exception as e:
                # still keep stream alive
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(2.0)

    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.route("/api/clear-cache", methods=["POST"])
def api_clear_cache():
    """
    Clear SQLite cache + raw scan state so the system can rescan from scratch.

    Safe reset (cache only):
    - deletes analytics_cache/analytics.db (raw_entries + meta)
    - deletes analytics_cache/raw_state.json
    - clears in-memory jobs
    """
    global db_initialized
    with scan_lock:
        # Clear in-memory jobs
        with jobs_lock:
            jobs.clear()

        # Remove files (best-effort)
        try:
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        except Exception:
            pass
        try:
            if os.path.exists(STATE_PATH):
                os.remove(STATE_PATH)
        except Exception:
            pass

        # Force re-init on next access
        db_initialized = False
        ensure_db_ready(force=True)

    return jsonify({"ok": True})


@app.route("/api/job/<job_id>")
def api_job(job_id: str):
    with jobs_lock:
        data = jobs.get(job_id)
    if not data:
        return jsonify({"error": "job not found"}), 404
    return jsonify(data)


@app.route("/api/scan", methods=["POST"])
def api_scan():
    payload = request.json or {}
    start_dt = payload.get("start_datetime")
    end_dt = payload.get("end_datetime")
    if not start_dt or not end_dt:
        return jsonify({"error": "start_datetime and end_datetime required"}), 400
    try:
        start_ca = CA_TZ.localize(datetime.strptime(start_dt, "%Y-%m-%d %H:%M"))
        end_ca = CA_TZ.localize(datetime.strptime(end_dt, "%Y-%m-%d %H:%M"))
    except Exception:
        return jsonify({"error": "datetime format must be YYYY-MM-DD HH:MM"}), 400
    # Clamp end to now so manual scan doesn't "reserve" future coverage.
    now_ca = datetime.now(CA_TZ).replace(microsecond=0)
    if end_ca > now_ca:
        end_ca = now_ca
    if start_ca > now_ca:
        return jsonify({"error": "start is in the future"}), 400

    job_id = new_job_id()
    set_job(job_id, status="queued", message="Queued")
    t = threading.Thread(target=run_scan_job, args=(job_id, start_ca, end_ca), daemon=True)
    t.start()
    return jsonify({"job_id": job_id, "status": "queued"})


@app.route("/api/query", methods=["POST"])
def api_query():
    payload = request.json or {}
    start_dt = payload.get("start_datetime")
    end_dt = payload.get("end_datetime")
    aggregation = (payload.get("aggregation") or "daily").strip().lower()
    if aggregation not in ("daily", "weekly", "monthly"):
        aggregation = "daily"

    if not start_dt or not end_dt:
        return jsonify({"error": "start_datetime and end_datetime required"}), 400
    try:
        start_ca = CA_TZ.localize(datetime.strptime(start_dt, "%Y-%m-%d %H:%M"))
        end_ca = CA_TZ.localize(datetime.strptime(end_dt, "%Y-%m-%d %H:%M"))
    except Exception:
        return jsonify({"error": "datetime format must be YYYY-MM-DD HH:MM"}), 400
    # Clamp end to now so cache/coverage logic stays truthful.
    now_ca = datetime.now(CA_TZ).replace(microsecond=0)
    if end_ca > now_ca:
        end_ca = now_ca
    if start_ca > now_ca:
        return jsonify({"error": "start is in the future"}), 400
    if end_ca <= start_ca:
        return jsonify({"error": "end must be after start"}), 400

    # IMPORTANT DESIGN CHANGE:
    # Query must NEVER auto-trigger scans (that causes loops / "scan nonstop").
    # Scanning is done by:
    # - Manual scan button (/api/scan)
    # - Background auto-scan loop
    # Query returns whatever is currently in SQLite + a coverage flag.
    start_ms = utc_ms(start_ca)
    end_ms = utc_ms(end_ca)
    data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
    is_fully_covered = (
        data_min_ca_ms is not None
        and data_max_ca_ms is not None
        and start_ms >= int(data_min_ca_ms)
        and end_ms <= int(data_max_ca_ms)
    )

    rows = query_entries_in_range(start_ca, end_ca)
    computed = compute_stats(rows, aggregation=aggregation)
    test_flow = compute_test_flow(rows)
    return jsonify(
        {
            "needs_scan": False,
            "aggregation": aggregation,
            "summary": computed["summary"],
            "sku_rows": computed["sku_rows"][:200],
            "breakdown_rows": computed["breakdown_rows"],
            "counts": {"raw_rows": len(rows), "unique_sns": len(set([r["sn"] for r in rows]))},
            "coverage": {"min_ca_ms": data_min_ca_ms, "max_ca_ms": data_max_ca_ms},
            "is_fully_covered": bool(is_fully_covered),
            "test_flow": test_flow,
        }
    )


@app.route("/api/sn-list", methods=["POST"])
def api_sn_list():
    """
    Drill-down list of SNs for a metric bucket.

    Payload:
      - start_datetime: "YYYY-MM-DD HH:MM"
      - end_datetime: "YYYY-MM-DD HH:MM"
      - segment: "bp" | "fresh" | "total"
      - metric: "tested" | "pass" | "fail"
    """
    payload = request.json or {}
    start_dt = payload.get("start_datetime")
    end_dt = payload.get("end_datetime")
    segment = (payload.get("segment") or "total").strip().lower()
    metric = (payload.get("metric") or "tested").strip().lower()
    sku = payload.get("sku")  # optional: filter by last part_number
    period = payload.get("period")  # optional: filter by bucket value (daily/weekly/monthly)
    aggregation = (payload.get("aggregation") or "").strip().lower()
    station = payload.get("station")  # optional: station drilldown
    station_outcome = (payload.get("station_outcome") or "").strip().lower()  # pass/fail/both

    if segment not in ("bp", "fresh", "total"):
        segment = "total"
    if metric not in ("tested", "pass", "fail"):
        metric = "tested"

    if not start_dt or not end_dt:
        return jsonify({"error": "start_datetime and end_datetime required"}), 400

    try:
        start_ca = CA_TZ.localize(datetime.strptime(start_dt, "%Y-%m-%d %H:%M"))
        end_ca = CA_TZ.localize(datetime.strptime(end_dt, "%Y-%m-%d %H:%M"))
    except Exception:
        return jsonify({"error": "datetime format must be YYYY-MM-DD HH:MM"}), 400

    now_ca = datetime.now(CA_TZ).replace(microsecond=0)
    if end_ca > now_ca:
        end_ca = now_ca
    if end_ca <= start_ca:
        return jsonify({"error": "end must be after start"}), 400

    rows = query_entries_in_range(start_ca, end_ca)

    # Optional: filter rows by requested bucket (time breakdown drilldown)
    if period and aggregation in ("daily", "weekly", "monthly"):
        key_field = {"daily": "ca_date", "weekly": "ca_week", "monthly": "ca_month"}[aggregation]
        try:
            rows = [r for r in rows if (r[key_field] == period)]
        except Exception:
            pass

    if station and station_outcome in ("pass", "fail"):
        details = compute_station_sn_list(rows, station=station, outcome=station_outcome, sku=sku)
    elif station and station_outcome == "both":
        details = compute_station_sn_list_both(rows, station=station, sku=sku)
    else:
        details = compute_sn_details(rows)

    # Optional: SKU filter (match the SN's latest part number in the selected slice)
    if sku:
        sku_norm = str(sku).strip()
        details = [d for d in details if (d.get("last_part_number") or "") == sku_norm]

    # Segment filter
    if segment == "bp":
        details = [d for d in details if d.get("is_bonepile") == 1]
    elif segment == "fresh":
        details = [d for d in details if d.get("is_bonepile") == 0]

    # Metric filter
    if metric == "pass":
        details = [d for d in details if d.get("is_pass") == 1]
    elif metric == "fail":
        details = [d for d in details if d.get("is_pass") == 0]

    return jsonify(
        {
            "segment": segment,
            "metric": metric,
            "sku": sku,
            "period": period,
            "aggregation": aggregation,
            "count": len(details),
            "rows": details[:5000],
        }
    )


# -----------------------------
# Background auto-scan
# -----------------------------


def auto_scan_loop():
    global next_auto_scan_ms
    # Run retention cleanup at most every 12 hours
    last_cleanup_time = 0.0
    while True:
        loop_started = time.time()
        try:
            now_ca = datetime.now(CA_TZ)
            data_min_ca_ms, data_max_ca_ms = get_db_data_range_ca_ms()
            if data_max_ca_ms is None:
                # Seed: scan the last 2 hours
                start_ca = now_ca - timedelta(hours=2)
                end_ca = now_ca
            else:
                end_ca = now_ca
                last_max = datetime.fromtimestamp(int(data_max_ca_ms) / 1000, CA_TZ)
                start_ca = last_max - timedelta(minutes=AUTO_SCAN_OVERLAP_MINUTES)
            ensure_coverage(start_ca, end_ca)

            # Retention cleanup (best-effort)
            if (loop_started - last_cleanup_time) >= (12 * 60 * 60):
                cleanup_retention(now_ca=now_ca)
                last_cleanup_time = loop_started
        except Exception:
            pass

        # Always pause full interval between runs (prevents "scan nonstop" when scan work > interval).
        with auto_status_lock:
            next_auto_scan_ms = int((time.time() + AUTO_SCAN_EVERY_SECONDS) * 1000)
        time.sleep(float(AUTO_SCAN_EVERY_SECONDS))


def main():
    ensure_db_ready(force=True)

    t = threading.Thread(target=auto_scan_loop, daemon=True)
    t.start()

    app.run(host="0.0.0.0", port=5555, debug=False)


if __name__ == "__main__":
    main()

