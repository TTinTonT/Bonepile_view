#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MFG scan: backward (backfill) and forward (every 5 min) scan using mfg_api.
Stores per-SN data in CSV for viewing. Phase 1-5 only; Phase 6 (replace old scan) later.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz

from mfg_api import (
    get_node_info,
    get_sn_pn_bonepile_from_node,
    search_log_items,
    search_recent,
)

# Paths
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYTICS_CACHE_DIR = os.path.join(APP_DIR, "analytics_cache")
MFG_CSV_PATH = os.path.join(ANALYTICS_CACHE_DIR, "mfg_sn_data.csv")
MFG_STATE_PATH = os.path.join(ANALYTICS_CACHE_DIR, "mfg_scan_state.json")
MFG_DB_PATH = os.path.join(ANALYTICS_CACHE_DIR, "analytics.db")

CA_TZ = pytz.timezone("America/Los_Angeles")
# API server (MFG) returns naive datetime in Taiwan time when no 'Z'
TW_TZ = pytz.timezone("Asia/Taipei")

# Station order for station_test (unique, sorted)
STATION_ORDER = ["FLA", "FLB", "FLC", "FLD", "FTS", "IOT", "RIN", "NVL"]
# Per-station columns in CSV/SQLite: pass/fail counts as "p/f"
STATION_COLUMNS = ["FLA", "FLB", "FLC", "FLD", "FTS", "IOT", "RIN", "AST", "NVL", "FCT"]

# Final pass rules (same as analytics_server)
PASS_AT_FCT_PART_NUMBERS = {"675-24109-0010-TS2"}


def get_pass_station_for_part_number(part_number: str) -> str:
    pn = "" if part_number is None else str(part_number).strip().upper()
    if pn in PASS_AT_FCT_PART_NUMBERS:
        return "FCT"
    if "TS2" in pn:
        return "NVL"
    return "FCT"


def is_final_pass(sn_status: str, station: str, part_number: str) -> bool:
    if sn_status != "P":
        return False
    st = str(station or "").strip().upper()
    pn = "" if part_number is None else str(part_number).strip()
    if not pn or pn.lower() == "unknown":
        return False
    return st == get_pass_station_for_part_number(pn)


# ---------- Phase 1: Helpers ----------


def parse_iso_to_utc_str(iso_str: Any) -> str:
    """
    Parse API timestamp (UTC, vd 2026-02-07T02:45:50.973Z), lưu nguyên giờ UTC.
    Return 'YYYY-MM-DD HH:MM:SS' (UTC) cho CSV.
    Bất kỳ giá trị nào không phải thời gian (null, rỗng, "null", invalid) → return "".
    """
    if iso_str is None:
        return ""
    s = str(iso_str).strip()
    if not s or s.lower() in ("null", "none"):
        return ""
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        utc = dt.astimezone(pytz.UTC)
        return utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def utc_str_to_ca_str(utc_str: str) -> str:
    """Convert 'YYYY-MM-DD HH:MM:SS' (UTC) sang 'YYYY-MM-DD HH:MM:SS' (CA) để ghi SQLite."""
    if not utc_str or not str(utc_str).strip():
        return ""
    s = str(utc_str).strip()
    try:
        if len(s) <= 16:
            dt = datetime.strptime(s[:16], "%Y-%m-%d %H:%M")
        else:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        dt = pytz.UTC.localize(dt)
        ca = dt.astimezone(CA_TZ)
        return ca.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _parse_iso_to_utc_dt(iso_str: Any) -> Optional[datetime]:
    """Parse ISO timestamp to UTC datetime. None/null/invalid → None."""
    s = parse_iso_to_utc_str(iso_str)
    if not s:
        return None
    try:
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return pytz.UTC.localize(dt)
    except Exception:
        return None


def get_node_basic_info(node_log_id: int) -> Dict[str, Any]:
    """
    Gọi get_node_info, trả về basic_info đã chuẩn hóa.
    Keys: start_utc, end_utc (chuỗi UTC hoặc ""), result (str lower), sfc_result (True nếu "True"/true),
    start_dt, end_dt (datetime UTC hoặc None) để so sánh.
    """
    out = {
        "start_utc": "",
        "end_utc": "",
        "result": "",
        "sfc_result": False,
        "start_dt": None,
        "end_dt": None,
    }
    try:
        info = get_node_info(node_log_id=node_log_id)
        basic = info.get("basic_info") or {}
        start_raw = basic.get("start_time")
        end_raw = basic.get("end_time")
        out["start_utc"] = parse_iso_to_utc_str(start_raw) if start_raw is not None else ""
        out["end_utc"] = parse_iso_to_utc_str(end_raw) if end_raw is not None else ""
        out["result"] = str(basic.get("result") or "").strip().lower()
        sfc = basic.get("sfc_result")
        out["sfc_result"] = sfc is True or str(sfc).strip().lower() == "true"
        out["start_dt"] = _parse_iso_to_utc_dt(start_raw)
        out["end_dt"] = _parse_iso_to_utc_dt(end_raw)
    except Exception:
        pass
    return out


def _node_info_decision(node: Dict[str, Any]) -> str:
    """
    Quyết định từ basic_info: "pass" | "fail" | "skip_testing" | "skip_exclude".
    - result == pass → pass
    - result == fail → fail
    - không có end_time và (result khác pass/fail và sfc_result khác True) → skip_testing (đang test)
    - có end_time và end > start (CA/UTC) và (result khác pass/fail và sfc_result khác True) → skip_exclude (bỏ, không tính)
    - còn lại → dùng result hoặc fail nếu không rõ
    """
    r = (node.get("result") or "").strip().lower()
    sfc_ok = node.get("sfc_result") is True
    end_utc = node.get("end_utc") or ""
    start_dt = node.get("start_dt")
    end_dt = node.get("end_dt")

    if r == "pass":
        return "pass"
    if r == "fail":
        return "fail"
    # result khác pass/fail
    if not end_utc:
        if not sfc_ok:
            return "skip_testing"  # đang test
        return "fail"  # không end_time nhưng sfc_ok → coi fail?
    # có end_time
    if start_dt and end_dt and end_dt > start_dt:
        if not sfc_ok:
            return "skip_exclude"  # bỏ SN, không tính
    if sfc_ok:
        return "pass"  # sfc_result True → pass
    return "fail"


def get_end_time_from_node_info(node_log_id: int) -> str:
    """Lấy end_time từ get_node_info basic_info. null/rỗng/invalid → \"\"."""
    return get_node_basic_info(node_log_id).get("end_utc") or ""


def get_start_time_from_node_info(node_log_id: int) -> str:
    """Lấy start_time từ get_node_info basic_info. null/rỗng/invalid → \"\"."""
    return get_node_basic_info(node_log_id).get("start_utc") or ""


def normalize_station(station: Any) -> str:
    """Strip SYSTEM_ prefix from station e.g. SYSTEM_FTS -> FTS."""
    s = str(station or "").strip().upper()
    if s.startswith("SYSTEM_"):
        s = s[7:]
    return s or ""


def merge_stations(current: str, new_station: str) -> str:
    """Merge new_station into current comma-separated list, unique and sorted by STATION_ORDER."""
    if not new_station:
        return current or ""
    stations = set()
    if current:
        for x in current.split(","):
            x = x.strip().upper()
            if x:
                stations.add(x)
    stations.add(new_station.strip().upper())
    order_map = {st: i for i, st in enumerate(STATION_ORDER)}
    sorted_list = sorted(stations, key=lambda s: order_map.get(s, 99))
    return ",".join(sorted_list)


def load_state() -> Dict[str, Any]:
    if not os.path.isfile(MFG_STATE_PATH):
        return {
            "last_scanned_node_log_id": 0,
            "last_forward_scan_utc": "",
            "pending_testing_ids": [],
        }
    try:
        with open(MFG_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("last_scanned_node_log_id", 0)
        data.setdefault("last_forward_scan_utc", "")
        data.setdefault("pending_testing_ids", [])
        return data
    except Exception:
        return {
            "last_scanned_node_log_id": 0,
            "last_forward_scan_utc": "",
            "pending_testing_ids": [],
        }


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(ANALYTICS_CACHE_DIR, exist_ok=True)
    with open(MFG_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


CSV_HEADERS = [
    "SN",
    "bonepile",
    "Status",
    "Partnumber",
    "so_luong_test",
    "so_luong_pass",
    "node_log_id",
    "last_station",
    "start_testing_time",
    "end_testing_time",
    "last_updated_utc",
] + STATION_COLUMNS


def _parse_pf(val: Any) -> Tuple[int, int]:
    """Parse 'p/f' string to (pass_count, fail_count). Default (0,0)."""
    s = (str(val or "").strip() or "0/0")
    parts = s.split("/")
    try:
        p = int(parts[0].strip()) if len(parts) > 0 else 0
        f = int(parts[1].strip()) if len(parts) > 1 else 0
        return (max(0, p), max(0, f))
    except (ValueError, IndexError):
        return (0, 0)


def _format_pf(p: int, f: int) -> str:
    return f"{max(0, p)}/{max(0, f)}"


def load_csv() -> Dict[str, Dict[str, Any]]:
    """Load CSV into dict sn -> row (all values strings). Station columns default '0/0'."""
    out: Dict[str, Dict[str, Any]] = {}
    if not os.path.isfile(MFG_CSV_PATH):
        return out
    with open(MFG_CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sn = (row.get("SN") or "").strip()
            if not sn:
                continue
            out[sn] = {}
            for k in CSV_HEADERS:
                val = (row.get(k) or "").strip()
                if k in STATION_COLUMNS and not val:
                    val = "0/0"
                out[sn][k] = val
    return out


def save_csv(sn_rows: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(ANALYTICS_CACHE_DIR, exist_ok=True)
    with open(MFG_CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        for sn in sorted(sn_rows.keys()):
            w.writerow(sn_rows[sn])
    sync_csv_to_sqlite(sn_rows)


def sync_csv_to_sqlite(sn_rows: Dict[str, Dict[str, Any]]) -> None:
    """
    Write MFG SN data to SQLite table mfg_sn_data.
    CSV lưu giờ UTC; khi ghi SQLite convert start_testing_time, end_testing_time sang giờ CA.
    """
    if not sn_rows:
        return
    os.makedirs(ANALYTICS_CACHE_DIR, exist_ok=True)
    cols = ", ".join(CSV_HEADERS)
    placeholders = ", ".join("?" for _ in CSV_HEADERS)
    try:
        conn = sqlite3.connect(MFG_DB_PATH)
        try:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS mfg_sn_data ("
                + ", ".join(f'"{h}" TEXT' for h in CSV_HEADERS)
                + ", PRIMARY KEY (\"SN\"))"
            )
            conn.execute("DELETE FROM mfg_sn_data")
            for sn in sorted(sn_rows.keys()):
                row = sn_rows[sn]
                # CSV có giờ UTC; SQLite lưu giờ CA cho frontend
                out = dict(row)
                for key in ("start_testing_time", "end_testing_time"):
                    if key in out and out.get(key):
                        out[key] = utc_str_to_ca_str(str(out[key])) or out[key]
                conn.execute(
                    f"INSERT OR REPLACE INTO mfg_sn_data ({cols}) VALUES ({placeholders})",
                    [str(out.get(h, "")) for h in CSV_HEADERS],
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _empty_row(sn: str) -> Dict[str, Any]:
    """Return empty row dict for SN (SN field set). Station columns default '0/0'."""
    row = {}
    for k in CSV_HEADERS:
        row[k] = "0/0" if k in STATION_COLUMNS else ""
    row["SN"] = sn
    return row


def _row_set(row: Dict[str, Any], **kwargs: Any) -> None:
    for k, v in kwargs.items():
        if k in row:
            row[k] = "" if v is None else str(v)


# ---------- Phase 3: Apply one log, optional get_node_info ----------


def _result_to_status(result: Any, partnumber: str, station: str) -> str:
    """Map API result + partnumber/station to Status: PASS, Fail, testing."""
    r = str(result or "").strip()
    if r == "":
        return "testing"
    if r.lower() == "pass":
        if partnumber and partnumber.lower() != "unknown":
            if is_final_pass("P", station, partnumber):
                return "PASS"
        return "Fail"  # pass at wrong station or no PN yet
    return "Fail"


def apply_log_to_sn_row(
    log: Dict[str, Any],
    current_row: Dict[str, Any],
    partnumber: str,
    bonepile: bool,
) -> Optional[Dict[str, Any]]:
    """
    Apply one log to SN row. Skip FVT_* stations; only count SYSTEM_* stations.
    Always accumulate so_luong_test, so_luong_pass, station_test; update node_log_id/start/end/status only when log is newer.
    Returns new row dict or None if no update (e.g. log skipped).
    """
    sn = (log.get("sn") or "").strip()
    if not sn:
        return None
    station_raw = (log.get("station") or "").strip()
    station_upper = station_raw.upper()
    if station_upper.startswith("FVT_"):
        return None
    if not station_upper.startswith("SYSTEM_"):
        return None
    station = normalize_station(station_raw)

    node_id = log.get("node_log_id")
    if node_id is None:
        return None
    try:
        node_id = int(node_id)
    except (TypeError, ValueError):
        return None
    current_id_str = (current_row.get("node_log_id") or "").strip()
    current_id = int(current_id_str) if current_id_str.isdigit() else 0

    result = log.get("result") or ""
    log_time_str = log.get("log_time") or ""
    sfc_date_str = log.get("sfc_event_date") or ""
    start_ts = parse_iso_to_utc_str(log_time_str)
    end_ts = parse_iso_to_utc_str(sfc_date_str)

    result_lower = str(result or "").strip().lower()
    # Khi status không rõ (khác pass/fail) → gọi get_node_info, dùng basic_info để quyết định
    if result_lower not in ("pass", "fail"):
        node_info = get_node_basic_info(node_id)
        decision = _node_info_decision(node_info)
        if decision == "skip_testing":
            return None  # đang test: ko end_time & result khác pass/fail & sfc_result khác True
        if decision == "skip_exclude":
            return None  # bỏ SN: end_time có, end>start, result khác pass/fail, sfc_result khác True
        if decision == "pass":
            result_lower = "pass"
            start_ts = start_ts or node_info.get("start_utc") or ""
            end_ts = end_ts or node_info.get("end_utc") or ""
        else:  # fail
            result_lower = "fail"
            start_ts = start_ts or node_info.get("start_utc") or ""
            end_ts = end_ts or node_info.get("end_utc") or ""

    is_pass = result_lower == "pass"
    so_test = int((current_row.get("so_luong_test") or "0").strip() or 0) + 1
    so_pass = int((current_row.get("so_luong_pass") or "0").strip() or 0)
    if is_pass:
        so_pass += 1
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if node_id > current_id:
        status = _result_to_status("pass" if is_pass else "fail", partnumber, station)
        use_node_id = node_id
        use_last_station = station
        use_start = start_ts or (current_row.get("start_testing_time") or "")
        use_end = end_ts or (current_row.get("end_testing_time") or "")
    else:
        status = current_row.get("Status") or ""
        use_node_id = current_id
        use_last_station = (current_row.get("last_station") or "").strip()
        use_start = current_row.get("start_testing_time") or ""
        use_end = current_row.get("end_testing_time") or ""

    new_row = _empty_row(sn)
    _row_set(
        new_row,
        SN=sn,
        bonepile="True" if bonepile else "False",
        Status=status,
        Partnumber=partnumber or (current_row.get("Partnumber") or ""),
        so_luong_test=so_test,
        so_luong_pass=so_pass,
        node_log_id=use_node_id,
        last_station=use_last_station,
        start_testing_time=use_start,
        end_testing_time=use_end,
        last_updated_utc=now_utc,
    )
    # Copy all station columns from current; add +1 pass or +1 fail for this station
    for st in STATION_COLUMNS:
        new_row[st] = current_row.get(st) or "0/0"
    if station in STATION_COLUMNS:
        p, f = _parse_pf(new_row[station])
        if is_pass:
            p += 1
        else:
            f += 1
        new_row[station] = _format_pf(p, f)
    return new_row


def fetch_node_info_and_fill(
    exe_log_id: Any,
    current_row: Dict[str, Any],
) -> Tuple[str, bool]:
    """Call get_node_info, parse SN/PN/Bonepile. Return (partnumber, is_bonepile)."""
    if exe_log_id is None:
        return (current_row.get("Partnumber") or "", False)
    try:
        eid = int(exe_log_id)
    except (TypeError, ValueError):
        return (current_row.get("Partnumber") or "", False)
    try:
        info = get_node_info(execute_log_id=eid)
        sn, pn, is_bp = get_sn_pn_bonepile_from_node(info)
        return (pn or current_row.get("Partnumber") or "", is_bp)
    except Exception:
        return (current_row.get("Partnumber") or "", False)


# ---------- Phase 4: Backward scan ----------


def _date_range_for_day(date_str: str) -> Tuple[str, str]:
    """Return (from_datetime, to_datetime) for one day in API format: YYYY-MM-DD 08:00:00 to next day 07:59:59."""
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    from_dt = f"{date_str} 08:00:00"
    to_date = (d + timedelta(days=1)).strftime("%Y-%m-%d")
    to_dt = f"{to_date} 07:59:59"
    return (from_dt, to_dt)


def scan_backward_one_day(
    date_str: str,
    sn_rows: Dict[str, Dict[str, Any]],
) -> None:
    """Fetch all logs for date_str (YYYY-MM-DD), apply each, optionally get_node_info. Updates sn_rows in place."""
    from_dt, to_dt = _date_range_for_day(date_str)
    page = 1
    total_pages = 1
    while page <= total_pages:
        try:
            resp = search_log_items(
                from_date=from_dt,
                to_date=to_dt,
                cur_page=page,
                cal_total=(page == 1),
            )
        except Exception as e:
            print(f"  [backward] API error {date_str} page {page}: {e}")
            break
        log_list = resp.get("log_list") or []
        if page == 1:
            total_pages = int(resp.get("total_pages") or 1)
        if not log_list:
            break
        # Sort theo node_log_id tăng dần để log mới nhất (vd Unfinished+end = Fail) luôn ghi đè
        def _node_id(log: Dict[str, Any]) -> int:
            try:
                return int(log.get("node_log_id") or 0)
            except (TypeError, ValueError):
                return 0
        log_list = sorted(log_list, key=_node_id)
        for log in log_list:
            sn = (log.get("sn") or "").strip()
            if not sn:
                continue
            current = sn_rows.get(sn) or _empty_row(sn)
            partnumber = (current.get("Partnumber") or "").strip()
            bonepile = (current.get("bonepile") or "").strip().lower() == "true"
            need_node_info = not partnumber or not bonepile
            if need_node_info:
                exe = log.get("exe_log_id") or log.get("execute_log_id")
                pn, bp = fetch_node_info_and_fill(exe, current)
                partnumber = pn
                bonepile = bp
            new_row = apply_log_to_sn_row(log, current, partnumber, bonepile)
            if new_row is not None:
                sn_rows[sn] = new_row
        page += 1


def run_backfill() -> None:
    """Backfill from today (CA) back to 2026-01-01."""
    os.makedirs(ANALYTICS_CACHE_DIR, exist_ok=True)
    sn_rows = load_csv()
    today_ca = datetime.now(CA_TZ).date()
    end = today_ca
    start = date(2026, 1, 1)
    d = end
    while d >= start:
        date_str = d.strftime("%Y-%m-%d")
        print(f"Backfill {date_str} ...")
        scan_backward_one_day(date_str, sn_rows)
        save_csv(sn_rows)
        d -= timedelta(days=1)
    print("Backfill done.")


# ---------- Phase 5: Forward scan ----------


def forward_scan() -> None:
    """Fetch 200 recent logs, update SNs with newer node_log_id, track pending testing."""
    state = load_state()
    sn_rows = load_csv()
    last_id = int(state.get("last_scanned_node_log_id") or 0)
    pending: List[int] = list(state.get("pending_testing_ids") or [])

    try:
        logs = search_recent(200)
    except Exception as e:
        print(f"Forward scan API error: {e}")
        return

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    max_id = last_id
    updated_sns = set()

    # Sort theo node_log_id tăng dần để log mới nhất (vd Unfinished+end = Fail) luôn ghi đè
    def _node_id(log: Dict[str, Any]) -> int:
        try:
            return int(log.get("node_log_id") or 0)
        except (TypeError, ValueError):
            return 0
    logs = sorted(logs, key=_node_id)

    for log in logs:
        node_id = log.get("node_log_id")
        if node_id is None:
            continue
        try:
            node_id = int(node_id)
        except (TypeError, ValueError):
            continue
        if node_id > max_id:
            max_id = node_id

        sn = (log.get("sn") or "").strip()
        if not sn:
            continue
        current = sn_rows.get(sn) or _empty_row(sn)
        partnumber = (current.get("Partnumber") or "").strip()
        bonepile = (current.get("bonepile") or "").strip().lower() == "true"
        need_node_info = not partnumber or not bonepile
        if need_node_info:
            exe = log.get("exe_log_id") or log.get("execute_log_id")
            pn, bp = fetch_node_info_and_fill(exe, current)
            partnumber = pn
            bonepile = bp
        new_row = apply_log_to_sn_row(log, current, partnumber, bonepile)
        if new_row is not None:
            sn_rows[sn] = new_row
            updated_sns.add(sn)
            result = str(log.get("result") or "").strip()
            if result == "":
                if node_id not in pending:
                    pending.append(node_id)

    # Resolve pending testing: if any of the 200 logs has that node_log_id with result/sfc filled, update SN
    for node_id in list(pending):
        for log in logs:
            if int(log.get("node_log_id") or 0) != node_id:
                continue
            sn = (log.get("sn") or "").strip()
            if not sn:
                continue
            result = str(log.get("result") or "").strip()
            sfc = str(log.get("sfc_event_date") or "").strip()
            current = sn_rows.get(sn)
            if not current:
                continue
            partnumber = (current.get("Partnumber") or "").strip()
            bonepile = (current.get("bonepile") or "").strip().lower() == "true"
            station = normalize_station(log.get("station"))
            result_lower = result.lower() if result else ""
            # Khi không có result/sfc từ search, vẫn thử get_node_info (node có thể đã cập nhật)
            if result_lower not in ("pass", "fail") or (result == "" and not sfc):
                node_info = get_node_basic_info(node_id)
                decision = _node_info_decision(node_info)
                if decision == "skip_testing":
                    continue  # vẫn đang test, giữ pending
                if decision == "skip_exclude":
                    pending.remove(node_id)
                    break  # bỏ SN, không cập nhật
                if decision in ("pass", "fail"):
                    result_lower = decision
                    status = _result_to_status("pass" if result_lower == "pass" else "fail", partnumber, station)
                    start_ts = parse_iso_to_utc_str(log.get("log_time")) or node_info.get("start_utc") or ""
                    end_ts = node_info.get("end_utc") or parse_iso_to_utc_str(log.get("sfc_event_date")) or ""
                else:
                    continue
            else:
                status = _result_to_status(result, partnumber, station)
                start_ts = parse_iso_to_utc_str(log.get("log_time"))
                end_ts = parse_iso_to_utc_str(log.get("sfc_event_date"))
            new_row = dict(current)
            new_row["Status"] = status
            new_row["node_log_id"] = str(log.get("node_log_id") or current.get("node_log_id") or "")
            new_row["last_station"] = station or (current.get("last_station") or "")
            if start_ts:
                new_row["start_testing_time"] = start_ts
            if end_ts:
                new_row["end_testing_time"] = end_ts
            new_row["last_updated_utc"] = now_utc
            sn_rows[sn] = new_row
            pending.remove(node_id)
            break

    state["last_scanned_node_log_id"] = max_id
    state["last_forward_scan_utc"] = now_utc
    state["pending_testing_ids"] = pending
    save_state(state)
    save_csv(sn_rows)
    print(f"Forward scan done. last_node_log_id={max_id}, updated {len(updated_sns)} SNs, pending_testing={len(pending)}")


def run_forward_loop(interval_sec: int = 300) -> None:
    """Run forward_scan every interval_sec (default 5 min)."""
    print(f"Forward scan loop every {interval_sec}s. Ctrl+C to stop.")
    while True:
        forward_scan()
        time.sleep(interval_sec)


# ---------- Main ----------


if __name__ == "__main__":
    import sys
    args = (sys.argv[1:] or ["forward"])
    cmd = args[0].lower()
    if cmd == "backfill":
        run_backfill()
    elif cmd == "forward":
        forward_scan()
    elif cmd == "run-forward-loop":
        interval = int(args[1]) if len(args) > 1 else 300
        run_forward_loop(interval)
    else:
        print("Usage: python mfg_scan.py [backfill|forward|run-forward-loop [interval_sec]]")
        print("  interval_sec: default 300 (5 min). Use 60 for 1 min (testing).")
        sys.exit(1)
