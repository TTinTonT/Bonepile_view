#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Debug: tra Pass/Fail cho SN và các log ID (node_log_id) từ MFG API.
Usage:
  python debug_sn_log.py --sn 1830326000034 --log-ids 103433,103384
  python debug_sn_log.py --sn 1830326000034
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, APP_DIR)

# MFG API (cần mfg_scan + mfg_api)
from mfg_scan import get_node_basic_info, _node_info_decision
from mfg_api import search_by_sn


def main() -> None:
    ap = argparse.ArgumentParser(description="Debug SN + log IDs: query MFG API for Pass/Fail")
    ap.add_argument("--sn", required=True, help="Serial number (e.g. 1830326000034)")
    ap.add_argument("--log-ids", default="103433,103384", help="Comma-separated node_log_id (e.g. 103433,103384)")
    ap.add_argument("--db", default=os.path.join(APP_DIR, "analytics_cache", "analytics.db"), help="SQLite path for local check")
    args = ap.parse_args()

    sn = (args.sn or "").strip()
    log_ids = [int(x.strip()) for x in (args.log_ids or "").split(",") if x.strip()]
    if not sn:
        print("--sn required")
        sys.exit(1)

    print(f"SN: {sn}")
    print(f"Log IDs to check: {log_ids}")
    print()

    # 1) Local DB (mfg_sn_data) – 1 row per SN
    db_path = args.db if os.path.isabs(args.db) else os.path.join(APP_DIR, args.db)
    if os.path.isfile(db_path):
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.execute('SELECT * FROM mfg_sn_data WHERE "SN" = ?', (sn,))
            row = cur.fetchone()
            conn.close()
            if row:
                r = dict(row)
                print("[Local DB mfg_sn_data]")
                print(f"  node_log_id: {r.get('node_log_id')}")
                print(f"  Status: {r.get('Status')}")
                print(f"  last_station: {r.get('last_station')}")
                print(f"  start_testing_time: {r.get('start_testing_time')}")
                print(f"  end_testing_time: {r.get('end_testing_time')}")
            else:
                print("[Local DB] No row for this SN.")
        except Exception as e:
            print(f"[Local DB] Error: {e}")
        print()
    else:
        print(f"[Local DB] File not found: {db_path}")
        print()

    # 2) MFG API: search logs by SN
    print("[MFG API] search_by_sn(...)")
    try:
        resp = search_by_sn(sn=sn, cal_total=True)
        log_list = resp.get("log_list") or []
        total = resp.get("total_count") or len(log_list)
        print(f"  total_count: {total}, returned: {len(log_list)}")
        for i, log in enumerate(log_list[:20]):
            nid = log.get("node_log_id")
            res = (log.get("result") or "").strip()
            st = (log.get("station") or "").strip()
            print(f"  #{i+1} node_log_id={nid} result={res!r} station={st}")
        if len(log_list) > 20:
            print(f"  ... and {len(log_list) - 20} more")
    except Exception as e:
        print(f"  Error: {e}")
        log_list = []
    print()

    # 3) For each requested log_id, get_node_info -> Pass/Fail
    print("[MFG API] get_node_info (basic_info) for each log ID:")
    for node_log_id in log_ids:
        try:
            info = get_node_basic_info(node_log_id)
            result_raw = (info.get("result") or "").strip().lower()
            decision = _node_info_decision(info)
            start_utc = info.get("start_utc") or ""
            end_utc = info.get("end_utc") or ""
            sfc_ok = info.get("sfc_result") is True
            # Hiển thị Pass/Fail theo quyết định dùng trong pipeline
            if decision == "pass":
                result_display = "PASS"
            elif decision == "fail":
                result_display = "FAIL"
            elif decision == "skip_testing":
                result_display = "Unfinished (skip_testing)"
            elif decision == "skip_exclude":
                result_display = "Excluded (skip_exclude)"
            else:
                result_display = result_raw or "(empty)"
            print(f"  Log #{node_log_id}:")
            print(f"    result (raw): {result_raw!r}")
            print(f"    decision: {decision} -> {result_display}")
            print(f"    start_utc: {start_utc}")
            print(f"    end_utc: {end_utc}")
            print(f"    sfc_result: {sfc_ok}")
        except Exception as e:
            print(f"  Log #{node_log_id}: Error - {e}")
        print()


if __name__ == "__main__":
    main()
