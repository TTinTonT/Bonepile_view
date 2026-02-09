#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Debug script: read MFG SQLite (mfg_sn_data table) and print tables/rows.
Usage:
  python read_mfg_sqlite.py
  python read_mfg_sqlite.py --db analytics_cache/analytics.db
  python read_mfg_sqlite.py --limit 20
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(APP_DIR, "analytics_cache", "analytics.db")
MFG_TABLE = "mfg_sn_data"


def main() -> None:
    ap = argparse.ArgumentParser(description="Read MFG SQLite for debugging")
    ap.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite file")
    ap.add_argument("--limit", type=int, default=50, help="Max rows to print from mfg_sn_data")
    ap.add_argument("--list-tables", action="store_true", help="Only list table names")
    args = ap.parse_args()

    db_path = args.db if os.path.isabs(args.db) else os.path.join(APP_DIR, args.db)
    if not os.path.isfile(db_path):
        print(f"DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [r[0] for r in cur.fetchall()]
        print(f"Tables: {tables}")

        if args.list_tables:
            return

        if MFG_TABLE not in tables:
            print(f"Table '{MFG_TABLE}' not found.")
            return

        cur = conn.execute(f"SELECT COUNT(*) FROM {MFG_TABLE}")
        n = cur.fetchone()[0]
        print(f"\n{MFG_TABLE} row count: {n}")

        cur = conn.execute(f"SELECT * FROM {MFG_TABLE} ORDER BY SN LIMIT ?", (args.limit,))
        rows = cur.fetchall()
        if not rows:
            print("(no rows)")
            return
        names = list(rows[0].keys())
        print(f"Columns: {names}\n")
        for r in rows:
            print("-" * 60)
            for k in names:
                v = r[k]
                if v is None or v == "":
                    continue
                print(f"  {k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
