#!/usr/bin/env python3

import os
import random
import subprocess
import sys
import time
from pathlib import Path

from psycopg import connect

DATABASE_URL = os.environ["NEON_DATABASE_URL"]
BASE_DIR = Path(__file__).resolve().parent
UPDATE_SCRIPT = BASE_DIR / "update_price_periods.py"

TARGET_FBO_NAME = "click here"
MIN_DELAY_SECONDS = 5
MAX_DELAY_SECONDS = 15


def fetch_target_airports(conn):
    sql = """
        SELECT DISTINCT airport_code
        FROM price_periods
        WHERE lower(btrim(fbo_name)) = lower(%s)
          AND airport_code IS NOT NULL
          AND btrim(airport_code) <> ''
        ORDER BY airport_code
    """
    with conn.cursor() as cur:
        cur.execute(sql, (TARGET_FBO_NAME,))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def dryrun(conn):
    airports = fetch_target_airports(conn)

    print(f'DRY RUN: airports with FBO name "{TARGET_FBO_NAME}"')
    print(f"Total unique airports selected: {len(airports)}")
    print()

    if not airports:
        print("No matching airports found.")
        return 0

    for code in airports:
        print(code)

    return 0


def execute_updates(conn):
    airports = fetch_target_airports(conn)

    print(f'EXECUTE MODE: airports with FBO name "{TARGET_FBO_NAME}"')
    print(f"Total unique airports selected: {len(airports)}")
    print(f"Random delay between runs: {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS} seconds")
    print()

    if not airports:
        print("No matching airports found.")
        return 0

    if not UPDATE_SCRIPT.exists():
        print(f"ERROR: update script not found: {UPDATE_SCRIPT}", file=sys.stderr)
        return 1

    failures = 0

    for idx, code in enumerate(airports, start=1):
        print(f"[{idx}/{len(airports)}] Updating {code} ...")
        result = subprocess.run(
            [sys.executable, str(UPDATE_SCRIPT), code],
            text=True,
        )

        if result.returncode != 0:
            failures += 1
            print(f"  FAILED: {code}", file=sys.stderr)
        else:
            print(f"  OK: {code}")

        if idx < len(airports):
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            print(f"  Sleeping {delay:.1f} seconds...")
            time.sleep(delay)

    print()
    print(f"Completed. Success: {len(airports) - failures}, Failed: {failures}")
    return 1 if failures else 0


def main():
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == "--execute":
        dry_run = False

    with connect(DATABASE_URL) as conn:
        if dry_run:
            return dryrun(conn)
        return execute_updates(conn)


if __name__ == "__main__":
    raise SystemExit(main())
