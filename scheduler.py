#!/usr/bin/env python3

import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg import connect

DATABASE_URL = os.environ["NEON_DATABASE_URL"]
BASE_DIR = Path(__file__).resolve().parent
UPDATE_SCRIPT = str(BASE_DIR / "update_price_periods.py")

# conservative defaults
MAX_AIRPORTS_PER_RUN = 25
MIN_DELAY_SECONDS = 10
MAX_DELAY_SECONDS = 30


def now_utc():
    return datetime.now(timezone.utc)


def random_delay_seconds() -> float:
    return random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)


def compute_next_check_at(
    airspace_class: str | None,
    changed: bool,
    consecutive_no_change_count: int,
) -> datetime:
    airspace = (airspace_class or "").upper()

    if changed:
        if airspace == "B":
            days = random.uniform(1.0, 2.0)
        elif airspace == "C":
            days = random.uniform(1.5, 2.5)
        elif airspace == "D":
            days = random.uniform(2.0, 4.0)
        elif airspace == "E":
            days = random.uniform(2.0, 4.0)
        elif airspace == "G":
            days = random.uniform(2.0, 4.0)
        else:
            days = random.uniform(3.0, 7.0)
    else:
        if airspace == "B":
            days = random.uniform(2.0, 4.0)
        elif airspace == "C":
            days = random.uniform(3.0, 5.0)
        elif airspace == "D":
            days = random.uniform(4.0, 7.0)
        elif airspace == "E":
            days = random.uniform(4.0, 7.0)
        elif airspace == "G":
            days = random.uniform(4.0, 7.0)
        else:
            days = random.uniform(6.0, 10.0)

        if consecutive_no_change_count >= 10:
            days *= 1.5
        elif consecutive_no_change_count >= 5:
            days *= 1.2

    return now_utc() + timedelta(days=days)


def fetch_due_airports(cur, limit: int):
    """
    Use airports_v2 for airport metadata and airport_scrape_status_v2
    for scheduler state.

    We select airports that:
    - have some fuel indicated in airports_v2.fuel_raw
    - have never been checked, or are due now
    """
    cur.execute(
        """
        SELECT
            a.airport_code,
            a.airspace_class,
            COALESCE(s.consecutive_no_change_count, 0) AS consecutive_no_change_count
        FROM airports_v2 a
        LEFT JOIN airport_scrape_status_v2 s
          ON s.airport_code = a.airport_code
        WHERE a.fuel_raw IS NOT NULL
          AND btrim(a.fuel_raw) <> ''
          AND upper(btrim(a.fuel_raw)) <> 'NONE'
          AND (
                s.last_checked_at IS NULL
                OR s.next_check_at IS NULL
                OR s.next_check_at <= NOW()
              )
        ORDER BY
            s.last_checked_at ASC NULLS FIRST,
            COALESCE(s.check_priority, 2) ASC,
            CASE WHEN s.last_checked_at IS NULL THEN random() ELSE 0 END,
            a.airport_code ASC
        LIMIT %s
        """,
        (limit,),
    )
    return cur.fetchall()


def get_site_no_for_airport(cur, airport_code: str) -> str:
    cur.execute(
        """
        SELECT site_no
        FROM airports_v2
        WHERE airport_code = %s
        """,
        (airport_code,),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"site_no not found in airports_v2 for airport_code={airport_code}")
    return row[0]


def get_price_snapshot(cur, airport_code: str):
    """
    Compare current open prices by site_no, not by legacy airport_code.
    This avoids code-change issues and matches the new update_price_periods.py logic.
    """
    site_no = get_site_no_for_airport(cur, airport_code)

    cur.execute(
        """
        SELECT
            fbo_name,
            fuel_type,
            service_type,
            price
        FROM price_periods
        WHERE site_no = %s
          AND valid_to IS NULL
        ORDER BY fbo_name, fuel_type, service_type
        """,
        (site_no,),
    )
    rows = cur.fetchall()
    return tuple(rows)


def update_airport_schedule(
    cur,
    airport_code: str,
    changed: bool,
    airspace_class: str | None,
    old_no_change_count: int,
):
    current_ts = now_utc()

    if changed:
        next_count = 0
        last_change_at = current_ts
    else:
        next_count = old_no_change_count + 1
        last_change_at = None

    next_check_at = compute_next_check_at(
        airspace_class=airspace_class,
        changed=changed,
        consecutive_no_change_count=next_count,
    )

    if changed:
        cur.execute(
            """
            INSERT INTO airport_scrape_status_v2 (
                airport_code,
                last_checked_at,
                next_check_at,
                check_priority,
                last_change_at,
                consecutive_no_change_count
            )
            VALUES (%s, %s, %s, 2, %s, %s)
            ON CONFLICT (airport_code) DO UPDATE
            SET last_checked_at = EXCLUDED.last_checked_at,
                next_check_at = EXCLUDED.next_check_at,
                last_change_at = EXCLUDED.last_change_at,
                consecutive_no_change_count = EXCLUDED.consecutive_no_change_count
            """,
            (airport_code, current_ts, next_check_at, last_change_at, next_count),
        )
    else:
        cur.execute(
            """
            INSERT INTO airport_scrape_status_v2 (
                airport_code,
                last_checked_at,
                next_check_at,
                check_priority,
                consecutive_no_change_count
            )
            VALUES (%s, %s, %s, 2, %s)
            ON CONFLICT (airport_code) DO UPDATE
            SET last_checked_at = EXCLUDED.last_checked_at,
                next_check_at = EXCLUDED.next_check_at,
                consecutive_no_change_count = EXCLUDED.consecutive_no_change_count
            """,
            (airport_code, current_ts, next_check_at, next_count),
        )


def run_update_script(airport_code: str):
    result = subprocess.run(
        [sys.executable, UPDATE_SCRIPT, airport_code],
        capture_output=True,
        text=True,
    )
    return result


def process_one_airport(
    conn,
    airport_code: str,
    airspace_class: str | None,
    consecutive_no_change_count: int,
):
    with conn.cursor() as cur:
        before_snapshot = get_price_snapshot(cur, airport_code)

    result = run_update_script(airport_code)

    if result.returncode != 0:
        raise RuntimeError(
            f"update failed for {airport_code}\n"
            f"STDOUT:\n{result.stdout}\n\n"
            f"STDERR:\n{result.stderr}"
        )

    with conn.cursor() as cur:
        after_snapshot = get_price_snapshot(cur, airport_code)
        changed = before_snapshot != after_snapshot

        update_airport_schedule(
            cur,
            airport_code=airport_code,
            changed=changed,
            airspace_class=airspace_class,
            old_no_change_count=consecutive_no_change_count,
        )

    conn.commit()
    return changed


def main():
    run_limit = MAX_AIRPORTS_PER_RUN
    if len(sys.argv) > 1:
        run_limit = int(sys.argv[1])

    processed = 0
    changed_count = 0
    unchanged_count = 0

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            due_airports = fetch_due_airports(cur, run_limit)

        if not due_airports:
            print("No airports due.")
            return

        for idx, (airport_code, airspace_class, consecutive_no_change_count) in enumerate(due_airports, start=1):
            print(f"[{idx}/{len(due_airports)}] Processing {airport_code} ...")

            try:
                changed = process_one_airport(
                    conn,
                    airport_code=airport_code,
                    airspace_class=airspace_class,
                    consecutive_no_change_count=consecutive_no_change_count,
                )
                if changed:
                    changed_count += 1
                    print(f"  changed: {airport_code}")
                else:
                    unchanged_count += 1
                    print(f"  unchanged: {airport_code}")

                processed += 1

            except Exception as e:
                conn.rollback()
                print(f"  failed: {airport_code}: {e}", file=sys.stderr)

            if idx < len(due_airports):
                delay = random_delay_seconds()
                print(f"  sleeping {delay:.1f} seconds")
                time.sleep(delay)

    print()
    print(f"Processed: {processed}")
    print(f"Changed: {changed_count}")
    print(f"Unchanged: {unchanged_count}")


if __name__ == "__main__":
    main()
