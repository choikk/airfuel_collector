#!/usr/bin/env python3

import json
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
            COALESCE(s.consecutive_no_change_count, 0) AS consecutive_no_change_count,
            COALESCE(s.check_priority, 2) AS current_priority
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


def get_scheduler_priority(cur, airport_code: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(check_priority, 2)
        FROM airport_scrape_status_v2
        WHERE airport_code = %s
        """,
        (airport_code,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 2


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


def snapshot_to_price_map(snapshot):
    return {
        (fbo_name, fuel_type, service_type): price
        for fbo_name, fuel_type, service_type, price in snapshot
    }


def diff_price_snapshots(before_snapshot, after_snapshot):
    before_map = snapshot_to_price_map(before_snapshot)
    after_map = snapshot_to_price_map(after_snapshot)
    changes = []

    for key in sorted(before_map.keys() & after_map.keys()):
        old_price = before_map[key]
        new_price = after_map[key]
        if old_price == new_price:
            continue

        fbo_name, fuel_type, service_type = key
        changes.append(
            {
                "fbo_name": fbo_name,
                "fuel_type": fuel_type,
                "service_type": service_type,
                "old_price": str(old_price),
                "new_price": str(new_price),
            }
        )

    return changes


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
                check_priority = CASE
                    WHEN COALESCE(airport_scrape_status_v2.check_priority, 2) >= 10 THEN 3
                    ELSE LEAST(COALESCE(airport_scrape_status_v2.check_priority, 2) + 1, 3)
                END,
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
                check_priority = CASE
                    WHEN COALESCE(airport_scrape_status_v2.check_priority, 2) >= 10 THEN 3
                    ELSE LEAST(COALESCE(airport_scrape_status_v2.check_priority, 2) + 1, 3)
                END,
                consecutive_no_change_count = EXCLUDED.consecutive_no_change_count
            """,
            (airport_code, current_ts, next_check_at, next_count),
        )


def record_attempt_only(cur, airport_code: str):
    """
    Even if update_price_periods.py failed to apply changes, record that the
    scheduler attempted this airport and bump the scheduler priority.
    """
    current_ts = now_utc()
    cur.execute(
        """
        INSERT INTO airport_scrape_status_v2 (
            airport_code,
            last_checked_at,
            check_priority
        )
        VALUES (%s, %s, 2)
        ON CONFLICT (airport_code) DO UPDATE
        SET last_checked_at = EXCLUDED.last_checked_at,
            check_priority = CASE
                WHEN COALESCE(airport_scrape_status_v2.check_priority, 2) >= 10 THEN 3
                ELSE LEAST(COALESCE(airport_scrape_status_v2.check_priority, 2) + 1, 3)
            END
        """,
        (airport_code, current_ts),
    )


def run_update_script(airport_code: str):
    result = subprocess.run(
        [sys.executable, UPDATE_SCRIPT, airport_code],
        capture_output=True,
        text=True,
    )
    return result


def scraped_has_prices(scraped: dict) -> bool:
    for provider in scraped.get("providers", []):
        for value in (provider.get("prices") or {}).values():
            if value not in (None, "", "-", "--", "---"):
                return True
    return False


def process_one_airport(
    conn,
    airport_code: str,
    airspace_class: str | None,
    consecutive_no_change_count: int,
    current_priority: int,
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

    scraped = json.loads(result.stdout)
    if not scraped_has_prices(scraped):
        with conn.cursor() as cur:
            after_priority = get_scheduler_priority(cur, airport_code)
        return {
            "changed": False,
            "no_prices_found": True,
            "before_priority": current_priority,
            "after_priority": after_priority,
            "price_changes": [],
        }

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
        after_priority = get_scheduler_priority(cur, airport_code)

    conn.commit()
    return {
        "changed": changed,
        "no_prices_found": False,
        "before_priority": current_priority,
        "after_priority": after_priority,
        "price_changes": diff_price_snapshots(before_snapshot, after_snapshot),
    }


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

        for idx, (airport_code, airspace_class, consecutive_no_change_count, current_priority) in enumerate(due_airports, start=1):
            print(
                f"[{idx}/{len(due_airports)}] Processing {airport_code} "
                f"(priority {current_priority}) ..."
            )

            try:
                result = process_one_airport(
                    conn,
                    airport_code=airport_code,
                    airspace_class=airspace_class,
                    consecutive_no_change_count=consecutive_no_change_count,
                    current_priority=current_priority,
                )
                before_priority = result["before_priority"]
                after_priority = result["after_priority"]

                if result["no_prices_found"]:
                    unchanged_count += 1
                    print(
                        f"  no FBO/prices found: {airport_code} "
                        f"(priority {before_priority} -> {after_priority})"
                    )
                elif result["changed"]:
                    changed_count += 1
                    print(
                        f"  changed: {airport_code} "
                        f"(priority {before_priority} -> {after_priority})"
                    )
                    for price_change in result["price_changes"]:
                        print(
                            "    "
                            f"{price_change['fbo_name']} "
                            f"{price_change['fuel_type']}_{price_change['service_type']}: "
                            f"{price_change['old_price']} -> {price_change['new_price']}"
                        )
                else:
                    unchanged_count += 1
                    print(
                        f"  unchanged: {airport_code} "
                        f"(priority {before_priority} -> {after_priority})"
                    )

                processed += 1

            except Exception as e:
                conn.rollback()
                with conn.cursor() as cur:
                    record_attempt_only(cur, airport_code)
                    after_priority = get_scheduler_priority(cur, airport_code)
                conn.commit()
                print(
                    f"  failed: {airport_code} "
                    f"(priority {current_priority} -> {after_priority}): {e}",
                    file=sys.stderr,
                )

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
