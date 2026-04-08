#!/usr/bin/env python3

import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from psycopg import connect

DATABASE_URL = os.environ["NEON_DATABASE_URL"]
BASE_DIR = Path(__file__).resolve().parent
SCRAPER_PATH = str(BASE_DIR / "airnav_fuel_scraper.py")


def now_utc():
    return datetime.now(timezone.utc)


def split_price_key(price_key: str):
    fuel_type, service_type = price_key.rsplit("_", 1)
    return fuel_type, service_type


def run_scraper(airport_code: str) -> dict:
    result = subprocess.run(
        [sys.executable, SCRAPER_PATH, airport_code],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"scraper failed for {airport_code}\n"
            f"STDOUT:\n{result.stdout}\n\n"
            f"STDERR:\n{result.stderr}"
        )

    return json.loads(result.stdout)


def resolve_airport_identity(cur, requested_airport_code: str):
    cur.execute(
        """
        SELECT airport_code, site_no
        FROM airports_v2
        WHERE airport_code = %s
        """,
        (requested_airport_code,),
    )
    row = cur.fetchone()

    if not row:
        raise RuntimeError(f"Unknown airport_code={requested_airport_code}")

    return row[0], row[1]


def get_open_rows_for_site(cur, site_no):
    cur.execute(
        """
        SELECT
            id,
            airport_code,
            site_no,
            fbo_name,
            fuel_type,
            service_type,
            price,
            reported_date,
            guaranteed
        FROM price_periods
        WHERE site_no = %s
          AND valid_to IS NULL
        """,
        (site_no,),
    )
    rows = cur.fetchall()

    out = {}
    for row in rows:
        (
            row_id,
            airport_code,
            row_site_no,
            fbo_name,
            fuel_type,
            service_type,
            price,
            reported_date,
            guaranteed,
        ) = row
        key = (fbo_name, fuel_type, service_type)
        out[key] = {
            "id": row_id,
            "airport_code": airport_code,
            "site_no": row_site_no,
            "fbo_name": fbo_name,
            "fuel_type": fuel_type,
            "service_type": service_type,
            "price": Decimal(price),
            "reported_date": reported_date,
            "guaranteed": bool(guaranteed),
        }
    return out


def close_open_row(cur, row_id, closed_at):
    cur.execute(
        """
        UPDATE price_periods
        SET valid_to = %s,
            last_seen_at = %s
        WHERE id = %s
        """,
        (closed_at, closed_at, row_id),
    )


def touch_open_rows_for_site(cur, site_no, seen_at):
    cur.execute(
        """
        UPDATE price_periods
        SET last_seen_at = %s
        WHERE site_no = %s
          AND valid_to IS NULL
        """,
        (seen_at, site_no),
    )


def insert_new_row(
    cur,
    airport_code,
    site_no,
    fbo_name,
    fuel_type,
    service_type,
    price,
    reported_date,
    guaranteed,
    ts,
):
    cur.execute(
        """
        INSERT INTO price_periods (
            airport_code,
            site_no,
            fbo_name,
            fuel_type,
            service_type,
            price,
            reported_date,
            guaranteed,
            valid_from,
            valid_to,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
        """,
        (
            airport_code,
            site_no,
            fbo_name,
            fuel_type,
            service_type,
            price,
            reported_date,
            guaranteed,
            ts,
            ts,
            ts,
        ),
    )


def rename_open_rows_fbo(cur, site_no, old_fbo_name, new_fbo_name):
    cur.execute(
        """
        UPDATE price_periods
        SET fbo_name = %s
        WHERE site_no = %s
          AND fbo_name = %s
          AND valid_to IS NULL
        """,
        (new_fbo_name, site_no, old_fbo_name),
    )


def normalize_scraped_prices(scraped: dict):
    """
    Convert scraper JSON into:
      {
        (fbo_name, fuel_type, service_type): {
            "price": Decimal(...),
            "reported_date": ...,
            "guaranteed": ...
        }
      }
    """
    out = {}

    for provider in scraped.get("providers", []):
        fbo_name = provider["fbo_name"]
        reported_date = provider.get("last_update_date")
        guaranteed = bool(provider.get("guaranteed", False))

        for price_key, price_str in provider.get("prices", {}).items():
            if price_str in (None, "", "-", "--", "---"):
                continue

            fuel_type, service_type = split_price_key(price_key)

            out[(fbo_name, fuel_type, service_type)] = {
                "price": Decimal(price_str),
                "reported_date": reported_date,
                "guaranteed": guaranteed,
            }

    return out


def bump_check_priority_only(cur, airport_code: str, checked_at):
    """
    No prices found from AirNav/FltPlan.
    Bump scheduler priority in airport_scrape_status_v2.
    """
    cur.execute(
        """
        INSERT INTO airport_scrape_status_v2 (
            airport_code,
            last_checked_at,
            check_priority
        )
        VALUES (%s, %s, 3)
        ON CONFLICT (airport_code) DO UPDATE
        SET last_checked_at = EXCLUDED.last_checked_at,
            check_priority = LEAST(
                COALESCE(airport_scrape_status_v2.check_priority, 2) + 1,
                5
            )
        """,
        (airport_code, checked_at),
    )


def mark_checked(cur, airport_code: str, checked_at):
    cur.execute(
        """
        INSERT INTO airport_scrape_status_v2 (
            airport_code,
            last_checked_at
        )
        VALUES (%s, %s)
        ON CONFLICT (airport_code) DO UPDATE
        SET last_checked_at = EXCLUDED.last_checked_at
        """,
        (airport_code, checked_at),
    )


def group_existing_open_rows_by_fbo(existing_open_rows):
    groups = defaultdict(list)
    for row in existing_open_rows.values():
        groups[row["fbo_name"]].append(row)

    out = {}
    for fbo_name, rows in groups.items():
        out[fbo_name] = {
            "rows": rows,
            "fuel_family": tuple(sorted({row["fuel_type"] for row in rows})),
        }
    return out


def group_scraped_prices_by_fbo(scraped_prices):
    groups = defaultdict(list)
    for key, data in scraped_prices.items():
        fbo_name, fuel_type, service_type = key
        groups[fbo_name].append((key, data))

    out = {}
    for fbo_name, items in groups.items():
        out[fbo_name] = {
            "items": items,
            "fuel_family": tuple(sorted({fuel_type for (_, fuel_type, _), _ in items})),
        }
    return out


def apply_fbo_name_corrections(cur, site_no, existing_open_rows, scraped_prices):
    """
    Rule:
    - Match within same airport via site_no
    - If fuel family matches exactly but FBO name differs,
      rename open rows instead of treating them as a different provider

    Safety:
    - Only rename when exactly one candidate existing FBO group matches the fuel family
    """
    if not existing_open_rows or not scraped_prices:
        return existing_open_rows

    existing_groups = group_existing_open_rows_by_fbo(existing_open_rows)
    scraped_groups = group_scraped_prices_by_fbo(scraped_prices)

    renamed_existing = dict(existing_open_rows)

    for new_fbo_name, scraped_group in scraped_groups.items():
        if new_fbo_name in existing_groups:
            continue

        matching_existing_fbos = [
            old_fbo_name
            for old_fbo_name, existing_group in existing_groups.items()
            if existing_group["fuel_family"] == scraped_group["fuel_family"]
        ]

        if len(matching_existing_fbos) != 1:
            continue

        old_fbo_name = matching_existing_fbos[0]
        if old_fbo_name == new_fbo_name:
            continue

        rename_open_rows_fbo(cur, site_no, old_fbo_name, new_fbo_name)

        updated = {}
        for key, row in renamed_existing.items():
            fbo_name, fuel_type, service_type = key
            if fbo_name == old_fbo_name:
                new_key = (new_fbo_name, fuel_type, service_type)
                new_row = dict(row)
                new_row["fbo_name"] = new_fbo_name
                updated[new_key] = new_row
            else:
                updated[key] = row

        renamed_existing = updated
        existing_groups = group_existing_open_rows_by_fbo(renamed_existing)

    return renamed_existing


def process_airport(requested_airport_code: str):
    ts = now_utc()

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            canonical_airport_code, site_no = resolve_airport_identity(
                cur, requested_airport_code
            )

            scraped = run_scraper(canonical_airport_code)
            scraped_prices = normalize_scraped_prices(scraped)

            existing_open_rows = get_open_rows_for_site(cur, site_no)

            # If both AirNav and FltPlan returned no prices, only bump priority
            if not scraped_prices:
                bump_check_priority_only(cur, canonical_airport_code, ts)
                conn.commit()
                return scraped

            mark_checked(cur, canonical_airport_code, ts)

            # If airport has no open rows yet, insert all scraped prices
            if not existing_open_rows:
                for (fbo_name, fuel_type, service_type), data in scraped_prices.items():
                    insert_new_row(
                        cur,
                        canonical_airport_code,
                        site_no,
                        fbo_name,
                        fuel_type,
                        service_type,
                        data["price"],
                        data["reported_date"],
                        data["guaranteed"],
                        ts,
                    )
                conn.commit()
                return scraped

            # Correct renamed FBOs first
            existing_open_rows = apply_fbo_name_corrections(
                cur,
                site_no,
                existing_open_rows,
                scraped_prices,
            )

            existing_price_map = {
                key: row["price"] for key, row in existing_open_rows.items()
            }
            scraped_price_map = {
                key: data["price"] for key, data in scraped_prices.items()
            }

            # If all prices identical, only update last_seen_at
            if existing_price_map == scraped_price_map:
                touch_open_rows_for_site(cur, site_no, ts)
                conn.commit()
                return scraped

            # Close rows that disappeared or changed
            for key, old_row in existing_open_rows.items():
                if key not in scraped_prices:
                    close_open_row(cur, old_row["id"], ts)
                    continue

                new_price = scraped_prices[key]["price"]
                if old_row["price"] != new_price:
                    close_open_row(cur, old_row["id"], ts)

            # Insert rows that are new or changed
            for key, new_data in scraped_prices.items():
                fbo_name, fuel_type, service_type = key

                if key not in existing_open_rows:
                    insert_new_row(
                        cur,
                        canonical_airport_code,
                        site_no,
                        fbo_name,
                        fuel_type,
                        service_type,
                        new_data["price"],
                        new_data["reported_date"],
                        new_data["guaranteed"],
                        ts,
                    )
                    continue

                old_row = existing_open_rows[key]
                if old_row["price"] != new_data["price"]:
                    insert_new_row(
                        cur,
                        canonical_airport_code,
                        site_no,
                        fbo_name,
                        fuel_type,
                        service_type,
                        new_data["price"],
                        new_data["reported_date"],
                        new_data["guaranteed"],
                        ts,
                    )

        conn.commit()

    return scraped


def main():
    if len(sys.argv) != 2:
        print("Usage: python update_price_periods.py <AIRPORT_CODE>")
        sys.exit(2)

    airport_code = sys.argv[1].strip().upper()
    result = process_airport(airport_code)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
