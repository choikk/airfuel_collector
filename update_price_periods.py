#!/usr/bin/env python3

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from psycopg import connect

DATABASE_URL = os.environ["NEON_DATABASE_URL"]
SCRAPER_PATH = "./airnav_fuel_scraper.py"


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

def run_scraper1(airport_code: str) -> dict:
    result = subprocess.run(
        [sys.executable, SCRAPER_PATH, airport_code],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def get_open_row(cur, airport_code, fbo_name, fuel_type, service_type):
    cur.execute(
        """
        SELECT id, price, reported_date, guaranteed
        FROM price_periods
        WHERE airport_code = %s
          AND fbo_name = %s
          AND fuel_type = %s
          AND service_type = %s
          AND valid_to IS NULL
        """,
        (airport_code, fbo_name, fuel_type, service_type),
    )
    return cur.fetchone()


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


def touch_open_row(cur, row_id, seen_at):
    cur.execute(
        """
        UPDATE price_periods
        SET last_seen_at = %s
        WHERE id = %s
        """,
        (seen_at, row_id),
    )


def insert_new_row(
    cur,
    airport_code,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
        """,
        (
            airport_code,
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


def process_airport(airport_code: str):
    scraped = run_scraper(airport_code)
    ts = now_utc()

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for provider in scraped.get("providers", []):
                fbo_name = provider["fbo_name"]
                reported_date = provider.get("last_update_date")
                guaranteed = bool(provider.get("guaranteed", False))

                for price_key, price_str in provider.get("prices", {}).items():
                    if price_str in (None, "", "-", "--", "---"):
                        continue

                    fuel_type, service_type = split_price_key(price_key)
                    price = Decimal(price_str)

                    open_row = get_open_row(
                        cur,
                        airport_code,
                        fbo_name,
                        fuel_type,
                        service_type,
                    )

                    if open_row is None:
                        insert_new_row(
                            cur,
                            airport_code,
                            fbo_name,
                            fuel_type,
                            service_type,
                            price,
                            reported_date,
                            guaranteed,
                            ts,
                        )
                        continue

                    row_id, old_price, old_reported_date, old_guaranteed = open_row

                    changed = (
                        Decimal(old_price) != price
                        or old_reported_date != reported_date
                        or bool(old_guaranteed) != guaranteed
                    )

                    if changed:
                        close_open_row(cur, row_id, ts)
                        insert_new_row(
                            cur,
                            airport_code,
                            fbo_name,
                            fuel_type,
                            service_type,
                            price,
                            reported_date,
                            guaranteed,
                            ts,
                        )
                    else:
                        touch_open_row(cur, row_id, ts)

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
