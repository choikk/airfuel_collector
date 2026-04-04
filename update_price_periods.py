#!/usr/bin/env python3

import json
import os
import subprocess
import sys
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


def get_open_rows_for_airport(cur, airport_code):
    cur.execute(
        """
        SELECT
            id,
            airport_code,
            fbo_name,
            fuel_type,
            service_type,
            price,
            reported_date,
            guaranteed
        FROM price_periods
        WHERE airport_code = %s
          AND valid_to IS NULL
        """,
        (airport_code,),
    )
    rows = cur.fetchall()

    out = {}
    for row in rows:
        row_id, airport_code, fbo_name, fuel_type, service_type, price, reported_date, guaranteed = row
        key = (fbo_name, fuel_type, service_type)
        out[key] = {
            "id": row_id,
            "airport_code": airport_code,
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


def touch_open_rows_for_airport(cur, airport_code, seen_at):
    cur.execute(
        """
        UPDATE price_periods
        SET last_seen_at = %s
        WHERE airport_code = %s
          AND valid_to IS NULL
        """,
        (seen_at, airport_code),
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


def bump_check_priority_only(cur, airport_code: str):
    cur.execute(
        """
        UPDATE airports
        SET check_priority = LEAST(COALESCE(check_priority, 2) + 1, 5)
        WHERE airport_code = %s
        """,
        (airport_code,),
    )


def process_airport(airport_code: str):
    scraped = run_scraper(airport_code)
    ts = now_utc()

    scraped_prices = normalize_scraped_prices(scraped)

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            existing_open_rows = get_open_rows_for_airport(cur, airport_code)

            # 0. If both AirNav and FltPlan returned no prices, only bump check_priority
            if not scraped_prices:
                bump_check_priority_only(cur, airport_code)
                conn.commit()
                return scraped

            # 1. If airport not in DB at all, write all scraped prices
            if not existing_open_rows:
                for (fbo_name, fuel_type, service_type), data in scraped_prices.items():
                    insert_new_row(
                        cur,
                        airport_code,
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

            # 2. Airport exists in DB -> compare all current prices
            existing_price_map = {
                key: row["price"] for key, row in existing_open_rows.items()
            }
            scraped_price_map = {
                key: data["price"] for key, data in scraped_prices.items()
            }

            # 3. If all prices identical -> update last_seen_at only
            if existing_price_map == scraped_price_map:
                touch_open_rows_for_airport(cur, airport_code, ts)
                conn.commit()
                return scraped

            # 4. If any price changed -> write price history
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
                        airport_code,
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
                        airport_code,
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
