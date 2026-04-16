#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import random
import time
from dataclasses import dataclass

from psycopg import connect

from airnav_fuel_scraper import scrape_prices


@dataclass
class AirportBackfillResult:
    airport_code: str
    updated_rows: int
    matched_fbos: int
    renamed_rows: int
    scraped_providers: int


def price_signature_from_provider(provider: dict) -> tuple[tuple[str, str, str], ...]:
    signature = []
    for price_key, price_str in (provider.get("prices") or {}).items():
        if price_str in (None, "", "-", "--", "---"):
            continue
        fuel_type, service_type = price_key.rsplit("_", 1)
        signature.append((fuel_type, service_type, str(price_str)))
    return tuple(sorted(signature))


def fetch_open_fbo_groups(cur, airport_code: str):
    cur.execute(
        """
        SELECT
            fbo_name,
            price_signature
        FROM (
            SELECT
                fbo_name,
                array_agg(
                    DISTINCT (fuel_type || '|' || service_type || '|' || price::text)
                    ORDER BY (fuel_type || '|' || service_type || '|' || price::text)
                ) AS price_signature
            FROM price_periods
            WHERE airport_code = %s
              AND valid_to IS NULL
            GROUP BY fbo_name
        ) grouped
        """,
        (airport_code,),
    )
    return {
        row[0]: {
            "price_signature": tuple(
                tuple(part for part in item.split("|", 2))
                for item in (row[1] or [])
            ),
        }
        for row in cur.fetchall()
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill missing fbo_phone values for open price_periods rows.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("NEON_DATABASE_URL"),
        help="Postgres connection string; defaults to NEON_DATABASE_URL",
    )
    parser.add_argument(
        "--airport",
        help="Backfill one airport only",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of airports to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without committing",
    )
    parser.add_argument(
        "--min-delay-seconds",
        type=float,
        default=2.0,
        help="Minimum delay between airports; default 2 seconds",
    )
    parser.add_argument(
        "--max-delay-seconds",
        type=float,
        default=5.0,
        help="Maximum delay between airports; default 5 seconds",
    )
    return parser.parse_args()


def fetch_target_airports(cur, airport_code: str | None, limit: int | None):
    if airport_code:
        return [airport_code.upper()]

    params: list[object] = []
    sql = """
        SELECT DISTINCT airport_code
        FROM price_periods
        WHERE valid_to IS NULL
          AND airport_code IS NOT NULL
          AND btrim(airport_code) <> ''
          AND (fbo_phone IS NULL OR btrim(fbo_phone) = '')
    """

    sql += " ORDER BY airport_code"

    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, params)
    return [row[0] for row in cur.fetchall()]


def update_airport_fbo_phones(cur, airport_code: str, dry_run: bool) -> AirportBackfillResult:
    scraped = scrape_prices(airport_code)
    providers = scraped.get("providers", [])
    open_fbo_groups = fetch_open_fbo_groups(cur, airport_code)

    updated_rows = 0
    matched_fbos = 0
    renamed_rows = 0

    for provider in providers:
        fbo_name = provider.get("fbo_name")
        fbo_phone = provider.get("fbo_phone")
        price_signature = price_signature_from_provider(provider)

        if not fbo_name or not fbo_phone:
            continue

        target_fbo_name = None

        if fbo_name in open_fbo_groups:
            target_fbo_name = fbo_name
        else:
            matching_existing_fbos = [
                existing_fbo_name
                for existing_fbo_name, group_data in open_fbo_groups.items()
                if group_data["price_signature"] == price_signature
            ]
            if len(matching_existing_fbos) == 1:
                target_fbo_name = matching_existing_fbos[0]

        if not target_fbo_name:
            continue

        if target_fbo_name != fbo_name and fbo_name not in open_fbo_groups:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM price_periods
                WHERE airport_code = %s
                  AND fbo_name = %s
                  AND valid_to IS NULL
                """,
                (airport_code, target_fbo_name),
            )
            rename_count = cur.fetchone()[0]

            if rename_count > 0:
                renamed_rows += rename_count
                if not dry_run:
                    cur.execute(
                        """
                        UPDATE price_periods
                        SET fbo_name = %s
                        WHERE airport_code = %s
                          AND fbo_name = %s
                          AND valid_to IS NULL
                        """,
                        (fbo_name, airport_code, target_fbo_name),
                    )

                open_fbo_groups[fbo_name] = open_fbo_groups.pop(target_fbo_name)
                target_fbo_name = fbo_name

        cur.execute(
            """
            SELECT COUNT(*)
            FROM price_periods
            WHERE airport_code = %s
              AND fbo_name = %s
              AND valid_to IS NULL
              AND (fbo_phone IS NULL OR btrim(fbo_phone) = '')
            """,
            (airport_code, target_fbo_name),
        )
        missing_count = cur.fetchone()[0]

        if missing_count == 0:
            continue

        matched_fbos += 1
        updated_rows += missing_count

        if not dry_run:
            cur.execute(
                """
                UPDATE price_periods
                SET fbo_phone = %s
                WHERE airport_code = %s
                  AND fbo_name = %s
                  AND valid_to IS NULL
                  AND (fbo_phone IS NULL OR btrim(fbo_phone) = '')
                """,
                (fbo_phone, airport_code, target_fbo_name),
            )

    return AirportBackfillResult(
        airport_code=airport_code,
        updated_rows=updated_rows,
        matched_fbos=matched_fbos,
        renamed_rows=renamed_rows,
        scraped_providers=len(providers),
    )


def process_one_airport(database_url: str, airport_code: str, dry_run: bool) -> AirportBackfillResult:
    with connect(database_url) as conn:
        with conn.cursor() as cur:
            result = update_airport_fbo_phones(
                cur,
                airport_code=airport_code,
                dry_run=dry_run,
            )
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    return result


def main():
    args = parse_args()

    if not args.database_url:
        raise SystemExit("NEON_DATABASE_URL not provided")
    if args.min_delay_seconds < 0 or args.max_delay_seconds < 0:
        raise SystemExit("Delay values must be non-negative")
    if args.min_delay_seconds > args.max_delay_seconds:
        raise SystemExit("--min-delay-seconds cannot be greater than --max-delay-seconds")

    with connect(args.database_url) as conn:
        with conn.cursor() as cur:
            airport_codes = fetch_target_airports(
                cur,
                airport_code=args.airport,
                limit=args.limit,
            )

            if not airport_codes:
                print("No airports need fbo_phone backfill.")
                return

            total_airports = len(airport_codes)
            total_rows = 0
            failures = 0

            for idx, airport_code in enumerate(airport_codes, start=1):
                try:
                    result = process_one_airport(
                        database_url=args.database_url,
                        airport_code=airport_code,
                        dry_run=args.dry_run,
                    )
                    total_rows += result.updated_rows
                    print(
                        f"[{idx}/{total_airports}] {result.airport_code}: "
                        f"providers={result.scraped_providers}, "
                        f"matched_fbos={result.matched_fbos}, "
                        f"renamed_rows={result.renamed_rows}, "
                        f"updated_rows={result.updated_rows}"
                    )
                except Exception as exc:
                    failures += 1
                    print(f"[{idx}/{total_airports}] {airport_code}: FAILED: {exc}")

                if idx < total_airports:
                    delay = random.uniform(args.min_delay_seconds, args.max_delay_seconds)
                    print(f"  sleeping {delay:.1f} seconds")
                    time.sleep(delay)

            if args.dry_run:
                print(
                    f"\nDry run only. Would update {total_rows} rows across "
                    f"{total_airports - failures} successful airports."
                )
            else:
                print(
                    f"\nUpdated {total_rows} rows across "
                    f"{total_airports - failures} successful airports."
                )
            if failures:
                print(f"Failures: {failures}")


if __name__ == "__main__":
    main()
