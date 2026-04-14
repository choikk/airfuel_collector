#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

from psycopg import connect


def parse_args():
    parser = argparse.ArgumentParser(description="Run a SQL migration file.")
    parser.add_argument(
        "sql_file",
        help="Path to a .sql migration file",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("NEON_DATABASE_URL"),
        help="Postgres connection string; defaults to NEON_DATABASE_URL",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.database_url:
        raise SystemExit("NEON_DATABASE_URL not provided")

    sql_path = Path(args.sql_file).resolve()
    if not sql_path.exists():
        raise SystemExit(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")

    with connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()

    print(f"Applied migration: {sql_path}")


if __name__ == "__main__":
    main()
