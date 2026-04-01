#!/usr/bin/env python3

import json
import os
from pathlib import Path

from psycopg import connect

BASE_DIR = Path(__file__).resolve().parent
JSON_PATH = BASE_DIR / "airport_base_info_with_runways_airspace_approaches.json"
DATABASE_URL = os.environ["NEON_DATABASE_URL"]

INSERT_SQL = """
INSERT INTO airports (
    airport_code,
    airport_name,
    city,
    state,
    lat,
    lon,
    fuel_raw,
    airspace_class
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (airport_code) DO UPDATE SET
    airport_name = EXCLUDED.airport_name,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    fuel_raw = EXCLUDED.fuel_raw,
    airspace_class = EXCLUDED.airspace_class
"""


def normalize_airspace_class(value):
    if value is None:
        return None
    s = str(value).strip().upper()
    if s in {"B", "C", "D", "E", "G"}:
        return s
    return None


def main():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    skipped = 0

    for airport_code, info in data.items():
        fuel_raw = info.get("fuel")

        # 연료 없는 공항 제외
        if fuel_raw in (None, "None", "", "NONE"):
            skipped += 1
            continue

        rows.append(
            (
                airport_code,
                info.get("airport_name"),
                info.get("city"),
                info.get("state"),
                info.get("lat"),
                info.get("lon"),
                fuel_raw,
                normalize_airspace_class(info.get("airspace")),
            )
        )

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)
        conn.commit()

    print(f"Inserted/updated: {len(rows)}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
