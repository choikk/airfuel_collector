#!/usr/bin/env python3
"""
airnav_fuel_scraper.py

Usage:
    python airnav_fuel_scraper.py KGAI

Output JSON example:
{
  "today_date": "2026-03-27",
  "airport_code": "KGAI",
  "source_url": "https://www.airnav.com/airport/KGAI",
  "scraped_at": "2026-03-27T18:08:42+00:00",
  "providers": [
    {
      "fbo_name": "DC Metro Aviation Services",
      "last_update_date": "2026-03-27",
      "guaranteed": true,
      "prices": {
        "100LL_FULL": "7.25",
        "100LL_SELF": "6.70",
        "JET_A": "7.23"
      }
    }
  ]
}

Rules:
- Parse only the "FBO, Fuel Providers, and Aircraft Ground Support" section
- Stop when "Alternatives at nearby airports" appears
- If last_update_date is missing and GUARANTEED is present, use today_date
- Ignore nearby-airport alternatives entirely
- Prices are always emitted as strings with 2 decimal places
- If FBO name starts with "More info and photos of ", strip that prefix
- In addition to 100LL and JET_A, also capture MOGAS, UL94, UL91 when present
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.airnav.com/airport/{code}"
USER_AGENT = "Mozilla/5.0 (compatible; FuelPriceTracker/0.1)"
TIMEOUT = 20

SUPPORTED_FUELS = [
    "100LL",
    "JET_A",
    "MOGAS",
    "UL94",
    "UL91",
]


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_airport_code(code: str) -> str:
    code = clean_text(code).upper()
    if not re.fullmatch(r"[A-Z0-9]{2,8}", code):
        raise ValueError(f"Invalid airport code: {code!r}")
    return code


def normalize_fbo_name(name: str) -> str:
    name = clean_text(name)
    name = re.sub(r"^More info and photos of\s+", "", name, flags=re.I)
    return clean_text(name)


def parse_price(token: str) -> Optional[float]:
    token = clean_text(token)
    if token in {"", "-", "--", "---"}:
        return None
    token = token.replace("$", "").replace(",", "")
    try:
        return float(token)
    except ValueError:
        return None


def format_price(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return f"{value:.2f}"


def parse_airnav_date(text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2}-[A-Za-z]{3}-\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d-%b-%Y").date().isoformat()
    except ValueError:
        return None


def fetch_airport_page(airport_code: str) -> str:
    url = BASE_URL.format(code=airport_code)
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.text


def find_section_start(soup: BeautifulSoup) -> Optional[Tag]:
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong", "font"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if "FBO, Fuel Providers, and Aircraft Ground Support" in txt:
            return tag
    return None


def find_section_end(start_tag: Tag) -> Optional[Tag]:
    for tag in start_tag.find_all_next():
        if not isinstance(tag, Tag):
            continue
        txt = clean_text(tag.get_text(" ", strip=True))
        if "Alternatives at nearby airports" in txt:
            return tag
    return None


def is_between(tag: Tag, start_tag: Tag, end_tag: Optional[Tag]) -> bool:
    try:
        if not start_tag.sourceline or not tag.sourceline:
            return True
        if end_tag and end_tag.sourceline:
            return start_tag.sourceline <= tag.sourceline < end_tag.sourceline
        return tag.sourceline >= start_tag.sourceline
    except Exception:
        return True


def extract_fbo_name_from_row(tr: Tag) -> Optional[str]:
    anchors = tr.find_all("a", href=True)
    for a in anchors:
        text = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")
        if not text:
            continue
        if text.lower() in {"write", "read", "comment", "comments", "web site", "email"}:
            continue
        if "mailto:" in href.lower():
            continue
        text = normalize_fbo_name(text)
        if text:
            return text
    return None


def detect_header_fuels(text: str) -> List[str]:
    """
    Detect fuel columns in header order from the row text.
    We only use supported fuels.
    """
    tokens_in_order = []
    patterns = [
        ("100LL", r"\b100LL\b"),
        ("JET_A", r"\bJet A\b|\bJET A\b|\bJet-A\b|\bJET-A\b"),
        ("MOGAS", r"\bMOGAS\b"),
        ("UL94", r"\bUL94\b"),
        ("UL91", r"\bUL91\b"),
    ]

    positions = []
    for fuel_name, pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.I):
            positions.append((m.start(), fuel_name))
            break

    positions.sort(key=lambda x: x[0])
    for _, fuel_name in positions:
        if fuel_name not in tokens_in_order:
            tokens_in_order.append(fuel_name)

    return tokens_in_order


def init_price_dict() -> Dict[str, Optional[float]]:
    prices: Dict[str, Optional[float]] = {}
    for fuel in SUPPORTED_FUELS:
        prices[f"{fuel}_FULL"] = None
        prices[f"{fuel}_SELF"] = None
        prices[f"{fuel}_RA"] = None
    return prices


def parse_prices_from_text(text: str) -> Dict[str, Optional[float]]:
    prices = init_price_dict()
    header_fuels = detect_header_fuels(text)

    row_matches = list(
        re.finditer(
            r"\b(FS|SS|RA)\b\s+"
            r"(\$?[\d.,]+|---)"
            r"(?:\s+(\$?[\d.,]+|---))?"
            r"(?:\s+(\$?[\d.,]+|---))?"
            r"(?:\s+(\$?[\d.,]+|---))?"
            r"(?:\s+(\$?[\d.,]+|---))?",
            text,
            flags=re.IGNORECASE,
        )
    )

    for m in row_matches:
        service = m.group(1).upper()
        raw_values = [m.group(i) for i in range(2, 7)]
        parsed_values = [parse_price(v) for v in raw_values if v is not None]

        if not header_fuels:
            continue

        for idx, fuel in enumerate(header_fuels):
            if idx >= len(parsed_values):
                break
            value = parsed_values[idx]
            suffix = {
                "FS": "FULL",
                "SS": "SELF",
                "RA": "RA",
            }[service]
            key = f"{fuel}_{suffix}"
            if key in prices:
                prices[key] = value

    return prices


def compact_formatted_prices(prices: Dict[str, Optional[float]]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for key, value in prices.items():
        if value is not None:
            out[key] = format_price(value)
    return out


def parse_provider_rows(
    soup: BeautifulSoup, section_start: Tag, section_end: Optional[Tag]
) -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).date().isoformat()
    providers: List[Dict[str, Any]] = []
    seen_names = set()

    for tr in soup.find_all("tr"):
        if not is_between(tr, section_start, section_end):
            continue

        row_text = clean_text(tr.get_text(" ", strip=True))
        if not row_text:
            continue
        if "Alternatives at nearby airports" in row_text:
            break

        if not any(
            token in row_text.upper()
            for token in ["100LL", "JET A", "JET-A", "MOGAS", "UL94", "UL91"]
        ):
            continue
        if "FS" not in row_text and "SS" not in row_text and "RA" not in row_text:
            continue

        fbo_name = extract_fbo_name_from_row(tr)
        if not fbo_name:
            continue
        if fbo_name in seen_names:
            continue

        guaranteed = "GUARANTEED" in row_text.upper()
        last_update_date = parse_airnav_date(row_text)
        if last_update_date is None and guaranteed:
            last_update_date = today_str

        raw_prices = parse_prices_from_text(row_text)
        prices = compact_formatted_prices(raw_prices)
        if not prices:
            continue

        providers.append(
            {
                "fbo_name": fbo_name,
                "last_update_date": last_update_date,
                "guaranteed": guaranteed,
                "prices": prices,
            }
        )
        seen_names.add(fbo_name)

    return providers


def scrape_airnav_prices(airport_code: str) -> Dict[str, Any]:
    airport_code = normalize_airport_code(airport_code)
    html = fetch_airport_page(airport_code)
    soup = BeautifulSoup(html, "lxml")

    section_start = find_section_start(soup)
    providers: List[Dict[str, Any]] = []

    if section_start is not None:
        section_end = find_section_end(section_start)
        providers = parse_provider_rows(soup, section_start, section_end)

    now = datetime.now(timezone.utc).replace(microsecond=0)

    return {
        "today_date": now.date().isoformat(),
        "airport_code": airport_code,
        "source_url": BASE_URL.format(code=airport_code),
        "scraped_at": now.isoformat(),
        "providers": providers,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python airnav_fuel_scraper.py <AIRPORT_CODE>", file=sys.stderr)
        return 2

    airport_code = sys.argv[1]

    try:
        result = scrape_airnav_prices(airport_code)
    except requests.HTTPError as e:
        print(
            json.dumps(
                {
                    "error": "http_error",
                    "airport_code": clean_text(airport_code).upper(),
                    "message": str(e),
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(
            json.dumps(
                {
                    "error": "scrape_error",
                    "airport_code": clean_text(airport_code).upper(),
                    "message": str(e),
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
