#!/usr/bin/env python3
"""
airnav_fuel_scraper.py

Usage:
    python airnav_fuel_scraper.py KGAI

Strategy:
- Primary source: AirNav
- Fallback source: FltPlan airport page
- AirNav values win when both sources provide the same key
- FltPlan fills only missing price keys
- If FltPlan contributes any actual missing values, source_url is switched to FltPlan
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

AIRNAV_BASE_URL = "https://www.airnav.com/airport/{code}"
FLTPLAN_BASE_URL = "https://www.fltplan.com/Airport.cgi?{code}"
USER_AGENT = "Mozilla/5.0 (compatible; FuelPriceTracker/0.3)"
TIMEOUT = 20

SUPPORTED_FUELS = [
    "100LL",
    "JET_A",
    "MOGAS",
    "UL94",
    "UL91",
]

SERVICE_MAP = {
    "FS": "FULL",
    "SS": "SELF",
    "RA": "RA",
    "AS": "SELF",  # Assisted/Self Service
}


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


def parse_fltplan_date(text: str) -> Optional[str]:
    m = re.search(r"LAST\s+UPDATE:\s*(\d{2}/\d{2}/\d{4})", text, flags=re.I)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def fetch_url(url: str) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.text


# ----------------------------
# AirNav parser
# ----------------------------

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
            r"\b(FS|SS|RA|AS)\b\s+"
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
            suffix = SERVICE_MAP[service]
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


def parse_airnav_provider_rows(
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

        upper_text = row_text.upper()

        if not any(
            token in upper_text
            for token in ["100LL", "JET A", "JET-A", "MOGAS", "UL94", "UL91"]
        ):
            continue

        if not re.search(r"\b(FS|SS|RA|AS)\b", row_text):
            continue

        fbo_name = extract_fbo_name_from_row(tr)
        if not fbo_name:
            continue
        if fbo_name in seen_names:
            continue

        guaranteed = "GUARANTEED" in upper_text
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


def scrape_airnav_prices(airport_code: str) -> List[Dict[str, Any]]:
    html = fetch_url(AIRNAV_BASE_URL.format(code=airport_code))
    soup = BeautifulSoup(html, "lxml")

    section_start = find_section_start(soup)
    if section_start is None:
        return []

    section_end = find_section_end(section_start)
    return parse_airnav_provider_rows(soup, section_start, section_end)


# ----------------------------
# FltPlan parser
# ----------------------------

def fltplan_candidate_codes(airport_code: str) -> List[str]:
    candidates = [airport_code]
    if len(airport_code) == 4 and airport_code[0] in {"K", "C", "P"}:
        candidates.append(airport_code[1:])
    return list(dict.fromkeys(candidates))


def normalize_fltplan_fuel(header: str) -> Optional[str]:
    s = clean_text(header).upper()

    if s == "100LL":
        return "100LL"
    if s in {"JET", "JET A", "JETA", "JET-A", "JETA+FSII", "JET A+FSII", "JET-A+FSII"}:
        return "JET_A"
    if s == "MOGAS":
        return "MOGAS"
    if s == "UL94":
        return "UL94"
    if s == "UL91":
        return "UL91"

    return None


def normalize_fltplan_service(label: str) -> Optional[str]:
    s = clean_text(label).upper()

    if s.startswith("FULL"):
        return "FULL"
    if s.startswith("SELF"):
        return "SELF"

    return None


def find_provider_name_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    """
    Search upward from the fuel header row.
    Accept only a row whose FIRST cell looks like the provider name
    and whose row also contains provider-style info such as Ph: or Freq:.
    """
    for j in range(header_row_idx - 1, -1, -1):
        cells = rows[j].find_all(["td", "th"])
        if not cells:
            continue

        first = clean_text(cells[0].get_text(" ", strip=True))
        if not first:
            continue

        row_texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
        row_joined = " ".join(t for t in row_texts if t)
        upper_first = first.upper()
        upper_row = row_joined.upper()

        # reject obvious non-provider rows
        if upper_first in {"SERVICE", "JET", "100LL", "80/87", "MOGAS", "UL94", "UL91"}:
            continue
        if "FBO & FLIGHT SERVICES INFO FOR" in upper_first:
            continue
        if "AIRPORT & FBO INFO FOR" in upper_first:
            continue
        if "AIRPORT INFO FOR" in upper_first:
            continue
        if upper_first.startswith("LAST UPDATE"):
            continue
        if upper_first.startswith("ADDRESS:"):
            continue
        if upper_first.startswith("FREQ:"):
            continue
        if upper_first.startswith("PH:"):
            continue
        if "FBO REVIEWS" in upper_first:
            continue

        # accept only provider/info rows
        if "PH:" in upper_row or "FREQ:" in upper_row:
            return first

    return None


def parse_fltplan_table(soup: BeautifulSoup, airport_code: str) -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).date().isoformat()
    providers: List[Dict[str, Any]] = []
    seen_provider_names = set()

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        header_row_idx = None
        header_cells_text = None

        # Find fuel header row
        for i, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
            upper = [t.upper() for t in texts]

            if not texts:
                continue

            if len(upper) >= 2 and upper[0] == "SERVICE":
                if any(h in upper for h in ["JET", "JET A", "JETA", "JET-A", "100LL", "MOGAS", "UL94", "UL91"]):
                    header_row_idx = i
                    header_cells_text = texts
                    break

        if header_row_idx is None or header_cells_text is None:
            continue

        provider_name = find_provider_name_above_header(rows, header_row_idx)
        if not provider_name:
            continue

        fuel_columns: Dict[int, str] = {}
        for idx, header_text in enumerate(header_cells_text[1:], start=1):
            fuel_name = normalize_fltplan_fuel(header_text)
            if fuel_name:
                fuel_columns[idx] = fuel_name

        if not fuel_columns:
            continue

        prices: Dict[str, str] = {}
        table_text = clean_text(table.get_text(" ", strip=True))
        last_update_date = parse_fltplan_date(table_text) or today_str

        # Parse only service rows below the header
        for row in rows[header_row_idx + 1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
            if not texts:
                continue

            service_type = normalize_fltplan_service(texts[0])
            if not service_type:
                if prices:
                    break
                continue

            for idx, fuel_name in fuel_columns.items():
                if idx >= len(texts):
                    continue

                cell_text = texts[idx]
                price = parse_price(cell_text)
                if price is None:
                    continue

                prices[f"{fuel_name}_{service_type}"] = format_price(price)

        if not prices:
            continue

        if provider_name in seen_provider_names:
            continue

        providers.append(
            {
                "fbo_name": provider_name,
                "last_update_date": last_update_date,
                "guaranteed": False,
                "prices": prices,
            }
        )
        seen_provider_names.add(provider_name)

    return providers


def scrape_fltplan_prices(airport_code: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    for candidate in fltplan_candidate_codes(airport_code):
        url = FLTPLAN_BASE_URL.format(code=candidate)

        try:
            html = fetch_url(url)
        except requests.HTTPError:
            continue

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n", strip=True).upper()

        if "WAS NOT FOUND" in text or "ERROR MESSAGE" in text:
            continue

        providers = parse_fltplan_table(soup, airport_code)
        if providers:
            return providers, url

    return [], None


# ----------------------------
# Merge + final output
# ----------------------------

def scrape_prices(airport_code: str) -> Dict[str, Any]:
    airport_code = normalize_airport_code(airport_code)

    airnav_providers: List[Dict[str, Any]] = []
    fltplan_providers: List[Dict[str, Any]] = []

    airnav_error = None
    fltplan_error = None

    source_url = AIRNAV_BASE_URL.format(code=airport_code)
    fltplan_url = None

    try:
        airnav_providers = scrape_airnav_prices(airport_code)
    except Exception as e:
        airnav_error = str(e)

    try:
        fltplan_providers, fltplan_url = scrape_fltplan_prices(airport_code)
    except Exception as e:
        fltplan_error = str(e)

    providers = airnav_providers

    if providers:
        if fltplan_providers:
            fallback_prices: Dict[str, str] = {}
            fallback_date = None

            for provider in fltplan_providers:
                for key, value in provider.get("prices", {}).items():
                    fallback_prices.setdefault(key, value)
                fallback_date = fallback_date or provider.get("last_update_date")

            merged = []
            used_fltplan = False

            for provider in providers:
                merged_prices = dict(provider.get("prices", {}))
                for key, value in fallback_prices.items():
                    if key not in merged_prices:
                        merged_prices[key] = value
                        used_fltplan = True

                provider_out = dict(provider)
                provider_out["prices"] = merged_prices

                if fallback_date and not provider_out.get("last_update_date"):
                    provider_out["last_update_date"] = fallback_date

                merged.append(provider_out)

            providers = merged

            if used_fltplan and fltplan_url:
                source_url = fltplan_url
    else:
        if fltplan_providers:
            providers = fltplan_providers
            if fltplan_url:
                source_url = fltplan_url

    now = datetime.now(timezone.utc).replace(microsecond=0)

    out = {
        "today_date": now.date().isoformat(),
        "airport_code": airport_code,
        "source_url": source_url,
        "scraped_at": now.isoformat(),
        "providers": providers,
    }

    if airnav_error:
        out["airnav_error"] = airnav_error
    if fltplan_error:
        out["fltplan_error"] = fltplan_error

    return out


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python airnav_fuel_scraper.py <AIRPORT_CODE>", file=sys.stderr)
        return 2

    airport_code = sys.argv[1]

    try:
        result = scrape_prices(airport_code)
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
