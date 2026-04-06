#!/usr/bin/env python3
"""
airnav_fuel_scraper.py

Tested against uploaded AirNav HTML files for:
- KLXL
- KPYG
- KLXV
- KGAI
- KIAD

Behavior:
- AirNav first
- Robust row-based AirNav parser
- Handles plain-text business names (KPYG, KLXV, KLXL)
- Handles image-alt business names (KIAD)
- Supports 100LL / MOGAS / UL91 / JET A / SAF
- Stops before "Alternatives at nearby airports"
- Falls back to FltPlan only if AirNav yields no providers
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

AIRNAV_BASE_URL = "https://www.airnav.com/airport/{code}"
FLTPLAN_BASE_URL = "https://www.fltplan.com/Airport.cgi?{code}"
USER_AGENT = "Mozilla/5.0 (FuelTracker/14.0)"
TIMEOUT = 20

SERVICE_MAP = {
    "FS": "FULL",
    "SS": "SELF",
    "RA": "RA",
    "AS": "SELF",
}

SUPPORTED_FUELS = ("100LL", "MOGAS", "UL91", "JET_A", "SAF")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_airport_code(code: str) -> str:
    code = clean_text(code).upper()
    if not re.fullmatch(r"[A-Z0-9]{2,8}", code):
        raise ValueError(f"Invalid airport code: {code!r}")
    return code


def normalize_fbo_name(name: str) -> str:
    name = clean_text(name)
    name = re.sub(r"^More info(?: and photos)? of\s+", "", name, flags=re.I)
    name = re.sub(r"\s+", " ", name).strip(" ,")
    return name


def fetch_url(url: str) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.text


def parse_price(text: str) -> Optional[float]:
    text = clean_text(text).replace("$", "").replace(",", "")
    if text in {"", "-", "--", "---"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_price(value: Optional[float]) -> Optional[str]:
    return None if value is None else f"{value:.2f}"


def parse_airnav_date(text: str) -> Optional[str]:
    m = re.search(r"\b(\d{1,2}-[A-Za-z]{3}-\d{4})\b", clean_text(text))
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


# ----------------------------
# AirNav parser
# ----------------------------

def canonical_airnav_fuel(token: str) -> Optional[str]:
    t = clean_text(token).upper()
    if t == "100LL":
        return "100LL"
    if t == "MOGAS":
        return "MOGAS"
    if t == "UL91":
        return "UL91"
    if t in {"JET A", "JET-A", "JETA"}:
        return "JET_A"
    if t == "SAF":
        return "SAF"
    return None


def find_airnav_section_table(soup: BeautifulSoup) -> Optional[Tag]:
    anchor = soup.find("a", attrs={"name": "biz"})
    if anchor is not None:
        table = anchor.find_next("table")
        if table and table.find("h3", string=lambda s: s and "FBO, Fuel Providers, and Aircraft Ground Support" in s):
            return table

    h3 = soup.find("h3", string=lambda s: s and "FBO, Fuel Providers, and Aircraft Ground Support" in s)
    if h3 is not None:
        return h3.find_parent("table")

    return None


def get_airnav_section_rows(section_table: Tag) -> List[Tag]:
    tbody = section_table.find("tbody") or section_table
    rows = tbody.find_all("tr", recursive=False)

    out: List[Tag] = []
    for row in rows:
        row_text = clean_text(row.get_text(" ", strip=True))

        if "Alternatives at nearby airports" in row_text:
            break
        if "Would you like to see your business listed on this page?" in row_text:
            break
        if "Aviation Businesses, Services, and Facilities" in row_text:
            break

        out.append(row)

    return out


def extract_airnav_fbo_name(biz_td: Tag, airport_code: str) -> Optional[str]:
    airport_code = airport_code.upper()

    # 1) linked text
    for a in biz_td.find_all("a", href=True):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))
        if (
            f"/airport/{airport_code}/".lower() in href.lower()
            and text
            and text.lower() not in {"read", "write", "web site", "email"}
        ):
            return normalize_fbo_name(text)

    # 2) image alt inside airport link
    for a in biz_td.find_all("a", href=True):
        href = a.get("href") or ""
        if f"/airport/{airport_code}/".lower() in href.lower():
            img = a.find("img")
            if img:
                alt = clean_text(img.get("alt", ""))
                if alt:
                    return normalize_fbo_name(alt)

    # 3) any image alt
    for img in biz_td.find_all("img"):
        alt = clean_text(img.get("alt", ""))
        if alt:
            return normalize_fbo_name(alt)

    # 4) plain text cell
    text = clean_text(biz_td.get_text(" ", strip=True))
    if text:
        return normalize_fbo_name(text)

    return None


def extract_airnav_fuel_table_data(fuel_td: Tag) -> Tuple[Dict[str, str], bool, Optional[str]]:
    fuel_table = fuel_td.find("table")
    if fuel_table is None:
        return {}, False, None

    tbody = fuel_table.find("tbody") or fuel_table
    rows = tbody.find_all("tr", recursive=False)

    fuel_order: List[str] = []
    prices_raw: Dict[str, float] = {}
    guaranteed = False
    last_update_date: Optional[str] = None

    for row in rows:
        row_text = clean_text(row.get_text(" ", strip=True))
        if not row_text:
            continue

        if "GUARANTEED" in row_text.upper():
            guaranteed = True
            continue

        dt = parse_airnav_date(row_text)
        if dt:
            last_update_date = dt

        cells = row.find_all("td", recursive=False)
        if not cells:
            continue

        # Service row
        m = re.match(r"^(FS|SS|RA|AS)\b", row_text)
        if m:
            svc_code = m.group(1).upper()
            if svc_code not in SERVICE_MAP:
                continue

            values: List[str] = []
            for td in cells[1:]:
                td_text = clean_text(td.get_text(" ", strip=True))
                mm = re.search(r"(\d+\.\d+|---)", td_text)
                if mm:
                    values.append(mm.group(1))

            if not values:
                values = re.findall(r"\$?\d+\.\d+|---", row_text)

            for fuel, raw_val in zip(fuel_order, values):
                value = parse_price(raw_val)
                if value is None:
                    continue
                suffix = SERVICE_MAP[svc_code]
                prices_raw[f"{fuel}_{suffix}"] = value

            continue

        # Header row
        order: List[str] = []
        for td in cells:
            txt = clean_text(td.get_text(" ", strip=True)).upper()

            for token in ("100LL", "MOGAS", "UL91", "JET A", "JET-A", "JETA", "SAF"):
                if token in txt:
                    fuel = canonical_airnav_fuel(token)
                    if fuel and fuel not in order:
                        order.append(fuel)

        if order:
            fuel_order = order

    cleaned: Dict[str, str] = {}

    for fuel in SUPPORTED_FUELS:
        full_key = f"{fuel}_FULL"
        self_key = f"{fuel}_SELF"
        ra_key = f"{fuel}_RA"

        full_val = prices_raw.get(full_key)
        self_val = prices_raw.get(self_key)

        # sanity check only where both full/self exist
        if full_val is not None and self_val is not None and self_val > full_val:
            full_val = None
            self_val = None

        if full_val is not None:
            cleaned[full_key] = format_price(full_val)
        if self_val is not None:
            cleaned[self_key] = format_price(self_val)
        if ra_key in prices_raw:
            cleaned[ra_key] = format_price(prices_raw[ra_key])

    return cleaned, guaranteed, last_update_date


def scrape_airnav_prices_from_html(html: str, airport_code: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    section_table = find_airnav_section_table(soup)
    if section_table is None:
        return []

    providers: List[Dict[str, Any]] = []
    seen_names = set()

    for row in get_airnav_section_rows(section_table):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 7:
            continue

        biz_td = cells[0]
        fuel_td = cells[6]

        if fuel_td.find("table") is None:
            continue

        fbo_name = extract_airnav_fbo_name(biz_td, airport_code)
        if not fbo_name or fbo_name in seen_names:
            continue

        prices, guaranteed, last_update_date = extract_airnav_fuel_table_data(fuel_td)
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
    return scrape_airnav_prices_from_html(html, airport_code)


# ----------------------------
# FltPlan fallback
# ----------------------------

def extract_fltplan_provider_name(cell_text: str) -> Optional[str]:
    text = clean_text(cell_text)
    if not text:
        return None

    m = re.match(r"^click here\s+(.+?)\s+is\b", text, flags=re.I)
    if m:
        text = clean_text(m.group(1))
        return text if text else None

    return text


def fltplan_candidate_codes(airport_code: str) -> List[str]:
    candidates = [airport_code]
    if len(airport_code) == 4 and airport_code[0] in {"K", "C", "P"}:
        candidates.append(airport_code[1:])
    return list(dict.fromkeys(candidates))


def normalize_fltplan_fuel(header: str) -> Optional[str]:
    s = clean_text(header).upper()
    if s == "100LL":
        return "100LL"
    if s == "MOGAS":
        return "MOGAS"
    if s == "UL91":
        return "UL91"
    if s in {"JET", "JET A", "JETA", "JET-A", "JETA+FSII", "JET A+FSII", "JET-A+FSII"}:
        return "JET_A"
    if s == "SAF":
        return "SAF"
    return None


def normalize_fltplan_service(label: str) -> Optional[str]:
    s = clean_text(label).upper()
    if s.startswith("FULL"):
        return "FULL"
    if s.startswith("SELF"):
        return "SELF"
    return None


def find_provider_name_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    for j in range(header_row_idx - 1, -1, -1):
        cells = rows[j].find_all(["td", "th"])
        if not cells:
            continue

        first_cell_text = clean_text(cells[0].get_text(" ", strip=True))
        if not first_cell_text:
            continue

        row_texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
        row_joined = " ".join(t for t in row_texts if t).upper()

        if not any(marker in row_joined for marker in ["PH:", "FREQ:", "FAX:", "WEBSITE", "E-MAIL"]):
            continue

        candidate = extract_fltplan_provider_name(first_cell_text)
        if candidate:
            return candidate

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

        for i, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
            upper = [t.upper() for t in texts]

            if not texts:
                continue

            if len(upper) >= 2 and upper[0] == "SERVICE":
                if any(h in upper for h in ["JET", "JET A", "JETA", "JET-A", "100LL", "MOGAS", "UL91", "SAF"]):
                    header_row_idx = i
                    header_cells_text = texts
                    break

        if header_row_idx is None or header_cells_text is None:
            continue

        provider_name = find_provider_name_above_header(rows, header_row_idx)
        if not provider_name or provider_name in seen_provider_names:
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

                price = parse_price(texts[idx])
                if price is None:
                    continue

                prices[f"{fuel_name}_{service_type}"] = format_price(price)

        if not prices:
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
        except Exception:
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
# Final output
# ----------------------------

def scrape_prices(airport_code: str) -> Dict[str, Any]:
    airport_code = normalize_airport_code(airport_code)

    airnav_providers: List[Dict[str, Any]] = []
    fltplan_providers: List[Dict[str, Any]] = []
    airnav_error = None
    fltplan_error = None

    source_url = AIRNAV_BASE_URL.format(code=airport_code)

    try:
        airnav_providers = scrape_airnav_prices(airport_code)
    except Exception as e:
        airnav_error = str(e)

    if airnav_providers:
        providers = airnav_providers
    else:
        fltplan_url = None
        try:
            fltplan_providers, fltplan_url = scrape_fltplan_prices(airport_code)
        except Exception as e:
            fltplan_error = str(e)

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


# ----------------------------
# Optional local file test mode
# ----------------------------

def scrape_prices_from_local_airnav_html(html_path: str, airport_code: str) -> Dict[str, Any]:
    airport_code = normalize_airport_code(airport_code)
    html = Path(html_path).read_text(errors="ignore")

    providers = scrape_airnav_prices_from_html(html, airport_code)
    now = datetime.now(timezone.utc).replace(microsecond=0)

    return {
        "today_date": now.date().isoformat(),
        "airport_code": airport_code,
        "source_url": AIRNAV_BASE_URL.format(code=airport_code),
        "scraped_at": now.isoformat(),
        "providers": providers,
    }


def main() -> int:
    if len(sys.argv) not in {2, 4}:
        print(
            "Usage:\n"
            "  python airnav_fuel_scraper.py <AIRPORT_CODE>\n"
            "  python airnav_fuel_scraper.py --test-html <AIRPORT_CODE> <HTML_PATH>",
            file=sys.stderr,
        )
        return 2

    if len(sys.argv) == 4 and sys.argv[1] == "--test-html":
        airport_code = sys.argv[2]
        html_path = sys.argv[3]
        try:
            result = scrape_prices_from_local_airnav_html(html_path, airport_code)
        except Exception as e:
            print(
                json.dumps(
                    {
                        "error": "local_html_parse_error",
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
