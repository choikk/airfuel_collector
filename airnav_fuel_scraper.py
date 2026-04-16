#!/usr/bin/env python3
"""
airnav_fuel_scraper.py

Tested against uploaded AirNav HTML files for:
- KMNM
- KLXL
- KPYG
- KLXV
- KGAI
- KIAD

Behavior:
- AirNav first
- Robust row-based AirNav parser
- Handles plain-text business names (KPYG, KLXV, KLXL, KMNM)
- Handles image-alt business names (KIAD)
- Supports 100LL / MOGAS / UL94 / UL91 / JET A / SAF
- Treats PS (pump service) as FULL
- Stops before "Alternatives at nearby airports"
- Falls back to FltPlan only if AirNav yields no providers
"""

from __future__ import annotations

import json
import re
import sys
import time
from functools import lru_cache
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

AIRNAV_BASE_URL = "https://www.airnav.com/airport/{code}"
FLTPLAN_BASE_URL = "https://www.fltplan.com/Airport.cgi?{code}"
USER_AGENT = "Mozilla/5.0 (FuelTracker/15.0)"
TIMEOUT = 20
AIRNAV_RETRY_DELAYS = (1.0, 2.0)

SERVICE_MAP = {
    "FS": "FULL",
    "SS": "SELF",
    "RA": "RA",
    "AS": "SELF",
    "PS": "FULL",
}

SUPPORTED_FUELS = ("100LL", "MOGAS", "UL94", "UL91", "JET_A", "SAF")
FLTPLAN_BRAND_ALIASES = {
    "ATLANTIC": "Atlantic Aviation",
    "SIGNATURE": "Signature Aviation",
}
FLTPLAN_DOMAIN_ALIASES = {
    "atlanticaviation.com": "Atlantic Aviation",
    "jetaviation.com": "Jet Aviation",
    "signatureaviation.com": "Signature Aviation",
    "signatureflight.com": "Signature Aviation",
}
PHONE_RE = re.compile(
    r"(?:(?:\+?1[\s.\-]*)?)"
    r"(?:\(?\d{3}\)?[\s.\-]*)"
    r"\d{3}[\s.\-]*\d{4}"
    r"(?:\s*(?:x|ext\.?|extension)\s*\d+)?",
    flags=re.I,
)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_airport_code(code: str) -> str:
    code = clean_text(code).upper()
    if not re.fullmatch(r"[A-Z0-9]{2,8}", code):
        raise ValueError(f"Invalid airport code: {code!r}")
    return code


def normalize_fbo_name(name: str) -> str:
    name = clean_text(name)
    name = re.sub(
        r"^More info(?:(?: and photos)? of| about)\s+",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(r"\s+", " ", name).strip(" ,")
    return name


def extract_phone(text: str) -> Optional[str]:
    match = PHONE_RE.search(clean_text(text))
    if not match:
        return None
    return clean_text(match.group(0))


def fetch_url(url: str) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.text


def fetch_airnav_url(url: str) -> str:
    last_error: Optional[Exception] = None

    for attempt in range(len(AIRNAV_RETRY_DELAYS) + 1):
        try:
            return fetch_url(url)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code is None or status_code < 500 or attempt >= len(AIRNAV_RETRY_DELAYS):
                raise
            last_error = exc
        except Exception as exc:
            last_error = exc
            if attempt >= len(AIRNAV_RETRY_DELAYS):
                raise

        time.sleep(AIRNAV_RETRY_DELAYS[attempt])

    if last_error:
        raise last_error

    raise RuntimeError("AirNav fetch failed without an exception")


@lru_cache(maxsize=256)
def fetch_fltplan_detail_name(url: str) -> Optional[str]:
    try:
        html = fetch_url(url)
    except Exception:
        return None

    m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    if not m:
        return None

    title = clean_text(m.group(1))
    if " - " in title:
        title = title.split(" - ", 1)[1]

    title = clean_text(title)
    if not title:
        return None

    return title


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
    if t == "UL94":
        return "UL94"
    if t in {"JET A", "JET-A", "JETA", "JET A+", "JET-A+", "JETA+"}:
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

    for a in biz_td.find_all("a", href=True):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))
        if (
            f"/airport/{airport_code}/".lower() in href.lower()
            and text
            and text.lower() not in {"read", "write", "web site", "email"}
        ):
            return normalize_fbo_name(text)

    for a in biz_td.find_all("a", href=True):
        href = a.get("href") or ""
        if f"/airport/{airport_code}/".lower() in href.lower():
            img = a.find("img")
            if img:
                for attr in ("alt", "title"):
                    value = clean_text(img.get(attr, ""))
                    if value:
                        return normalize_fbo_name(value)

    for img in biz_td.find_all("img"):
        for attr in ("alt", "title"):
            value = clean_text(img.get(attr, ""))
            if value:
                return normalize_fbo_name(value)

    text = clean_text(biz_td.get_text(" ", strip=True))
    if text:
        return normalize_fbo_name(text)

    return None


def extract_airnav_fbo_phone_from_cells(cells: List[Tag]) -> Optional[str]:
    for td in cells:
        for a in td.find_all("a", href=True):
            href = clean_text(a.get("href", ""))
            if href.lower().startswith("tel:"):
                return clean_text(href[4:])

        phone = extract_phone(td.get_text(" ", strip=True))
        if phone:
            return phone

    return None


def is_probable_airnav_name_text(text: str) -> bool:
    text = clean_text(text)
    if not text:
        return False

    upper = text.upper()
    blocked_markers = (
        "WEB SITE",
        "EMAIL",
        "ASRI",
        "FREQ",
        "AIRCARD",
        "CONTRACT FUEL",
        "GPU",
        "HANGAR",
        "AIRCRAFT GROUND",
        "AVIATION FUEL",
        "MORE INFO",
    )
    if any(marker in upper for marker in blocked_markers):
        return False
    if extract_phone(text):
        return False
    if "," in text:
        return False

    return bool(re.search(r"[A-Za-z]", text))


def is_probable_airnav_name_value(name: str) -> bool:
    name = clean_text(name)
    if not name:
        return False

    upper = name.upper()
    blocked_markers = (
        "WEB SITE",
        "EMAIL",
        "ASRI",
        "UNICOM",
        "AIRCARD",
        "CONTRACT FUEL",
        "MORE INFO",
        "WORLD FUEL",
        "AIR ELITE",
        "AIRBOSS",
        "SAFETY 1ST",
        "GOVERNMENT CONTRACT FUEL",
        "GOVERNMENT AIR CARD",
        "CAA PREFERRED",
        "AEGFUELS",
        "EVEREST FUEL",
        "MULTI SERVICE AVIATION",
        "GO RENTALS",
        "HERTZ",
        "WIFI",
        "MEMBERS ONLY",
        "DISCOUNTS",
        "GUARANTEED",
    )
    if any(marker in upper for marker in blocked_markers):
        return False
    if extract_phone(name):
        return False
    if len(name) > 120:
        return False

    return bool(re.search(r"[A-Za-z]", name))


def extract_airnav_more_info_name(cells: List[Tag], fuel_td: Tag) -> Optional[str]:
    for td in cells:
        if td is fuel_td:
            break

        for a in td.find_all("a", href=True):
            text = clean_text(a.get_text(" ", strip=True))
            if not text.lower().startswith("more info"):
                continue

            candidate = normalize_fbo_name(text)
            if is_probable_airnav_name_value(candidate):
                return candidate

    return None


def extract_airnav_fbo_name_from_cells(cells: List[Tag], airport_code: str, fuel_td: Tag) -> Optional[str]:
    candidate = extract_airnav_more_info_name(cells, fuel_td)
    if candidate:
        return candidate

    for td in cells:
        if td is fuel_td:
            break

        candidate = extract_airnav_fbo_name(td, airport_code)
        if candidate and is_probable_airnav_name_value(candidate):
            return candidate

    return None


def score_airnav_name_cell(td: Tag, airport_code: str) -> int:
    score = 0
    airport_code = airport_code.upper()

    for a in td.find_all("a", href=True):
        href = (a.get("href") or "").lower()
        if f"/airport/{airport_code}/" in href:
            score += 4
        if clean_text(a.get_text(" ", strip=True)):
            score += 1

    for img in td.find_all("img"):
        if clean_text(img.get("alt", "")):
            score += 3

    text = clean_text(td.get_text(" ", strip=True))
    if is_probable_airnav_name_text(text):
        score += 2

    return score


def find_airnav_fuel_cell(cells: List[Tag]) -> Optional[Tag]:
    for td in cells:
        if td.find("table") is None:
            continue

        prices, _, _ = extract_airnav_fuel_table_data(td)
        if prices:
            return td

    return None


def find_airnav_name_cell(cells: List[Tag], airport_code: str, fuel_td: Tag) -> Optional[Tag]:
    for td in cells:
        if td is fuel_td:
            break

        candidate = extract_airnav_fbo_name(td, airport_code)
        if candidate and is_probable_airnav_name_value(candidate):
            return td

    best_td = None
    best_score = 0
    for td in cells:
        if td is fuel_td:
            break

        score = score_airnav_name_cell(td, airport_code)
        if score > best_score:
            best_td = td
            best_score = score

    return best_td


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

        m = re.match(r"^(FS|SS|RA|AS|PS)\b", row_text)
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

        order: List[str] = []
        for td in cells:
            txt = clean_text(td.get_text(" ", strip=True)).upper()

            for token in (
                "100LL",
                "MOGAS",
                "UL91",
                "UL94",
                "JET A+",
                "JET-A+",
                "JETA+",
                "JET A",
                "JET-A",
                "JETA",
                "SAF",
            ):
                if token in txt:
                    fuel = canonical_airnav_fuel(token)
                    if fuel and fuel not in order:
                        order.append(fuel)

        if order:
            fuel_order = order

    cleaned: Dict[str, str] = {}

    for fuel in SUPPORTED_FUELS:
        for suffix in ("FULL", "SELF", "RA"):
            key = f"{fuel}_{suffix}"
            if key in prices_raw:
                cleaned[key] = format_price(prices_raw[key])

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
        if not cells:
            continue

        fuel_td = find_airnav_fuel_cell(cells)
        if fuel_td is None:
            continue
        biz_td = find_airnav_name_cell(cells, airport_code, fuel_td)
        if biz_td is None:
            continue

        fbo_name = extract_airnav_fbo_name_from_cells(cells, airport_code, fuel_td)
        if not fbo_name or fbo_name in seen_names:
            continue
        fuel_idx = cells.index(fuel_td)
        fbo_phone = extract_airnav_fbo_phone_from_cells(cells[:fuel_idx])

        prices, guaranteed, last_update_date = extract_airnav_fuel_table_data(fuel_td)
        if not prices:
            continue

        providers.append(
            {
                "fbo_name": fbo_name,
                "fbo_phone": fbo_phone,
                "last_update_date": last_update_date,
                "guaranteed": guaranteed,
                "prices": prices,
            }
        )
        seen_names.add(fbo_name)

    return providers


def scrape_airnav_prices(airport_code: str) -> List[Dict[str, Any]]:
    html = fetch_airnav_url(AIRNAV_BASE_URL.format(code=airport_code))
    return scrape_airnav_prices_from_html(html, airport_code)


# ----------------------------
# FltPlan fallback
# ----------------------------

def extract_fltplan_provider_name(cell_text: str) -> Optional[str]:
    text = clean_text(cell_text)
    if not text:
        return None

    m = re.search(
        r"\bclick here\s+(.+?)(?:\s+is\b|\s+ph:|\s+fax:|\s+freq:|\s+website\b|\s+e-mail\b|\s+service\b|\s+last update:|\s+misc\. info:|\s+address:)",
        text,
        flags=re.I,
    )
    if m:
        text = clean_text(m.group(1))
        return text if text else None

    return text


def normalize_fltplan_provider_name(name: str) -> Optional[str]:
    name = clean_text(name)
    if not name:
        return None

    upper = name.upper()
    if upper in FLTPLAN_BRAND_ALIASES:
        return FLTPLAN_BRAND_ALIASES[upper]

    if upper == name and re.fullmatch(r"[A-Z0-9][A-Z0-9 .&()/+-]*", name):
        parts = []
        for token in name.split():
            if len(token) <= 3 and token.isalpha():
                parts.append(token.upper())
            else:
                parts.append(token.title())
        name = " ".join(parts)

    return name


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
    if s == "UL94":
        return "UL94"
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


def find_provider_name_from_links_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    for j in range(header_row_idx - 1, -1, -1):
        for a in rows[j].find_all("a", href=True):
            href = clean_text(a.get("href", "")).lower()
            if not href:
                continue

            for domain, brand_name in FLTPLAN_DOMAIN_ALIASES.items():
                if domain in href:
                    return brand_name

    return None


def find_provider_detail_url_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    for j in range(header_row_idx - 1, -1, -1):
        for a in rows[j].find_all("a", href=True):
            href = clean_text(a.get("href", ""))
            if not href:
                continue
            if "fbo.cfm?fid=" in href.lower():
                return urljoin("https://www.fltplan.com/", href)
    return None


def find_provider_name_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    linked_brand_name = find_provider_name_from_links_above_header(rows, header_row_idx)
    if linked_brand_name:
        return linked_brand_name

    detail_url = find_provider_detail_url_above_header(rows, header_row_idx)
    if detail_url:
        detail_name = fetch_fltplan_detail_name(detail_url)
        detail_name = normalize_fltplan_provider_name(detail_name or "")
        if detail_name:
            return detail_name

    # Prefer rows that explicitly contain the provider name/description block.
    for j in range(header_row_idx - 1, -1, -1):
        cells = rows[j].find_all(["td", "th"])
        if not cells:
            continue

        for cell in cells:
            cell_text = clean_text(cell.get_text(" ", strip=True))
            if not cell_text:
                continue

            m = re.match(r"^([A-Z][A-Za-z0-9&'()./-]+(?: [A-Z][A-Za-z0-9&'()./-]+){0,4})\s+(?:is|offers|at)\b", cell_text)
            if m:
                normalized = normalize_fltplan_provider_name(m.group(1))
                if normalized:
                    return normalized

        row_texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
        row_joined = " ".join(t for t in row_texts if t)
        candidate = extract_fltplan_provider_name(row_joined)
        if candidate and candidate.lower() != row_joined.lower():
            normalized = normalize_fltplan_provider_name(candidate)
            if normalized:
                return normalized

    # Fall back to the contact row only if no richer provider-name row exists.
    for j in range(header_row_idx - 1, -1, -1):
        cells = rows[j].find_all(["td", "th"])
        if not cells:
            continue

        first_cell_text = clean_text(cells[0].get_text(" ", strip=True))
        if not first_cell_text:
            continue

        row_texts = [clean_text(c.get_text(" ", strip=True)) for c in cells]
        row_joined = " ".join(t for t in row_texts if t)
        row_joined_upper = row_joined.upper()

        if not any(marker in row_joined_upper for marker in ["PH:", "FREQ:", "FAX:", "WEBSITE", "E-MAIL"]):
            continue

        name_candidate = clean_text(cells[0].get_text(" ", strip=True))
        if name_candidate and not any(
            marker in name_candidate.upper()
            for marker in ["PH:", "FAX:", "FREQ:", "WEBSITE", "E-MAIL"]
        ):
            normalized = normalize_fltplan_provider_name(name_candidate)
            if normalized:
                return normalized

        candidate = extract_fltplan_provider_name(first_cell_text)
        normalized = normalize_fltplan_provider_name(candidate or "")
        if normalized:
            return normalized

    return None


def find_provider_phone_above_header(rows: List[Tag], header_row_idx: int) -> Optional[str]:
    for j in range(header_row_idx - 1, -1, -1):
        row_text = clean_text(rows[j].get_text(" ", strip=True))
        if not row_text:
            continue

        phone = extract_phone(row_text)
        if phone:
            return phone

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
                if any(h in upper for h in ["JET", "JET A", "JETA", "JET-A", "100LL", "MOGAS", "UL94", "UL91", "SAF"]):
                    header_row_idx = i
                    header_cells_text = texts
                    break

        if header_row_idx is None or header_cells_text is None:
            continue

        provider_name = find_provider_name_above_header(rows, header_row_idx)
        if not provider_name or provider_name in seen_provider_names:
            continue
        provider_phone = find_provider_phone_above_header(rows, header_row_idx)

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
                "fbo_phone": provider_phone,
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
