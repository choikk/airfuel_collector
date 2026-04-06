#!/usr/bin/env python3
"""
airnav_fuel_scraper.py

- AirNav 우선
- AirNav structured parser 우선
- KPYG / KEDU 같은 flat-text AirNav layout 은 text-block fallback 사용
- FltPlan은 AirNav가 완전히 비었을 때만 fallback
- KPYG 전용 regex fallback 추가
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
USER_AGENT = "Mozilla/5.0 (FuelTracker/11.0)"
TIMEOUT = 20

SERVICE_MAP = {
    "FS": "FULL",
    "SS": "SELF",
    "RA": "RA",
    "AS": "SELF",
}

TEXT_FUEL_KEY_MAP = {
    ("100LL", "FS"): "100LL_FULL",
    ("100LL", "SS"): "100LL_SELF",
    ("100LL", "RA"): "100LL_RA",
    ("100LL", "AS"): "100LL_SELF",
    ("JET_A", "FS"): "JET_A_FULL",
    ("JET_A", "SS"): "JET_A_SELF",
    ("JET_A", "RA"): "JET_A_RA",
    ("JET_A", "AS"): "JET_A_SELF",
    ("SAF", "FS"): "SAF_FULL",
    ("SAF", "SS"): "SAF_SELF",
    ("SAF", "RA"): "SAF_RA",
    ("SAF", "AS"): "SAF_SELF",
}

FUEL_TOKENS = ["100LL", "JET A", "JET-A", "JETA", "SAF"]

PHONE_PAT = re.compile(
    r"(?:\+?1[\s\.-]?)?(?:\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{4})"
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
    name = re.sub(r"^More info(?: and photos)? of\s+", "", name, flags=re.I)
    return clean_text(name)


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


def normalize_split_airnav_dates(text: str) -> str:
    text = text or ""
    text = re.sub(
        r"(\d{1,2})-\s*([A-Za-z]{3})-\s*(\d{4})",
        r"\1-\2-\3",
        text,
        flags=re.I,
    )
    return text


def parse_airnav_date(text: str) -> Optional[str]:
    text = normalize_split_airnav_dates(text)
    m = re.search(r"\b(\d{1,2}-[A-Za-z]{3}-\d{4})\b", text)
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
# AirNav structured parser
# ----------------------------

def find_airnav_section_container(soup: BeautifulSoup) -> Optional[Tag]:
    for h3 in soup.find_all("h3"):
        if clean_text(h3.get_text(" ", strip=True)) == "FBO, Fuel Providers, and Aircraft Ground Support":
            tr = h3.find_parent("tr")
            if tr is None:
                continue
            tbody = tr.find_parent("tbody")
            if tbody is not None:
                return tbody
            table = tr.find_parent("table")
            if table is not None:
                return table
    return None


def extract_provider_rows(section: Tag, airport_code: str) -> List[Tag]:
    airport_code = airport_code.upper()
    rows: List[Tag] = []

    for row in section.find_all("tr"):
        row_text = clean_text(row.get_text(" ", strip=True))

        if "Alternatives at nearby airports" in row_text:
            break
        if "Would you like to see your business listed on this page?" in row_text:
            break
        if "Aviation Businesses, Services, and Facilities" in row_text:
            break

        for a in row.find_all("a", href=True):
            href = (a.get("href") or "").upper()
            if f"/AIRPORT/{airport_code}/" in href:
                rows.append(row)
                break

    return rows


def extract_fbo_name(row: Tag, airport_code: str) -> Optional[str]:
    airport_code = airport_code.upper()

    for a in row.find_all("a", href=True):
        href = (a.get("href") or "").upper()
        name = clean_text(a.get_text(" ", strip=True))

        if not name:
            continue
        if name.lower() in {"web site", "email", "write", "read", "download", "click here"}:
            continue
        if f"/AIRPORT/{airport_code}/" in href:
            return normalize_fbo_name(name)

    return None


def find_fuel_table(row: Tag) -> Optional[Tag]:
    candidates: List[Tuple[int, Tag]] = []

    for table in row.find_all("table"):
        txt = clean_text(table.get_text(" ", strip=True)).upper()

        has_fuel = ("100LL" in txt) or ("JET A" in txt) or ("JET-A" in txt) or ("SAF" in txt)
        has_service = bool(re.search(r"\b(FS|SS|RA|AS)\b", txt))
        if not (has_fuel and has_service):
            continue

        score = 0
        if "100LL" in txt:
            score += 1
        if "JET A" in txt or "JET-A" in txt:
            score += 1
        if "SAF" in txt:
            score += 1
        if re.search(r"\bFS\b", txt):
            score += 1
        if re.search(r"\bSS\b", txt):
            score += 1
        if "GUARANTEED" in txt:
            score += 1

        candidates.append((score, table))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def extract_fuel_order(table: Tag) -> List[str]:
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        texts = [clean_text(c.get_text(" ", strip=True)).upper() for c in cells]
        joined = " | ".join(texts)

        has_fuel = ("100LL" in joined) or ("JET A" in joined) or ("JET-A" in joined) or ("SAF" in joined)
        has_service = bool(re.search(r"\b(FS|SS|RA|AS)\b", joined))
        if not has_fuel or has_service:
            continue

        order: List[str] = []
        for txt in texts:
            if "100LL" in txt and "100LL" not in order:
                order.append("100LL")
            elif ("JET A" in txt or "JET-A" in txt or txt == "JETA") and "JET_A" not in order:
                order.append("JET_A")
            elif "SAF" in txt and "SAF" not in order:
                order.append("SAF")
        if order:
            return order

    return []


def parse_service_rows(table: Tag, fuel_order: List[str]) -> Dict[str, float]:
    prices: Dict[str, float] = {}

    for row in table.find_all("tr"):
        row_text = clean_text(row.get_text(" ", strip=True))
        m = re.match(r"^(FS|SS|RA|AS)\b", row_text)
        if not m:
            continue

        svc_code = m.group(1).upper()
        if svc_code not in SERVICE_MAP:
            continue
        service = SERVICE_MAP[svc_code]

        nums = re.findall(r"\$?\d+\.\d+", row_text)
        if not nums:
            continue

        for idx, num in enumerate(nums):
            if idx >= len(fuel_order):
                break
            fuel = fuel_order[idx]
            value = parse_price(num)
            if value is None:
                continue
            prices[f"{fuel}_{service}"] = value

    return prices


def apply_price_sanity(prices_raw: Dict[str, float]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}

    for fuel in ("100LL", "JET_A"):
        full_key = f"{fuel}_FULL"
        self_key = f"{fuel}_SELF"

        full_val = prices_raw.get(full_key)
        self_val = prices_raw.get(self_key)

        if full_val is not None and self_val is not None and self_val > full_val:
            full_val = None
            self_val = None

        if full_val is not None:
            cleaned[full_key] = format_price(full_val)
        if self_val is not None:
            cleaned[self_key] = format_price(self_val)

    for key, value in prices_raw.items():
        if key.startswith("SAF_") and value is not None:
            cleaned[key] = format_price(value)

    return cleaned


def parse_fuel_table(table: Tag) -> Dict[str, str]:
    fuel_order = extract_fuel_order(table)
    if not fuel_order:
        return {}

    raw_prices = parse_service_rows(table, fuel_order)
    if not raw_prices:
        return {}

    return apply_price_sanity(raw_prices)


def scrape_airnav_prices_structured(airport_code: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    section = find_airnav_section_container(soup)
    if section is None:
        return []

    providers: List[Dict[str, Any]] = []
    seen_names = set()

    for row in extract_provider_rows(section, airport_code):
        fbo_name = extract_fbo_name(row, airport_code)
        if not fbo_name or fbo_name in seen_names:
            continue

        fuel_table = find_fuel_table(row)
        if fuel_table is None:
            continue

        prices = parse_fuel_table(fuel_table)
        if not prices:
            continue

        row_text = clean_text(row.get_text(" ", strip=True))
        providers.append(
            {
                "fbo_name": fbo_name,
                "last_update_date": parse_airnav_date(row_text),
                "guaranteed": "GUARANTEED" in row_text.upper(),
                "prices": prices,
            }
        )
        seen_names.add(fbo_name)

    return providers


# ----------------------------
# AirNav text-block fallback
# ----------------------------

def trim_to_fuel_header(line: str) -> str:
    up = line.upper()

    candidates = []
    for token in FUEL_TOKENS:
        idx = up.find(token)
        if idx != -1:
            candidates.append(idx)

    if not candidates:
        return line.strip()

    start = min(candidates)
    return line[start:].strip()


def contains_fuel_header(line: str) -> bool:
    up = line.upper()
    return any(token in up for token in FUEL_TOKENS)


def extract_inline_fuel_order(line: str) -> List[str]:
    line = trim_to_fuel_header(line)
    up = line.upper()

    order: List[str] = []

    if "100LL" in up:
        order.append("100LL")
    if ("JET A" in up) or ("JET-A" in up) or ("JETA" in up):
        order.append("JET_A")
    if "SAF" in up:
        order.append("SAF")

    return order


def is_probable_provider_start(line: str) -> bool:
    up = line.upper()

    if not line:
        return False
    if line in {"Business Name", "Contact", "Services / Description", "Fuel Prices", "Comments"}:
        return False
    if up.startswith("UPDATED "):
        return False
    if re.match(r"^(FS|SS|RA|AS)\b", up):
        return False
    if line.lower() in {"no information available", "write", "read", "web site", "email"}:
        return False
    if line.startswith("If you are affiliated with "):
        return False
    if line.startswith("Located at "):
        return False
    if line.startswith("SS=") or line.startswith("FS="):
        return False
    if "Would you like to see your business listed on this page?" in line:
        return False
    if contains_fuel_header(line):
        return False
    return True


def extract_provider_blocks_from_section(section: Tag) -> List[str]:
    text = section.get_text("\n", strip=True)
    text = normalize_split_airnav_dates(text)

    lines = [clean_text(x) for x in text.splitlines()]
    lines = [x for x in lines if x]

    start_idx = 0
    for i, line in enumerate(lines):
        if line == "Business Name":
            start_idx = i + 1
            break

    lines = lines[start_idx:]

    stop_markers = (
        "Would you like to see your business listed on this page?",
        "Other Pages about",
        "Alternatives at nearby airports",
        "Aviation Businesses, Services, and Facilities",
    )

    trimmed: List[str] = []
    for line in lines:
        if any(marker in line for marker in stop_markers):
            break
        trimmed.append(line)

    blocks: List[List[str]] = []
    cur: List[str] = []

    for line in trimmed:
        if is_probable_provider_start(line):
            if cur:
                blocks.append(cur)
            cur = [line]
        else:
            if cur:
                cur.append(line)

    if cur:
        blocks.append(cur)

    return ["\n".join(block) for block in blocks if block]


def extract_fbo_name_from_block(block_text: str) -> Optional[str]:
    lines = [clean_text(x) for x in block_text.splitlines() if clean_text(x)]
    if not lines:
        return None

    first = lines[0]
    first = PHONE_PAT.sub("", first).strip()

    if not first or first.lower() == "no information available":
        m = re.search(
            r"If you are affiliated with (.+?) and would like to show here",
            block_text,
            flags=re.I,
        )
        if m:
            first = clean_text(m.group(1))

    return normalize_fbo_name(first) if first else None


def parse_airnav_text_prices(block_text: str) -> Dict[str, str]:
    block_text = normalize_split_airnav_dates(block_text)

    lines = [clean_text(x) for x in block_text.splitlines() if clean_text(x)]
    prices_raw: Dict[str, float] = {}
    fuel_order: List[str] = []

    for line in lines:
        line = trim_to_fuel_header(line)
        up = line.upper()

        if contains_fuel_header(up):
            inline_order = extract_inline_fuel_order(up)
            if inline_order:
                fuel_order = inline_order
            continue

        m = re.match(r"^(FS|SS|RA|AS)\b", up)
        if m and fuel_order:
            svc_code = m.group(1).upper()
            nums = re.findall(r"\$?\d+\.\d+", line)

            for idx, num in enumerate(nums):
                if idx >= len(fuel_order):
                    break
                fuel = fuel_order[idx]
                key = TEXT_FUEL_KEY_MAP.get((fuel, svc_code))
                if not key:
                    continue
                value = parse_price(num)
                if value is not None:
                    prices_raw[key] = value

    if not prices_raw:
        return {}

    return apply_price_sanity(prices_raw)


def scrape_airnav_prices_text_fallback(airport_code: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    section = find_airnav_section_container(soup)
    if section is None:
        return []

    providers: List[Dict[str, Any]] = []
    seen_names = set()

    for block_text in extract_provider_blocks_from_section(section):
        block_text = normalize_split_airnav_dates(block_text)

        fbo_name = extract_fbo_name_from_block(block_text)
        if not fbo_name or fbo_name in seen_names:
            continue

        prices = parse_airnav_text_prices(block_text)
        if not prices:
            continue

        providers.append(
            {
                "fbo_name": fbo_name,
                "last_update_date": parse_airnav_date(block_text),
                "guaranteed": "GUARANTEED" in block_text.upper(),
                "prices": prices,
            }
        )
        seen_names.add(fbo_name)

    return providers


# ----------------------------
# KPYG exact regex fallback
# ----------------------------

def scrape_airnav_prices_kpyg_regex_fallback(airport_code: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    if airport_code.upper() != "KPYG":
        return []

    section = find_airnav_section_container(soup)
    if section is None:
        return []

    text = normalize_split_airnav_dates(section.get_text("\n", strip=True))

    m_name = re.search(
        r"Town of Pageland \(self-serve fuel\)\s+(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})?",
        text,
        flags=re.I,
    )
    m_price = re.search(r"\bSS\s*\$([0-9]+(?:\.[0-9]+)?)\b", text, flags=re.I)
    m_date = re.search(r"\bUpdated\s+(\d{1,2}-[A-Za-z]{3}-\d{4})\b", text, flags=re.I)

    if not (m_name and m_price):
        return []

    fbo_name = "Town of Pageland (self-serve fuel)"
    last_update_date = None
    if m_date:
        try:
            last_update_date = datetime.strptime(m_date.group(1), "%d-%b-%Y").date().isoformat()
        except ValueError:
            last_update_date = None

    return [
        {
            "fbo_name": fbo_name,
            "last_update_date": last_update_date,
            "guaranteed": False,
            "prices": {
                "100LL_SELF": format_price(parse_price(m_price.group(1))),
            },
        }
    ]


def scrape_airnav_prices(airport_code: str) -> List[Dict[str, Any]]:
    html = fetch_url(AIRNAV_BASE_URL.format(code=airport_code))
    soup = BeautifulSoup(html, "lxml")

    providers = scrape_airnav_prices_structured(airport_code, soup)
    if providers:
        return providers

    providers = scrape_airnav_prices_text_fallback(airport_code, soup)
    if providers:
        return providers

    providers = scrape_airnav_prices_kpyg_regex_fallback(airport_code, soup)
    if providers:
        return providers

    return []


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
    if s in {"JET", "JET A", "JETA", "JET-A", "JETA+FSII", "JET A+FSII", "JET-A+FSII"}:
        return "JET_A"
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
                if any(h in upper for h in ["JET", "JET A", "JETA", "JET-A", "100LL"]):
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
# Final merge/output
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
