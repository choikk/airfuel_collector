# Aviation Fuel Price Collector

Lightweight Python tool for collecting aviation fuel price data (100LL, Jet-A, etc.) from public airport information pages.

## Overview

This script collects fuel price information per airport and outputs structured JSON data.

Supported fuel types:

* 100LL
* Jet-A
* MOGAS
* UL94
* UL91

Supported service types:

* Full Service (FS)
* Self Service (SS)
* Restricted Access (RA)
* Assisted/Self (AS)

## Features

* Collects fuel pricing data per airport (ICAO code)
* Extracts:

  * FBO name
  * Fuel prices
  * Service types
  * Last updated date
  * Guaranteed pricing flag
* Outputs normalized JSON
* Handles variations in formatting and service types

## Usage

```bash
python airnav_fuel_scraper.py KGAI
```

Example output:

```json
{
  "today_date": "2026-03-30",
  "airport_code": "KGAI",
  "source_url": "https://www.airnav.com/airport/KGAI",
  "scraped_at": "2026-03-30T15:00:00+00:00",
  "providers": [
    {
      "fbo_name": "DC Metro Aviation Services",
      "last_update_date": "2026-03-30",
      "guaranteed": true,
      "prices": {
        "100LL_FULL": "7.25",
        "100LL_SELF": "6.70",
        "JET_A_FULL": "7.23"
      }
    }
  ]
}
```

## Installation

Requires Python 3.10+

```bash
pip install requests beautifulsoup4 lxml
```

## Output Format

Price keys follow this structure:

```
{FUEL_TYPE}_{SERVICE_TYPE}
```

Examples:

* `100LL_FULL`
* `100LL_SELF`
* `JET_A_FULL`
* `MOGAS_SELF`

All prices are formatted as strings with two decimal places.

## Notes

* Focuses only on fuel provider sections within airport pages
* Stops parsing when unrelated sections begin (e.g., nearby airport alternatives)
* If no update date is available but pricing is marked as guaranteed, today's date is used
* Duplicate provider entries are ignored

## Limitations

* Depends on the structure of public airport pages
* May break if page layouts change
* Does not attempt to bypass anti-bot protections
* Data completeness is not guaranteed

## Data Source

This tool collects publicly available aviation fuel data from airport-related web pages.

All data rights remain with the original providers.

## License

MIT License
