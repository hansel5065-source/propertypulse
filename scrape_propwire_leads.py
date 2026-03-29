"""
scrape_propwire_leads.py — Pull distressed property leads from PropWire for all 4 counties.

Uses your REAL Chrome browser to bypass bot detection — completely free.

SETUP (one time):
    1. Close Chrome completely
    2. Open Chrome with remote debugging:
       "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
    3. Log into PropWire in that Chrome window (propwire.com)
    4. Run this script: python scrape_propwire_leads.py

Usage:
    python scrape_propwire_leads.py                       # all 4 counties, all free lead types
    python scrape_propwire_leads.py --county union        # single county
    python scrape_propwire_leads.py --types foreclosure absentee_owner
    python scrape_propwire_leads.py --output results_propwire.json

Output format matches Apify scraper so pipeline.py merges it automatically.
"""

import asyncio
import json
import os
import re
import sys
from datetime import date, datetime

from playwright.async_api import async_playwright

# ── Config ─────────────────────────────────────────────────────────────────────
CHROME_CDP_URL = "http://localhost:9222"   # Chrome remote debugging port

COUNTIES = {
    "mecklenburg": {"state": "NC", "county": "Mecklenburg", "search": "Mecklenburg County, NC"},
    "union":        {"state": "NC", "county": "Union",        "search": "Union County, NC"},
    "gaston":       {"state": "NC", "county": "Gaston",       "search": "Gaston County, NC"},
    "york":         {"state": "SC", "county": "York",         "search": "York County, SC"},
}

# Free lead types on PropWire + their pipeline category
FREE_LEAD_TYPES = {
    "absentee_owner":    ("Absentee Owners",    "other"),
    "adjustable_loan":   ("Adjustable Loans",   "other"),
    "assumable_loan":    ("Assumable Loans",    "other"),
    "auction":           ("Auctions",           "auction"),
    "bank_owned":        ("Bank Owned (REOs)",  "reo"),
    "pre_foreclosure":   ("Pre-Foreclosure",    "lis_pendens"),
    "foreclosure":       ("Foreclosure",        "foreclosure"),
    "tax_delinquent":    ("Tax Delinquent",     "tax_delinquent"),
    # Lien categories — scraper clicks these if PropWire shows them as filter options
    "hoa_lien":          ("HOA Liens",          "hoa_lien"),
    "mechanic_lien":     ("Mechanic's Liens",   "mechanic_lien"),
    "irs_lien":          ("IRS/Tax Liens",      "irs_lien"),
    "municipality_lien": ("Municipal Liens",    "municipality_lien"),
}

# Lien keywords to detect in page text (on card bodies, even if not a filter type)
LIEN_KEYWORDS = {
    "HOA Lien":          "hoa_lien",
    "HOA Liens":         "hoa_lien",
    "Mechanic":          "mechanic_lien",
    "Mechanic's Lien":   "mechanic_lien",
    "IRS Lien":          "irs_lien",
    "Federal Tax Lien":  "irs_lien",
    "Tax Lien":          "irs_lien",
    "Municipal Lien":    "municipality_lien",
    "Municipality Lien": "municipality_lien",
    "Code Violation":    "municipality_lien",
    "Code Enforcement":  "municipality_lien",
    "City Lien":         "municipality_lien",
}

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results_propwire.json")


# ── Connect to real Chrome ─────────────────────────────────────────────────────

async def connect_chrome(p):
    """Connect to the user's real Chrome browser via CDP."""
    try:
        browser = await p.chromium.connect_over_cdp(CHROME_CDP_URL)
        print(f"[Chrome] Connected to real Chrome browser")
        return browser
    except Exception as e:
        print(f"""
[ERROR] Could not connect to Chrome. Make sure Chrome is running with remote debugging:

  1. Close ALL Chrome windows first
  2. Run this command:
     "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222

  OR for Edge:
     "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222

  3. Log into propwire.com in that browser
  4. Then run this script again

Error: {e}
""")
        sys.exit(1)


# ── PropWire search ────────────────────────────────────────────────────────────

async def get_or_create_page(browser):
    """Get existing PropWire tab or create a new one."""
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "propwire.com" in pg.url:
                print(f"[Chrome] Reusing existing PropWire tab")
                return pg
    # Create new page in first context
    ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
    page = await ctx.new_page()
    return page


async def apply_county_filter(page, county_config: dict) -> bool:
    """Navigate to PropWire search filtered to a specific county."""
    search_term = county_config["search"]
    county = county_config["county"]
    state = county_config["state"]

    print(f"  Navigating to PropWire search for {county} County, {state}...")

    # Navigate to search
    await page.goto("https://propwire.com/search?filters=%7B%7D", timeout=30000)
    await asyncio.sleep(3)

    # Check if we're blocked
    page_text = await page.inner_text("body")
    if "access is temporarily restricted" in page_text.lower():
        print("  [!] PropWire is blocking this session. Try logging in first.")
        return False

    # Click visible search input by mouse coordinates (works with React)
    rect = await page.evaluate('''() => {
        const inputs = document.querySelectorAll('input[name=search]');
        for (const inp of inputs) {
            const r = inp.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) return {x: r.x + r.width/2, y: r.y + r.height/2};
        }
        return null;
    }''')
    if not rect:
        print(f"  [!] No visible search input found")
        return False

    await page.mouse.click(rect['x'], rect['y'])
    await asyncio.sleep(0.4)
    await page.keyboard.press('Control+a')
    await page.keyboard.press('Delete')
    await asyncio.sleep(0.2)
    await page.keyboard.type(county, delay=80)
    await asyncio.sleep(3)

    # Click the matching county suggestion
    clicked = await page.evaluate(f'''() => {{
        const county = "{county}".toLowerCase();
        const all = document.querySelectorAll("li, [role=option], [class*=suggestion]");
        for (const el of all) {{
            const r = el.getBoundingClientRect();
            const t = (el.innerText||"").toLowerCase();
            if (r.width > 0 && r.height > 0 && t.includes(county)) {{
                el.click();
                return el.innerText.slice(0,60);
            }}
        }}
        return null;
    }}''')
    if clicked:
        print(f"  Selected: {clicked.strip()}")
        await asyncio.sleep(3)
    else:
        await page.keyboard.press("Enter")
        await asyncio.sleep(3)

    return True


async def apply_lead_type_filters(page, lead_types: list):
    """Click Lead Types dropdown and select the specified types."""
    try:
        # Open Lead Types dropdown
        lead_btn = page.locator('button:has-text("Lead Types"), [class*="lead-type"]').first
        if await lead_btn.count() > 0:
            await lead_btn.click()
            await asyncio.sleep(1)

            # Click each lead type checkbox
            for lt_key in lead_types:
                label, _ = FREE_LEAD_TYPES.get(lt_key, (lt_key, "other"))
                checkbox = page.locator(f'text="{label}"').first
                if await checkbox.count() > 0:
                    await checkbox.click()
                    await asyncio.sleep(0.3)

            # Close dropdown by clicking elsewhere
            await page.keyboard.press("Escape")
            await asyncio.sleep(2)
    except Exception as e:
        print(f"  [!] Could not apply lead type filter: {e}")


async def capture_search_api(page) -> list:
    """
    Capture PropWire's search API response by intercepting network calls.
    Returns raw property records if API is found; empty list otherwise.
    """
    api_results = []

    async def handle_response(response):
        try:
            if any(kw in response.url for kw in ["/api/", "/search", "graphql", "properties"]):
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        # Look for array of property objects
                        if isinstance(data, list) and len(data) > 0:
                            api_results.extend(data)
                        elif isinstance(data, dict):
                            for key in ["results", "properties", "data", "items", "listings"]:
                                if isinstance(data.get(key), list):
                                    api_results.extend(data[key])
                                    break
        except Exception:
            pass

    page.on("response", handle_response)


async def scrape_county(page, county_key: str, config: dict, lead_types: list) -> list:
    """Scrape PropWire search results for one county."""
    records = []
    county = config["county"]
    state = config["state"]

    # Set up API capture
    api_results = []

    async def handle_response(response):
        try:
            if any(kw in response.url for kw in ["/api/", "search", "graphql", "propert"]):
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if isinstance(data, list) and len(data) > 0:
                            api_results.extend(data)
                        elif isinstance(data, dict):
                            for key in ["results", "properties", "data", "items", "listings", "content"]:
                                if isinstance(data.get(key), list) and data[key]:
                                    api_results.extend(data[key])
                                    break
        except Exception:
            pass

    page.on("response", handle_response)

    ok = await apply_county_filter(page, config)
    if not ok:
        page.remove_listener("response", handle_response)
        return records

    if lead_types:
        await apply_lead_type_filters(page, lead_types)

    await asyncio.sleep(4)

    # Try API results first (cleanest data)
    if api_results:
        print(f"  API capture: {len(api_results)} records")
        for item in api_results:
            rec = _normalize_api_record(item, county, state)
            if rec:
                records.append(rec)
    else:
        # Fall back to parsing page text
        print(f"  No API capture — parsing page text")
        body_text = await page.inner_text("body")
        records = _parse_page_text(body_text, county, state, page.url)

    # Paginate
    page_num = 2
    while page_num <= 10:
        api_results.clear()
        next_btn = page.locator(
            'button[aria-label*="next" i], button:has-text("Next"), [class*="next-page"]'
        ).first
        if await next_btn.count() == 0:
            break
        enabled = await next_btn.is_enabled()
        if not enabled:
            break

        await next_btn.click()
        await asyncio.sleep(3)
        print(f"  Page {page_num}...")

        if api_results:
            for item in api_results:
                rec = _normalize_api_record(item, county, state)
                if rec:
                    records.append(rec)
        else:
            body_text = await page.inner_text("body")
            page_records = _parse_page_text(body_text, county, state, page.url)
            if not page_records:
                break
            records.extend(page_records)

        page_num += 1

    page.remove_listener("response", handle_response)
    print(f"  {county}: {len(records)} leads found")
    return records


def _fmt_money(val) -> str:
    """Format a raw money value to '$X,XXX' string, or '' if empty."""
    if not val:
        return ""
    try:
        return f"${float(str(val).replace(',', '').replace('$', '')):,.0f}"
    except (ValueError, TypeError):
        return str(val)


def _normalize_api_record(item: dict, county: str, state: str) -> dict | None:
    """Convert a PropWire API record to pipeline format."""
    if not isinstance(item, dict):
        return None

    # Try common address fields
    address = (
        item.get("address") or item.get("street_address") or
        item.get("property_address") or item.get("situs_address") or ""
    ).strip()

    if not address or not re.match(r"^\d+", address):
        return None

    city = item.get("city") or item.get("property_city") or ""
    zip_code = str(item.get("zip") or item.get("zip_code") or item.get("postal_code") or "")
    owner = item.get("owner_name") or item.get("owner") or ""

    # Lead type / tags
    tags = item.get("lead_types") or item.get("tags") or item.get("labels") or []
    listing_type = "other"
    lien_types_found = []
    for tag in (tags if isinstance(tags, list) else [tags]):
        tag_str = str(tag)
        tag_lower = tag_str.lower().replace(" ", "_").replace("-", "_")
        for lt_key, (_, lt_type) in FREE_LEAD_TYPES.items():
            if lt_key in tag_lower or tag_lower in lt_key:
                listing_type = lt_type
                break
        # Also check for lien keywords in tags
        for kw, lien_cat in LIEN_KEYWORDS.items():
            if kw.lower() in tag_str.lower():
                if lien_cat not in lien_types_found:
                    lien_types_found.append(lien_cat)
                if listing_type == "other":
                    listing_type = lien_cat

    # Check lien keywords in description/notes fields too
    for field in ["description", "notes", "remarks", "tags_text"]:
        field_val = str(item.get(field) or "")
        for kw, lien_cat in LIEN_KEYWORDS.items():
            if kw.lower() in field_val.lower():
                if lien_cat not in lien_types_found:
                    lien_types_found.append(lien_cat)

    # Value fields
    est_value = item.get("estimated_value") or item.get("avm") or item.get("market_value") or ""
    if est_value:
        try:
            est_value = f"${float(str(est_value).replace(',', '').replace('$', '')):,.0f}"
        except (ValueError, TypeError):
            est_value = str(est_value)

    # Ownership / financial fields
    purchase_date = str(
        item.get("purchase_date") or item.get("last_sale_date") or
        item.get("acquisition_date") or item.get("sale_date") or ""
    )
    estimated_equity = _fmt_money(
        item.get("estimated_equity") or item.get("equity") or
        item.get("equity_value") or item.get("equity_amount")
    )
    loan_balance = _fmt_money(
        item.get("mortgage_balance") or item.get("loan_balance") or
        item.get("open_mortgage") or item.get("estimated_mortgage") or
        item.get("open_lien") or item.get("lien_amount")
    )

    return {
        "address": address.title(),
        "city": str(city).title(),
        "state": state,
        "county": county,
        "zip": zip_code,
        "ownerName": str(owner).title(),
        "ownerAddress": item.get("owner_address") or item.get("mailing_address") or "",
        "listingType": listing_type,
        "taxValue": est_value,
        "openingBid": "",
        "saleDate": str(item.get("auction_date") or item.get("sale_date") or ""),
        "filingDate": str(item.get("filing_date") or date.today().isoformat()),
        "caseNumber": str(item.get("case_number") or ""),
        "parcelId": str(item.get("apn") or item.get("parcel_id") or ""),
        "beds": str(item.get("bedrooms") or item.get("beds") or ""),
        "baths": str(item.get("bathrooms") or item.get("baths") or ""),
        "sqft": str(item.get("sqft") or item.get("living_sqft") or ""),
        "yearBuilt": str(item.get("year_built") or ""),
        "purchaseDate": purchase_date,
        "estimatedEquity": estimated_equity,
        "loanBalance": loan_balance,
        "source": "propwire",
        "sourceName": f"PropWire — {county} County",
        "sourceUrl": "https://propwire.com",
        "listingUrl": f"https://propwire.com/realestate/{address.lower().replace(' ', '-')}-{city.lower().replace(' ', '-')}-{state}-{zip_code}",
        "tags": tags if isinstance(tags, list) else [str(tags)],
        "lienTypes": lien_types_found,
        "scrapedAt": datetime.utcnow().isoformat() + "Z",
    }


def _parse_page_text(body_text: str, county: str, state: str, url: str) -> list:
    """Fallback: parse PropWire search results from page body text."""
    records = []
    today = date.today().isoformat()

    # Match property address blocks
    card_re = re.compile(
        r'(\d+\s+[A-Z][A-Za-z0-9\s]+(?:Rd|Dr|St|Ave|Blvd|Ln|Ct|Way|Pl|Cir|Pkwy|Hwy|Trl|Run|Loop)\b.{0,400}?)(?=\d+\s+[A-Z]|\Z)',
        re.DOTALL
    )

    for match in card_re.finditer(body_text):
        card = match.group(1)
        addr_m = re.match(r'(\d+\s+[^\n,]{4,60})', card)
        if not addr_m:
            continue
        address = addr_m.group(1).strip()
        if len(address) > 80:
            continue

        city_m = re.search(r'([A-Za-z\s]{3,25}),?\s+(?:NC|SC)\s+(\d{5})', card)
        city = city_m.group(1).strip() if city_m else ""
        zip_code = city_m.group(2) if city_m else ""

        listing_type = "other"
        tags_found = []
        lien_types_found = []
        for lt_key, (label, lt_type) in FREE_LEAD_TYPES.items():
            if label.lower() in card.lower():
                tags_found.append(label)
                listing_type = lt_type
                break
        # Detect lien keywords in card text
        for kw, lien_cat in LIEN_KEYWORDS.items():
            if kw.lower() in card.lower():
                if lien_cat not in lien_types_found:
                    lien_types_found.append(lien_cat)
                    tags_found.append(kw)
                if listing_type == "other":
                    listing_type = lien_cat

        val_m = re.search(r'\$([0-9]{3}[0-9,]+)', card)
        tax_value = f"${val_m.group(1)}" if val_m else ""

        # Try to extract equity / loan balance from card text
        equity_m = re.search(r'[Ee]quity[:\s]+\$?([0-9,]+)', card)
        equity_val = f"${equity_m.group(1)}" if equity_m else ""
        loan_m = re.search(r'[Ll]oan\s*[Bb]alance[:\s]+\$?([0-9,]+)|[Mm]ortgage[:\s]+\$?([0-9,]+)', card)
        loan_val = f"${(loan_m.group(1) or loan_m.group(2))}" if loan_m else ""

        # Try to extract purchase/last sale date
        date_m = re.search(r'[Pp]urchased?[:\s]+(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})', card)
        purchase_date = date_m.group(1) if date_m else ""

        records.append({
            "address": address.title(),
            "city": city.title(),
            "state": state,
            "county": county,
            "zip": zip_code,
            "ownerName": "",
            "ownerAddress": "",
            "listingType": listing_type,
            "taxValue": tax_value,
            "openingBid": "",
            "saleDate": "",
            "filingDate": today,
            "caseNumber": "",
            "parcelId": "",
            "beds": "", "baths": "", "sqft": "", "yearBuilt": "",
            "purchaseDate": purchase_date,
            "estimatedEquity": equity_val,
            "loanBalance": loan_val,
            "source": "propwire",
            "sourceName": f"PropWire — {county} County",
            "sourceUrl": url,
            "listingUrl": url,
            "tags": tags_found,
            "lienTypes": lien_types_found,
            "scrapedAt": datetime.utcnow().isoformat() + "Z",
        })

    return records


def dedupe(records: list) -> list:
    seen = {}
    for r in records:
        key = r["address"].lower().strip()
        if key not in seen:
            seen[key] = r
    return list(seen.values())


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    args = sys.argv[1:]
    target_county = None
    output_path = OUTPUT_PATH
    lead_types = list(FREE_LEAD_TYPES.keys())  # all free types by default

    i = 0
    while i < len(args):
        if args[i] == "--county" and i + 1 < len(args):
            target_county = args[i + 1].lower()
            i += 2
        elif args[i] == "--output" and i + 1 < len(args):
            output_path = args[i + 1]
            i += 2
        elif args[i] == "--types":
            lead_types = []
            i += 1
            while i < len(args) and not args[i].startswith("--"):
                lead_types.append(args[i])
                i += 1
        else:
            i += 1

    counties = {k: v for k, v in COUNTIES.items() if not target_county or k == target_county}
    if not counties:
        print(f"ERROR: Unknown county '{target_county}'. Options: {', '.join(COUNTIES.keys())}")
        sys.exit(1)

    print(f"\n[PropWire Lead Scraper]")
    print(f"Counties: {', '.join(counties.keys())}")
    print(f"Lead types: {', '.join(lead_types)}")
    print(f"Output: {output_path}\n")

    all_records = []

    async with async_playwright() as p:
        browser = await connect_chrome(p)
        page = await get_or_create_page(browser)

        for county_key, config in counties.items():
            county_records = await scrape_county(page, county_key, config, lead_types)
            all_records.extend(county_records)
            await asyncio.sleep(2)  # Polite pause between counties

    deduped = dedupe(all_records)
    print(f"\n[PropWire] {len(all_records)} raw -> {len(deduped)} after dedup")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)
    print(f"[PropWire] Saved -> {output_path}")

    by_county = {}
    by_type = {}
    for r in deduped:
        by_county[r["county"]] = by_county.get(r["county"], 0) + 1
        by_type[r["listingType"]] = by_type.get(r["listingType"], 0) + 1
    print(f"  By county: {by_county}")
    print(f"  By type:   {by_type}")

    print(f"""
Next steps:
  1. Run pipeline.py to merge into PropertyPulse data.json
     python pipeline.py

  2. Or merge directly:
     python pipeline.py --append
""")


if __name__ == "__main__":
    asyncio.run(main())
