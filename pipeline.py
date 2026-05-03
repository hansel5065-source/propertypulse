"""
pipeline.py — Convert Apify scraper output to PropertyPulse data.json with scoring.

Usage:
    python pipeline.py                          # uses default input path
    python pipeline.py path/to/results.json     # custom input
    python pipeline.py --append                 # append to existing data.json instead of overwrite
"""

import json
import sys
import os
import re
from datetime import datetime, date

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(
    os.path.dirname(SCRIPT_DIR),
    r"..\..\Users\hanse\OneDrive\Desktop\n8n agent builder\apify-foreclosure-scraper\results_2026-03-14.json"
)
# Also check common locations
FALLBACK_INPUTS = [
    r"C:\Users\hanse\OneDrive\Desktop\n8n agent builder\apify-foreclosure-scraper\results_2026-03-14.json",
    r"C:\Projects\property project\lead-scraper\results_2026-03-14.json",
]
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "data.json")

TODAY = date.today()

# ── Scoring ────────────────────────────────────────────────────────────────────
TYPE_SCORES = {
    "foreclosure": 5,
    "tax_foreclosure": 5,
    "lis_pendens": 6,        # earliest signal = high value
    "master_sale": 5,
    "auction": 4,
    "tax_delinquent": 3,
    "tax_sale": 4,
    "county_sale": 4,
    "probate": 3,
    "reo": 3,
    # Lien types — signal financial distress or encumbered title
    "irs_lien": 7,           # federal tax lien = serious distress
    "mechanic_lien": 6,      # unpaid contractor = owner short on cash
    "hoa_lien": 5,           # HOA delinquency, often foreclosure precursor
    "municipality_lien": 5,  # code violation / city lien
    "other": 2,
}

SOURCE_BONUS = {
    # Active trustee/law firm foreclosures = highest signal
    "kania_mecklenburg": 2,
    "kania_all": 2,
    "hutchens_nc": 2,
    "hutchens_sc": 2,
    "rbcwb_tax_foreclosures": 2,
    "union_foreclosure_schedule_pdf": 2,
    "mcclatchy_charlotte_legals": 2,
    "mcclatchy_rockhill_legals": 2,
    # GIS / county data — good data quality but not necessarily active foreclosures
    "gaston_gis": 0,
    "gaston_tax_foreclosure": 1,
    "gaston_previous_foreclosure": 0,
    # REO / auction
    "hubzu": 1,
    "realtytrac_meck": 1,
    "foreclosure_listings_meck": 1,
}


def parse_date(s):
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except ValueError:
            pass
    # Try just extracting 4-digit year
    m = re.search(r'\b(20\d\d)\b', str(s))
    if m:
        return date(int(m.group(1)), 1, 1)
    return None


def score_record(r):
    """Score a lead 1–10 based on type, equity, urgency, data quality."""
    listing_type = (r.get("listingType") or "other").lower()
    base = TYPE_SCORES.get(listing_type, 2)

    # Source bonus
    source_bonus = SOURCE_BONUS.get(r.get("source", ""), 0)

    # Equity bonus — how much assessed value exceeds the bid/price
    tax_val = r.get("taxValue") or 0
    bid = r.get("openingBid") or r.get("salePrice") or r.get("delinquentAmount") or 0
    try:
        tax_val = float(str(tax_val).replace(",", "").replace("$", ""))
        bid = float(str(bid).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        tax_val = bid = 0

    equity_bonus = 0
    if tax_val > 0 and bid > 0:
        ratio = tax_val / bid
        if ratio >= 4:
            equity_bonus = 3
        elif ratio >= 2.5:
            equity_bonus = 2
        elif ratio >= 1.5:
            equity_bonus = 1

    # Urgency bonus — sale date approaching
    urgency_bonus = 0
    sale_date = parse_date(r.get("saleDate") or r.get("filingDate") or r.get("listingDate"))
    if sale_date:
        days_away = (sale_date - TODAY).days
        if 0 <= days_away <= 30:
            urgency_bonus = 2
        elif 0 <= days_away <= 60:
            urgency_bonus = 1
        elif days_away < 0:
            urgency_bonus = 0  # past

    # Lien stacking bonus — multiple lien types = deeper distress
    lien_types = r.get("lienTypes") or []
    lien_bonus = min(len(lien_types), 2)  # +1 per lien type, max +2

    # Data quality bonus
    quality_bonus = 0
    if r.get("ownerName"):
        quality_bonus += 1

    total = base + source_bonus + equity_bonus + urgency_bonus + lien_bonus + quality_bonus
    return max(1, min(10, total))


def _calc_years_owned(purchase_date_str: str) -> str:
    """Return '12 yrs' style string, or '' if date unavailable."""
    d = parse_date(purchase_date_str)
    if not d:
        return ""
    years = (TODAY - d).days / 365.25
    if years < 1:
        months = int(years * 12)
        return f"{months} mo" if months > 0 else ""
    return f"{years:.0f} yrs"


def normalize(r):
    """Convert Apify record to PropertyPulse app format."""
    address = (r.get("address") or "").strip().title()
    city = (r.get("city") or "").strip().title()
    state = (r.get("state") or "").strip().upper()
    county = (r.get("county") or "").strip()
    zip_code = str(r.get("zip") or "").strip()

    # Build display address
    full_address = address
    if city and state:
        full_address = f"{address}, {city}, {state}"
    elif city:
        full_address = f"{address}, {city}"

    # Map URL
    map_query = f"{address} {city} {state} {zip_code}".strip()
    map_url = f"https://www.google.com/maps/search/?api=1&query={map_query.replace(' ', '%20')}"

    # Category label cleanup
    category = (r.get("listingType") or "other").lower()

    tax_val = r.get("taxValue")
    if tax_val:
        try:
            tax_val = f"${float(str(tax_val).replace(',','').replace('$','')):,.0f}"
        except (ValueError, TypeError):
            tax_val = str(tax_val)

    sale_price = r.get("openingBid") or r.get("salePrice") or r.get("delinquentAmount")
    if sale_price:
        try:
            sale_price = f"${float(str(sale_price).replace(',','').replace('$','')):,.0f}"
        except (ValueError, TypeError):
            sale_price = str(sale_price)

    # Slug for PDF lookup
    slug = re.sub(r'[^a-z0-9]+', '_', full_address.lower())[:60].strip('_')

    return {
        "address": full_address,
        "rawAddress": address,
        "city": city,
        "state": state,
        "county": county,
        "zip": zip_code,
        "category": category,
        "ownerName": (r.get("ownerName") or "").strip().title(),
        "ownerAddress": r.get("ownerAddress") or "",
        "saleDate": str(r.get("saleDate") or r.get("filingDate") or ""),
        "salePrice": sale_price or "",
        "taxValue": tax_val or "",
        "source": r.get("sourceName") or r.get("source") or "",
        "sourceKey": r.get("source") or "",
        "listingUrl": r.get("listingUrl") or r.get("sourceUrl") or "",
        "mapUrl": map_url,
        "documentUrl": r.get("documentUrl") or "",
        "parcelId": r.get("parcelId") or "",
        "caseNumber": r.get("caseNumber") or r.get("courtFileNumber") or "",
        "plaintiff": r.get("plaintiff") or r.get("attorney") or "",
        "beds": r.get("bedrooms") or "",
        "baths": r.get("bathrooms") or "",
        "sqft": r.get("sqft") or "",
        "yearBuilt": r.get("yearBuilt") or "",
        "propertyType": r.get("propertyType") or "",
        "notes": r.get("description") or "",
        "lienTypes": r.get("lienTypes") or [],
        "estimatedEquity": r.get("estimatedEquity") or "",
        "loanBalance": r.get("loanBalance") or "",
        "yearsOwned": _calc_years_owned(r.get("purchaseDate") or ""),
        "purchaseDate": r.get("purchaseDate") or "",
        "score": score_record(r),
        "status": r.get("status") or "Active",
        "analyzed": False,
        "pdfSlug": slug,
        "scrapedAt": r.get("scrapedAt") or "",
    }


def dedupe(records):
    """Remove duplicate addresses, keep the one with higher score."""
    seen = {}
    for r in records:
        key = (r["rawAddress"].lower().strip(), r["county"].lower())
        if key not in seen or r["score"] > seen[key]["score"]:
            seen[key] = r
    return list(seen.values())


def main():
    append_mode = "--append" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    # Find input file
    input_path = args[0] if args else None
    if not input_path:
        for p in [DEFAULT_INPUT] + FALLBACK_INPUTS:
            if os.path.exists(p):
                input_path = p
                break

    if not input_path or not os.path.exists(input_path):
        print(f"ERROR: Could not find input file. Tried:\n  {DEFAULT_INPUT}")
        for p in FALLBACK_INPUTS:
            print(f"  {p}")
        sys.exit(1)

    print(f"Reading: {input_path}")
    with open(input_path, encoding="utf-8", errors="replace") as f:
        raw = json.load(f)

    print(f"Total records (Apify): {len(raw)}")

    # Merge PropWire leads if available
    propwire_path = os.path.join(SCRIPT_DIR, "results_propwire.json")
    if os.path.exists(propwire_path):
        with open(propwire_path, encoding="utf-8") as f:
            propwire_raw = json.load(f)
        print(f"Merging PropWire: {len(propwire_raw)} leads")
        raw = raw + propwire_raw
        print(f"Combined total: {len(raw)}")
    else:
        print("PropWire results not found — run scrape_propwire_leads.py first to include them")

    # Filter to records with usable address + county
    def is_real_address(r):
        addr = str(r.get("address") or "").strip()
        if not addr or not r.get("county"):
            return False
        # Filter out legal notice text masquerading as addresses
        garbage_phrases = [
            "which is the subject", "will be sold", "notice of", "foreclosure sale",
            "the property", "described as", "tax map no", "located at the",
        ]
        addr_lower = addr.lower()
        if any(p in addr_lower for p in garbage_phrases):
            return False
        # Must look like an address: starts with number or common street prefix
        if not re.match(r'^(\d+|[A-Z]{1,3}\s*\d)', addr, re.IGNORECASE):
            # Allow addresses starting with cardinal directions
            if not re.match(r'^(north|south|east|west|n\.|s\.|e\.|w\.)\s', addr_lower):
                return False
        return True

    # Filter to target counties only (your 4-county focus area)
    TARGET_COUNTIES = {"mecklenburg", "gaston", "union", "york"}
    usable = [r for r in raw if is_real_address(r)
              and str(r.get("county") or "").strip().lower() in TARGET_COUNTIES]
    print(f"With address: {len(usable)}")

    # Normalize + score
    normalized = [normalize(r) for r in usable]

    # Dedupe
    deduped = dedupe(normalized)
    print(f"After dedup: {len(deduped)}")

    # Sort by score desc, then saleDate asc
    deduped.sort(key=lambda r: (-r["score"], r.get("saleDate") or "9999"))

    # If append mode, merge with existing data.json
    if append_mode and os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            existing = json.load(f)
        existing_keys = {(r.get("rawAddress","").lower(), r.get("county","").lower()) for r in existing}
        new_records = [r for r in deduped if (r["rawAddress"].lower(), r["county"].lower()) not in existing_keys]
        deduped = existing + new_records
        print(f"Merged: {len(existing)} existing + {len(new_records)} new = {len(deduped)}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)

    # Summary
    hot = sum(1 for r in deduped if r["score"] >= 7)
    warm = sum(1 for r in deduped if 4 <= r["score"] < 7)
    cool = sum(1 for r in deduped if r["score"] < 4)
    counties = {}
    for r in deduped:
        counties[r["county"]] = counties.get(r["county"], 0) + 1

    print(f"\nWritten to: {OUTPUT_PATH}")
    print(f"  Hot (7+): {hot}  |  Warm (4-6): {warm}  |  Cool (1-3): {cool}")
    print(f"  By county: {counties}")
    print(f"  Top 5 deals:")
    for r in deduped[:5]:
        print(f"    [{r['score']}] {r['address']} ({r['category']}) — {r['taxValue'] or 'no value'}")


if __name__ == "__main__":
    main()
