"""
merge_new_data.py — Merge standalone scraper output into data.json
Normalizes schema, filters to Charlotte metro, deduplicates, scores.
"""
import json, re, os, urllib.parse, urllib.request
from datetime import datetime, date

import argparse as _ap
_args = _ap.ArgumentParser(); _args.add_argument("--input", default=None); _parsed = _args.parse_args()
INPUT_NEW   = _parsed.input or r"C:\Users\hanse\AppData\Local\Temp\standalone_test.json"
INPUT_EXIST = r"C:\Projects\property project\property-app\data.json"
OUTPUT      = r"C:\Projects\property project\property-app\data.json"
TODAY       = date.today()

# ── Target counties (Charlotte metro + SC border) ──────────────────────────
TARGET_COUNTIES = {
    "mecklenburg", "gaston", "union", "york"
}

# ── Residential property type filter (for tax_delinquent) ─────────────────
RESIDENTIAL_TYPES = {
    "residential 1 family", "residential 2 family", "residential multi-family",
    "residential multifamily", "townhouse", "townhome", "condominium", "condo",
    "mobile home", "manufactured home", "single family", "single family residential",
    "sfr", "duplex", "triplex", "quadplex", "residential",
}
NON_RESIDENTIAL_TYPES = {
    "commercial", "industrial", "vacant land", "vacant", "agriculture",
    "agricultural", "government", "institutional", "special use", "utility",
    "exempt", "church", "office", "retail", "warehouse", "storage",
    "parking", "mixed use", "hotel", "motel",
}

def is_residential(prop_type: str) -> bool:
    """Return True if property type is residential or unknown (keep unknown)."""
    if not prop_type:
        return True  # unknown — keep it
    pt = prop_type.strip().lower()
    if any(r in pt for r in NON_RESIDENTIAL_TYPES):
        return False
    return True  # residential match or unrecognized — keep

# ── Scoring ────────────────────────────────────────────────────────────────
TYPE_SCORES = {
    "irs_lien": 7, "lis_pendens": 6, "mechanic_lien": 6,
    "foreclosure": 5, "tax_foreclosure": 5, "master_sale": 5,
    "hoa_lien": 5, "municipality_lien": 5,
    "auction": 4, "tax_sale": 4, "county_sale": 4,
    "tax_delinquent": 3, "probate": 3, "reo": 3, "other": 2,
}

SOURCE_BONUS = {
    "kania_mecklenburg": 2, "kania_all": 2, "hutchens_nc": 2,
    "hutchens_sc": 2, "rbcwb_tax_foreclosures": 2,
    "union_foreclosure_schedule_pdf": 2, "mcclatchy_charlotte_legals": 2,
    "mcclatchy_rockhill_legals": 2, "gaston_tax_foreclosure": 1,
    "hubzu": 1, "foreclosure_com_meck": 1, "foreclosure_listings_meck": 1,
}

DISTRESS_MAP = {
    "foreclosure": "active_sale", "tax_foreclosure": "active_sale",
    "auction": "active_sale", "master_sale": "active_sale",
    "tax_sale": "active_sale", "county_sale": "active_sale",
    "irs_lien": "lien_legal", "mechanic_lien": "lien_legal",
    "hoa_lien": "lien_legal", "municipality_lien": "lien_legal",
    "lis_pendens": "lien_legal",
    "tax_delinquent": "delinquent",
}

# City → county lookup for zip_loop scrapers that set all records to one county
CITY_COUNTY = {
    # Mecklenburg NC
    "charlotte": "Mecklenburg", "matthews": "Mecklenburg", "pineville": "Mecklenburg",
    "huntersville": "Mecklenburg", "davidson": "Mecklenburg", "cornelius": "Mecklenburg",
    "mint hill": "Mecklenburg", "stallings": "Mecklenburg",
    # Gaston NC
    "gastonia": "Gaston", "belmont": "Gaston", "bessemer city": "Gaston",
    "cherryville": "Gaston", "cramerton": "Gaston", "dallas": "Gaston",
    "lowell": "Gaston", "mount holly": "Gaston", "stanley": "Gaston",
    "alexis": "Gaston",
    # Union NC
    "monroe": "Union", "indian trail": "Union", "waxhaw": "Union",
    "wingate": "Union", "marshville": "Union", "marvin": "Union",
    "weddington": "Union", "hemby bridge": "Union", "fairview": "Union",
    "mineral springs": "Union", "new london": "Union",
    # York SC
    "rock hill": "York", "fort mill": "York", "clover": "York",
    "york": "York", "tega cay": "York", "lake wylie": "York",
    "sharon": "York", "smyrna": "York",
    # Out-of-area cities (correct county assigned → will be rejected by TARGET_COUNTIES filter)
    "kings mountain": "Cleveland", "lincolnton": "Lincoln", "midland": "Cabarrus",
    "mooresville": "Iredell", "peachland": "Anson", "morven": "Anson",
    "maiden": "Lincoln", "vale": "Lincoln", "denver": "Lincoln",
}

def county_from_address(address: str) -> str:
    """Parse city from multiline address and return county. Returns '' if unknown."""
    if not address:
        return ""
    # Format: "Street Name\nCity, ST ZIP" or "Street, City, ST ZIP"
    m = re.search(r'\n([^,\n]+),\s*[A-Z]{2}', address)
    if not m:
        m = re.search(r',\s*([^,]+),\s*[A-Z]{2}\s*\d{5}', address)
    if m:
        city = m.group(1).strip().lower()
        return CITY_COUNTY.get(city, "")
    return ""

def normalize_county(c):
    if not c:
        return ""
    c = str(c).strip().title()
    fixes = {"Mecklenburg County": "Mecklenburg", "Gaston County": "Gaston",
             "Union County": "Union", "York County": "York",
             "Sc": "York", "Nc": ""}
    return fixes.get(c, c)

def parse_money(v):
    if not v:
        return 0
    try:
        return float(re.sub(r"[^0-9.]", "", str(v)))
    except:
        return 0

def parse_date(s):
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except:
            pass
    m = re.search(r'\b(20\d\d)\b', str(s))
    return date(int(m.group(1)), 1, 1) if m else None

def slug(addr):
    return re.sub(r'[^a-z0-9]+', '_', (addr or "").lower()).strip('_')

def score_record(r):
    cat = (r.get("category") or "other").lower()

    # Tax delinquent uses dedicated risk scorer
    if cat == "tax_delinquent":
        return score_risk(r)

    base = TYPE_SCORES.get(cat, 2)
    bonus = SOURCE_BONUS.get(r.get("sourceKey", ""), 0)

    tax_val = parse_money(r.get("taxValue"))
    bid = parse_money(r.get("salePrice") or r.get("openingBid") or 0)
    equity_bonus = 1 if (tax_val > 0 and bid > 0 and (tax_val - bid) / tax_val > 0.3) else 0

    sale_d = parse_date(r.get("saleDate"))
    date_bonus = 0
    if sale_d:
        days = (sale_d - TODAY).days
        if 0 <= days <= 30:   date_bonus = 3
        elif 0 <= days <= 60: date_bonus = 2
        else:                  date_bonus = 1

    addr_bonus    = 1 if r.get("address") else 0
    bid_bonus     = 1 if (0 < bid < 100000) else 0
    sfr_bonus     = 1 if str(r.get("propertyType", "")).lower() in ("sfr","single family","residential") else 0
    has_val_bonus = 1 if tax_val > 0 else 0

    return min(10, base + bonus + equity_bonus + date_bonus + addr_bonus + bid_bonus + sfr_bonus + has_val_bonus)


def score_risk(r):
    """
    Risk score for tax_delinquent records (Track 2 — Tax Watch).
    Higher = more distressed / higher priority for investors.
    Scale 1-10.
    """
    score = 3  # base for tax_delinquent

    # Delinquent amount — more owed = more pressure on owner
    delinquent = parse_money(r.get("salePrice") or r.get("notes") or 0)
    if delinquent > 10000:  score += 3
    elif delinquent > 5000: score += 2
    elif delinquent > 1000: score += 1

    # Equity signal — high assessed value vs low delinquency = owner has equity & likely motivated
    tax_val = parse_money(r.get("taxValue"))
    if tax_val > 0 and delinquent > 0:
        delinquent_ratio = delinquent / tax_val
        if delinquent_ratio < 0.05 and tax_val > 100000:
            # Small delinquency on high-value home = equity present, owner might sell
            score += 2
        elif delinquent_ratio < 0.10:
            score += 1

    # Address present
    if r.get("address"):
        score += 1

    # Source quality
    bonus = SOURCE_BONUS.get(r.get("sourceKey", ""), 0)
    score += bonus

    # Residential confirmed
    pt = str(r.get("propertyType", "")).lower()
    if pt and any(res in pt for res in ("residential", "sfr", "single family", "townhouse", "condo")):
        score += 1

    return min(10, score)

def dedup_key(r):
    addr = re.sub(r'\s+', ' ', (r.get("address") or "").strip().lower())
    county = (r.get("county") or "").lower()
    pid = (r.get("parcelId") or "").strip().lower()
    if pid:
        return f"pid:{pid}"
    if addr:
        return f"addr:{addr}|{county}"
    return None

def normalize_new(r):
    """Convert standalone scraper schema → data.json schema.
    Returns (record, None) on success or (None, skip_reason) on skip.
    """
    category = (r.get("listingType") or "other").lower()
    county = normalize_county(r.get("county"))
    # For zip_loop sites that set all records to one county (e.g. all → "Mecklenburg"),
    # override with city lookup from address
    addr_county = county_from_address(r.get("address") or "")
    if addr_county:
        county = addr_county
    if county.lower() not in TARGET_COUNTIES:
        return None, "county"

    # Drop non-residential tax_delinquent records (Track 2 = residential only)
    if category == "tax_delinquent":
        prop_type = r.get("propertyType") or r.get("useType") or r.get("landUse") or ""
        if not is_residential(prop_type):
            return None, "nonresidential"
        # Drop very low-value properties (< $50K assessed) — not worth pursuing
        tv = parse_money(r.get("taxValue") or r.get("assessedValue") or 0)
        if tv > 0 and tv < 50000:
            return None, "nonresidential"

    # Drop REO, forfeiture, surplus (not useful for two-track system)
    if category in ("reo", "forfeiture", "surplus"):
        return None, "nonresidential"

    address = r.get("address") or ""
    city = r.get("city") or ""
    state = r.get("state") or ""
    zip_ = r.get("zip") or ""

    # Build full address string
    full_addr = address.strip()
    if city and city.lower() not in full_addr.lower():
        full_addr = f"{full_addr}, {city}, {state}".strip(", ")

    sale_price = (
        r.get("currentBid") or r.get("openingBid") or
        r.get("upsetBid") or r.get("delinquentAmount") or
        r.get("salePrice") or ""
    )
    if sale_price:
        val = parse_money(sale_price)
        sale_price = f"${val:,.0f}" if val else ""

    tax_val = r.get("taxValue") or ""
    if tax_val:
        val = parse_money(tax_val)
        tax_val = f"${val:,.0f}" if val else ""

    distress = DISTRESS_MAP.get(category, "other")

    # Compute equity % for foreclosure records
    estimated_equity = ""
    if category in ("foreclosure", "tax_foreclosure", "hoa_foreclosure", "master_sale", "auction"):
        tax_val_num = parse_money(tax_val)
        bid_num = parse_money(sale_price)
        if tax_val_num > 0 and bid_num > 0 and tax_val_num > bid_num:
            equity_pct = (tax_val_num - bid_num) / tax_val_num * 100
            estimated_equity = f"{equity_pct:.0f}%"

    rec = {
        "address": full_addr,
        "rawAddress": address,
        "city": city,
        "state": state,
        "county": county,
        "zip": zip_,
        "category": category,
        "distressLevel": distress,
        "ownerName": r.get("ownerName") or r.get("defendant") or "",
        "ownerAddress": r.get("ownerAddress") or "",
        "saleDate": str(r.get("saleDate") or ""),
        "salePrice": sale_price,
        "taxValue": tax_val,
        "source": r.get("sourceName") or r.get("source") or "",
        "sourceKey": r.get("source") or "",
        "listingUrl": r.get("listingUrl") or "",
        "mapUrl": r.get("gisMapUrl") or (
            f"https://www.google.com/maps/search/?api=1&query={full_addr.replace(' ', '+')}" if full_addr else ""
        ),
        "documentUrl": r.get("documentUrl") or "",
        "parcelId": r.get("parcelId") or r.get("gisId") or "",
        "caseNumber": r.get("caseNumber") or r.get("courtFileNumber") or "",
        "plaintiff": r.get("plaintiff") or r.get("attorney") or "",
        "beds": str(r.get("bedrooms") or ""),
        "baths": str(r.get("bathrooms") or ""),
        "sqft": str(r.get("sqft") or ""),
        "yearBuilt": str(r.get("yearBuilt") or ""),
        "propertyType": r.get("propertyType") or r.get("useType") or r.get("landUse") or "",
        "notes": r.get("description") or "",
        "lienTypes": [],
        "estimatedEquity": estimated_equity,
        "loanBalance": "",
        "yearsOwned": "",
        "purchaseDate": "",
        "status": r.get("status") or "Active",
        "analyzed": False,
        "pdfSlug": slug(full_addr),
        "scrapedAt": r.get("scrapedAt") or datetime.utcnow().isoformat() + "Z",
    }
    rec["score"] = score_record(rec)
    # Drop tax_delinquent with no enrichment signals (score < 4 = bare address, no useful data)
    if category == "tax_delinquent" and rec["score"] < 4:
        return None, "nonresidential"
    return rec, None

def add_distress(r):
    """Add/fix distressLevel and apply residential filter to existing records."""
    cat = (r.get("category") or "other").lower()
    if not r.get("distressLevel"):
        r["distressLevel"] = DISTRESS_MAP.get(cat, "other")
    # Re-score with updated scoring (score_risk for tax_delinquent)
    r["score"] = score_record(r)
    return r

def keep_existing(r):
    """Return False to drop an existing record (apply same filters as new)."""
    cat = (r.get("category") or "other").lower()
    # Drop non-residential tax_delinquent and low-value (<$50K)
    if cat == "tax_delinquent":
        prop_type = r.get("propertyType") or ""
        if not is_residential(prop_type):
            return False
        tv = parse_money(r.get("taxValue") or 0)
        if tv > 0 and tv < 50000:
            return False
    # Drop REO, forfeiture, surplus (not useful for two-track system)
    if cat in ("reo", "forfeiture", "surplus"):
        return False
    return True

# ── Load existing ──────────────────────────────────────────────────────────
print("Loading existing data.json...")
with open(INPUT_EXIST, encoding="utf-8", errors="replace") as f:
    existing = json.load(f)
existing = existing if isinstance(existing, list) else existing.get("properties", [])
print(f"  Existing: {len(existing)} records")

# Add distressLevel + re-score, drop non-residential tax_delinquent and junk categories
existing_filtered = [r for r in existing if keep_existing(r)]
existing = [add_distress(r) for r in existing_filtered]
# Drop tax_delinquent with no enrichment signals (score == 3 = bare address, no data)
existing = [r for r in existing if not (r.get("category") == "tax_delinquent" and r.get("score", 0) < 4)]
print(f"  After filter:  {len(existing)} records")

# Build dedup index from existing
seen = {}
for r in existing:
    k = dedup_key(r)
    if k:
        seen[k] = True

# ── Load new ───────────────────────────────────────────────────────────────
print("Loading new scraper data...")
with open(INPUT_NEW, encoding="utf-8", errors="replace") as f:
    new_raw = json.load(f)
new_raw = new_raw if isinstance(new_raw, list) else []
print(f"  New raw: {len(new_raw)} records")

# ── Normalize, filter, deduplicate ────────────────────────────────────────
added = 0
skipped_county = 0
skipped_dup = 0
skipped_empty = 0
skipped_nonresidential = 0

merged = list(existing)

for r in new_raw:
    norm, skip_reason = normalize_new(r)
    if norm is None:
        if skip_reason == "nonresidential":
            skipped_nonresidential += 1
        else:
            skipped_county += 1
        continue
    if not norm["address"] and not norm["parcelId"]:
        skipped_empty += 1
        continue
    k = dedup_key(norm)
    if k and k in seen:
        skipped_dup += 1
        continue
    if k:
        seen[k] = True
    merged.append(norm)
    added += 1

# ── Sort by score desc ─────────────────────────────────────────────────────
merged.sort(key=lambda x: x.get("score", 0), reverse=True)

print(f"\nResults:")
print(f"  Existing kept:        {len(existing)}")
print(f"  New added:            {added}")
print(f"  Skipped (county):     {skipped_county}")
print(f"  Skipped (non-resid):  {skipped_nonresidential}")
print(f"  Skipped (dup):        {skipped_dup}")
print(f"  Skipped (empty):      {skipped_empty}")
print(f"  TOTAL:                {len(merged)}")

# Category breakdown
cats = {}
for r in merged:
    c = r.get("category", "unknown")
    cats[c] = cats.get(c, 0) + 1
print("\nBy Category:")
for k, v in sorted(cats.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")

# County breakdown
counties = {}
for r in merged:
    c = r.get("county", "unknown")
    counties[c] = counties.get(c, 0) + 1
print("\nBy County:")
for k, v in sorted(counties.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")

# ── Equity Enrichment ─────────────────────────────────────────────────────
# For foreclosure records that have an opening bid but no assessed value,
# look up the assessed value from county GIS (spatialest for NC, York portal for SC).
# Uses 14426 Arlandes Drive as the reference example:
#   Assessed: $332,500 (spatialest) | Opening Bid: $147,300 (foreclosure notice)
#   Equity: $185,200 = 55.7%

FORECLOSURE_CATS_ENRICH = {"foreclosure", "tax_foreclosure", "hoa_foreclosure", "master_sale", "auction"}
SPATIALEST_COUNTIES = {"mecklenburg": "mecklenburg", "gaston": "gaston", "union": "union"}

def _polaris_address_lookup(page, parcel_id: str) -> str:
    """Use POLARIS (Mecklenburg) to get full situs address for a parcel ID.
    Returns 'HOUSE# STREET NAME, CITY, NC ZIP' or '' on failure."""
    try:
        # POLARIS search by PIN (parcel ID number)
        url = f"https://polaris3g.mecklenburgcountync.gov/?pin={parcel_id}"
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(3000)
        content = page.content()
        # Try to extract situs address from the page
        m = re.search(r'[Ss]itus[^<>]*?(\d+\s+[A-Z0-9 ]+(?:ST|DR|AVE|LN|RD|BLVD|CT|WAY|PL|CIR|TER|PKWY|HWY)[^<"\n]*)', content)
        if m:
            return m.group(1).strip().title()
        # Also try page URL after redirect
        current_url = page.url
        if "/situs/" in current_url:
            addr_part = current_url.split("/situs/")[-1]
            addr_part = urllib.parse.unquote(addr_part).replace("+", " ")
            return addr_part.title()
    except Exception:
        pass
    return ""

def _spatialest_lookup(page, query: str, county_slug: str) -> float:
    """Look up assessed total value from spatialest by address. Returns 0 on failure."""
    try:
        base = f"https://property.spatialest.com/nc/{county_slug}"
        page.goto(base, timeout=20000)
        page.wait_for_timeout(3000)
        # Use street address only (no city/state) — spatialest searches by address within county
        search_term = query.split('\n')[0].strip()
        search_term = re.sub(r',.*$', '', search_term).strip()  # strip ", City, NC ZIP"
        search_box = page.locator('input[type="search"], input[placeholder*="search" i]').first
        if search_box.count() == 0:
            search_box = page.locator("input[type='text']").first
        search_box.fill(search_term)
        page.wait_for_timeout(1000)
        search_box.press("Enter")
        page.wait_for_timeout(5000)
        # If only one result, spatialest auto-navigates to property page
        # If multiple results, click the first result row (not the nav logo)
        if "#/property/" not in page.url:
            result_link = page.locator("a[href*='#/property/']").first
            if result_link.count() > 0:
                result_link.click()
                page.wait_for_timeout(5000)
        # Extract "Total Appraised Value" from rendered page text
        # spatialest renders: line="Total Appraised Value", next line="$332,500"
        body_text = page.inner_text("body")
        lines = [l.strip() for l in body_text.split("\n") if l.strip()]
        for i, line in enumerate(lines):
            if "Total Appraised Value" in line or line == "Total Appraised Value":
                next_line = lines[i + 1] if i + 1 < len(lines) else ""
                m = re.search(r'\$?([\d,]+)', next_line)
                if m:
                    return float(m.group(1).replace(",", ""))
            # Fallback: "Total" followed by dollar amount on next line
            if line == "Total":
                next_line = lines[i + 1] if i + 1 < len(lines) else ""
                if "$" in next_line:
                    m = re.search(r'\$?([\d,]+)', next_line)
                    if m:
                        return float(m.group(1).replace(",", ""))
    except Exception as e:
        pass
    return 0

def _york_lookup(page, address: str) -> float:
    """Look up appraised value from York County SC tax portal. Returns 0 on failure."""
    try:
        page.goto("https://www.secured-server.biz/YorkCounty/HP/", wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(2000)
        # Strip direction and suffix for broader match
        addr_clean = re.sub(r'\b(N|S|E|W|NE|NW|SE|SW)\b\.?', '', address, flags=re.IGNORECASE)
        addr_clean = re.sub(r'\b(St|Ave|Dr|Rd|Ln|Blvd|Ct|Pl|Way|Cir|Ter|Pkwy|Hwy)\b\.?$', '', addr_clean.strip(), flags=re.IGNORECASE).strip()
        search = page.locator("#searchQuery, input[name='search'], input[type='text']").first
        search.fill(addr_clean)
        type_filter = page.locator("#typeFilter, select[name='type']").first
        if type_filter.count() > 0:
            type_filter.select_option("Property")
        page.keyboard.press("Enter")
        page.wait_for_timeout(3000)
        content = page.content()
        m = re.search(r'Appraised\s+Value[^$]*\$\s*([\d,]+)', content, re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", ""))
    except Exception as e:
        pass
    return 0

def enrich_equity(records: list) -> int:
    """Fill in taxValue + estimatedEquity for foreclosure records missing assessed value.
    Uses address or parcel ID for lookup. Returns count of records enriched."""
    to_enrich = [
        r for r in records
        if r.get("category") in FORECLOSURE_CATS_ENRICH
        and not r.get("taxValue")
        and (r.get("openingBid") or r.get("salePrice"))
        and (r.get("address") or r.get("parcelId"))
        # Skip foreclosure.com: multiline with no house number AND no parcel ID
        and not (
            "\n" in r.get("address", "")
            and not r.get("parcelId")
            and not re.match(r'^\d+\s', (r.get("address") or "").split("\n")[0])
        )
    ]
    if not to_enrich:
        return 0

    print(f"\nEnriching equity for {len(to_enrich)} foreclosure records...")
    enriched = 0

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = context.new_page()

            for r in to_enrich:
                address = r.get("address", "").strip()
                parcel_id = r.get("parcelId", "").strip()
                county = (r.get("county") or "").lower()
                assessed = 0

                first_line = address.split('\n')[0].strip() if address else ""
                has_house_num = bool(re.match(r'^\d+\s', first_line))

                # For Mecklenburg records without a house number, resolve full address via POLARIS
                if not has_house_num and parcel_id and county == "mecklenburg":
                    full_addr = _polaris_address_lookup(page, parcel_id)
                    if full_addr:
                        r["address"] = full_addr  # enrich the record with full address
                        address = full_addr
                        first_line = full_addr.split(',')[0]
                        has_house_num = True
                        print(f"  > resolved parcel {parcel_id} -> {full_addr}")

                search_query = address if has_house_num else (parcel_id or address)

                if county in SPATIALEST_COUNTIES and search_query:
                    assessed = _spatialest_lookup(page, search_query, SPATIALEST_COUNTIES[county])
                elif county == "york" and address:
                    assessed = _york_lookup(page, first_line or address)

                if assessed > 0:
                    bid = parse_money(r.get("openingBid") or r.get("salePrice") or 0)
                    r["taxValue"] = f"${assessed:,.0f}"
                    if bid > 0 and assessed > bid:
                        equity_pct = (assessed - bid) / assessed * 100
                        r["estimatedEquity"] = f"{equity_pct:.0f}%"
                    enriched += 1
                    label = first_line or parcel_id
                    print(f"  OK {label[:50]} | assessed=${assessed:,.0f} | equity={r.get('estimatedEquity','?')}")

            context.close()
            browser.close()
    except Exception as e:
        print(f"  Equity enrichment error: {e}")

    print(f"  Enriched: {enriched}/{len(to_enrich)}")
    return enriched

enrich_equity(merged)

# ── Save ───────────────────────────────────────────────────────────────────
print(f"\nSaving to {OUTPUT}...")
with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(merged, f, ensure_ascii=False, separators=(",", ":"))
print("Done.")
