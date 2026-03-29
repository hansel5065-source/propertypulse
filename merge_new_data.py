"""
merge_new_data.py — Merge standalone scraper output into data.json
Normalizes schema, filters to Charlotte metro, deduplicates, scores.
"""
import json, re, os
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
    """Convert standalone scraper schema → data.json schema."""
    category = (r.get("listingType") or "other").lower()
    county = normalize_county(r.get("county"))
    if county.lower() not in TARGET_COUNTIES:
        return None

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
        "propertyType": r.get("propertyType") or "",
        "notes": r.get("description") or "",
        "lienTypes": [],
        "estimatedEquity": "",
        "loanBalance": "",
        "yearsOwned": "",
        "purchaseDate": "",
        "status": r.get("status") or "Active",
        "analyzed": False,
        "pdfSlug": slug(full_addr),
        "scrapedAt": r.get("scrapedAt") or datetime.utcnow().isoformat() + "Z",
    }
    rec["score"] = score_record(rec)
    return rec

def add_distress(r):
    """Add/fix distressLevel on existing records."""
    cat = (r.get("category") or "other").lower()
    if not r.get("distressLevel"):
        r["distressLevel"] = DISTRESS_MAP.get(cat, "other")
    return r

# ── Load existing ──────────────────────────────────────────────────────────
print("Loading existing data.json...")
with open(INPUT_EXIST, encoding="utf-8", errors="replace") as f:
    existing = json.load(f)
existing = existing if isinstance(existing, list) else existing.get("properties", [])
print(f"  Existing: {len(existing)} records")

# Add distressLevel to existing records
existing = [add_distress(r) for r in existing]

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

merged = list(existing)

for r in new_raw:
    norm = normalize_new(r)
    if norm is None:
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
print(f"  Existing kept:    {len(existing)}")
print(f"  New added:        {added}")
print(f"  Skipped (county): {skipped_county}")
print(f"  Skipped (dup):    {skipped_dup}")
print(f"  Skipped (empty):  {skipped_empty}")
print(f"  TOTAL:            {len(merged)}")

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

# ── Save ───────────────────────────────────────────────────────────────────
print(f"\nSaving to {OUTPUT}...")
with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(merged, f, ensure_ascii=False, separators=(",", ":"))
print("Done.")
