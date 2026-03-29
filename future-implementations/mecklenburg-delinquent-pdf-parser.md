# Mecklenburg Delinquent Tax PDF Parser

## What It Does
Mecklenburg County publishes its tax delinquent list as a PDF.
This parser extracts all delinquent properties into structured records
that feed directly into the pipeline.

## Current State
- `pdfplumber` is already installed ✓
- PDF download from county site works ✓
- Parser not yet written ✗

## PDF Source
Mecklenburg County delinquent tax list — published periodically at:
https://www.mecknc.gov/TaxCollections/Pages/DelinquentTaxList.aspx

## PDF Format
Typically a table with columns:
- Parcel ID
- Owner Name
- Property Address
- Amount Owed
- Tax Year(s)

## Implementation
```python
import pdfplumber

def parse_meck_delinquent_pdf(pdf_path: str) -> list:
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table[1:]:  # skip header
                if not row or not row[0]:
                    continue
                records.append({
                    "parcelId": row[0],
                    "ownerName": row[1],
                    "address": row[2],
                    "delinquentAmount": row[3],
                    "taxYears": row[4],
                    "county": "Mecklenburg",
                    "state": "NC",
                    "listingType": "tax_delinquent",
                    "source": "meck_delinquent_pdf",
                })
    return records
```

## Files to Create/Modify
| File | Change |
|------|--------|
| `scrapers/mecklenburg.py` | Add `parse_delinquent_pdf()` function |
| `pipeline.py` | Add Mecklenburg PDF as optional merge input |

## Notes
- PDF format may change — check column order after each new download
- Cross-reference with Apify foreclosure data to find overlap (delinquent + pre-foreclosure = hot lead)
