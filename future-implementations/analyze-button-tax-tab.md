# Analyze Button on Tax Delinquent Tab Cards

## What It Does
Adds the same Analyze + View PDF buttons (currently on Leads tab and Dashboard)
to every card on the Tax Delinquent tab.

## Current State
- Leads tab: has Analyze + View PDF buttons ✓
- Dashboard Top Deals: has Analyze buttons ✓
- Tax Delinquent tab: cards show risk score + tags but NO Analyze button ✗

## Implementation
In `index.html`, find the Tax Delinquent card render function and add the same
button HTML pattern used in the Leads tab:

```html
<button onclick="analyzeProperty('${slug}')" class="btn-analyze">Analyze</button>
<button onclick="viewPdf('${slug}')" class="btn-pdf" id="pdf-${slug}" style="display:none">View PDF</button>
```

The `analyzeProperty()` and `viewPdf()` JS functions already exist — just need
the buttons wired up in the Tax tab card template.

## Files to Modify
| File | Change |
|------|--------|
| `index.html` | Add Analyze/PDF buttons to Tax tab card render |

## Estimated Effort
~30 minutes — copy/paste button pattern from Leads tab into Tax tab render loop.
