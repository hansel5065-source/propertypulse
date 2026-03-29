# Google Street View Embed in PDF Reports

## What It Does
Embeds a Street View image of the property directly in the title search PDF report.
Gives instant visual context without opening a browser.

## Requirements
- Google Maps API key (Static Street View API)
- ~$0.007 per image — essentially free at low volume

## Implementation
- In `generate_pdf.py` / `reports/generate_analysis.py`: add Street View image section
- API call: `https://maps.googleapis.com/maps/api/streetview?size=600x300&location={address}&key={API_KEY}`
- Save image to temp file → embed in PDF via ReportLab `drawImage()`
- Coordinates already available from FEMA geocoder in title search output (`environmental.fema_coords`)

## Files to Modify
| File | Change |
|------|--------|
| `reports/generate_analysis.py` | Add Street View image block |
| `reports/generate_pdf.py` | Add Street View image block |
| `.env` or `server.js` | Add `GOOGLE_MAPS_API_KEY` env var |

## Notes
- Use coords from `result["environmental"]["fema_coords"]` when available (more accurate than address string)
- Fall back to address string geocoding if coords missing
- Wrap in try/except — skip image gracefully if API key missing or address not found
