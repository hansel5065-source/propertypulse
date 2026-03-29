# Fix Gaston / Union devnetwedge Scraper Timeouts

## Problem
Gaston County and Union County both use the devnetwedge portal for tax/property data.
The scraper times out inconsistently — sometimes works, sometimes hangs for 30+ seconds
then returns no results.

## Root Cause (suspected)
- devnetwedge portals are slow to load search results (~8-15s response time)
- Current timeout may be too short for peak load times
- The portal may also rate-limit repeated requests

## Affected Files
- `scrapers/gaston.py`
- `scrapers/union.py`

## Fix Approach
1. Increase `asyncio.sleep()` waits after search submission (try 10s instead of 5s)
2. Add retry logic — if no results found, wait 5s and try again (max 3 attempts)
3. Add explicit `waitForSelector` on the results table before parsing
4. Consider randomized delay between requests to avoid rate limiting

## Example Fix Pattern
```python
# Instead of fixed sleep:
await asyncio.sleep(5)

# Use wait_for_selector with longer timeout:
try:
    await page.wait_for_selector("#resultsTable, .search-results", timeout=20000)
except:
    # Retry once
    await page.press("#searchQuery", "Enter")
    await page.wait_for_selector("#resultsTable, .search-results", timeout=20000)
```

## Testing
Run against known addresses in each county and verify consistent results:
- Gaston: `1234 any known Gaston address`
- Union: `any known Union County address`
