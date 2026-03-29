# Upset Bid Period Tracker

## What It Does
After a foreclosure auction in NC/SC, the winning bid can be "upset" (outbid)
during a 10-day upset bid period. Tracks which properties are in this window
and flags them as active opportunities.

## Why It Matters
- NC: 10-day upset bid period after auction — anyone can outbid by 5% or $750
- SC: Similar process at Master in Equity
- This is a low-competition window — most investors don't watch for it
- Properties in upset bid = you can still get in with a higher bid

## Data Sources
- NC: Clerk of Court upset bid filings (county-specific)
- SC: Master in Equity sale roster + 30-day confirmation period
- Mecklenburg: https://www.mecklenburgcountync.gov/government/courts
- York SC: Master in Equity office (803) 684-8510

## Implementation
- Add `upsetBidActive` boolean + `upsetBidDeadline` date to lead records
- New scraper: `scrape_upset_bids.py` — check clerk of court filings
- Dashboard badge: "UPSET BID OPEN — X days left" on relevant cards
- Alert/highlight in red when deadline is within 3 days

## Files to Create/Modify
| File | Change |
|------|--------|
| `scrape_upset_bids.py` | New — scrape clerk of court upset bid filings |
| `pipeline.py` | Add `upsetBidActive` field + urgency boost in scoring |
| `index.html` | Add upset bid badge to card UI |
| `server.js` | Optional: daily cron to check + notify |
