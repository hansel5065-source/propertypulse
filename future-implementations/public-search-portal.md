# Public Search Portal — Simple Address → PDF

## What It Does
Single-page web interface for sharing deep title search with others.
No dashboard access. No configuration. Just: address in → PDF out.

## User Flow
1. Person visits your URL (e.g. `https://abc.trycloudflare.com/search`)
2. Types address: `618 Brickdust Court, Fort Mill, SC`
3. Clicks Search
4. Spinner shows (~2 min while title search runs)
5. PDF downloads automatically

## What to Build
- New `/search` route in `server.js` — serves a minimal HTML page
- Address input + Search button + progress spinner
- Calls existing `POST /api/analyze` behind the scenes
- On completion, auto-triggers PDF download via `GET /api/pdf/:slug`
- Optional: simple password gate so only invited users can access

## Infrastructure
- Host on Mac Mini (always on) + Cloudflare Tunnel (free)
- Main dashboard at `/` stays private (your use only)
- `/search` is the shareable link

## Why Mac Mini + Cloudflare Tunnel
- Cost: $0/month ongoing
- Mac Mini idle power: ~10W
- Cloudflare Tunnel: free permanent public URL
- No Railway/Heroku needed
- PropWire scraper can also run on schedule automatically

## Files to Create/Modify
| File | Change |
|------|--------|
| `server.js` | Add `GET /search` route serving new HTML page |
| `public/search.html` | New minimal search UI (~50 lines) |
| `server.js` | Optional: add `SEARCH_PASSWORD` env var check |

## Notes
- Existing `/api/analyze` and `/api/pdf/:slug` endpoints already do all the work
- This is purely a UI wrapper — no backend logic changes needed
- Estimated build time: 1-2 hours
