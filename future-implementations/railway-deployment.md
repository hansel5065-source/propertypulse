# Railway Backend Deployment

## What It Does
Deploys `server.js` to Railway so the Analyze button works on the public
GitHub Pages site — not just locally.

## Current Limitation
- GitHub Pages hosts the frontend (static HTML) for free ✓
- But Analyze calls `POST /api/analyze` which needs the Node.js backend
- That backend only runs locally right now
- Result: Analyze button is disabled/broken on the public GitHub Pages URL

## Why Railway
- $5/month hobby plan covers a small Node.js app easily
- Auto-deploys from GitHub on push
- Environment variables set in Railway dashboard

## Alternative: Mac Mini + Cloudflare Tunnel
- $0/month if Mac Mini is always on
- See `public-search-portal.md` for full setup
- Recommended over Railway for cost savings

## Railway Setup Steps
1. Create Railway account at railway.app
2. Connect GitHub repo
3. Set environment variables:
   - `PORT=8080`
   - `TITLE_SEARCH_PATH=../title-search/title_search.py`
   - Any API keys needed
4. Update `index.html` API base URL to point to Railway URL instead of localhost
5. Re-enable Analyze button on GitHub Pages

## Files to Modify
| File | Change |
|------|--------|
| `index.html` | Change `API_BASE` from `localhost:8080` to Railway URL |
| `server.js` | Ensure `PORT` reads from `process.env.PORT` (already done) |
| `Procfile` | Add: `web: node server.js` |

## Notes
- Python + Playwright deps need to be installed on Railway — may need a `nixpacks.toml`
- PropWire scraper WON'T work on Railway (needs real Chrome session) — that stays local
- Title search scraper should work fine (uses headless Playwright)
