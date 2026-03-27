/**
 * PropertyPulse Backend
 * Serves static files + API for title search analysis and PDF delivery.
 *
 * Endpoints:
 *   GET  /                        → index.html
 *   GET  /api/leads               → data.json (all leads)
 *   GET  /api/leads/top?n=10      → top N leads by score
 *   POST /api/analyze             → { address, county } → starts title search job
 *   GET  /api/status/:slug        → job status (queued|running|done|error)
 *   GET  /api/pdf/:slug           → stream the analysis PDF
 *   POST /api/pipeline            → re-run pipeline.py to refresh data.json
 *   POST /api/geocode             → batch geocode via Census API (bypasses CORS)
 *   GET  /api/geocache            → return persisted geocache
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const url = require('url');

const PORT = process.env.PORT || 8080;
const APP_DIR = __dirname;
const TITLE_SEARCH_DIR = path.join(APP_DIR, '..', 'title-search');
const REPORTS_DIR = path.join(TITLE_SEARCH_DIR, 'reports');
const DATA_JSON = path.join(APP_DIR, 'data.json');
const GEOCACHE_JSON = path.join(APP_DIR, 'geocache.json');

// ── Geocache (persisted to disk) ──────────────────────────────────────────────
let geocache = {};
try {
  if (fs.existsSync(GEOCACHE_JSON)) {
    geocache = JSON.parse(fs.readFileSync(GEOCACHE_JSON, 'utf8'));
    console.log(`Loaded geocache: ${Object.keys(geocache).length} addresses`);
  }
} catch (e) {
  console.warn('Could not load geocache.json:', e.message);
}

function saveGeoCache() {
  try { fs.writeFileSync(GEOCACHE_JSON, JSON.stringify(geocache)); } catch (e) { /* ignore */ }
}

// ── Job tracker ───────────────────────────────────────────────────────────────
// slug → { status, address, county, startedAt, pdfPath, error, log }
const jobs = {};

// ── MIME types ────────────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.pdf':  'application/pdf',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.ico':  'image/x-icon',
};

// ── Helpers ───────────────────────────────────────────────────────────────────
function slugify(address) {
  return address.toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 60).replace(/^_|_$/g, '');
}

function jsonResponse(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => (data += chunk));
    req.on('end', () => {
      try { resolve(JSON.parse(data)); }
      catch (e) { resolve({}); }
    });
    req.on('error', reject);
  });
}

// ── Find PDF for a slug ───────────────────────────────────────────────────────
function findPdf(slug) {
  if (!fs.existsSync(REPORTS_DIR)) return null;
  const files = fs.readdirSync(REPORTS_DIR);
  // Exact match first
  const exact = files.find(f => f === `${slug}_analysis.pdf`);
  if (exact) return path.join(REPORTS_DIR, exact);
  // Partial match — slug is contained in filename
  const partial = files.find(f => f.includes(slug) && f.endsWith('.pdf'));
  if (partial) return path.join(REPORTS_DIR, partial);
  return null;
}

// ── Run title search analysis ─────────────────────────────────────────────────
function runAnalysis(slug, address) {
  jobs[slug] = {
    status: 'running',
    address,
    startedAt: new Date().toISOString(),
    pdfPath: null,
    error: null,
    log: [],
  };

  const titleSearchScript = path.join(TITLE_SEARCH_DIR, 'title_search.py');
  const proc = spawn('python', [titleSearchScript, address, '--analysis'], {
    cwd: TITLE_SEARCH_DIR,
    env: { ...process.env },
  });

  // Kill after 3 minutes to prevent infinite hangs (e.g. POLARIS not finding address)
  const killTimer = setTimeout(() => {
    if (jobs[slug].status === 'running') {
      proc.kill('SIGTERM');
      jobs[slug].status = 'error';
      jobs[slug].error = 'Timed out after 3 minutes — address may not be in county GIS system';
      jobs[slug].log.push('ERR: Process killed after 3-minute timeout');
      console.error(`[${slug}] Killed after timeout`);
    }
  }, 3 * 60 * 1000);

  proc.stdout.on('data', d => {
    const line = d.toString().trim();
    jobs[slug].log.push(line);
    console.log(`[${slug}] ${line}`);
  });

  proc.stderr.on('data', d => {
    const line = d.toString().trim();
    jobs[slug].log.push(`ERR: ${line}`);
    console.error(`[${slug}] ${line}`);
  });

  proc.on('close', code => {
    clearTimeout(killTimer);
    if (code === 0) {
      const pdfPath = findPdf(slug);
      jobs[slug].status = 'done';
      jobs[slug].pdfPath = pdfPath;
      jobs[slug].completedAt = new Date().toISOString();
      console.log(`[${slug}] Analysis done. PDF: ${pdfPath}`);
    } else if (jobs[slug].status !== 'error') {
      jobs[slug].status = 'error';
      // Pull most informative error line from log
      const errLine = jobs[slug].log.slice().reverse().find(l => l.includes('ERR:') || l.includes('Error') || l.includes('error')) || `Process exited with code ${code}`;
      jobs[slug].error = errLine.replace('ERR: ', '');
      console.error(`[${slug}] Analysis failed (code ${code}): ${jobs[slug].error}`);
    }
  });

  proc.on('error', err => {
    clearTimeout(killTimer);
    jobs[slug].status = 'error';
    jobs[slug].error = err.message;
  });
}

// ── Request handler ───────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const method = req.method.toUpperCase();

  // CORS preflight
  if (method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  // ── GET /api/leads ──────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/leads') {
    try {
      const raw = fs.readFileSync(DATA_JSON, 'utf8');
      const leads = JSON.parse(raw);
      // Attach analyzed status from jobs
      const enriched = leads.map(l => ({
        ...l,
        analyzed: jobs[l.pdfSlug]?.status === 'done' || !!findPdf(l.pdfSlug),
        jobStatus: jobs[l.pdfSlug]?.status || (findPdf(l.pdfSlug) ? 'done' : null),
      }));
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify(enriched));
    } catch (e) {
      jsonResponse(res, 500, { error: e.message });
    }
    return;
  }

  // ── GET /api/leads/top ──────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/leads/top') {
    try {
      const n = parseInt(parsed.query.n) || 10;
      const raw = fs.readFileSync(DATA_JSON, 'utf8');
      const leads = JSON.parse(raw);
      const top = leads
        .filter(l => l.score >= 7)
        .slice(0, n)
        .map(l => ({
          ...l,
          analyzed: !!findPdf(l.pdfSlug),
          jobStatus: jobs[l.pdfSlug]?.status || (findPdf(l.pdfSlug) ? 'done' : null),
        }));
      jsonResponse(res, 200, top);
    } catch (e) {
      jsonResponse(res, 500, { error: e.message });
    }
    return;
  }

  // ── POST /api/analyze ───────────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/analyze') {
    const body = await readBody(req);
    const { address } = body;
    if (!address) return jsonResponse(res, 400, { error: 'address required' });

    const slug = slugify(address);

    // If already running, return current status
    if (jobs[slug] && jobs[slug].status === 'running') {
      return jsonResponse(res, 200, { slug, status: 'running', message: 'Analysis already in progress' });
    }

    // If already done and PDF exists, return immediately
    const existingPdf = findPdf(slug);
    if (existingPdf && (!jobs[slug] || jobs[slug].status !== 'error')) {
      return jsonResponse(res, 200, { slug, status: 'done', pdfReady: true });
    }

    // Start analysis
    runAnalysis(slug, address);
    jsonResponse(res, 202, { slug, status: 'running', message: 'Analysis started. Poll /api/status/' + slug });
    return;
  }

  // ── GET /api/status/:slug ───────────────────────────────────────────────────
  if (method === 'GET' && pathname.startsWith('/api/status/')) {
    const slug = pathname.replace('/api/status/', '');
    const job = jobs[slug];
    const pdfPath = findPdf(slug);

    if (!job && !pdfPath) {
      return jsonResponse(res, 200, { slug, status: 'not_started' });
    }

    if (pdfPath && (!job || job.status !== 'running')) {
      return jsonResponse(res, 200, {
        slug,
        status: 'done',
        pdfReady: true,
        address: job?.address,
        completedAt: job?.completedAt,
      });
    }

    return jsonResponse(res, 200, {
      slug,
      status: job.status,
      pdfReady: job.status === 'done' && !!pdfPath,
      error: job.error,
      startedAt: job.startedAt,
      log: job.log.slice(-5),
    });
  }

  // ── GET /api/pdf/:slug ──────────────────────────────────────────────────────
  if (method === 'GET' && pathname.startsWith('/api/pdf/')) {
    const slug = decodeURIComponent(pathname.replace('/api/pdf/', ''));
    const pdfPath = findPdf(slug);

    if (!pdfPath) {
      return jsonResponse(res, 404, { error: 'PDF not found. Run analysis first.' });
    }

    const stat = fs.statSync(pdfPath);
    res.writeHead(200, {
      'Content-Type': 'application/pdf',
      'Content-Length': stat.size,
      'Content-Disposition': `inline; filename="${path.basename(pdfPath)}"`,
      'Access-Control-Allow-Origin': '*',
    });
    fs.createReadStream(pdfPath).pipe(res);
    return;
  }

  // ── POST /api/pipeline ──────────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/pipeline') {
    const pipelineScript = path.join(APP_DIR, 'pipeline.py');
    const proc = spawn('python', [pipelineScript], { cwd: APP_DIR });
    let output = '';
    proc.stdout.on('data', d => (output += d.toString()));
    proc.stderr.on('data', d => (output += d.toString()));
    proc.on('close', code => {
      jsonResponse(res, code === 0 ? 200 : 500, {
        success: code === 0,
        output: output.trim(),
      });
    });
    return;
  }

  // ── GET /api/geocache ────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/geocache') {
    return jsonResponse(res, 200, geocache);
  }

  // ── POST /api/geocode ────────────────────────────────────────────────────────
  // Body: { items: [{id, street, city, state, zip}] }
  // Returns: { results: {id: {lat, lng}} }
  if (method === 'POST' && pathname === '/api/geocode') {
    const body = await readBody(req);
    const items = body.items || [];
    if (!items.length) return jsonResponse(res, 200, { results: {} });

    // Filter out already cached
    const uncached = items.filter(it => !geocache[it.id]);
    const results = {};

    // Return cached hits immediately to avoid re-fetching
    items.forEach(it => { if (geocache[it.id]) results[it.id] = geocache[it.id]; });

    if (uncached.length === 0) {
      return jsonResponse(res, 200, { results });
    }

    // Build Census batch CSV (max 500 per call — we chunk server-side too)
    const CHUNK = 500;

    for (let i = 0; i < uncached.length; i += CHUNK) {
      const chunk = uncached.slice(i, i + CHUNK);
      // Use integer index as ID (Census echoes it back) — avoid commas in ID field
      const idxToAddr = {};
      const csvLines = chunk.map((it, idx) => {
        idxToAddr[idx] = it.id; // map index back to address string
        const street = (it.street || '').replace(/[,"]/g, ' ');
        const city   = (it.city   || '').replace(/[,"]/g, ' ');
        const state  = (it.state  || '').replace(/[,"]/g, ' ');
        const zip    = (it.zip    || '').replace(/[,"]/g, ' ');
        return `${idx},"${street}","${city}","${state}","${zip}"`;
      });
      const csvBody = csvLines.join('\n');

      // Multipart form-data boundary
      const boundary = '----CensusBatch' + Date.now();
      const CRLF = '\r\n';
      const parts = [
        `--${boundary}${CRLF}Content-Disposition: form-data; name="addressFile"; filename="addr.csv"${CRLF}Content-Type: text/csv${CRLF}${CRLF}${csvBody}${CRLF}`,
        `--${boundary}${CRLF}Content-Disposition: form-data; name="benchmark"${CRLF}${CRLF}Public_AR_Current${CRLF}`,
        `--${boundary}--${CRLF}`,
      ];
      const formBody = parts.join('');
      const formBuf  = Buffer.from(formBody, 'utf8');

      try {
        const censusRes = await new Promise((resolve, reject) => {
          const opts = {
            hostname: 'geocoding.geo.census.gov',
            path: '/geocoder/locations/addressbatch',
            method: 'POST',
            headers: {
              'Content-Type': `multipart/form-data; boundary=${boundary}`,
              'Content-Length': formBuf.length,
              'User-Agent': 'PropertyPulse/1.0',
            },
            timeout: 30000,
          };
          const req2 = https.request(opts, resolve);
          req2.on('error', reject);
          req2.on('timeout', () => { req2.destroy(); reject(new Error('Census timeout')); });
          req2.write(formBuf);
          req2.end();
        });

        let raw = '';
        await new Promise(resolve => {
          censusRes.on('data', d => (raw += d.toString()));
          censusRes.on('end', resolve);
        });

        // Parse quoted CSV response — split on `","` to handle commas inside fields
        // Format: id,"inputStreet","city","state","zip","Match/No_Match","Exact","matchedAddr","lon,lat","tigerID","side"
        console.log(`[geocode] Census response ${raw.length} bytes`);
        // Census CSV: "id","inputAddr","Match/No_Match","matchType","matchedAddr","lon,lat","tigerID","side"
        raw.split('\n').forEach(line => {
          line = line.trim();
          if (!line) return;
          const cols = line.split('","').map(s => s.replace(/^"|"$/g, ''));
          if (cols.length < 7) return;
          const idx      = parseInt(cols[0]);
          const match    = (cols[2] || '').trim().toLowerCase(); // "Match", "No_Match", "Tie"
          const coordStr = (cols[5] || '').trim(); // "lon,lat" — comma separates lon from lat
          const commaPos = coordStr.indexOf(',');
          const lng      = commaPos !== -1 ? parseFloat(coordStr.slice(0, commaPos)) : NaN;
          const lat      = commaPos !== -1 ? parseFloat(coordStr.slice(commaPos + 1))  : NaN;
          const addr     = idxToAddr[idx];
          if ((match === 'match' || match === 'tie') && !isNaN(lat) && !isNaN(lng) && addr) {
            results[addr] = { lat, lng };
            geocache[addr] = { lat, lng };
          }
        });
      } catch (e) {
        console.warn('Census geocode error:', e.message);
      }
    }

    saveGeoCache();
    return jsonResponse(res, 200, { results });
  }

  // ── Static files ────────────────────────────────────────────────────────────
  let filePath = path.join(APP_DIR, pathname === '/' ? 'index.html' : pathname);
  // Security: stay inside APP_DIR
  if (!filePath.startsWith(APP_DIR)) {
    res.writeHead(403); return res.end('Forbidden');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(filePath);
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'text/plain',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`PropertyPulse running on http://localhost:${PORT}`);
  console.log(`API:   GET  /api/leads`);
  console.log(`       GET  /api/leads/top?n=10`);
  console.log(`       POST /api/analyze   { address }`);
  console.log(`       GET  /api/status/:slug`);
  console.log(`       GET  /api/pdf/:slug`);
  console.log(`       POST /api/pipeline`);
  console.log(`       POST /api/geocode   { items: [{id,street,city,state,zip}] }`);
  console.log(`       GET  /api/geocache`);
});
