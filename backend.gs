// ═══════════════════════════════════════════════════════════════
// Property Leads — Google Apps Script Backend
// ═══════════════════════════════════════════════════════════════
//
// SETUP (one-time, ~3 minutes):
//
//  1. Open your Google Sheet that n8n writes to
//  2. Click Extensions → Apps Script
//  3. Delete the default code, paste this entire file
//  4. Save (Ctrl+S) — name it anything you like
//  5. Click Deploy → New Deployment
//       Type:            Web app
//       Execute as:      Me
//       Who has access:  Anyone
//  6. Click Deploy → copy the URL that appears
//  7. Open app/index.html and paste that URL into:
//       CONFIG.scriptUrl = 'https://script.google.com/macros/s/…/exec'
//  8. Open the app — it will load your live data!
//
// RE-DEPLOY after edits:
//  Deploy → Manage Deployments → Edit → Version: New → Deploy
//
// ═══════════════════════════════════════════════════════════════

const SHEET_NAME    = 'property_leads';
const STATUS_COL    = 'user_status';   // Column the app writes status back to
const SHEET_ID      = '16pAAfN2wPmjwBe5IQlkcoKST8EtSKNPjSNHY7q_MRJU';

// ─── ROUTER ───────────────────────────────────────────────────
function doGet(e) {
  const action = (e.parameter && e.parameter.action) || 'getLeads';

  try {
    if (action === 'updateStatus') {
      return updateStatus(e.parameter.record_uid, e.parameter.status);
    }
    return getLeads();
  } catch (err) {
    return json({ error: err.message });
  }
}

// ─── GET LEADS ────────────────────────────────────────────────
function getLeads() {
  const sheet = getSheet();
  const data  = sheet.getDataRange().getValues();
  if (data.length < 2) return json([]);

  // Ensure user_status column exists
  const headers = ensureStatusCol(sheet, data[0].map(String));

  const leads = data.slice(1)
    .map(row => {
      const obj = {};
      headers.forEach((h, i) => { obj[h] = row[i] != null ? String(row[i]) : ''; });
      return obj;
    })
    .filter(l => l.record_uid); // skip blank rows

  return json(leads);
}

// ─── UPDATE STATUS ────────────────────────────────────────────
function updateStatus(recordUid, status) {
  if (!recordUid || !status) return json({ error: 'Missing params' });

  const sheet   = getSheet();
  const data    = sheet.getDataRange().getValues();
  const headers = ensureStatusCol(sheet, data[0].map(String));

  const uidIdx    = headers.indexOf('record_uid');
  const statusIdx = headers.indexOf(STATUS_COL);

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][uidIdx]) === String(recordUid)) {
      sheet.getRange(i + 1, statusIdx + 1).setValue(status);
      SpreadsheetApp.flush();
      return json({ success: true, row: i + 1 });
    }
  }

  return json({ error: 'Record not found: ' + recordUid });
}

// ─── HELPERS ──────────────────────────────────────────────────
function getSheet() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error('Sheet "' + SHEET_NAME + '" not found');
  return sheet;
}

function ensureStatusCol(sheet, headers) {
  if (!headers.includes(STATUS_COL)) {
    const col = headers.length + 1;
    sheet.getRange(1, col).setValue(STATUS_COL);
    headers.push(STATUS_COL);
    SpreadsheetApp.flush();
  }
  return headers;
}

function json(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}
