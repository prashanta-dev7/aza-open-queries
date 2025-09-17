// api/backend.js
// Vercel — Node runtime so we can read /data/*.csv
export const config = { runtime: 'nodejs' };

import fs from 'fs';
import path from 'path';

/* ---------------- CSV parsing (robust) ---------------- */

function parseCsvRobust(text) {
  const rows = [];
  let i = 0, field = '', row = [], inQ = false;
  while (i < text.length) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      } else { field += c; i++; continue; }
    } else {
      if (c === '"') { inQ = true; i++; continue; }
      if (c === ',') { row.push(field); field = ''; i++; continue; }
      if (c === '\r') { i++; continue; }
      if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
      field += c; i++;
    }
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

function loadMappingFromCsvText(csvText) {
  const rows = parseCsvRobust(csvText);
  if (!rows.length) return new Map();
  const header = rows[0].map(h => (h || '').toLowerCase().trim());
  const idx = {
    pid: header.indexOf('pid') !== -1 ? header.indexOf('pid') : header.indexOf('product_id'),
    designer: header.indexOf('designer_name') !== -1 ? header.indexOf('designer_name') : header.indexOf('designer'),
    merch: header.indexOf('merch_name') !== -1 ? header.indexOf('merch_name') :
           header.indexOf('merch') !== -1 ? header.indexOf('merch') : header.indexOf('merchandiser'),
  };
  const map = new Map();
  for (let r = 1; r < rows.length; r++) {
    const cols = rows[r];
    const rawPid = (idx.pid >= 0 ? (cols[idx.pid] || '') : '').toString().trim();
    const pid = (rawPid.match(/\b(\d{6})\b/) || [])[1] || ''; // exact 6-digit
    if (!pid) continue;
    const designer = idx.designer >= 0 ? (cols[idx.designer] || '').toString().trim() : '';
    const merch = idx.merch >= 0 ? (cols[idx.merch] || '').toString().trim() : '';
    if (!map.has(pid)) map.set(pid, { designer, merch });
  }
  return map;
}

/* ---------------- Load all /data/dump_*.csv ---------------- */

let PID_MAP = null;
let MAPPING_READY = null;

async function loadMappingOnce() {
  if (MAPPING_READY) return MAPPING_READY;
  MAPPING_READY = new Promise((resolve) => {
    try {
      const dataDir = path.join(process.cwd(), 'data');
      const files = fs.existsSync(dataDir)
        ? fs.readdirSync(dataDir).filter(f => /^dump_.*\.csv$/i.test(f))
        : [];
      const map = new Map();
      let fileCount = 0;
      for (const f of files) {
        const full = path.join(dataDir, f);
        const txt = fs.readFileSync(full, 'utf8');
        const m = loadMappingFromCsvText(txt);
        fileCount++;
        for (const [pid, v] of m.entries()) {
          if (!map.has(pid)) map.set(pid, v);
        }
      }
      PID_MAP = map;
      console.log(`[mapping] files=${fileCount}, uniquePIDs=${map.size}`);
      resolve();
    } catch (e) {
      console.error('Mapping load error:', e);
      PID_MAP = new Map();
      resolve();
    }
  });
  return MAPPING_READY;
}

/* ---------------- WhatsApp parsing ---------------- */

// Android: "17/10/2024, 9:40 pm - Name: Text"
// iOS:     "[17/10/2024, 9:40 pm] Name: Text"
// 24h:     "17/10/2024, 21:40 - Name: Text"
const RE_ANDROID = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4}),\s+(\d{1,2}):(\d{2})\s*(am|pm)?\s*-\s([^:]+):\s([\s\S]*)$/i;
const RE_IOS     = /^\[(\d{1,2})\/(\d{1,2})\/(\d{2,4}),\s+(\d{1,2}):(\d{2})\s*(am|pm)?\]\s([^:]+):\s([\s\S]*)$/i;

function parseIstDate(d, m, y, hh, mm, ampm) {
  const year = (String(y).length === 2) ? (parseInt(y,10) + 2000) : parseInt(y,10);
  let hour = parseInt(hh, 10);
  const minute = parseInt(mm, 10);
  if (ampm) {
    const ap = ampm.toLowerCase();
    if (ap === 'pm' && hour < 12) hour += 12;
    if (ap === 'am' && hour === 12) hour = 0;
  }
  const iso = `${year.toString().padStart(4,'0')}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}T${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}:00.000+05:30`;
  return new Date(iso);
}

function fmtIst(dt) {
  if (!dt) return '';
  const pad = (n)=> String(n).padStart(2,'0');
  const y = dt.getFullYear();
  const m = pad(dt.getMonth()+1);
  const d = pad(dt.getDate());
  const hh = pad(dt.getHours());
  const mm = pad(dt.getMinutes());
  return `${y}-${m}-${d} ${hh}:${mm} IST`;
}

function extractPids(text) {
  const out = new Set();
  const re = /\b(?:pid\s*[:\-]?\s*)?(\d{6})\b/gi;
  let m;
  while ((m = re.exec(text)) !== null) out.add(m[1]);
  return Array.from(out);
}

/* ---------------- Core per SLA rule (next mention must be different sender) ---------------- */

function buildPidTimelines(msgs) {
  const map = new Map(); // pid -> [ msg ]
  for (const m of msgs) {
    const pids = extractPids(m.text);
    if (!pids.length) continue;
    for (const pid of pids) {
      if (!map.has(pid)) map.set(pid, []);
      map.get(pid).push(m);
    }
  }
  for (const arr of map.values()) arr.sort((a,b) => a.ts - b.ts);
  return map;
}

function summarizeRows(pidMap, timelines, now, slaMinutes) {
  const rows = [];
  const slaMs = Math.max(1, parseInt(slaMinutes || 60, 10)) * 60 * 1000;

  for (const [pid, arr] of timelines.entries()) {
    if (!arr.length) continue;

    const meta = pidMap.get(pid) || { designer: '', merch: '' };

    // First mention after cutoff
    const first = arr[0];
    // Latest mention overall (for preview only)
    const latest = arr[arr.length - 1];
    // Very next mention after first (if any)
    const next = arr.length >= 2 ? arr[1] : null;

    let include = false;
    let status = 'Open';

    if (next) {
      const gap = next.ts - first.ts;

      // Only count as a "reply" if the next mention is from a DIFFERENT sender
      if (next.sender !== first.sender) {
        if (gap > slaMs) {
          include = true;                 // reply arrived after SLA
          status = 'Open — Breached';
        } else {
          include = false;                // reply within SLA -> exclude
          status = 'Closed';
        }
      } else {
        // Same sender again: still open; include only if age > SLA
        const age = now - first.ts;
        include = age > slaMs;
        status = include ? 'Open — Breached' : 'Open';
      }
    } else {
      // No next mention at all; include only if age > SLA
      const age = now - first.ts;
      include = age > slaMs;
      status = include ? 'Open — Breached' : 'Open';
    }

    if (!include) continue;

    rows.push({
      pid,
      designer: meta.designer || '',
      assigned_merch: meta.merch || '',
      status,
      first_cs_preview: (first.text || '').slice(0,160),
      first_cs_ts_ist: fmtIst(first.ts),
      latest_cs_preview: (latest.text || '').slice(0,160),
      cs_ts_ist: fmtIst(latest.ts)
    });
  }

  // Sort: breached first, then by newest first-mention
  rows.sort((a,b) => {
    const rank = s => s.startsWith('Open — Breached') ? 0 : 1;
    const r = rank(a.status) - rank(b.status);
    if (r !== 0) return r;
    return (b.first_cs_ts_ist || '').localeCompare(a.first_cs_ts_ist || '');
  });

  return rows;
}

/* ---------------- Handler ------------------------------- */

export default async function handler(req, res) {
  if (req.method !== 'POST') { res.status(405).send('Method Not Allowed'); return; }
  try {
    await loadMappingOnce();

    const { chatText, cutoffDate, slaMinutes } = req.body || {};
    if (!chatText || !cutoffDate) { res.status(400).send('chatText and cutoffDate are required'); return; }

    // Only analyze messages strictly AFTER the cutoff date (00:00 IST)
    const cutoff = new Date(`${cutoffDate}T00:00:00.000+05:30`);
    if (isNaN(cutoff)) { res.status(400).send('Invalid cutoffDate'); return; }

    const sla = Math.max(1, parseInt(slaMinutes ?? 60, 10));
    const now = new Date();

    // Parse WhatsApp lines (Android & iOS)
    const msgs = [];
    for (const raw of chatText.split(/\r?\n/)) {
      let m = RE_ANDROID.exec(raw);
      if (m) {
        const [, d, mo, y, hh, mm, ampm, sender, text] = m;
        const ts = parseIstDate(d, mo, y, hh, mm, ampm || '');
        if (ts > cutoff) msgs.push({ ts, sender: (sender||'').trim(), text: (text||'').trim() });
        continue;
      }
      m = RE_IOS.exec(raw);
      if (m) {
        const [, d, mo, y, hh, mm, ampm, sender, text] = m;
        const ts = parseIstDate(d, mo, y, hh, mm, ampm || '');
        if (ts > cutoff) msgs.push({ ts, sender: (sender||'').trim(), text: (text||'').trim() });
      }
    }

    const timelines = buildPidTimelines(msgs);
    const rows = summarizeRows(PID_MAP || new Map(), timelines, now, sla);

    // CSV with the same columns the UI shows
    const headers = ['pid','designer','assigned_merch','status','first_cs_preview','first_cs_ts_ist','latest_cs_preview','cs_ts_ist'];
    const esc = (v)=> {
      const s = (v ?? '').toString();
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    };
    const csv = [headers.join(',')].concat(
      rows.map(r => headers.map(h => esc(r[h])).join(','))
    ).join('\n');

    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({ rows, csv, meta: { mappingCount: PID_MAP?.size || 0 } });
  } catch (e) {
    console.error(e);
    res.status(500).send('Internal Server Error');
  }
}
