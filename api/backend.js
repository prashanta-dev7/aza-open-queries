// api/backend.js
// Vercel serverless function (Node 18+)

import fs from 'fs';
import path from 'path';

// --- Helpers ---------------------------------------------------------------

function parseCsv(content) {
  // Very tolerant CSV (no quoted commas in your dumps assumed).
  // Columns expected: pid, designer_code?, designer_name, merch_name, merch_phone?
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length === 0) return [];
  const header = lines[0].split(',').map(s => s.trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',').map(s => s.trim());
    const obj = {};
    header.forEach((h, idx) => obj[h] = parts[idx] ?? '');
    rows.push(obj);
  }
  return rows;
}

function normName(s) {
  return (s || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .trim();
}

function isLikelyCsMessage(text) {
  const t = (text || '').toLowerCase();
  const cues = [
    '?','any update','please update','pls update','need','possible','price','lead time',
    'can we','please confirm','pls confirm','update?','any updates','status','dispatch','deliver',
    'photo','video','images','material','size','timeline'
  ];
  return cues.some(c => t.includes(c)) || /\?\s*$/.test(t);
}

function extractPids(text) {
  // 6-digit numbers, with optional "PID" prefix variations
  const pids = new Set();
  const regex = /\b(?:pid\s*[:\-]?\s*)?(\d{6})\b/gi;
  let m;
  while ((m = regex.exec(text)) !== null) pids.add(m[1]);
  return Array.from(pids);
}

// WhatsApp date formats (Android/iOS).
// Examples:
// "17/10/2024, 9:40 pm - Name: Text"
// "17/10/2024, 21:40 - Name: Text"
const lineRegex = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4}),\s+(\d{1,2}):(\d{2})\s*(am|pm)?\s*-\s([^:]+):\s([\s\S]*)$/i;

function parseIstDate(d, m, y, hh, mm, ampm) {
  const year = (y.length === 2) ? (parseInt(y,10) + 2000) : parseInt(y,10);
  let hour = parseInt(hh, 10);
  const minute = parseInt(mm, 10);
  if (ampm) {
    const ap = ampm.toLowerCase();
    if (ap === 'pm' && hour < 12) hour += 12;
    if (ap === 'am' && hour === 12) hour = 0;
  }
  // Interpret as IST by adding +05:30 offset.
  // Build a Date as if local then adjust to IST string for consistency.
  // On server, we'll treat it as UTC-equivalent epoch with IST assumption.
  const iso = `${year.toString().padStart(4,'0')}-${m.toString().padStart(2,'0')}-${d.toString().padStart(2,'0')}T${hour.toString().padStart(2,'0')}:${minute.toString().padStart(2,'0')}:00.000+05:30`;
  return new Date(iso);
}

function fmtIst(dt) {
  if (!dt) return '';
  // Return yyyy-mm-dd HH:MM IST
  const pad = (n)=> String(n).padStart(2,'0');
  const y = dt.getFullYear();
  const m = pad(dt.getMonth()+1);
  const d = pad(dt.getDate());
  const hh = pad(dt.getHours());
  const mm = pad(dt.getMinutes());
  return `${y}-${m}-${d} ${hh}:${mm} IST`;
}

function diffToHHMM(ms) {
  if (ms == null || isNaN(ms)) return '';
  const totalMin = Math.max(0, Math.floor(ms/60000));
  const hh = Math.floor(totalMin/60);
  const mm = totalMin%60;
  return `${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}`;
}

// Simple fuzzy match: merch name tokens should be mostly contained in sender name.
function senderMatchesMerch(sender, merchName) {
  const s = normName(sender);
  const m = normName(merchName);
  if (!s || !m) return false;
  if (s.includes(m) || m.includes(s)) return true;
  const mt = m.split(' ').filter(Boolean);
  const matched = mt.filter(t => s.includes(t)).length;
  return matched >= Math.max(1, Math.ceil(mt.length * 0.6));
}

// --- Load Mapping (once per cold start) ------------------------------------

let PID_MAP = null; // pid -> { designer, merch }
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
      for (const f of files) {
        const full = path.join(dataDir, f);
        const txt = fs.readFileSync(full, 'utf8');
        const rows = parseCsv(txt);
        for (const r of rows) {
          const pid = (r.pid || r.product_id || '').trim();
          if (!/^\d{6}$/.test(pid)) continue;
          const designer = (r.designer_name || r.designer || '').trim();
          const merch = (r.merch_name || r.merch || r.merchandiser || '').trim();
          if (!map.has(pid)) map.set(pid, { designer, merch });
        }
      }
      PID_MAP = map;
      resolve();
    } catch (e) {
      console.error('Mapping load error:', e);
      PID_MAP = new Map(); // proceed without mapping if needed
      resolve();
    }
  });
  return MAPPING_READY;
}

// --- Core Logic -------------------------------------------------------------

function parseChat(chatText, cutoffDate) {
  const lines = chatText.split(/\r?\n/);
  const msgs = [];
  for (const raw of lines) {
    const m = lineRegex.exec(raw);
    if (!m) continue;
    const [, d, mo, y, hh, mm, ampm, sender, text] = m;
    const ts = parseIstDate(d, mo, y, hh, mm, ampm || '');
    if (!(ts instanceof Date) || isNaN(ts)) continue;
    if (ts <= cutoffDate) continue; // only after cutoff
    msgs.push({ ts, sender: (sender||'').trim(), text: (text||'').trim(), raw });
  }
  return msgs;
}

function buildPidTimelines(msgs) {
  const map = new Map(); // pid -> { messages: [{ts, sender, text, isCsLike, isAssignedMerch}] }
  for (const m of msgs) {
    const pids = extractPids(m.text);
    if (pids.length === 0) continue;
    for (const pid of pids) {
      if (!map.has(pid)) map.set(pid, { messages: [] });
      map.get(pid).messages.push({ ...m });
    }
  }
  // Sort each by time just in case
  for (const [pid, o] of map) {
    o.messages.sort((a,b) => a.ts - b.ts);
  }
  return map;
}

function enrichAndSummarize(pidMap, timelines, now, slaMinutes) {
  const rows = [];
  for (const [pid, o] of timelines) {
    const meta = pidMap.get(pid) || { designer: '', merch: '' };
    const assignedMerch = meta.merch || '';

    // Mark CS-like vs assigned-merch replies
    const msgs = o.messages.map(m => {
      const isCs = isLikelyCsMessage(m.text);
      const isAssigned = senderMatchesMerch(m.sender, assignedMerch);
      return { ...m, isCsLike: isCs, isAssignedMerch: isAssigned };
    });

    // Find latest CS-like and latest assigned merch message (for that PID)
    const latestCs = [...msgs].reverse().find(x => x.isCsLike);
    const latestAssigned = [...msgs].reverse().find(x => x.isAssignedMerch);

    let status = 'Closed';
    let csTs = null, merchTs = null, age = null;
    let latestCsPreview = '', lastMerchPreview = '';
    let notes = '';

    if (latestCs) {
      csTs = latestCs.ts;
      latestCsPreview = latestCs.text.slice(0, 160);
      if (!latestAssigned || latestAssigned.ts <= latestCs.ts) {
        status = 'Open';
        age = now - csTs;
      } else {
        status = 'Closed';
        merchTs = latestAssigned.ts;
        lastMerchPreview = latestAssigned.text.slice(0, 160);
      }
    }

    if (status === 'Open') {
      // Breach if SLA exceeded
      if (age != null && age > slaMinutes * 60 * 1000) {
        status = 'Open — Breached';
      }
    }

    rows.push({
      pid,
      designer: meta.designer || '',
      assigned_merch: assignedMerch || '',
      status,
      age_hhmm: age != null ? diffToHHMM(age) : '',
      latest_cs_preview: latestCsPreview || '',
      cs_ts_ist: csTs ? fmtIst(csTs) : '',
      last_merch_preview: lastMerchPreview || '',
      merch_ts_ist: merchTs ? fmtIst(merchTs) : '',
      notes
    });
  }

  // Only keep ones that are still open by definition?
  // You asked to generate the open queries list; we’ll return all, and the UI shows Open count.
  // If you want only open rows, uncomment next line:
  // return rows.filter(r => r.status.toLowerCase().startsWith('open'));

  return rows;
}

function toCsv(rows) {
  const headers = [
    'pid','designer','assigned_merch','status','age_hhmm',
    'latest_cs_preview','cs_ts_ist','last_merch_preview','merch_ts_ist','notes'
  ];
  const esc = (v)=> {
    if (v == null) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  const lines = [headers.join(',')];
  for (const r of rows) {
    lines.push(headers.map(h => esc(r[h])).join(','));
  }
  return lines.join('\n');
}

// --- Handler ---------------------------------------------------------------

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).send('Method Not Allowed');
    return;
  }

  try {
    await loadMappingOnce(); // PID_MAP ready (may be empty if data not present)
    const { chatText, cutoffDate, slaMinutes } = req.body || {};
    if (!chatText || !cutoffDate) {
      res.status(400).send('chatText and cutoffDate are required');
      return;
    }

    const cutoff = new Date(`${cutoffDate}T00:00:00.000+05:30`);
    if (isNaN(cutoff)) {
      res.status(400).send('Invalid cutoffDate');
      return;
    }
    const sla = Math.max(1, parseInt(slaMinutes ?? 120, 10));
    const now = new Date(); // server time is fine; age is approximate and sufficient here

    const parsed = parseChat(chatText, cutoff);
    const timelines = buildPidTimelines(parsed);
    const rows = enrichAndSummarize(PID_MAP || new Map(), timelines, now, sla);
    const csv = toCsv(rows);

    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({ rows, csv });
  } catch (e) {
    console.error(e);
    res.status(500).send('Internal Server Error');
  }
}

