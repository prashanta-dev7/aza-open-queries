// api/backend.js
// Two-file upload flow: mappingCsvText + chatText come in the request body.
// No filesystem, no env vars, no vercel.json required.

/* ---------- helpers ---------- */
function stripBOM(s){ return s && s.charCodeAt(0)===0xFEFF ? s.slice(1): s; }
function escCsv(v){ const s=(v??'').toString(); return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s; }

/* ---------- CSV parsing (robust) ---------- */
function parseCsvRobust(text){
  text = stripBOM(text||'');
  const rows=[]; let i=0, field='', row=[], inQ=false;
  while(i<text.length){
    const c=text[i];
    if(inQ){
      if(c===`"`){ if(text[i+1]===`"`){ field+=`"`; i+=2; continue; } inQ=false; i++; continue; }
      field+=c; i++; continue;
    }else{
      if(c===`"`){ inQ=true; i++; continue; }
      if(c===`,`) { rows.push([...row,field]); row=[]; field=''; i++; continue; }
      if(c===`\r`){ i++; continue; }
      if(c===`\n`){ rows.push([...row,field]); row=[]; field=''; i++; continue; }
      field+=c; i++;
    }
  }
  if(field.length||row.length) rows.push([...row,field]);
  return rows;
}

function loadMappingFromCsvText(csvText){
  const rows = parseCsvRobust(csvText);
  if(!rows.length) return new Map();
  const header = rows[0].map(h => stripBOM((h||'')).toLowerCase().trim());

  // Your final headers + fallbacks
  const idx = {
    pid:
      header.indexOf('pid') !== -1 ? header.indexOf('pid') :
      header.indexOf('product_id') !== -1 ? header.indexOf('product_id') :
      header.indexOf('productid'),
    designer:
      header.indexOf('designer_name') !== -1 ? header.indexOf('designer_name') :
      header.indexOf('designer') !== -1 ? header.indexOf('designer') :
      header.indexOf('designername'),
    merch:
      header.indexOf('merch_name') !== -1 ? header.indexOf('merch_name') :
      header.indexOf('merch') !== -1 ? header.indexOf('merch') :
      header.indexOf('merchandiser') !== -1 ? header.indexOf('merchandiser') :
      header.indexOf('merchandisername'),
  };

  const map = new Map();
  for(let r=1;r<rows.length;r++){
    const cols = rows[r];
    const rawPid = (idx.pid>=0 ? (cols[idx.pid]||'') : '').toString().trim();
    const pid = (rawPid.match(/\b(\d{6})\b/)||[])[1] || '';
    if(!pid) continue;
    const designer = idx.designer>=0 ? (cols[idx.designer]||'').toString().trim() : '';
    const merch    = idx.merch>=0    ? (cols[idx.merch]||'').toString().trim()    : '';
    if(!map.has(pid)) map.set(pid, { designer, merch });
  }
  return map;
}

/* ---------- WhatsApp parsing ---------- */
const RE_ANDROID = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4}),\s+(\d{1,2}):(\d{2})\s*(am|pm)?\s*-\s([^:]+):\s([\s\S]*)$/i;
const RE_IOS     = /^\[(\d{1,2})\/(\d{1,2})\/(\d{2,4}),\s+(\d{1,2}):(\d{2})\s*(am|pm)?\]\s([^:]+):\s([\s\S]*)$/i;

function parseIstDate(d,m,y,hh,mm,ampm){
  const year = (String(y).length===2) ? (parseInt(y,10)+2000) : parseInt(y,10);
  let hour = parseInt(hh,10); const minute=parseInt(mm,10);
  if(ampm){ const ap=ampm.toLowerCase(); if(ap==='pm'&&hour<12) hour+=12; if(ap==='am'&&hour===12) hour=0; }
  return new Date(`${year}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}T${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}:00.000+05:30`);
}
function fmtIst(dt){ if(!dt) return ''; const p=n=>String(n).padStart(2,'0');
  return `${dt.getFullYear()}-${p(dt.getMonth()+1)}-${p(dt.getDate())} ${p(dt.getHours())}:${p(dt.getMinutes())} IST`; }

function extractPids(text){
  const out=new Set(); const re=/\b(?:pid\s*[:\-]?\s*)?(\d{6})\b/gi; let m;
  while((m=re.exec(text))!==null) out.add(m[1]); return Array.from(out);
}

/* ---------- SLA logic (next mention must be different sender) ---------- */
function buildPidTimelines(msgs){
  const map=new Map();
  for(const m of msgs){
    const pids=extractPids(m.text);
    if(!pids.length) continue;
    for(const pid of pids){
      if(!map.has(pid)) map.set(pid, []);
      map.get(pid).push(m);
    }
  }
  for(const arr of map.values()) arr.sort((a,b)=>a.ts-b.ts);
  return map;
}

function summarizeRows(pidMap, timelines, now, slaMinutes){
  const rows=[]; const slaMs=Math.max(1, parseInt(slaMinutes||60,10))*60*1000;
  let matchedPidCount=0;

  for(const [pid, arr] of timelines.entries()){
    if(!arr.length) continue;
    const meta = pidMap.get(pid) || { designer:'', merch:'' };
    if(meta.designer || meta.merch) matchedPidCount++;

    const first = arr[0];
    const latest = arr[arr.length-1];
    const next = arr.length>=2 ? arr[1] : null;

    let include=false; let status='Open';
    if(next){
      const gap=next.ts-first.ts;
      if(next.sender !== first.sender){
        if(gap>slaMs){ include=true; status='Open — Breached'; }
        else { include=false; status='Closed'; }
      }else{
        const age=now-first.ts;
        include = age>slaMs;
        status = include ? 'Open — Breached' : 'Open';
      }
    }else{
      const age=now-first.ts;
      include = age>slaMs;
      status = include ? 'Open — Breached' : 'Open';
    }
    if(!include) continue;

    rows.push({
      pid,
      designer: meta.designer || '',
      assigned_merch: meta.merch || '',
      status,
      first_cs_preview: (first.text||'').slice(0,160),
      first_cs_ts_ist: fmtIst(first.ts),
      latest_cs_preview: (latest.text||'').slice(0,160),
      cs_ts_ist: fmtIst(latest.ts),
    });
  }

  rows.sort((a,b)=>{
    const rank = s => s.startsWith('Open — Breached') ? 0 : 1;
    const r = rank(a.status)-rank(b.status);
    if(r!==0) return r;
    return (b.first_cs_ts_ist||'').localeCompare(a.first_cs_ts_ist||'');
  });

  return { rows, matchedPidCount };
}

/* ---------- Handler ---------- */
export default async function handler(req, res){
  if(req.method!=='POST'){ res.status(405).send('Method Not Allowed'); return; }
  try{
    const { chatText, mappingCsvText, cutoffDate, slaMinutes } = req.body || {};
    if(!chatText || !mappingCsvText || !cutoffDate){
      res.status(400).send('chatText, mappingCsvText and cutoffDate are required'); return;
    }

    const cutoff = new Date(`${cutoffDate}T00:00:00.000+05:30`);
    if(isNaN(cutoff)) { res.status(400).send('Invalid cutoffDate'); return; }

    const sla = Math.max(1, parseInt(slaMinutes ?? 60, 10));
    const now = new Date();

    // Parse mapping from the uploaded CSV
    const PID_MAP = loadMappingFromCsvText(mappingCsvText);

    // Parse WhatsApp lines (Android & iOS), only AFTER cutoff
    const msgs=[];
    for(const raw of chatText.split(/\r?\n/)){
      let m = RE_ANDROID.exec(raw);
      if(m){
        const [,d,mo,y,hh,mm,ampm,sender,text]=m;
        const ts=parseIstDate(d,mo,y,hh,mm,ampm||'');
        if(ts>cutoff) msgs.push({ ts, sender:(sender||'').trim(), text:(text||'').trim() });
        continue;
      }
      m = RE_IOS.exec(raw);
      if(m){
        const [,d,mo,y,hh,mm,ampm,sender,text]=m;
        const ts=parseIstDate(d,mo,y,hh,mm,ampm||'');
        if(ts>cutoff) msgs.push({ ts, sender:(sender||'').trim(), text:(text||'').trim() });
      }
    }

    const timelines = buildPidTimelines(msgs);
    const { rows, matchedPidCount } = summarizeRows(PID_MAP || new Map(), timelines, now, sla);

    const headers=['pid','designer','assigned_merch','status','first_cs_preview','first_cs_ts_ist','latest_cs_preview','cs_ts_ist'];
    const csv = [headers.join(',')].concat(rows.map(r=>headers.map(h=>escCsv(r[h])).join(','))).join('\n');

    res.setHeader('Content-Type','application/json');
    res.status(200).json({
      rows, csv,
      meta: {
        mappingCount: PID_MAP?.size || 0,
        matchedPidCount
      }
    });
  }catch(e){
    console.error(e);
    res.status(500).send('Internal Server Error');
  }
}
