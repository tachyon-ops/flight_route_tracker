#!/usr/bin/env python3
"""
adsb_tracker.py - live ADS-B area scanner + lock-on tracker.

Backend  : FastAPI proxy to free community ADS-B aggregators (adsb.fi /
           adsb.lol). It centralizes the ~1 req/sec rate limit, falls back
           between providers, and keeps a short server-side trail history
           per aircraft so a trail exists the moment you lock on.
Frontend : a single Leaflet "radar console" page served at /.

Run:
    pip install fastapi uvicorn httpx
    python adsb_tracker.py            # then open http://127.0.0.1:8000
    python adsb_tracker.py --port 9000 --host 0.0.0.0

Use:
    - Pick a center + radius (or hit a preset), the map scans that circle.
    - Click any aircraft to lock on: it draws the trail and (optionally) follows.
    - Or lock directly by registration / hex if you already know the tail.

Reality check: these are volunteer-feeder networks. Coverage over China and
Mongolia is thin, so a westbound flight can go silent and reappear over
Kazakhstan / the Gulf. Non-commercial use only; cite the provider.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

UA = "adsb_tracker/1.0 (personal, non-commercial)"

# ADSBExchange-v2-compatible free providers. Listed in fallback order.
PROVIDERS: dict[str, dict[str, str]] = {
    "adsbfi": {
        "area": "https://opendata.adsb.fi/api/v3/lat/{lat}/lon/{lon}/dist/{dist}",
        "hex": "https://opendata.adsb.fi/api/v2/hex/{hex}",
        "reg": "https://opendata.adsb.fi/api/v2/registration/{reg}",
    },
    "adsblol": {
        "area": "https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/{dist}",
        "hex": "https://api.adsb.lol/v2/hex/{hex}",
        "reg": "https://api.adsb.lol/v2/registration/{reg}",
    },
}

MIN_INTERVAL = 1.1   # seconds between *any* two upstream calls (global budget)
CACHE_TTL = 4.0      # seconds to reuse an identical upstream response
TRAIL_MAX = 2000     # points kept per aircraft

_last_call = 0.0
_throttle_lock = asyncio.Lock()
_cache: dict[str, tuple[float, Any]] = {}
TRAILS: dict[str, deque] = defaultdict(lambda: deque(maxlen=TRAIL_MAX))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(timeout=12, headers={"User-Agent": UA})
    try:
        yield
    finally:
        await app.state.client.aclose()


app = FastAPI(title="adsb_tracker", lifespan=lifespan)


async def _throttled_get(client: httpx.AsyncClient, url: str) -> Any:
    global _last_call
    now = time.monotonic()
    hit = _cache.get(url)
    if hit and now - hit[0] < CACHE_TTL:
        return hit[1]
    async with _throttle_lock:
        wait = MIN_INTERVAL - (time.monotonic() - _last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        finally:
            _last_call = time.monotonic()
    _cache[url] = (time.monotonic(), data)
    return data


async def fetch(client: httpx.AsyncClient, provider: str, kind: str, **kw) -> Any:
    order = [provider] + [p for p in PROVIDERS if p != provider]
    last_err: Exception | None = None
    for p in order:
        tmpl = PROVIDERS.get(p, {}).get(kind)
        if not tmpl:
            continue
        try:
            return await _throttled_get(client, tmpl.format(**kw))
        except Exception as e:  # noqa: BLE001 - try the next provider
            last_err = e
    raise HTTPException(502, f"all providers failed ({kind}): {last_err}")


def _record(ac_list: list[dict]) -> None:
    ts = time.time()
    for a in ac_list:
        h = a.get("hex")
        lat, lon = a.get("lat"), a.get("lon")
        if not h or lat is None or lon is None:
            continue
        d = TRAILS[h]
        if not d or d[-1][1] != lat or d[-1][2] != lon:
            d.append((ts, lat, lon, a.get("alt_baro"), a.get("gs"), a.get("track")))


@app.get("/api/area")
async def area(lat: float, lon: float, dist: int = 250, provider: str = "adsbfi"):
    dist = max(1, min(int(dist), 250))
    data = await fetch(app.state.client, provider, "area", lat=lat, lon=lon, dist=dist)
    ac = data.get("ac") or []
    _record(ac)
    # Include trail history for each aircraft
    for a in ac:
        h = a.get("hex")
        if h:
            a["trail"] = [[la, lo] for (_t, la, lo, *_r) in TRAILS.get(h.lower(), [])]
    return {"ac": ac, "count": len(ac), "provider": provider}


@app.get("/api/hex/{hex}")
async def by_hex(hex: str, provider: str = "adsbfi"):
    data = await fetch(app.state.client, provider, "hex", hex=hex.lower())
    ac = data.get("ac") or []
    _record(ac)
    cur = ac[0] if ac else None
    trail = [[la, lo] for (_t, la, lo, *_r) in TRAILS.get(hex.lower(), [])]
    return {"ac": cur, "trail": trail, "seen": cur is not None}


@app.get("/api/reg/{reg}")
async def by_reg(reg: str, provider: str = "adsbfi"):
    data = await fetch(app.state.client, provider, "reg", reg=reg.upper())
    ac = data.get("ac") or []
    _record(ac)
    cur = ac[0] if ac else None
    return {"ac": cur, "hex": (cur or {}).get("hex"), "seen": cur is not None}


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>ADS-B console</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
  :root{
    --bg:#0a0e13; --panel:#0f161e; --line:#1d2a36; --ink:#c4d2de;
    --dim:#6b7d8d; --amber:#f5a623; --lock:#2ee6a6; --warn:#ff5d5d;
    --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  }
  *{box-sizing:border-box}
  html,body{height:100%;margin:0;background:var(--bg);color:var(--ink);
    font:13px/1.45 var(--mono)}
  #app{display:grid;grid-template-columns:320px 1fr;height:100%}
  @media(max-width:760px){#app{grid-template-columns:1fr;grid-template-rows:auto 1fr}}
  aside{background:var(--panel);border-right:1px solid var(--line);
    padding:14px;overflow-y:auto;display:flex;flex-direction:column;gap:14px}
  h1{font-size:12px;letter-spacing:.18em;text-transform:uppercase;margin:0;
    color:var(--dim);font-weight:600}
  h1 b{color:var(--amber)}
  .group{display:flex;flex-direction:column;gap:6px}
  label{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--dim)}
  input,select{background:#0a1017;border:1px solid var(--line);color:var(--ink);
    border-radius:4px;padding:7px 8px;font:12px var(--mono);width:100%}
  input:focus,select:focus{outline:none;border-color:var(--amber)}
  .row{display:flex;gap:8px}
  .row>*{flex:1}
  .presets{display:flex;gap:6px;flex-wrap:wrap}
  button{background:#13202b;border:1px solid var(--line);color:var(--ink);
    border-radius:4px;padding:7px 9px;font:11px var(--mono);cursor:pointer;
    letter-spacing:.04em}
  button:hover{border-color:var(--amber);color:#fff}
  button.go{background:var(--amber);border-color:var(--amber);color:#0a0e13;font-weight:700}
  button.go:hover{filter:brightness(1.08)}
  .preset{flex:1;min-width:0;padding:6px 4px}
  .status{font-size:11px;color:var(--dim);min-height:16px}
  .center-handle{width:16px;height:16px;border:2px solid #f5a623;
    border-radius:50%;background:rgba(245,166,35,.9);
    box-shadow:0 0 0 4px rgba(245,166,35,.2);}
  .status.live{color:var(--lock)} .status.err{color:var(--warn)}
  /* locked-aircraft data strip: the one place with weight */
  .strip{border:1px solid var(--line);border-radius:6px;overflow:hidden;display:none}
  .strip.on{display:block}
  .strip header{background:#0a1017;border-bottom:1px solid var(--line);
    padding:9px 10px;display:flex;align-items:baseline;justify-content:space-between}
  .strip header .call{color:var(--lock);font-size:16px;font-weight:700;letter-spacing:.06em}
  .strip header .typ{color:var(--dim);font-size:11px}
  .kv{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--line)}
  .kv div{background:var(--panel);padding:7px 10px}
  .kv .k{color:var(--dim);font-size:9px;letter-spacing:.12em;text-transform:uppercase}
  .kv .v{font-size:14px;color:#eaf2f8;margin-top:2px}
  .strip footer{padding:8px 10px;display:flex;gap:8px;align-items:center;
    border-top:1px solid var(--line)}
  .strip footer label{display:flex;gap:6px;align-items:center;cursor:pointer;
    text-transform:none;letter-spacing:0;color:var(--ink)}
  .strip footer .seen{margin-left:auto;color:var(--dim);font-size:10px}
  .note{font-size:10px;color:var(--dim);line-height:1.5;border-top:1px solid var(--line);
    padding-top:12px}
  #map{height:100%}
  .leaflet-container{background:#05080b}
  .plane{transition:transform .25s linear}
</style>
</head>
<body>
<div id="app">
  <aside>
    <h1>ADS&#8209;B&nbsp;<b>console</b></h1>

    <div class="group">
      <label>Center (lat, lon)</label>
      <div class="row">
        <input id="lat" value="43.0" inputmode="decimal"/>
        <input id="lon" value="80.0" inputmode="decimal"/>
      </div>
      <label>Radius (NM, max 250) &middot; refresh (s)</label>
      <div class="row">
        <input id="dist" value="250" inputmode="numeric"/>
        <input id="every" value="5" inputmode="numeric"/>
      </div>
      <label>Provider</label>
      <select id="provider">
        <option value="adsbfi">adsb.fi</option>
        <option value="adsblol">adsb.lol</option>
      </select>
      <div class="presets">
        <button class="preset" data-lat="39.51" data-lon="116.41" data-d="250">PKX</button>
        <button class="preset" data-lat="43.0" data-lon="80.0" data-d="250">Mid&nbsp;route</button>
        <button class="preset" data-lat="29.0" data-lon="55.0" data-d="250">Gulf&nbsp;approach</button>
        <button class="preset" data-lat="25.27" data-lon="51.61" data-d="200">DOH</button>
        <button class="preset" data-lat="28.43" data-lon="77.10" data-d="100">DEL</button>
        <button class="preset" data-lat="35.41" data-lon="139.77" data-d="100">NRT</button>
      </div>
      <button class="go" id="scan">Scan this circle</button>
      <button id="scanview">Scan the current map view</button>
    </div>

    <div class="group">
      <label>Lock directly by registration / hex</label>
      <div class="row">
        <input id="regq" placeholder="A7-BEI or 06a1f2"/>
        <button id="locklookup" style="flex:0 0 auto">Lock</button>
      </div>
    </div>

    <div class="status" id="status">Idle. Set a center and scan.</div>

    <div class="strip" id="strip">
      <header><span class="call" id="s-call">&mdash;</span><span class="typ" id="s-typ"></span></header>
      <div class="kv">
        <div><div class="k">Airline</div><div class="v" id="s-airline">&mdash;</div></div>
        <div><div class="k">Aircraft type</div><div class="v" id="s-typ"></div></div>
        <div><div class="k">Alt (baro)</div><div class="v" id="s-alt">&mdash;</div></div>
        <div><div class="k">Ground spd</div><div class="v" id="s-gs">&mdash;</div></div>
        <div><div class="k">Track</div><div class="v" id="s-trk">&mdash;</div></div>
        <div><div class="k">Reg / hex</div><div class="v" id="s-reg">&mdash;</div></div>
        <div><div class="k">Lat</div><div class="v" id="s-lat">&mdash;</div></div>
        <div><div class="k">Lon</div><div class="v" id="s-lon">&mdash;</div></div>
      </div>
      <footer>
        <label><input type="checkbox" id="follow" checked/> Follow</label>
        <button id="unlock" style="flex:0 0 auto;padding:5px 8px">Release</button>
        <span class="seen" id="s-seen"></span>
      </footer>
    </div>

    <div class="note">
      Community feeders only. Expect silence over China &amp; Mongolia; a westbound
      target usually reappears over Kazakhstan / the Gulf. If a locked target goes
      quiet the last trail stays drawn until it&rsquo;s heard again.
    </div>
  </aside>
  <div id="map"></div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const $ = id => document.getElementById(id);
const map = L.map('map', {zoomControl:true, worldCopyJump:true}).setView([43,80], 4);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution:'&copy; OpenStreetMap &copy; CARTO &middot; data: adsb.fi / adsb.lol',
  subdomains:'abcd', maxZoom:18
}).addTo(map);

let markers = {};            // hex -> {marker, trail}
let ring = null;             // scan-radius circle
let centerHandle = null;     // draggable center handle
let locked = null;          // hex string
let trail = null;           // polyline for locked target
let trailHead = null;       // last-position marker for locked
let timer = null;
let cfg = {lat:43, lon:80, dist:250};

function planeIcon(track, color){
  const t = (track==null?0:track);
  const svg =
    `<svg class="plane" width="26" height="26" viewBox="0 0 24 24"
      style="transform:rotate(${t}deg)">
      <path d="M12 2 L14.2 11 L22 14.5 L22 16.2 L14.2 14 L13.4 20 L16 21.6 L16 22.6
        L12 21.4 L8 22.6 L8 21.6 L10.6 20 L9.8 14 L2 16.2 L2 14.5 L9.8 11 Z"
        fill="${color}" stroke="#05080b" stroke-width="0.6"/>
    </svg>`;
  return L.divIcon({html:svg, className:'', iconSize:[26,26], iconAnchor:[13,13]});
}

function setStatus(msg, cls){ const s=$('status'); s.textContent=msg; s.className='status '+(cls||''); }

function nm2m(nm){ return nm*1852; }

function drawRing(){
  if(ring) map.removeLayer(ring);
  if(centerHandle) map.removeLayer(centerHandle);
  ring = L.circle([cfg.lat,cfg.lon], {radius:nm2m(cfg.dist), color:'#f5a623',
    weight:1, opacity:.35, fill:false, dashArray:'4 6'}).addTo(map);
  centerHandle = L.marker([cfg.lat,cfg.lon], {
    draggable:true,
    icon:L.divIcon({className:'center-handle', iconSize:[16,16], iconAnchor:[8,8]})
  }).addTo(map);
  centerHandle.on('dragend', e=>{
    const p = e.target.getLatLng();
    cfg.lat = p.lat; cfg.lon = p.lng;
    $('lat').value = p.lat.toFixed(4);
    $('lon').value = p.lng.toFixed(4);
    drawRing();
    if(timer){ scanArea(); }
  });
}

function acColor(hex){ return hex===locked ? getCSS('--lock') : getCSS('--amber'); }
function getCSS(v){ return getComputedStyle(document.documentElement).getPropertyValue(v).trim(); }

// Common airline callsign prefixes → full names
const airlineMap = {
  'QR': 'Qatar Airways', 'BA': 'British Airways', 'UA': 'United', 'AA': 'American',
  'DL': 'Delta', 'SW': 'Southwest', 'AF': 'Air France', 'LH': 'Lufthansa',
  'KL': 'KLM', 'SQ': 'Singapore Airlines', 'NH': 'ANA', 'JL': 'JAL',
  'CX': 'Cathay Pacific', 'EK': 'Emirates', 'AY': 'Finnair', 'SU': 'Aeroflot',
  'TK': 'Turkish', 'AI': 'Air India', 'VT': 'AirAsia', 'MH': 'Malaysia Airlines',
  'TG': 'Thai Airways', 'PK': 'Pakistan Intl', 'FX': 'FedEx', 'DH': 'DHL',
  'UPS': 'UPS', 'ASH': 'Air Shuttle', 'SVA': 'Saudia', 'EZY': 'EasyJet',
  'RY': 'Royal Air', 'RA': 'Royal Jordanian', 'MS': 'EgyptAir', 'ME': 'Middle East',
  'OA': 'Oman Air', 'WY': 'Oman Air', 'G9': 'Air Arabia', 'FZ': 'Flydubai',
  'EY': 'Etihad', 'BG': 'Biman Bangladesh', 'VN': 'Vietnam Airlines', 'PR': 'Philippine',
  'CI': 'China Airlines', 'BR': 'EVA Air', 'CA': 'Air China', 'CZ': 'China Southern',
  'MU': 'China Eastern', 'BX': 'Air Busan', 'KE': 'Korean Air', 'OZ': 'Asiana',
  'LJ': 'Lao Airlines', 'QF': 'Qantas', 'NZ': 'Air New Zealand', 'VA': 'Virgin Australia',
  'JQ': 'Jetstar', 'FJ': 'Fiji Airways', 'SB': 'Air Caledonie', 'PX': 'Air Niugini',
  'AC': 'Air Canada', 'WS': 'WestJet', 'B6': 'JetBlue', 'NK': 'Spirit', 'F9': 'Frontier',
  'AS': 'Alaska', 'G4': 'Allegiant', 'YX': 'Republic', 'XE': 'Expressjet',
  'HA': 'Hawaiian', 'OO': 'Sky West', 'MX': 'Mexicana', 'AM': 'AeroMexico',
  'Y4': 'Volotea', 'V7': 'Volotea', 'UX': 'Air Europa', 'IB': 'Iberia',
  'VY': 'Vueling', 'U2': 'EasyJet', 'FR': 'Ryanair', 'W6': 'Wizz Air',
  'LO': 'LATAM', 'LA': 'LATAM', 'LP': 'LATAM Peru', 'UP': 'Bahamasair',
  'AD': 'Adria Airways', 'JU': 'Air Serbia', 'OU': 'Croatia Airlines', 'OS': 'Austrian',
  'LX': 'Swiss', 'SR': 'Swissair', 'AZ': 'Alitalia', 'U2': 'EasyJet',
  'SN': 'Brussels', 'TP': 'TAP Portugal', 'RJ': 'Royal Jordanian', 'FV': 'Endeavor',
  'EI': 'Aer Lingus', 'IE': 'Aer Lingus', 'BD': 'BMI', 'BA': 'British Airways',
  'QF': 'Qantas', 'GA': 'Garuda', 'MH': 'Malaysia', 'PG': 'Bangkok Airways',
  'KL': 'KLM', 'NW': 'Northwest', 'CO': 'Continental', 'NX': 'Nonstop'
};

function getAirline(flight){
  if(!flight) return '';
  flight = flight.trim().toUpperCase();
  // Try 3-letter code first, then 2-letter
  for(let len = 3; len >= 2; len--){
    const prefix = flight.substring(0, len);
    if(airlineMap[prefix]) return airlineMap[prefix];
  }
  return '';
}

async function jget(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error('HTTP '+r.status);
  return r.json();
}

async function scanArea(){
  const p = $('provider').value;
  let data;
  try{
    data = await jget(`/api/area?lat=${cfg.lat}&lon=${cfg.lon}&dist=${cfg.dist}&provider=${p}`);
  }catch(e){ setStatus('Provider unreachable - retrying next cycle.', 'err'); return; }

  const seen = new Set();
  for(const a of data.ac){
    if(a.lat==null || a.lon==null) continue;
    seen.add(a.hex);
    const color = acColor(a.hex);
    if(markers[a.hex]){
      markers[a.hex].marker.setLatLng([a.lat,a.lon]).setIcon(planeIcon(a.track,color));
    }else{
      const m = L.marker([a.lat,a.lon], {icon:planeIcon(a.track,color), riseOnHover:true});
      m.on('click', ()=>lock(a.hex));
      const lbl = (a.flight||a.hex||'').trim();
      m.bindTooltip(lbl, {direction:'top', offset:[0,-12], opacity:.9});
      m.addTo(map);
      markers[a.hex] = {marker:m, trail:null};
    }
    // Draw or update trail for this aircraft
    if(a.trail && a.trail.length > 1){
      if(!markers[a.hex].trail){
        markers[a.hex].trail = L.polyline(a.trail, {
          color:getCSS('--dim'), weight:1, opacity:0.25, dashArray:'2 4'
        }).addTo(map);
      }else{
        markers[a.hex].trail.setLatLngs(a.trail);
      }
    }
  }
  // drop stale markers (but never the locked one)
  for(const h of Object.keys(markers)){
    if(!seen.has(h) && h!==locked){
      if(markers[h].trail) map.removeLayer(markers[h].trail);
      map.removeLayer(markers[h].marker);
      delete markers[h];
    }
  }
  if(!locked){
    setStatus(data.count ? `Scanning - ${data.count} aircraft in range.`
                         : 'No aircraft in range. Widen the radius or move the center west.',
              data.count?'live':'');
  }
}

async function pollLocked(){
  if(!locked) return;
  const p = $('provider').value;
  let d;
  try{ d = await jget(`/api/hex/${locked}?provider=${p}`); }
  catch(e){ setStatus('Locked target: provider unreachable - retrying.', 'err'); return; }

  if(d.trail && d.trail.length){
    if(!trail){
      trail = L.polyline(d.trail, {
        color:getCSS('--lock'), weight:2, opacity:0.6, dashArray:'8 6'
      }).addTo(map);
    } else {
      trail.setLatLngs(d.trail);
    }
    const lastPoint = d.trail[d.trail.length-1];
    if(!trailHead){
      trailHead = L.circleMarker(lastPoint, {
        radius:6, color:getCSS('--lock'), fillColor:getCSS('--lock'),
        fillOpacity:1, weight:2, opacity:1
      }).addTo(map);
    } else {
      trailHead.setLatLng(lastPoint);
    }
    // Brighten this aircraft's trail if it has one
    if(markers[locked] && markers[locked].trail){
      markers[locked].trail.setStyle({color:getCSS('--lock'), opacity:0.5});
    }
  } else {
    if(trail){ trail.setLatLngs([]); }
    if(trailHead){ map.removeLayer(trailHead); trailHead = null; }
  }
  const a = d.ac;
  if(a && a.lat!=null){
    if(markers[locked]) markers[locked].marker.setLatLng([a.lat,a.lon]).setIcon(planeIcon(a.track, getCSS('--lock')));
    else { const m=L.marker([a.lat,a.lon],{icon:planeIcon(a.track,getCSS('--lock'))}).addTo(map); markers[locked]={marker:m, trail:null}; m.on('click',()=>{}); }
    fillStrip(a, true);
    if($('follow').checked) map.panTo([a.lat,a.lon], {animate:true});
    setStatus(`Locked on ${(a.flight||locked).trim()}.`, 'live');
  }else{
    fillStrip(null, false);
    setStatus(`Locked on ${locked} - no contact right now (coverage gap). Trail held.`, '');
  }
}

function fillStrip(a, live){
  $('strip').classList.add('on');
  if(!a){ $('s-seen').textContent='no contact'; return; }
  console.log('Aircraft data:', a);
  $('s-call').textContent = (a.flight||'(no callsign)').trim();
  $('s-airline').textContent = getAirline(a.flight) || '(unknown)';
  $('s-typ').textContent  = a.t||'';
  $('s-alt').textContent  = a.alt_baro==null?'-':(a.alt_baro==='ground'?'ground':a.alt_baro+' ft');
  $('s-gs').textContent   = a.gs==null?'-':Math.round(a.gs)+' kt';
  $('s-trk').textContent  = a.track==null?'-':Math.round(a.track)+'°';
  $('s-reg').textContent  = (a.r||'-')+' / '+(a.hex||'-');
  $('s-lat').textContent  = a.lat==null?'-':a.lat.toFixed(4);
  $('s-lon').textContent  = a.lon==null?'-':a.lon.toFixed(4);
  $('s-seen').textContent = live ? 'live' : 'stale';
}

function recolorAll(){ for(const h in markers){ const m=markers[h].marker; m.setIcon(planeIcon(0, acColor(h))); } }

function lock(hex){
  locked = hex;
  if(trail){ map.removeLayer(trail); trail=null; }
  recolorAll();
  pollLocked();
}

function unlock(){
  locked = null;
  if(trail){ map.removeLayer(trail); trail=null; }
  if(trailHead){ map.removeLayer(trailHead); trailHead=null; }
  // Restore all trails to dim color
  for(const h in markers){
    if(markers[h].trail){
      markers[h].trail.setStyle({color:getCSS('--dim'), opacity:0.25});
    }
  }
  $('strip').classList.remove('on');
  recolorAll();
  setStatus('Released. Scanning.', '');
}

function readCfg(){
  cfg.lat = parseFloat($('lat').value)||0;
  cfg.lon = parseFloat($('lon').value)||0;
  cfg.dist = Math.max(1, Math.min(parseInt($('dist').value)||250, 250));
}

function restart(){
  readCfg(); drawRing();
  map.setView([cfg.lat,cfg.lon], map.getZoom()<4?5:map.getZoom());
  if(timer) clearInterval(timer);
  const every = Math.max(2, parseInt($('every').value)||5)*1000;
  const tick = ()=>{ scanArea(); pollLocked(); };
  tick();
  timer = setInterval(tick, every);
}

// wiring
$('scan').onclick = restart;
$('scanview').onclick = ()=>{ const c=map.getCenter(); $('lat').value=c.lat.toFixed(3); $('lon').value=c.lng.toFixed(3); restart(); };
document.querySelectorAll('.preset').forEach(b=>b.onclick=()=>{
  $('lat').value=b.dataset.lat; $('lon').value=b.dataset.lon; $('dist').value=b.dataset.d; restart();
});
$('unlock').onclick = unlock;
$('locklookup').onclick = async ()=>{
  const q = $('regq').value.trim(); if(!q) return;
  const p = $('provider').value;
  const isHex = /^[0-9a-fA-F]{6}$/.test(q);
  setStatus('Looking up '+q+'...', '');
  try{
    if(isHex){ lock(q.toLowerCase()); map.setView([cfg.lat,cfg.lon]); }
    else{
      const d = await jget(`/api/reg/${encodeURIComponent(q)}?provider=${p}`);
      if(d.hex){ lock(d.hex); if(d.ac&&d.ac.lat!=null) map.setView([d.ac.lat,d.ac.lon],6); }
      else setStatus('No contact for '+q+' yet. It may be out of coverage - try again later.', '');
    }
  }catch(e){ setStatus('Lookup failed - check the value or provider.', 'err'); }
};

restart();
</script>
</body>
</html>"""


def main():
    ap = argparse.ArgumentParser(description="Live ADS-B area scanner + lock-on tracker")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()