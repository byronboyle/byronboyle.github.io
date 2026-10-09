// Cloudflare Pages Function: GET /api/health   (deploy as functions/api/health.js, next to live.js)
//
// A yes/no answer for an outside uptime monitor (UptimeRobot or similar). It is the check that does NOT depend on Supabase's own
// scheduler: if the site, this function, the database or a Pi stops working, the monitor sees a non-200 and notifies you, even when
// the Telegram alerts (which run inside Supabase) have gone quiet.
//
//   /api/health                      200 if every station that has sent anything in the last 7 days sent a reading in the last
//                                    20 minutes; 503 otherwise. "Host wanted" placeholders that have never reported are ignored,
//                                    and a retired meter drops out of the check 7 days after its last reading.
//   /api/health?station=meter-01     the same for one station (one monitor per meter, if you like). 404 for an unknown station.
//   &max_minutes=N                   change the 20-minute limit (5 to 1440).
//
// Unlike /api/live it never serves a stale copy: it asks Supabase itself (the same read-only public_snapshot the site uses), so
// "Supabase is down" shows up here as a 503. Answers are shared for 30 seconds per URL so the endpoint cannot be used to make
// Supabase work harder; a monitor checking every 5 minutes is unaffected. The body is small JSON, readable by a person.

const SUPABASE_URL = 'https://canlnrismlirrnfqpqmv.supabase.co';
const SUPABASE_KEY = 'sb_publishable_kmsN-gDTrMOuhLIBPOMEdA_pi-LqN3p';   // the public read-only key the website uses
const RPC = 'public_snapshot';
const DEFAULT_MAX_MINUTES = 20;     // matches the quiet-Pi alert (alert_settings.quiet_minutes)
const ACTIVE_DAYS = 7;              // stations silent longer than this are treated as not deployed
const SHARE_SECONDS = 30;
const TIMEOUT_MS = 8000;

function answer(status, body, extra = {}) {
  return new Response(JSON.stringify(body, null, 1), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff', ...extra }
  });
}

async function snapshot(env) {
  const base = (env && env.SUPABASE_URL) || SUPABASE_URL, key = (env && env.SUPABASE_KEY) || SUPABASE_KEY;
  const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(`${base}/rest/v1/rpc/${RPC}`, {
      method: 'POST',
      headers: { apikey: key, Authorization: `Bearer ${key}`, 'content-type': 'application/json', accept: 'application/json' },
      body: '{}',
      signal: ctl.signal
    });
    if (!res.ok) throw new Error(`database answered HTTP ${res.status}`);
    const data = await res.json();
    if (!data || !Array.isArray(data.stations)) throw new Error('database answered with something unexpected');
    return data;
  } finally { clearTimeout(timer); }
}

export function judge(data, now, station, maxMinutes) {
  const ageMin = ts => { const t = Date.parse(ts || ''); return Number.isFinite(t) ? Math.max(0, (now - t) / 60000) : null; };
  const rows = data.stations
    .filter(s => s && typeof s.id === 'string')
    .map(s => ({ id: s.id, last_reading: s.ts || null, minutes_ago: ageMin(s.ts) }));
  const pick = station ? rows.filter(r => r.id === station) : rows.filter(r => r.minutes_ago !== null && r.minutes_ago <= ACTIVE_DAYS * 1440);
  if (station && !pick.length) return { status: 404, body: { ok: false, problem: `no active station called ${station}` } };
  const stations = pick.map(r => ({ ...r, minutes_ago: r.minutes_ago === null ? null : Math.round(r.minutes_ago), ok: r.minutes_ago !== null && r.minutes_ago <= maxMinutes }));
  const quiet = stations.filter(s => !s.ok).map(s => s.id);
  let problem = null;
  if (!stations.length) problem = `no station has reported in the last ${ACTIVE_DAYS} days`;
  else if (quiet.length) problem = `no reading for over ${maxMinutes} minutes from: ${quiet.join(', ')}`;
  return { status: problem ? 503 : 200, body: { ok: !problem, problem, max_minutes: maxMinutes, checked_at: new Date(now).toISOString(), stations } };
}

export async function onRequest(context) {
  const { request, env, waitUntil } = context;
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    return new Response('Method not allowed', { status: 405, headers: { allow: 'GET, HEAD', 'cache-control': 'no-store' } });
  }
  const url = new URL(request.url);
  const station = (url.searchParams.get('station') || '').trim().slice(0, 64) || null;
  const asked = parseInt(url.searchParams.get('max_minutes') || '', 10);
  const maxMinutes = Number.isFinite(asked) ? Math.min(1440, Math.max(5, asked)) : DEFAULT_MAX_MINUTES;

  // Only these two parameters make a different cache entry, so random query strings cannot bypass it.
  const key = new Request(`${url.origin}/__health?station=${encodeURIComponent(station || '')}&max=${maxMinutes}`);
  const cache = typeof caches !== 'undefined' ? caches.default : null;
  if (cache) {
    const hit = await cache.match(key);
    if (hit) {
      const out = new Response(request.method === 'HEAD' ? null : hit.body, hit);
      out.headers.set('cache-control', 'no-store');
      return out;
    }
  }

  let result;
  try {
    result = judge(await snapshot(env), Date.now(), station, maxMinutes);
  } catch (err) {
    result = { status: 503, body: { ok: false, problem: `database unreachable: ${err && err.message ? err.message : 'error'}`, checked_at: new Date(Date.now()).toISOString() } };
  }
  const res = answer(result.status, result.body);
  if (cache) {
    const keep = cache.put(key, new Response(JSON.stringify(result.body, null, 1), {
      status: result.status,
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': `public, max-age=${SHARE_SECONDS}`, 'x-content-type-options': 'nosniff' }
    })).catch(() => {});
    if (waitUntil) waitUntil(keep); else await keep;
  }
  return request.method === 'HEAD' ? new Response(null, { status: res.status, headers: res.headers }) : res;
}
