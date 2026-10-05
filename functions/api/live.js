// Cloudflare Pages Function: GET /api/live
//
// The live snapshot: the newest reading of every station, its loud-event counts for the last hour and day, and the 311 total.
// The map and the station panels read this instead of asking Supabase about each station.
//
// How it works: the first visitor in a Cloudflare data center asks Supabase (one request, one function call, a few kilobytes) and
// Cloudflare keeps the answer for 15 seconds; everyone else in that data center is served that copy from the edge, so Supabase egress
// does not grow with visitors. If Supabase is down or slow, the last good copy (kept 10 minutes) is served instead, marked with the
// header "x-edge-cache: STALE", so the site keeps working. If there is no copy at all the answer is a 503 and the page falls back to
// asking Supabase directly, as it did before this function existed.
//
// Each Cloudflare data center keeps its own copy, so Supabase is asked about once per 15 seconds per data center that has visitors.
// Needs: the database function public_snapshot (edge_snapshot.sql). Optional Pages settings (Settings > Variables and Secrets):
// SUPABASE_URL and SUPABASE_KEY, which default to the same public address and read-only key the site itself uses.
// The Cache API only caches on a custom domain: on a *.pages.dev preview address every request asks Supabase.

const SUPABASE_URL = 'https://canlnrismlirrnfqpqmv.supabase.co';
const SUPABASE_KEY = 'sb_publishable_kmsN-gDTrMOuhLIBPOMEdA_pi-LqN3p';   // the public key the website uses; it can only read what the public may read
const CACHE_PATH = '/api/live';
const RPC = 'public_snapshot';
const RPC_ARGS = {};
const FRESH_SECONDS = 15;       // one copy is shared this long before Supabase is asked again
const STALE_SECONDS = 600;       // the last good copy is kept this long, to serve if Supabase is unavailable
const BROWSER_SECONDS = 5;   // how long a visitor's own browser may reuse an answer
const DEFAULT_TIMEOUT_MS = 8000;

const validShape = data => !!data && typeof data === 'object' && Array.isArray(data.stations);

async function askSupabase(env) {
  const base = (env && env.SUPABASE_URL) || SUPABASE_URL, key = (env && env.SUPABASE_KEY) || SUPABASE_KEY;
  const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), Number(env && env.UPSTREAM_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS);
  try {
    const res = await fetch(`${base}/rest/v1/rpc/${RPC}`, {
      method: 'POST',
      headers: { apikey: key, Authorization: `Bearer ${key}`, 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify(RPC_ARGS),
      signal: ctl.signal
    });
    if (!res.ok) throw new Error(`Supabase answered HTTP ${res.status}`);
    const data = await res.json();
    if (!validShape(data)) throw new Error('Supabase answered with something unexpected');
    return data;
  } finally { clearTimeout(timer); }
}

function deliver(res, state) {
  const out = new Response(res.body, res);
  const made = res.headers.get('x-edge-generated');
  out.headers.set('cache-control', `public, max-age=${BROWSER_SECONDS}`);
  out.headers.set('x-edge-cache', state);
  out.headers.set('x-content-type-options', 'nosniff');
  if (made) out.headers.set('x-edge-age', String(Math.max(0, Math.round((Date.now() - Date.parse(made)) / 1000))));
  return out;
}

export async function onRequest(context) {
  const { request, env, waitUntil } = context;
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    return new Response('Method not allowed', { status: 405, headers: { allow: 'GET, HEAD', 'cache-control': 'no-store' } });
  }
  const cache = caches.default, origin = new URL(request.url).origin;
  // The keys ignore the query string, so adding ?x=1 cannot get around the cache and make Supabase work harder.
  const freshKey = new Request(`${origin}${CACHE_PATH}`), staleKey = new Request(`${origin}/__edge_stale${CACHE_PATH}`);

  const hit = await cache.match(freshKey);
  if (hit) return deliver(hit, 'HIT');

  try {
    const body = JSON.stringify(await askSupabase(env)), made = new Date(Date.now()).toISOString();
    const headers = ttl => ({ 'content-type': 'application/json; charset=utf-8', 'cache-control': `public, max-age=${ttl}`, 'x-edge-generated': made });
    const fresh = new Response(body, { headers: headers(FRESH_SECONDS) });
    const keep = [cache.put(freshKey, fresh.clone()), cache.put(staleKey, new Response(body, { headers: headers(STALE_SECONDS) }))];
    if (waitUntil) waitUntil(Promise.all(keep).catch(() => {})); else await Promise.all(keep).catch(() => {});
    return deliver(fresh, 'MISS');
  } catch (err) {
    const old = await cache.match(staleKey);
    if (old) return deliver(old, 'STALE');
    return new Response(JSON.stringify({ error: 'unavailable' }), {
      status: 503, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'retry-after': '15' }
    });
  }
}
