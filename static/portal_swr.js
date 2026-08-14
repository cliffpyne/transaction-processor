// Portal stale-while-revalidate helper — the client half of "Instagram-fast".
// Renders the last-saved copy of a fetch INSTANTLY (0ms, from localStorage),
// then fetches fresh in the background and re-renders when it arrives. Pairs
// with the server-side SWR cache in ui_blueprint.py so the dashboard never
// waits on the ~1s Render round-trip twice for the same data.
(function () {
  'use strict';
  var PREFIX = 'swr:';
  var MAX_AGE_MS = 24 * 60 * 60 * 1000; // drop cached copies older than a day

  function read(key) {
    try {
      var raw = localStorage.getItem(PREFIX + key);
      if (!raw) return null;
      var obj = JSON.parse(raw);
      if (!obj || (Date.now() - (obj.ts || 0)) > MAX_AGE_MS) return null;
      return obj;
    } catch (e) { return null; }
  }
  function write(key, data) {
    try { localStorage.setItem(PREFIX + key, JSON.stringify({ ts: Date.now(), data: data })); }
    catch (e) { /* quota / private mode — cache is best-effort */ }
  }

  window.PortalSWR = {
    // load(cacheKey, url, onData, onError)
    //   onData(data, isCached) — called up to twice: once with the cached copy
    //   (isCached=true) if present, then once with fresh data (isCached=false).
    load: function (cacheKey, url, onData, onError) {
      var cached = read(cacheKey);
      var servedCache = false;
      if (cached && cached.data !== undefined) {
        servedCache = true;
        try { onData(cached.data, true); } catch (e) { /* ignore render error on stale */ }
      }
      return fetch(url, { credentials: 'same-origin' })
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
        .then(function (res) {
          if (!res.ok || (res.j && res.j.error)) {
            // Keep the cached view on screen; only surface an error if we had nothing.
            if (!servedCache && onError) onError(new Error((res.j && res.j.error) || 'request failed'));
            return;
          }
          write(cacheKey, res.j);
          onData(res.j, false);
        })
        .catch(function (e) { if (!servedCache && onError) onError(e); });
    },
    // Expose raw helpers for pages that manage their own accumulation (recordings).
    read: read,
    write: write,
  };
})();
