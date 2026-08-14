// Call Recordings — migrated from eleganskyboda.com/admin into the portal.
// Data via the /api/m6pm proxy: date filter (call date, TZ-correct), pagination,
// caller/role/search filters, and audio streamed through the proxy.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var cache = [], shown = 20;

  function todayISO() { var d = new Date(); return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'); }
  function esc(s) { return (s == null ? '' : String(s)).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  var _PILL = { success: 'success', warning: 'warning', destructive: 'danger', info: 'info', secondary: 'muted', primary: 'info' };
  function badge(t, k) { return '<span class="epill epill-' + (_PILL[k] || 'muted') + '">' + esc(t) + '</span>'; }

  function render() {
    var q = ($('rec-search').value || '').trim().toLowerCase(), roleF = $('rec-role').value, personF = $('rec-person').value;
    var rows = cache.filter(function (r) {
      var role = r.agent_role || 'agent';
      if (roleF !== 'all' && role !== roleF) return false;
      if (personF && r.agent_name !== personF) return false;
      if (!q) return true;
      return (r.customer_name || '').toLowerCase().indexOf(q) > -1 || (r.plate || '').toLowerCase().indexOf(q) > -1 ||
             (r.phone || '').toLowerCase().indexOf(q) > -1 || (r.agent_name || '').toLowerCase().indexOf(q) > -1;
    });
    if (!rows.length) { $('rec-body').innerHTML = '<tr><td colspan="7" class="text-center p-4 text-secondary-foreground">no recordings match</td></tr>'; return; }
    var view = rows.slice(0, shown), step = parseInt($('rec-pagesize').value, 10) || 20;
    var more = rows.length > shown
      ? '<tr><td colspan="7" style="text-align:center;padding:12px"><button class="kt-btn kt-btn-sm kt-btn-outline" id="rec-more">Load ' + Math.min(step, rows.length - shown) + ' more</button> <span class="text-secondary-foreground text-xs ms-2">showing ' + view.length + ' of ' + rows.length + '</span></td></tr>'
      : '<tr><td colspan="7" class="text-center text-secondary-foreground text-xs p-2">all ' + rows.length + ' shown</td></tr>';
    $('rec-body').innerHTML = view.map(function (r) {
      var t = r.call_time ? new Date(r.call_time) : (r.uploaded_at ? new Date(r.uploaded_at) : null);
      var ts = t ? t.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', month: 'short', day: '2-digit' }) : '—';
      var dur = r.duration_seconds ? (r.duration_seconds >= 60 ? Math.floor(r.duration_seconds / 60) + 'm ' + (r.duration_seconds % 60) + 's' : r.duration_seconds + 's') : '—';
      var sk = ({ called: 'success', called_back: 'info', not_answered: 'warning', no_airtime: 'destructive' })[r.call_status] || 'secondary';
      return '<tr><td class="ps-5 py-2 text-xs font-mono">' + esc(ts) + '</td>' +
        '<td>' + esc(r.agent_name || '—') + '</td>' +
        '<td><div class="font-medium">' + esc(r.customer_name || '—') + '</div><div class="text-xs text-secondary-foreground font-mono">' + esc(r.plate || '') + ' · ' + esc(r.phone || '') + '</div></td>' +
        '<td class="text-end font-mono">' + (r.attempt_number || '—') + '</td>' +
        '<td>' + badge(r.call_status || 'unknown', sk) + '</td>' +
        '<td class="text-end font-mono">' + dur + '</td>' +
        '<td class="pe-5"><audio controls preload="none" style="height:34px;width:210px" src="/api/m6pm/mobile/boss/recording/' + r.id + '"></audio></td></tr>';
    }).join('') + more;
    var mb = $('rec-more'); if (mb) mb.addEventListener('click', function () { shown += step; render(); });
  }

  // Set cache, rebuild the caller dropdown, and render. Shared by the instant
  // cached view and the fresh fetch.
  function applyData(all) {
    cache = all || [];
    var callers = Array.from(new Set(cache.map(function (r) { return r.agent_name; }).filter(Boolean))).sort();
    var prev = $('rec-person').value;
    $('rec-person').innerHTML = '<option value="">Any caller</option>' + callers.map(function (a) { return '<option value="' + esc(a) + '"' + (a === prev ? ' selected' : '') + '>' + esc(a) + '</option>'; }).join('');
    var pre = new URLSearchParams(location.search).get('caller'); if (pre && callers.indexOf(pre) > -1) $('rec-person').value = pre;
    render();
  }

  function load() {
    var day = $('rec-date').value || todayISO();
    shown = parseInt($('rec-pagesize').value, 10) || 20;
    var ckey = 'recordings:' + day;
    // Instant paint from the last-saved copy of this day (if any).
    var served = false;
    if (window.PortalSWR) {
      var cached = PortalSWR.read(ckey);
      if (cached && Array.isArray(cached.data)) { served = true; applyData(cached.data); }
    }
    if (!served) $('rec-body').innerHTML = '<tr><td colspan="7" class="text-center p-4"><span class="kt-spinner"></span></td></tr>';
    var all = [], offset = 0;
    (function next() {
      fetch('/api/m6pm/mobile/boss/recordings?date=' + encodeURIComponent(day) + '&limit=1000&offset=' + offset, { credentials: 'same-origin' })
        .then(function (r) { return r.json(); })
        .then(function (page) {
          if (Array.isArray(page) && page.length) {
            all = all.concat(page); offset += 1000;
            if (page.length === 1000 && offset < 50000) return next();
          }
          if (window.PortalSWR) PortalSWR.write(ckey, all);
          applyData(all);
        })
        .catch(function (e) { if (!served) $('rec-body').innerHTML = '<tr><td colspan="7" class="text-center p-4 text-destructive">' + esc(e.message || 'error') + '</td></tr>'; });
    })();
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('rec-date').value = todayISO();
    ['rec-role', 'rec-person', 'rec-search', 'rec-pagesize'].forEach(function (id) { $(id).addEventListener('input', render); $(id).addEventListener('change', render); });
    $('rec-date').addEventListener('change', load);
    $('rec-refresh').addEventListener('click', load);
    load();
  });
})();
