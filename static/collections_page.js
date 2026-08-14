// Field Collections dashboard — migrated from eleganskyboda.com/admin.
// Pulls the mobile backend's boss/today (agents + officers, passed/%) through
// the portal's /api/m6pm proxy and renders it in Metronic.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var data = { agents: [], summary: {} };

  function pctColor(p) { return p >= 80 ? 'var(--color-success)' : p >= 50 ? 'var(--color-warning)' : 'var(--color-destructive)'; }
  function fmt(n) { return (n == null) ? '—' : Number(n).toLocaleString(); }
  var _PILL = { success: 'success', warning: 'warning', destructive: 'danger', info: 'info', secondary: 'muted', primary: 'info' };
  function badge(txt, kind) { return '<span class="epill epill-' + (_PILL[kind] || 'muted') + '">' + txt + '</span>'; }

  function render() {
    var s = data.summary || {};
    var total = s.total_customers || 0, passed = s.total_passed || 0;
    var pct = total ? Math.round(passed / total * 100) : 0;
    $('kpi_total').textContent = fmt(total);
    $('kpi_passed').innerHTML = '<span style="color:var(--color-success)">' + fmt(passed) + '</span>';
    $('kpi_pct').innerHTML = '<span style="color:' + pctColor(pct) + '">' + pct + '%</span>';

    var roleF = $('col_role').value, q = ($('col_search').value || '').trim().toLowerCase();
    var rows = (data.agents || []).filter(function (a) {
      if (roleF !== 'all' && a.role !== roleF) return false;
      if (q && (a.name || '').toLowerCase().indexOf(q) === -1) return false;
      return true;
    });
    // Only count people who actually have customers as "working"
    $('kpi_people').textContent = fmt((data.agents || []).filter(function (a) { return (a.total || 0) > 0; }).length);

    if (!rows.length) { $('col_body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-secondary-foreground">no one matches</td></tr>'; return; }
    // busiest first
    rows.sort(function (a, b) { return (b.total || 0) - (a.total || 0); });
    $('col_body').innerHTML = rows.map(function (a) {
      var t = a.total || 0, p = a.passed || 0, pc = t ? Math.round(p / t * 100) : 0;
      return '<tr class="hover:bg-muted/40 cursor-pointer" onclick="colOpen(' + JSON.stringify(a.name) + ',' + JSON.stringify(a.role) + ')">' +
        '<td class="ps-5 py-2 font-medium">' + (a.name || '—') + '</td>' +
        '<td>' + badge(a.role === 'officer' ? 'Officer' : 'Agent', a.role === 'officer' ? 'info' : 'secondary') + '</td>' +
        '<td class="text-end font-mono">' + fmt(t) + '</td>' +
        '<td class="text-end font-mono">' + fmt(p) + '</td>' +
        '<td class="text-end pe-5">' + (t ? badge(pc + '%', pc >= 80 ? 'success' : pc >= 50 ? 'warning' : 'destructive') : '—') + '</td>' +
        '</tr>';
    }).join('');
  }

  window.colOpen = function (name, role) {
    // Drill-down page ships next; for now go to the recordings filtered to them.
    window.location.href = '/home/recordings?caller=' + encodeURIComponent(name);
  };

  function load() {
    $('col_refresh').disabled = true;
    // Instant render from the browser's last-saved copy, then swap in fresh
    // data the moment it lands. PortalSWR falls back to a plain fetch if the
    // helper isn't present. Keeps the dashboard from ever waiting ~1s twice.
    var apply = function (d) {
      data.agents = d.agents || (Array.isArray(d) ? d : []);
      data.summary = d.summary || {};
      render();
    };
    if (window.PortalSWR) {
      PortalSWR.load('collections:today', '/api/m6pm/mobile/boss/today', apply,
        function (e) { if (!data.agents.length) $('col_body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-destructive">' + (e.message || 'error') + '</td></tr>'; }
      ).finally(function () { $('col_refresh').disabled = false; });
      return;
    }
    fetch('/api/m6pm/mobile/boss/today', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(apply)
      .catch(function (e) { $('col_body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-destructive">' + (e.message || 'error') + '</td></tr>'; })
      .finally(function () { $('col_refresh').disabled = false; });
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('col_role').addEventListener('change', render);
    $('col_search').addEventListener('input', render);
    $('col_refresh').addEventListener('click', load);
    load();
  });
})();
