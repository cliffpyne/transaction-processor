// Field Sessions drill-down — migrated from eleganskyboda.com/admin.
// agents  →  that agent's sessions for a date  →  session customers  →  call history.
// All data via the /api/m6pm proxy (boss JWT). Reuses Metronic table + modal.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  // Navigation state: which level we're on + the drill context.
  var state = { level: 'agents', agent: null, role: null, session: null, label: null };
  var cache = { agents: [], sessions: [], customers: [] };

  function todayISO() { var d = new Date(); return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'); }
  function esc(s) { return (s == null ? '' : String(s)).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  function badge(t, k) { return '<span class="kt-badge kt-badge-sm kt-badge-' + k + '">' + esc(t) + '</span>'; }
  function fmt(n) { return (n == null) ? '—' : Number(n).toLocaleString(); }
  function num(v) { return parseInt(v, 10) || 0; }

  // ── Pass/fail rule mirrors the app: reached (>10s answered) OR (3 trials + SMS) ──
  function isReached(c) { return !!c.has_real_reach || (num(c.duration_seconds) > 10 && (c.call_status === 'called' || c.call_status === 'called_back')); }
  function isPassed(c) {
    var maxed = num(c.outgoing_attempt_count) >= 3;
    return isReached(c) || (maxed && !!c.sms_sent_at);
  }

  function get(path) {
    return fetch('/api/m6pm' + path, { credentials: 'same-origin' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  function spinner() { $('ses-body').innerHTML = '<tr><td class="text-center p-4"><span class="kt-spinner"></span></td></tr>'; }
  function errRow(e, cols) { $('ses-body').innerHTML = '<tr><td colspan="' + cols + '" class="text-center p-4 text-destructive">' + esc(e.message || 'error') + '</td></tr>'; }

  // ── Breadcrumb ─────────────────────────────────────────────────────────────
  function crumb() {
    var parts = ['<a href="#" class="hover:text-primary" data-level="agents">All agents</a>'];
    if (state.agent) parts.push('<i class="ki-filled ki-right text-xs"></i><a href="#" class="hover:text-primary" data-level="sessions">' + esc(state.agent) + '</a>');
    if (state.session) parts.push('<i class="ki-filled ki-right text-xs"></i><span class="text-mono">' + esc(state.label || ('Session ' + state.session)) + '</span>');
    $('ses-crumb').innerHTML = parts.join(' ');
    Array.prototype.forEach.call($('ses-crumb').querySelectorAll('a[data-level]'), function (a) {
      a.addEventListener('click', function (ev) {
        ev.preventDefault();
        var lvl = a.getAttribute('data-level');
        if (lvl === 'agents') { state.agent = null; state.role = null; state.session = null; state.label = null; loadAgents(); }
        else if (lvl === 'sessions') { state.session = null; state.label = null; loadSessions(); }
      });
    });
  }

  // ── Level 1: agents ────────────────────────────────────────────────────────
  function loadAgents() {
    state.level = 'agents'; crumb();
    $('ses-head').innerHTML = '<tr class="text-secondary-foreground"><th class="text-start py-2.5 ps-5">Name</th><th>Role</th><th>Status</th><th class="pe-5"></th></tr>';
    var apply = function (rows) { cache.agents = Array.isArray(rows) ? rows : []; renderAgents(); };
    if (window.PortalSWR) {
      PortalSWR.load('sessions:agents', '/api/m6pm/mobile/boss/agents', apply,
        function (e) { if (!cache.agents.length) errRow(e, 4); });
      return;
    }
    spinner();
    get('/mobile/boss/agents').then(apply).catch(function (e) { errRow(e, 4); });
  }
  function renderAgents() {
    var q = ($('ses-search').value || '').trim().toLowerCase();
    var rows = cache.agents.filter(function (a) { return !q || (a.name || '').toLowerCase().indexOf(q) > -1; });
    if (!rows.length) { $('ses-body').innerHTML = '<tr><td colspan="4" class="text-center p-4 text-secondary-foreground">no agents</td></tr>'; return; }
    $('ses-body').innerHTML = rows.map(function (a) {
      var status = a.needs_password ? badge('needs password', 'warning') : (a.active === false ? badge('inactive', 'secondary') : badge('active', 'success'));
      return '<tr class="hover:bg-muted/40 cursor-pointer" data-agent="' + esc(a.name) + '" data-role="' + esc(a.role) + '">' +
        '<td class="ps-5 py-2 font-medium">' + esc(a.name) + '</td>' +
        '<td>' + badge(a.role === 'officer' ? 'Officer' : 'Agent', a.role === 'officer' ? 'info' : 'secondary') + '</td>' +
        '<td>' + status + '</td>' +
        '<td class="pe-5 text-end text-secondary-foreground"><i class="ki-filled ki-right"></i></td></tr>';
    }).join('');
    bindRows('agent', function (tr) { state.agent = tr.getAttribute('data-agent'); state.role = tr.getAttribute('data-role'); loadSessions(); });
  }

  // ── Level 2: an agent's sessions for the date ──────────────────────────────
  function loadSessions() {
    state.level = 'sessions'; crumb();
    $('ses-head').innerHTML = '<tr class="text-secondary-foreground"><th class="text-start py-2.5 ps-5">Session</th><th>Mode</th><th class="text-end">Customers</th><th class="text-end">Called</th><th class="pe-5 text-end">%</th></tr>';
    spinner();
    get('/mobile/boss/agent/' + encodeURIComponent(state.agent) + '/sessions?date=' + encodeURIComponent($('ses-date').value || todayISO())).then(function (rows) {
      cache.sessions = Array.isArray(rows) ? rows : [];
      renderSessions();
    }).catch(function (e) { errRow(e, 5); });
  }
  function renderSessions() {
    var rows = cache.sessions;
    if (!rows.length) { $('ses-body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-secondary-foreground">no sessions on this date</td></tr>'; return; }
    $('ses-body').innerHTML = rows.map(function (s) {
      var t = num(s.customer_count), c = num(s.called_count), pc = t ? Math.round(c / t * 100) : 0;
      return '<tr class="hover:bg-muted/40 cursor-pointer" data-session="' + s.id + '" data-label="' + esc(s.label || ('Session ' + s.id)) + '">' +
        '<td class="ps-5 py-2 font-medium">' + esc(s.label || ('Session ' + s.id)) + '</td>' +
        '<td>' + badge(s.mode || '—', 'secondary') + '</td>' +
        '<td class="text-end font-mono">' + fmt(t) + '</td>' +
        '<td class="text-end font-mono">' + fmt(c) + '</td>' +
        '<td class="pe-5 text-end">' + (t ? badge(pc + '%', pc >= 80 ? 'success' : pc >= 50 ? 'warning' : 'destructive') : '—') + '</td></tr>';
    }).join('');
    bindRows('session', function (tr) { state.session = tr.getAttribute('data-session'); state.label = tr.getAttribute('data-label'); loadCustomers(); });
  }

  // ── Level 3: customers in a session ────────────────────────────────────────
  function loadCustomers() {
    state.level = 'customers'; crumb();
    $('ses-head').innerHTML = '<tr class="text-secondary-foreground"><th class="text-start py-2.5 ps-5">Customer · Plate</th><th class="text-end">Amount</th><th class="text-end">Trials</th><th>Last status</th><th>SMS</th><th class="pe-5">Result</th></tr>';
    spinner();
    get('/mobile/boss/session/' + encodeURIComponent(state.session) + '?agent=' + encodeURIComponent(state.agent)).then(function (rows) {
      cache.customers = Array.isArray(rows) ? rows : [];
      renderCustomers();
    }).catch(function (e) { errRow(e, 6); });
  }
  function renderCustomers() {
    var q = ($('ses-search').value || '').trim().toLowerCase();
    var rows = cache.customers.filter(function (c) {
      return !q || (c.customer_name || '').toLowerCase().indexOf(q) > -1 || (c.plate || '').toLowerCase().indexOf(q) > -1 || (c.phone || '').toLowerCase().indexOf(q) > -1;
    });
    if (!rows.length) { $('ses-body').innerHTML = '<tr><td colspan="6" class="text-center p-4 text-secondary-foreground">no customers</td></tr>'; return; }
    var sk = { called: 'success', called_back: 'success', not_answered: 'warning', no_airtime: 'destructive', missed_callback: 'warning' };
    $('ses-body').innerHTML = rows.map(function (c, i) {
      var passed = isPassed(c);
      return '<tr class="hover:bg-muted/40 cursor-pointer" data-cust="' + i + '">' +
        '<td class="ps-5 py-2"><div class="font-medium">' + esc(c.customer_name || '—') + (c.excuse_flag ? ' ' + badge('excused', 'destructive') : '') + '</div>' +
          '<div class="text-xs text-secondary-foreground font-mono">' + esc(c.plate || '') + ' · ' + esc(c.phone || '') + '</div></td>' +
        '<td class="text-end font-mono">' + fmt(c.amount) + '</td>' +
        '<td class="text-end font-mono">' + num(c.outgoing_attempt_count) + '/3</td>' +
        '<td>' + (c.call_status ? badge(c.call_status, sk[c.call_status] || 'secondary') : '—') + '</td>' +
        '<td>' + (c.sms_sent_at ? badge('sent', 'info') : '—') + '</td>' +
        '<td class="pe-5">' + (passed ? badge('✓ passed', 'success') : badge('pending', 'secondary')) + '</td></tr>';
    }).join('');
    bindRows('cust', function (tr) { openHistory(cache.customers[num(tr.getAttribute('data-cust'))]); });
  }

  function bindRows(attr, fn) {
    Array.prototype.forEach.call($('ses-body').querySelectorAll('tr[data-' + attr + ']'), function (tr) {
      tr.addEventListener('click', function () { fn(tr); });
    });
  }

  // ── Customer history modal ─────────────────────────────────────────────────
  function openHistory(c) {
    if (!c) return;
    $('ses-hist-title').textContent = c.customer_name || 'Customer history';
    $('ses-hist-body').innerHTML = '<div class="text-center p-4"><span class="kt-spinner"></span></div>';
    if (window.KTModal) { var m = KTModal.getInstance($('ses-hist-modal')) || new KTModal($('ses-hist-modal')); m.show(); }
    get('/mobile/boss/customer-history?session_id=' + encodeURIComponent(state.session) +
        '&customer_name=' + encodeURIComponent(c.customer_name) + '&agent_name=' + encodeURIComponent(state.agent))
      .then(function (h) {
        var att = (h.attempts || []), com = (h.comments || []);
        var sk = { called: 'success', called_back: 'success', not_answered: 'warning', no_airtime: 'destructive', missed_callback: 'warning' };
        var attHtml = att.length ? att.map(function (a) {
          var dur = num(a.duration_seconds), durTxt = dur >= 60 ? Math.floor(dur / 60) + 'm ' + (dur % 60) + 's' : dur + 's';
          var t = a.call_time || a.logged_at;
          return '<div class="flex items-center justify-between py-1.5 border-b border-border last:border-0">' +
            '<span class="text-xs font-mono text-secondary-foreground">' + esc(t ? new Date(t).toLocaleString() : '—') + '</span>' +
            '<span>' + badge(a.call_status || '?', sk[a.call_status] || 'secondary') + '</span>' +
            '<span class="text-xs font-mono">' + durTxt + '</span></div>';
        }).join('') : '<div class="text-secondary-foreground text-sm p-2">no call attempts</div>';
        var comHtml = com.length ? com.map(function (m) {
          return '<div class="py-1.5 border-b border-border last:border-0"><div class="text-sm">' + esc(m.comment_text) + '</div>' +
            '<div class="text-xs text-secondary-foreground">' + esc(m.author_name || '') + ' · ' + esc(m.author_role || '') + ' · ' + esc(m.created_at ? new Date(m.created_at).toLocaleString() : '') + '</div></div>';
        }).join('') : '<div class="text-secondary-foreground text-sm p-2">no comments</div>';
        $('ses-hist-body').innerHTML =
          '<div class="mb-2 text-xs text-secondary-foreground font-mono">' + esc(c.plate || '') + ' · ' + esc(c.phone || '') + ' · ' + fmt(c.amount) + '</div>' +
          '<h4 class="text-sm font-semibold text-mono mt-3 mb-1">Call attempts (' + att.length + ')</h4>' + attHtml +
          '<h4 class="text-sm font-semibold text-mono mt-4 mb-1">Comments (' + com.length + ')</h4>' + comHtml;
      })
      .catch(function (e) { $('ses-hist-body').innerHTML = '<div class="text-destructive p-2">' + esc(e.message || 'error') + '</div>'; });
  }

  // ── Wiring ─────────────────────────────────────────────────────────────────
  function reload() {
    if (state.level === 'agents') loadAgents();
    else if (state.level === 'sessions') loadSessions();
    else loadCustomers();
  }
  function reRender() {
    if (state.level === 'agents') renderAgents();
    else if (state.level === 'customers') renderCustomers();
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('ses-date').value = todayISO();
    $('ses-search').addEventListener('input', reRender);
    $('ses-date').addEventListener('change', function () { if (state.level === 'sessions') loadSessions(); });
    $('ses-refresh').addEventListener('click', reload);
    loadAgents();
  });
})();
