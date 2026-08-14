// Team & Officers — migrated from eleganskyboda.com/admin.
// Manage agents/officers (role, password, hide/unhide), create officers, and
// big-customer routing (officer-assignments). All via the /api/m6pm proxy.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var agents = [];                 // full list-agents rows
  var assign = { assignments: {}, officers: [], agents: [] };
  var modalMode = 'password';      // 'password' | 'create'
  var modalTarget = null;

  function esc(s) { return (s == null ? '' : String(s)).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  function badge(t, k) { return '<span class="kt-badge kt-badge-sm kt-badge-' + k + '">' + esc(t) + '</span>'; }
  function status(kind, html) { var b = $('ofc-status'); b.className = 'mb-4 kt-alert kt-alert-' + kind; b.innerHTML = html; b.classList.remove('hidden'); }

  function get(path) { return fetch('/api/m6pm' + path, { credentials: 'same-origin' }).then(function (r) { return r.json(); }); }
  function send(path, method, body) {
    return fetch('/api/m6pm' + path, {
      method: method, credentials: 'same-origin',
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    }).then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); });
  }

  // ── Team table ─────────────────────────────────────────────────────────────
  function renderAgents() {
    var roleF = $('ofc-role-filter').value, q = ($('ofc-search').value || '').trim().toLowerCase();
    var rows = agents.filter(function (a) {
      if (a.excluded) return false; // hide system/excluded rows
      if (roleF !== 'all' && a.role !== roleF) return false;
      if (q && (a.name || '').toLowerCase().indexOf(q) === -1) return false;
      return true;
    });
    if (!rows.length) { $('ofc-body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-secondary-foreground">no one matches</td></tr>'; return; }
    $('ofc-body').innerHTML = rows.map(function (a) {
      var st = a.needs_password ? badge('needs password', 'warning')
             : a.hidden ? badge('hidden', 'secondary')
             : a.active ? badge('active', 'success') : badge('inactive', 'secondary');
      var otherRole = a.role === 'officer' ? 'agent' : 'officer';
      return '<tr class="hover:bg-muted/40">' +
        '<td class="ps-5 py-2 font-medium">' + esc(a.name) + '</td>' +
        '<td>' + badge(a.role === 'officer' ? 'Officer' : 'Agent', a.role === 'officer' ? 'info' : 'secondary') + '</td>' +
        '<td class="font-mono text-secondary-foreground">' + esc(a.phone || '—') + '</td>' +
        '<td>' + st + '</td>' +
        '<td class="pe-5 text-end whitespace-nowrap">' +
          '<button class="kt-btn kt-btn-xs kt-btn-outline" data-act="pass" data-name="' + esc(a.name) + '"><i class="ki-filled ki-lock-2"></i> Password</button> ' +
          '<button class="kt-btn kt-btn-xs kt-btn-outline" data-act="role" data-name="' + esc(a.name) + '" data-role="' + otherRole + '">Make ' + otherRole + '</button> ' +
          '<button class="kt-btn kt-btn-xs kt-btn-ghost" data-act="hide" data-name="' + esc(a.name) + '">' + (a.hidden ? 'Unhide' : 'Hide') + '</button>' +
        '</td></tr>';
    }).join('');
    Array.prototype.forEach.call($('ofc-body').querySelectorAll('button[data-act]'), function (b) {
      b.addEventListener('click', function () { rowAction(b.getAttribute('data-act'), b.getAttribute('data-name'), b.getAttribute('data-role')); });
    });
  }

  function rowAction(act, name, role) {
    if (act === 'pass') { openModal('password', name); return; }
    if (act === 'role') {
      if (!confirm('Change ' + name + ' to ' + role + '?')) return;
      send('/admin/set-role', 'POST', { name: name, role: role }).then(function (res) {
        if (!res.ok || res.j.error) { status('destructive', 'Role change failed: ' + esc((res.j && res.j.error) || 'error')); return; }
        status('success', name + ' is now ' + role + '. They must log out/in for it to take effect.');
        loadAgents();
      });
      return;
    }
    if (act === 'hide') {
      send('/mobile/boss/agents/' + encodeURIComponent(name) + '/toggle-hidden', 'POST', {}).then(function (res) {
        if (!res.ok || (res.j && res.j.error)) { status('destructive', 'Toggle failed: ' + esc((res.j && res.j.error) || 'error')); return; }
        loadAgents();
      });
    }
  }

  // ── Modal (set password / create officer) ──────────────────────────────────
  function openModal(mode, name) {
    modalMode = mode; modalTarget = name || null;
    $('ofc-modal-err').classList.add('hidden');
    $('ofc-modal-pass').value = '';
    if (mode === 'create') {
      $('ofc-modal-title').textContent = 'New officer';
      $('ofc-modal-namewrap').classList.remove('hidden');
      $('ofc-modal-name').value = '';
      $('ofc-modal-forlbl').textContent = 'Password';
    } else {
      $('ofc-modal-title').textContent = 'Set password';
      $('ofc-modal-namewrap').classList.add('hidden');
      $('ofc-modal-forlbl').textContent = 'New password for ' + name;
    }
    if (window.KTModal) { (KTModal.getInstance($('ofc-modal')) || new KTModal($('ofc-modal'))).show(); }
  }
  function modalSave() {
    var pass = $('ofc-modal-pass').value || '';
    if (pass.length < 4) { $('ofc-modal-err').textContent = 'Password must be at least 4 characters'; $('ofc-modal-err').classList.remove('hidden'); return; }
    var req;
    if (modalMode === 'create') {
      var nm = ($('ofc-modal-name').value || '').trim();
      if (!nm) { $('ofc-modal-err').textContent = 'Officer name required'; $('ofc-modal-err').classList.remove('hidden'); return; }
      req = send('/mobile/boss/officers/create', 'POST', { name: nm, password: pass });
    } else {
      req = send('/admin/set-password', 'POST', { name: modalTarget, password: pass });
    }
    $('ofc-modal-save').disabled = true;
    req.then(function (res) {
      if (!res.ok || res.j.error) { $('ofc-modal-err').textContent = res.j.error || 'failed'; $('ofc-modal-err').classList.remove('hidden'); return; }
      if (window.KTModal) { (KTModal.getInstance($('ofc-modal')) || new KTModal($('ofc-modal'))).hide(); }
      status('success', modalMode === 'create' ? 'Officer created.' : 'Password updated.');
      loadAgents();
    }).finally(function () { $('ofc-modal-save').disabled = false; });
  }

  // ── Big-customer routing ───────────────────────────────────────────────────
  function renderAssignments() {
    $('asg-agent').innerHTML = '<option value="">Agent…</option>' + (assign.agents || []).map(function (a) { return '<option value="' + esc(a) + '">' + esc(a) + '</option>'; }).join('');
    $('asg-officer').innerHTML = '<option value="">Officer…</option>' + (assign.officers || []).map(function (o) { return '<option value="' + esc(o) + '">' + esc(o) + '</option>'; }).join('');
    var map = assign.assignments || {}, keys = Object.keys(map);
    if (!keys.length) { $('asg-body').innerHTML = '<tr><td colspan="3" class="text-center p-4 text-secondary-foreground">no routing set — agents’ big customers stay with them</td></tr>'; return; }
    $('asg-body').innerHTML = keys.sort().map(function (agent) {
      return '<tr class="hover:bg-muted/40"><td class="ps-5 py-2 font-medium">' + esc(agent) + '</td>' +
        '<td>' + badge(map[agent], 'info') + '</td>' +
        '<td class="pe-5 text-end"><button class="kt-btn kt-btn-xs kt-btn-ghost text-destructive" data-del="' + esc(agent) + '"><i class="ki-filled ki-trash"></i> Remove</button></td></tr>';
    }).join('');
    Array.prototype.forEach.call($('asg-body').querySelectorAll('button[data-del]'), function (b) {
      b.addEventListener('click', function () {
        var agent = b.getAttribute('data-del');
        send('/mobile/boss/officer-assignments/' + encodeURIComponent(agent), 'DELETE').then(function () { loadAssignments(); });
      });
    });
  }
  function addAssignment() {
    var agent = $('asg-agent').value, officer = $('asg-officer').value;
    if (!agent || !officer) { status('warning', 'Pick both an agent and an officer.'); return; }
    send('/mobile/boss/officer-assignments', 'POST', { agent_name: agent, officer_name: officer }).then(function (res) {
      if (!res.ok || (res.j && res.j.error)) { status('destructive', 'Failed: ' + esc((res.j && res.j.error) || 'error')); return; }
      status('success', agent + '’s big customers now route to ' + officer + '.');
      loadAssignments();
    });
  }

  // ── Loaders ────────────────────────────────────────────────────────────────
  function loadAgents() {
    // Use boss/agents (agent+officer, SQL-excluded) — /admin/list-agents 500s on
    // the backend (it selects a non-existent phone column). This shape has no
    // phone/excluded fields; renderAgents handles their absence gracefully.
    get('/mobile/boss/agents').then(function (rows) { agents = Array.isArray(rows) ? rows : []; renderAgents(); })
      .catch(function (e) { $('ofc-body').innerHTML = '<tr><td colspan="5" class="text-center p-4 text-destructive">' + esc(e.message || 'error') + '</td></tr>'; });
  }
  function loadAssignments() {
    get('/mobile/boss/officer-assignments').then(function (d) { assign = d || assign; renderAssignments(); })
      .catch(function (e) { $('asg-body').innerHTML = '<tr><td colspan="3" class="text-center p-4 text-destructive">' + esc(e.message || 'error') + '</td></tr>'; });
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('ofc-role-filter').addEventListener('change', renderAgents);
    $('ofc-search').addEventListener('input', renderAgents);
    $('ofc-reload').addEventListener('click', function () { loadAgents(); loadAssignments(); });
    $('ofc-create').addEventListener('click', function () { openModal('create'); });
    $('ofc-modal-save').addEventListener('click', modalSave);
    $('asg-add').addEventListener('click', addAssignment);
    loadAgents();
    loadAssignments();
  });
})();
