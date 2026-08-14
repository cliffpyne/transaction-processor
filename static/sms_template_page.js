// SMS Template editor — migrated from eleganskyboda.com/admin.
// Edit full + partial templates (max 480), insert placeholders, reset to
// default, save, and live-preview against real customers. Via /api/m6pm proxy.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var VARS = ['{name}', '{amount}', '{plate}', '{day}', '{breakdown}', '{total}', '{officer_phone}'];
  var defaults = { full: '', part: '' };
  var lastFocused = null; // which textarea to insert placeholders into

  function esc(s) { return (s == null ? '' : String(s)).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  function status(kind, html) { var b = $('tpl-status'); b.className = 'mb-4 kt-alert kt-alert-' + kind; b.innerHTML = html; b.classList.remove('hidden'); }
  function clearStatus() { $('tpl-status').classList.add('hidden'); }

  function counts() {
    $('tpl-full-count').textContent = ($('tpl-full').value || '').length;
    $('tpl-part-count').textContent = ($('tpl-part').value || '').length;
  }

  function insertVar(v) {
    var ta = lastFocused || $('tpl-full');
    var s = ta.selectionStart || ta.value.length, e = ta.selectionEnd || ta.value.length;
    ta.value = ta.value.slice(0, s) + v + ta.value.slice(e);
    ta.focus(); ta.selectionStart = ta.selectionEnd = s + v.length;
    counts();
  }

  function renderVars() {
    $('tpl-vars').innerHTML = VARS.map(function (v) {
      return '<button type="button" class="kt-badge kt-badge-outline kt-badge-sm font-mono cursor-pointer" data-var="' + esc(v) + '">' + esc(v) + '</button>';
    }).join('');
    Array.prototype.forEach.call($('tpl-vars').querySelectorAll('button[data-var]'), function (b) {
      b.addEventListener('click', function () { insertVar(b.getAttribute('data-var')); });
    });
  }

  function load() {
    clearStatus();
    fetch('/api/m6pm/admin/sms/template', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        $('tpl-full').value = d.template || d.default || '';
        $('tpl-part').value = d.template_partial || d.default_partial || '';
        defaults.full = d.default || '';
        defaults.part = d.default_partial || '';
        $('tpl-invoice').textContent = (d.daily_invoice != null ? Number(d.daily_invoice).toLocaleString() : '—');
        counts();
      })
      .catch(function (e) { status('destructive', 'Could not load templates: ' + esc(e.message || 'error')); });
  }

  function save() {
    var full = ($('tpl-full').value || '').trim(), part = ($('tpl-part').value || '').trim();
    if (!full || !part) { status('destructive', 'Both templates must be non-empty.'); return; }
    if (full.length > 480 || part.length > 480) { status('destructive', 'Templates must be ≤ 480 characters.'); return; }
    var btn = $('tpl-save'); btn.disabled = true;
    status('warning', '<span class="kt-spinner kt-spinner-sm me-2"></span> Saving…');
    fetch('/api/m6pm/admin/sms/template', {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ template: full, template_partial: part }),
    })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (!res.ok || res.j.error) { status('destructive', 'Save failed: ' + esc(res.j.error || 'error')); return; }
        status('success', 'Templates saved. New messages use them immediately.');
      })
      .catch(function (e) { status('destructive', 'Save failed: ' + esc(e.message || 'error')); })
      .finally(function () { btn.disabled = false; });
  }

  function preview() {
    var mode = $('tpl-prev-mode').value;
    $('tpl-prev-body').innerHTML = '<span class="kt-spinner kt-spinner-sm me-2"></span> rendering…';
    fetch('/api/m6pm/admin/sms/preview?mode=' + encodeURIComponent(mode), { credentials: 'same-origin' })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (!res.ok || res.j.error) { $('tpl-prev-body').innerHTML = '<span class="text-destructive">' + esc(res.j.error || 'error') + '</span>'; return; }
        var d = res.j, sample = d.sample || [];
        if (!sample.length) { $('tpl-prev-body').innerHTML = '<span class="text-secondary-foreground">no eligible customers to preview</span>'; return; }
        $('tpl-prev-body').innerHTML =
          '<div class="text-xs text-secondary-foreground mb-2">' + (d.total || 0) + ' eligible in session ' + esc(d.session_id) + ' · showing ' + sample.length + '</div>' +
          sample.map(function (m) {
            return '<div class="p-3 mb-2 rounded-lg border border-border bg-muted/30">' +
              '<div class="text-xs text-secondary-foreground font-mono mb-1">' + esc(m.customer_name || '') + ' · ' + esc(m.phone || '') + ' · ' + esc(m.plate || '') + '</div>' +
              '<div class="text-mono whitespace-pre-wrap">' + esc(m.message || '') + '</div>' +
              '<div class="text-xs text-secondary-foreground mt-1">' + (m.message || '').length + ' chars</div></div>';
          }).join('');
      })
      .catch(function (e) { $('tpl-prev-body').innerHTML = '<span class="text-destructive">' + esc(e.message || 'error') + '</span>'; });
  }

  document.addEventListener('DOMContentLoaded', function () {
    renderVars();
    ['tpl-full', 'tpl-part'].forEach(function (id) {
      var ta = $(id);
      ta.addEventListener('focus', function () { lastFocused = ta; });
      ta.addEventListener('input', counts);
    });
    lastFocused = $('tpl-full');
    $('tpl-full-default').addEventListener('click', function () { $('tpl-full').value = defaults.full; counts(); });
    $('tpl-part-default').addEventListener('click', function () { $('tpl-part').value = defaults.part; counts(); });
    $('tpl-save').addEventListener('click', save);
    $('tpl-reload').addEventListener('click', load);
    $('tpl-prev-run').addEventListener('click', preview);
    load();
  });
})();
