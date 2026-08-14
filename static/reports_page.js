// Daily Reports — migrated from eleganskyboda.com/admin.
// Lists per-agent Excel reports for a date, downloads via the /api/m6pm proxy,
// and re-runs generation on demand. Files stream through the proxy (bytes).
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };

  function todayISO() { var d = new Date(); return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'); }
  function esc(s) { return (s == null ? '' : String(s)).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  function human(bytes) {
    if (bytes == null) return '—';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(2) + ' MB';
  }
  // Download URL through the proxy — the `download` attr on the anchor names
  // the file (the proxy passes Content-Type but not the disposition filename).
  function dlUrl(date, name) { return '/api/m6pm/admin/reports/file?date=' + encodeURIComponent(date) + '&name=' + encodeURIComponent(name); }

  function status(kind, html) {
    var box = $('rep-status');
    box.className = 'mb-4 kt-alert kt-alert-' + kind;
    box.innerHTML = html;
    box.classList.remove('hidden');
  }
  function clearStatus() { $('rep-status').classList.add('hidden'); }

  function render(data) {
    var date = data.date || $('rep-date').value;
    // Master workbook card
    if (data.master) {
      $('rep-master-name').textContent = data.master.filename;
      $('rep-master-size').textContent = human(data.master.size);
      $('rep-master-dl').href = dlUrl(date, data.master.filename);
      $('rep-master-dl').setAttribute('download', data.master.filename);
      $('rep-master-wrap').classList.remove('hidden');
    } else {
      $('rep-master-wrap').classList.add('hidden');
    }
    // Per-agent files
    var files = data.files || [];
    if (!files.length) {
      $('rep-body').innerHTML = '<tr><td colspan="3" class="text-center p-4 text-secondary-foreground">no reports for this date — hit “Generate now” to build them</td></tr>';
      return;
    }
    $('rep-body').innerHTML = files.map(function (f) {
      return '<tr class="hover:bg-muted/40">' +
        '<td class="ps-5 py-2 font-medium">' + esc(f.filename) + '</td>' +
        '<td class="text-end font-mono text-secondary-foreground">' + human(f.size) + '</td>' +
        '<td class="pe-5 text-end"><a class="kt-btn kt-btn-sm kt-btn-outline" href="' + dlUrl(date, f.filename) + '" download="' + esc(f.filename) + '"><i class="ki-filled ki-exit-down"></i> Download</a></td></tr>';
    }).join('');
  }

  function load() {
    var date = $('rep-date').value || todayISO();
    $('rep-body').innerHTML = '<tr><td colspan="3" class="text-center p-4"><span class="kt-spinner"></span></td></tr>';
    fetch('/api/m6pm/admin/reports/list?date=' + encodeURIComponent(date), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(render)
      .catch(function (e) { $('rep-body').innerHTML = '<tr><td colspan="3" class="text-center p-4 text-destructive">' + esc(e.message || 'error') + '</td></tr>'; });
  }

  function generate() {
    var date = $('rep-date').value || todayISO();
    var btn = $('rep-generate'); btn.disabled = true;
    status('warning', '<span class="kt-spinner kt-spinner-sm me-2"></span> Generating reports for ' + esc(date) + '… this can take a minute.');
    fetch('/api/m6pm/admin/reports/generate', {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: date }),
    })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (!res.ok || res.j.error) { status('destructive', 'Generation failed: ' + esc(res.j.error || 'error')); return; }
        status('success', 'Done — ' + (res.j.file_count || 0) + ' agent report(s), ' + (res.j.total_rows || 0) + ' rows. Master: ' + esc(res.j.master_name || '—'));
        load();
      })
      .catch(function (e) { status('destructive', 'Generation failed: ' + esc(e.message || 'error')); })
      .finally(function () { btn.disabled = false; });
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('rep-date').value = todayISO();
    $('rep-refresh').addEventListener('click', function () { clearStatus(); load(); });
    $('rep-date').addEventListener('change', function () { clearStatus(); load(); });
    $('rep-generate').addEventListener('click', generate);
    load();
  });
})();
