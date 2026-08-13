// App Health dashboard — fetches the mobile backend's /api/admin/metrics (proxied
// by the portal) and renders it with Metronic + ApexCharts. Read-only, polls live.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var timeChart = null, statusChart = null, timer = null;

  function fmt(n) { return (n == null) ? '—' : Number(n).toLocaleString(); }
  function ms(v) { return (v == null) ? '—' : (v >= 1000 ? (v / 1000).toFixed(1) + 's' : Math.round(v) + 'ms'); }
  function pill(txt, kind) { return '<span class="kt-badge kt-badge-sm kt-badge-' + kind + '">' + txt + '</span>'; }
  function rateKind(p) { return p >= 5 ? 'destructive' : p >= 1 ? 'warning' : 'success'; }
  function latKind(v) { return v >= 5000 ? 'destructive' : v >= 2000 ? 'warning' : 'success'; }

  function color(kind) {
    return { success: '#17c653', warning: '#f6b100', destructive: '#dc2626', primary: '#1b84ff' }[kind] || '#1b84ff';
  }

  function render(d) {
    var s = d.summary || {};
    $('kpi_requests').textContent = fmt(s.requests);
    $('kpi_errrate').innerHTML = '<span style="color:' + color(rateKind(s.error_rate || 0)) + '">' + (s.error_rate || 0) + '%</span>';
    $('kpi_cerrrate').textContent = (s.client_error_rate || 0) + '%';
    $('kpi_p95').innerHTML = '<span style="color:' + color(latKind(s.p95_ms || 0)) + '">' + ms(s.p95_ms) + '</span>';
    $('kpi_p99').textContent = ms(s.p99_ms);

    // Traffic + errors over time
    var series = d.series || [];
    var cats = series.map(function (p) { return (p.t || '').slice(11, 16); });
    var reqs = series.map(function (p) { return p.count || 0; });
    var errs = series.map(function (p) { return p.errors || 0; });
    var topts = {
      chart: { type: 'area', height: 280, toolbar: { show: false }, fontFamily: 'inherit' },
      series: [{ name: 'Requests', data: reqs }, { name: 'Errors', data: errs }],
      colors: ['#1b84ff', '#dc2626'], dataLabels: { enabled: false },
      stroke: { curve: 'smooth', width: 2 }, fill: { type: 'gradient', gradient: { opacityFrom: 0.3, opacityTo: 0.02 } },
      xaxis: { categories: cats, labels: { rotate: 0 } }, legend: { position: 'top' },
      tooltip: { theme: document.documentElement.classList.contains('dark') ? 'dark' : 'light' }
    };
    if (timeChart) { timeChart.updateOptions(topts); } else { timeChart = new ApexCharts($('hlt_chart_time'), topts); timeChart.render(); }

    // Status donut
    var st = s.status || {};
    var dopts = {
      chart: { type: 'donut', height: 280, fontFamily: 'inherit' },
      series: [st['2xx'] || 0, st['3xx'] || 0, st['4xx'] || 0, st['5xx'] || 0],
      labels: ['2xx', '3xx', '4xx', '5xx'], colors: ['#17c653', '#1b84ff', '#f6b100', '#dc2626'],
      legend: { position: 'bottom' }, dataLabels: { enabled: true }
    };
    if (statusChart) { statusChart.updateOptions(dopts); } else { statusChart = new ApexCharts($('hlt_chart_status'), dopts); statusChart.render(); }

    // Heartbeats
    var hb = d.heartbeats || [];
    $('hlt_heartbeats').innerHTML = hb.length ? hb.map(function (h) {
      var age = h.ok_age_s;
      var kind = (age == null) ? 'destructive' : (age <= 900 ? 'success' : age <= 3600 ? 'warning' : 'destructive');
      var label = h.name + (age == null ? ' · never' : ' · ' + (age < 90 ? age + 's' : Math.round(age / 60) + 'm') + ' ago');
      return pill(label, kind);
    }).join(' ') : '<span class="text-sm text-secondary-foreground">no heartbeats yet</span>';

    // Per-endpoint
    var eps = d.endpoints || [];
    $('hlt_endpoints').innerHTML = eps.length ? eps.map(function (e) {
      return '<tr><td class="ps-5 py-2 font-mono text-xs">' + e.endpoint + '</td>' +
        '<td>' + pill(e.method, 'outline') + '</td>' +
        '<td class="text-end">' + fmt(e.count) + '</td>' +
        '<td class="text-end">' + pill(e.err_rate + '%', rateKind(e.err_rate)) + '</td>' +
        '<td class="text-end font-mono">' + ms(e.p95_ms) + '</td>' +
        '<td class="text-end pe-5 font-mono">' + ms(e.max_ms) + '</td></tr>';
    }).join('') : '<tr><td colspan="6" class="text-center p-4 text-secondary-foreground">no traffic in window</td></tr>';

    // Recent failures
    var re = d.recent_errors || [];
    $('hlt_errors').innerHTML = re.length ? re.map(function (x) {
      var k = x.status >= 500 ? 'destructive' : 'warning';
      return '<tr><td class="ps-5 py-2 text-xs">' + (x.t || '').slice(11, 19) + '</td>' +
        '<td>' + pill(x.status, k) + '</td>' +
        '<td class="font-mono text-xs">' + x.method + ' ' + x.endpoint + '</td>' +
        '<td class="pe-5 text-xs">' + (x.agent || '—') + '</td></tr>';
    }).join('') : '<tr><td colspan="4" class="text-center p-4 text-secondary-foreground">no failures 🎉</td></tr>';
  }

  function load() {
    var win = $('hlt_window').value;
    $('hlt_live').className = 'kt-badge kt-badge-outline kt-badge-warning';
    fetch('/api/admin/metrics?window=' + encodeURIComponent(win), { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d.error) throw new Error(d.error);
        render(d);
        $('hlt_live').className = 'kt-badge kt-badge-outline kt-badge-success';
        $('hlt_live').textContent = '● live';
      })
      .catch(function (e) {
        $('hlt_live').className = 'kt-badge kt-badge-outline kt-badge-destructive';
        $('hlt_live').textContent = '● ' + (e.message || 'error');
      });
  }

  function start() {
    load();
    if (timer) clearInterval(timer);
    timer = setInterval(load, 15000);   // live refresh every 15s
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('hlt_window').addEventListener('change', load);
    $('hlt_refresh').addEventListener('click', load);
    start();
  });
})();
