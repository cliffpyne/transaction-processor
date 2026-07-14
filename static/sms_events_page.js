// SMS Events page — reads /api/sms_events, filter by outcome + search.

(function () {
  const state = {
    page: 1, size: 25, search: '',
    outcome: '',
    sort: 'processed_at.desc',
    total: 0,
  };

  const $tbody   = document.getElementById('sms_tbody');
  const $showing = document.getElementById('sms_showing');
  const $info    = document.getElementById('sms_info');
  const $pager   = document.getElementById('sms_pager');
  const $search  = document.getElementById('sms_search');
  const $outcome = document.getElementById('sms_outcome');
  const $sort    = document.getElementById('sms_sort');
  const $perpage = document.getElementById('sms_perpage');

  const OUTCOME_PILL = {
    'rescued':              { label: 'Rescued',           cls: 'kt-badge-success' },
    'already_rescued':      { label: 'Already rescued',   cls: 'kt-badge-info' },
    'not_a_failed_row':     { label: 'In PASSED',         cls: 'kt-badge-primary' },
    'ref_not_found':        { label: 'Ref not found',     cls: 'kt-badge-destructive' },
    'plate_not_in_records': { label: 'Plate unknown',     cls: 'kt-badge-warning' },
    'extract_failed':       { label: 'Extract failed',    cls: 'kt-badge-secondary' },
    'server_error':         { label: 'Server error',      cls: 'kt-badge-destructive' },
  };

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const fmtWhen = (iso) => {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    const pad = n => String(n).padStart(2, '0');
    return `${pad(d.getDate())} ${d.toLocaleString('en-GB', { month: 'short' })} · ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  const renderRow = (r) => {
    const oc = OUTCOME_PILL[r.outcome] || { label: r.outcome || '—', cls: 'kt-badge-secondary' };
    return `
      <tr data-id="${r.id}">
        <td class="text-foreground font-normal text-sm">${esc(fmtWhen(r.processed_at))}</td>
        <td><span class="kt-badge kt-badge-sm kt-badge-outline ${oc.cls}">${esc(oc.label)}</span></td>
        <td class="text-foreground font-normal text-sm">${esc(r.sender || '—')}</td>
        <td class="text-foreground font-medium text-sm">${esc(r.extracted_plate || '—')}</td>
        <td class="text-foreground font-normal text-xs" style="word-break:break-all">${esc(r.extracted_ref || '—')}</td>
        <td class="text-secondary-foreground text-xs align-top py-2" title="${esc(r.body || '')}">
          <div style="max-width:440px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;white-space:normal;line-height:1.35;">${esc(r.body || '—')}</div>
        </td>
        <td class="text-foreground font-normal text-sm">
          ${r.rescued_row_id
            ? `<a class="text-primary hover:underline" href="/home/transactions#${r.rescued_row_id}"
                  title="${esc(r.rescued_source_tab || '')}">#${r.rescued_row_id}${
                r.rescued_source_tab
                  ? ` <span class="text-xs text-muted-foreground">${esc(r.rescued_source_tab)}</span>`
                  : ''}</a>`
            : '<span class="text-muted-foreground">—</span>'}
        </td>
      </tr>
    `;
  };

  const renderPager = (page, lastPage) => {
    if (lastPage <= 1) { $pager.innerHTML = ''; return; }
    const btn = (p, label, disabled, active) =>
      `<button class="kt-btn kt-btn-sm ${active ? 'kt-btn-primary' : 'kt-btn-outline'}"
               ${disabled ? 'disabled' : ''} data-page="${p}">${label}</button>`;
    const parts = [];
    parts.push(btn(page - 1, '‹', page <= 1, false));
    const start = Math.max(1, page - 2);
    const end   = Math.min(lastPage, start + 4);
    for (let p = start; p <= end; p++) parts.push(btn(p, String(p), false, p === page));
    parts.push(btn(page + 1, '›', page >= lastPage, false));
    $pager.innerHTML = parts.join('');
    $pager.querySelectorAll('button[data-page]').forEach(b => {
      b.addEventListener('click', () => {
        const p = Number(b.dataset.page);
        if (p >= 1 && p <= lastPage && p !== page) { state.page = p; load(); }
      });
    });
  };

  const load = async () => {
    const params = new URLSearchParams({
      page: String(state.page),
      size: String(state.size),
      search: state.search,
    });
    if (state.sort) {
      const [field, ...rest] = state.sort.split('.');
      params.set('sort[0][field]', field);
      params.set('sort[0][dir]',   rest.join('.'));
    }
    if (state.outcome) {
      params.set('filter[0][field]', 'outcome');
      params.set('filter[0][value]', state.outcome);
      params.set('filter[0][type]',  'eq');
    }
    $tbody.innerHTML =
      '<tr><td class="text-center text-secondary-foreground py-6" colspan="7">Loading…</td></tr>';
    let json;
    try {
      const r = await fetch('/api/sms_events?' + params.toString(),
                            { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML =
        `<tr><td class="text-center text-destructive py-6" colspan="7">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }
    const rows = json.data || [];
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;
    $tbody.innerHTML = rows.length
      ? rows.map(renderRow).join('')
      : '<tr><td class="text-center text-secondary-foreground py-6" colspan="7">No SMS events yet.</td></tr>';
    const from = state.total ? (state.page - 1) * state.size + 1 : 0;
    const to   = Math.min(state.page * state.size, state.total);
    $showing.textContent = state.total
      ? `Showing ${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()} SMS events`
      : 'No SMS events yet';
    $info.textContent = state.total
      ? `${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()}`
      : '';
    renderPager(state.page, lastPage);
  };

  let searchTimer = null;
  $search.addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = e.target.value.trim();
      state.page = 1;
      load();
    }, 250);
  });
  $outcome.addEventListener('change', (e) => { state.outcome = e.target.value; state.page = 1; load(); });
  $sort.addEventListener('change',    (e) => { state.sort    = e.target.value; state.page = 1; load(); });
  $perpage.addEventListener('change', (e) => { state.size    = Number(e.target.value) || 25; state.page = 1; load(); });

  load();
})();
