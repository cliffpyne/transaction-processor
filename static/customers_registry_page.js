// Customer Registry page — list + create (admin/editor only).

(function () {
  const state = { page: 1, size: 25, search: '', type: '', total: 0 };

  const $ = (id) => document.getElementById(id);
  const $tbody   = $('reg_tbody');
  const $showing = $('reg_showing');
  const $info    = $('reg_info');
  const $pager   = $('reg_pager');
  const $search  = $('reg_search');
  const $type    = $('reg_type');
  const $perpage = $('reg_perpage');

  const TYPE_BADGE = {
    boda:   { label: 'BODA',   cls: 'kt-badge-primary'   },
    savcom: { label: 'SAVCOM', cls: 'kt-badge-info'      },
    iphone: { label: 'iPhone', cls: 'kt-badge-secondary' },
  };

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const fmtTZS = (v) => {
    if (v == null || v === '') return '—';
    const n = Number(v);
    return isFinite(n) ? n.toLocaleString() : String(v);
  };

  const renderRow = (r) => {
    const t = TYPE_BADGE[r.customer_type] || { label: r.customer_type || '—', cls: 'kt-badge-secondary' };
    return `
      <tr data-id="${r.id}">
        <td class="text-secondary-foreground text-xs">${r.id}</td>
        <td><span class="kt-badge kt-badge-sm kt-badge-outline ${t.cls}">${esc(t.label)}</span></td>
        <td class="text-foreground font-medium text-sm">${esc(r.customer_name || '')}</td>
        <td class="text-foreground text-sm">${esc(r.plate || '—')}</td>
        <td class="text-foreground text-sm">${esc(r.phone || '—')}</td>
        <td class="text-foreground text-sm">${esc(r.bank_account_name || '—')}</td>
        <td class="text-foreground text-sm">${esc(r.start_date || '—')}</td>
        <td class="text-foreground text-sm">${fmtTZS(r.loan_amount_tsh)}</td>
        <td class="text-secondary-foreground text-xs">${esc(r.sav_customer_id || '—')}</td>
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

  const loadStats = async () => {
    try {
      const r = await fetch('/api/customer_registry/summary', { credentials: 'same-origin' });
      if (!r.ok) return;
      const s = await r.json();
      $('stat_total').textContent  = (s.total  ?? 0).toLocaleString();
      $('stat_boda').textContent   = (s.boda   ?? 0).toLocaleString();
      $('stat_savcom').textContent = (s.savcom ?? 0).toLocaleString();
      $('stat_iphone').textContent = (s.iphone ?? 0).toLocaleString();
    } catch (_) {}
  };

  const load = async () => {
    const params = new URLSearchParams({
      page: String(state.page),
      size: String(state.size),
    });
    if (state.search) params.set('search', state.search);
    if (state.type)   params.set('customer_type', state.type);
    $tbody.innerHTML = '<tr><td class="text-center text-secondary-foreground py-6" colspan="9">Loading…</td></tr>';
    let json;
    try {
      const r = await fetch('/api/customer_registry?' + params.toString(), { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML = `<tr><td class="text-center text-destructive py-6" colspan="9">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }
    const rows = json.data || [];
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;
    $tbody.innerHTML = rows.length
      ? rows.map(renderRow).join('')
      : '<tr><td class="text-center text-secondary-foreground py-6" colspan="9">No customers match.</td></tr>';
    const from = state.total ? (state.page - 1) * state.size + 1 : 0;
    const to   = Math.min(state.page * state.size, state.total);
    $showing.textContent = state.total
      ? `Showing ${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()} customers`
      : 'No customers';
    $info.textContent = state.total
      ? `${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()}`
      : '';
    renderPager(state.page, lastPage);
  };

  // ── Create modal wiring (only present if role is admin/editor) ──
  const $modal      = $('reg_modal');
  const $btnOpen    = $('btn_open_create');
  const $btnClose   = $('reg_modal_close');
  const $btnCancel  = $('reg_modal_cancel');
  const $form       = $('reg_form');
  const $formErr    = $('reg_form_err');
  const $btnSubmit  = $('reg_submit');

  const openModal = () => {
    if (!$modal) return;
    $formErr.textContent = '';
    $form.reset();
    $modal.style.display = 'flex';
    $modal.classList.remove('hidden');
  };
  const closeModal = () => {
    if (!$modal) return;
    $modal.style.display = 'none';
    $modal.classList.add('hidden');
  };
  if ($btnOpen)   $btnOpen.addEventListener('click', openModal);
  if ($btnClose)  $btnClose.addEventListener('click', closeModal);
  if ($btnCancel) $btnCancel.addEventListener('click', closeModal);
  if ($modal) $modal.addEventListener('click', (e) => {
    if (e.target === $modal) closeModal();
  });

  if ($form) $form.addEventListener('submit', async (e) => {
    e.preventDefault();
    $formErr.textContent = '';
    $btnSubmit.disabled = true;
    const fd = new FormData($form);
    const payload = {};
    for (const [k, v] of fd.entries()) {
      const s = String(v).trim();
      if (s) payload[k] = s;
    }
    if (payload.plate) payload.plate = payload.plate.replace(/\s+/g, '').toUpperCase();
    if (payload.customer_name) payload.customer_name = payload.customer_name.trim();

    try {
      const r = await fetch('/api/customer_registry', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || r.statusText);
      closeModal();
      state.page = 1;
      await Promise.all([load(), loadStats()]);
    } catch (err) {
      $formErr.textContent = 'Save failed: ' + err.message;
    } finally {
      $btnSubmit.disabled = false;
    }
  });

  // ── Filter wiring ──
  let searchTimer = null;
  $search.addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = e.target.value.trim();
      state.page = 1;
      load();
    }, 250);
  });
  $type.addEventListener('change',    (e) => { state.type = e.target.value; state.page = 1; load(); });
  $perpage.addEventListener('change', (e) => { state.size = Number(e.target.value) || 25; state.page = 1; load(); });

  loadStats();
  load();
  setInterval(loadStats, 60_000);
})();
