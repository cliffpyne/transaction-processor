// Customer Registry page — list + create + per-column filters.
//
// Modal open/close is driven by the HTML5 `hidden` attribute (not a CSS
// class), so nothing in the Tailwind/Metronic layer can override it via
// specificity accidents. Every input filter debounces to keep PostgREST
// happy at 2k+ rows.

(function () {
  const state = {
    page: 1,
    size: 25,
    // Global quick-search + type dropdown from the card header
    search: '',
    type: '',
    // Per-column filters (from the collapsible Filters panel)
    filters: {
      name: '',
      plate: '',
      phone: '',
      bank_name: '',
      start_from: '',
      start_to: '',
      loan_min: '',
      loan_max: '',
      created_from: '',
      created_to: '',
    },
    total: 0,
  };

  const $ = (id) => document.getElementById(id);
  const $tbody   = $('reg_tbody');
  const $showing = $('reg_showing');
  const $info    = $('reg_info');
  const $pager   = $('reg_pager');
  const $search  = $('reg_search');
  const $type    = $('reg_type');
  const $perpage = $('reg_perpage');
  const $filterPanel   = $('reg_filters');
  const $btnFilters    = $('btn_toggle_filters');
  const $btnClear      = $('btn_clear_filters');

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

  // "2026-07-24T09:41:03.220Z" → "2026-07-24 09:41" (server-side timestamptz)
  const fmtCreated = (v) => {
    if (!v) return '—';
    const d = new Date(v);
    if (isNaN(d.getTime())) return esc(String(v));
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
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
        <td class="text-secondary-foreground text-xs">${fmtCreated(r.created_at)}</td>
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

  const anyFilterActive = () =>
    Object.values(state.filters).some(v => v && String(v).trim());

  const buildQuery = () => {
    const p = new URLSearchParams({
      page: String(state.page),
      size: String(state.size),
    });
    if (state.search) p.set('search', state.search);
    if (state.type)   p.set('customer_type', state.type);
    const f = state.filters;
    if (f.name)         p.set('name',              f.name);
    if (f.plate)        p.set('plate',             f.plate);
    if (f.phone)        p.set('phone',             f.phone);
    if (f.bank_name)    p.set('bank_account_name', f.bank_name);
    if (f.start_from)   p.set('start_date_from',   f.start_from);
    if (f.start_to)     p.set('start_date_to',     f.start_to);
    if (f.loan_min)     p.set('loan_min',          f.loan_min);
    if (f.loan_max)     p.set('loan_max',          f.loan_max);
    if (f.created_from) p.set('created_from',      f.created_from);
    if (f.created_to)   p.set('created_to',        f.created_to);
    return p;
  };

  const load = async () => {
    const params = buildQuery();
    $tbody.innerHTML = '<tr><td class="text-center text-secondary-foreground py-6" colspan="10">Loading…</td></tr>';
    // Show/hide Clear button based on whether any advanced filter is set
    if ($btnClear) $btnClear.classList.toggle('hidden', !anyFilterActive());

    let json;
    try {
      const r = await fetch('/api/customer_registry?' + params.toString(), { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML = `<tr><td class="text-center text-destructive py-6" colspan="10">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }
    const rows = json.data || [];
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;
    $tbody.innerHTML = rows.length
      ? rows.map(renderRow).join('')
      : '<tr><td class="text-center text-secondary-foreground py-6" colspan="10">No customers match.</td></tr>';
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
  const $modal     = $('reg_modal');
  const $btnOpen   = $('btn_open_create');
  const $btnClose  = $('reg_modal_close');
  const $btnCancel = $('reg_modal_cancel');
  const $form      = $('reg_form');
  const $formErr   = $('reg_form_err');
  const $btnSubmit = $('reg_submit');

  // Use setProperty(..., 'important') so this beats any Tailwind/Metronic
  // class-based display rule regardless of stylesheet load order.
  const showModal = (el) => el.style.setProperty('display', 'flex', 'important');
  const hideModal = (el) => el.style.setProperty('display', 'none', 'important');

  const openModal = () => {
    if (!$modal) { alert('Add-customer form is not loaded — please refresh.'); return; }
    if ($formErr) $formErr.textContent = '';
    if ($form)    $form.reset();
    showModal($modal);
    document.body.style.overflow = 'hidden'; // prevent scroll behind
  };
  const closeModal = () => {
    if (!$modal) return;
    hideModal($modal);
    document.body.style.overflow = '';
  };
  if ($btnOpen)   $btnOpen.addEventListener('click', openModal);
  if ($btnClose)  $btnClose.addEventListener('click', closeModal);
  if ($btnCancel) $btnCancel.addEventListener('click', closeModal);
  if ($modal) $modal.addEventListener('click', (e) => {
    if (e.target === $modal) closeModal();
  });
  // Esc closes the modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && $modal && $modal.style.display !== 'none') closeModal();
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

  // ── Filter wiring (quick-search bar + type dropdown + per-column) ──
  const debounce = (fn, ms) => {
    let t = null;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  };

  const runLoad = debounce(() => { state.page = 1; load(); }, 300);

  $search.addEventListener('input',  (e) => { state.search = e.target.value.trim(); runLoad(); });
  $type.addEventListener('change',   (e) => { state.type   = e.target.value;         state.page = 1; load(); });
  $perpage.addEventListener('change',(e) => { state.size   = Number(e.target.value) || 25; state.page = 1; load(); });

  // Advanced filter panel — toggle visibility
  if ($btnFilters && $filterPanel) {
    $btnFilters.addEventListener('click', () => {
      $filterPanel.hidden = !$filterPanel.hidden;
      $btnFilters.classList.toggle('kt-btn-primary', !$filterPanel.hidden);
      $btnFilters.classList.toggle('kt-btn-outline', $filterPanel.hidden);
    });
  }

  const FILTER_BINDINGS = [
    ['f_name',         'name',         'input'],
    ['f_plate',        'plate',        'input'],
    ['f_phone',        'phone',        'input'],
    ['f_bank_name',    'bank_name',    'input'],
    ['f_start_from',   'start_from',   'change'],
    ['f_start_to',     'start_to',     'change'],
    ['f_loan_min',     'loan_min',     'input'],
    ['f_loan_max',     'loan_max',     'input'],
    ['f_created_from', 'created_from', 'change'],
    ['f_created_to',   'created_to',   'change'],
  ];
  FILTER_BINDINGS.forEach(([id, key, evt]) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener(evt, (e) => {
      state.filters[key] = String(e.target.value || '').trim();
      runLoad();
    });
  });

  if ($btnClear) {
    $btnClear.addEventListener('click', () => {
      Object.keys(state.filters).forEach(k => { state.filters[k] = ''; });
      FILTER_BINDINGS.forEach(([id]) => {
        const el = $(id);
        if (el) el.value = '';
      });
      state.page = 1;
      load();
    });
  }

  // Start hidden — modal opens only via the Add Customer button
  if ($modal) hideModal($modal);

  loadStats();
  load();
  setInterval(loadStats, 60_000);
})();
