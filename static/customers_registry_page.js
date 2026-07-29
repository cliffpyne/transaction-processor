// Customer Registry page — list + per-column filters + Add-Customer modal.
//
// Modal is Metronic-native (kt-modal + data-kt-modal-toggle/dismiss),
// dynamic form fields toggled by customer_type. Debounced filters keep
// PostgREST happy at 2k+ rows.
//
// URL query string honoured on load:
//   ?customer_type=boda|savcom|iphone → pre-set the type filter
//   ?open=add                         → auto-open Add-Customer modal

(function () {
  // ── URL query params for pre-filtering + auto-open ──
  const urlParams = new URLSearchParams(window.location.search);
  const initialType = urlParams.get('customer_type') || '';

  // Row cache — every row that comes back from /api/customer_registry gets
  // stored here keyed by id, so Edit clicks open the modal without a fresh
  // fetch. Cleared each time load() re-runs (a new page / filter / search
  // discards stale rows).
  const rowCache = new Map();
  // Edit mode: null → Add-mode (POSTs to /api/customer_registry);
  //            number → Edit-mode for that registry id (PATCHes to
  //            /api/customer_registry/<id>).
  let editModeRegId = null;

  const state = {
    page: 1,
    size: 25,
    // Global quick-search + type dropdown from the card header
    search: '',
    type: ['boda', 'savcom', 'iphone'].includes(initialType) ? initialType : '',
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

  const canEdit = ($tbody && $tbody.dataset.canEdit === 'true');

  const renderRow = (r) => {
    const t = TYPE_BADGE[r.customer_type] || { label: r.customer_type || '—', cls: 'kt-badge-secondary' };
    const actionsCell = canEdit
      ? `<td>
           <div class="flex gap-1">
             <button type="button" class="kt-btn kt-btn-sm kt-btn-outline kt-btn-icon reg-edit-btn"
                     data-row-id="${r.id}" title="Edit customer">
               <i class="ki-filled ki-pencil"></i>
             </button>
             <button type="button" class="kt-btn kt-btn-sm kt-btn-outline kt-btn-icon reg-delete-btn text-destructive"
                     data-row-id="${r.id}"
                     data-row-name="${esc(r.customer_name || '')}"
                     title="Delete customer">
               <i class="ki-filled ki-trash"></i>
             </button>
           </div>
         </td>`
      : '';
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
        ${actionsCell}
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

  // Column span for empty/loading rows depends on whether the Actions
  // column is present (only for admin/editor).
  const COLSPAN = canEdit ? 11 : 10;

  const load = async () => {
    const params = buildQuery();
    $tbody.innerHTML = `<tr><td class="text-center text-secondary-foreground py-6" colspan="${COLSPAN}">Loading…</td></tr>`;
    // Show/hide Clear button based on whether any advanced filter is set
    if ($btnClear) $btnClear.classList.toggle('hidden', !anyFilterActive());

    let json;
    try {
      const r = await fetch('/api/customer_registry?' + params.toString(), { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML = `<tr><td class="text-center text-destructive py-6" colspan="${COLSPAN}">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }
    const rows = json.data || [];
    // Cache rows for the Edit modal to open without a fresh fetch.
    rowCache.clear();
    for (const r of rows) rowCache.set(r.id, r);
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;
    $tbody.innerHTML = rows.length
      ? rows.map(renderRow).join('')
      : `<tr><td class="text-center text-secondary-foreground py-6" colspan="${COLSPAN}">No customers match.</td></tr>`;
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

  // ── Add-Customer form submit ──────────────────────────────────────
  // The Metronic modal (kt-modal + data-kt-modal-toggle/dismiss) is
  // opened/closed entirely by core.bundle.js. We only handle the form
  // submit here: POST to /api/customer_registry, close the modal via
  // Metronic's own API, then refresh the list + stats.
  const $addForm   = $('reg_add_form');
  const $addErr    = $('reg_add_err');
  const $addSubmit = $('reg_add_submit');
  const $addModal  = $('reg_add_modal');
  const $addType   = $('reg_add_type');

  // ── Dynamic fields: show/hide by customer type ─────────────────────
  // Each conditional field carries data-reg-show-for="boda savcom" etc.
  // The plate column disappears for iPhone, sav_customer_id only shows
  // for SAVCOM, additional phones only for iPhone. Hidden fields ALSO
  // have their inputs cleared and disabled so the form submit never
  // sends stale values from a type the user switched away from.
  const applyTypeVisibility = (t) => {
    document.querySelectorAll('#reg_add_form [data-reg-field]').forEach(el => {
      const showFor = (el.dataset.regShowFor || '').split(/\s+/).filter(Boolean);
      const visible = showFor.length === 0 || showFor.includes(t);
      el.style.display = visible ? '' : 'none';
      el.querySelectorAll('input, select, textarea').forEach(inp => {
        if (!visible) {
          inp.value = '';
          inp.disabled = true;
        } else {
          inp.disabled = false;
        }
      });
    });
  };
  if ($addType) {
    applyTypeVisibility($addType.value || 'boda');
    $addType.addEventListener('change', (e) => applyTypeVisibility(e.target.value));
  }

  const closeAddModal = () => {
    if (!$addModal) return;
    // Metronic exposes an instance API via getInstance() when available.
    try {
      if (window.KTModal && window.KTModal.getInstance) {
        const inst = window.KTModal.getInstance($addModal);
        if (inst && inst.hide) { inst.hide(); return; }
      }
    } catch (_) {}
    // Fallback for older builds: click a dismiss button.
    const btn = $addModal.querySelector('[data-kt-modal-dismiss="true"]');
    if (btn) btn.click();
  };

  // ── Modal open, in Add-mode OR Edit-mode ──
  // The Add-Customer BUTTON in the header is the only Metronic-native
  // trigger for the modal (data-kt-modal-toggle="#reg_add_modal"). We
  // hook its click to decide which mode we're opening in:
  //   - if `pendingEditRow` was set by an Edit click just before,
  //     populate the form with that row's values and switch to
  //     Edit-mode (title = "Edit customer #<id>", button = "Save
  //     changes", submit → PATCH).
  //   - otherwise reset to a clean Add-mode form (title = "Add
  //     Customer", button = "Save customer", submit → POST).
  //
  // The pending-row flag closes the race we had before: the previous
  // implementation set editModeRegId in openEditRow() THEN clicked the
  // trigger, which fired an unconditional reset handler that
  // immediately blew editModeRegId back to null. Result: every
  // "Save changes" submit was actually a POST that created a
  // duplicate row instead of editing.
  const $addTrigger = document.querySelector('[data-kt-modal-toggle="#reg_add_modal"]');
  let pendingEditRow = null;

  const applyAddMode = () => {
    editModeRegId = null;
    const t = $('reg_modal_title');
    if (t) t.textContent = 'Add Customer';
    if ($addSubmit) $addSubmit.textContent = 'Save customer';
    if ($addForm)   $addForm.reset();
    if ($addErr)    $addErr.textContent = '';
    if ($addType)   applyTypeVisibility($addType.value || 'boda');
  };

  const applyEditMode = (row) => {
    editModeRegId = row.id;
    if ($addForm) $addForm.reset();
    if ($addErr)  $addErr.textContent = '';
    const setField = (name, value) => {
      const el = $addForm && $addForm.querySelector(`[name="${name}"]`);
      if (!el) return;
      el.disabled = false;                      // may be disabled from previous type
      el.value = value == null ? '' : String(value);
    };
    setField('customer_name',     row.customer_name);
    setField('customer_type',     row.customer_type || 'boda');
    setField('plate',             row.plate);
    setField('phone',             row.phone);
    setField('sav_customer_id',   row.sav_customer_id);
    setField('bank_account_name', row.bank_account_name);
    setField('start_date',        row.start_date);
    setField('loan_amount_tsh',   row.loan_amount_tsh);
    setField('notes',             row.notes);
    // phones_extra = every phone in phones[] EXCEPT the primary phone
    const extras = (row.phones || [])
      .filter(p => p && p !== row.phone)
      .join(', ');
    setField('phones_extra', extras);
    // Apply type-visibility for the row's own customer_type
    applyTypeVisibility(row.customer_type || 'boda');
    // Title + submit label
    const title = $('reg_modal_title');
    if (title) title.textContent = `Edit customer #${row.id}`;
    if ($addSubmit) $addSubmit.textContent = 'Save changes';
  };

  if ($addTrigger) {
    $addTrigger.addEventListener('click', () => {
      if (pendingEditRow) {
        const row = pendingEditRow;
        pendingEditRow = null;
        applyEditMode(row);
      } else {
        applyAddMode();
      }
    });
  }

  // ── Delegate Edit / Delete clicks from the table.
  //   • Edit: stage the row, then fire the same Metronic trigger so the
  //     modal opens with our populated form. Metronic handles show/hide;
  //     our click handler above sees pendingEditRow and switches modes
  //     without a race.
  //   • Delete: browser confirm() → DELETE /api/customer_registry/<id>
  //     → remove the row from DOM + rowCache, refresh stats. The row
  //     count in `state.total` is decremented so pagination stays sane;
  //     if the deleted row was the last on this page we snap page back
  //     to 1 and reload from the server (simpler than trying to hold
  //     partial pages).
  if ($tbody) {
    $tbody.addEventListener('click', async (e) => {
      const editBtn = e.target.closest('.reg-edit-btn');
      if (editBtn) {
        const id = Number(editBtn.dataset.rowId);
        const row = rowCache.get(id);
        if (!row) return;
        pendingEditRow = row;
        if ($addTrigger) $addTrigger.click();
        return;
      }
      const delBtn = e.target.closest('.reg-delete-btn');
      if (delBtn) {
        const id = Number(delBtn.dataset.rowId);
        const name = delBtn.dataset.rowName || `#${id}`;
        if (!window.confirm(
          `Delete "${name}" (id ${id}) from the customer registry?\n\n` +
          `This cannot be undone. Historical transactions already resolved ` +
          `against this customer are NOT affected — but new transactions ` +
          `carrying this plate / phone / bank_account_name will no longer ` +
          `match a customer.`
        )) return;
        delBtn.disabled = true;
        try {
          const r = await fetch(`/api/customer_registry/${id}`, {
            method: 'DELETE',
            credentials: 'same-origin',
          });
          const j = await r.json().catch(() => ({}));
          if (!r.ok) throw new Error(j.error || r.statusText);
          const tr = $tbody.querySelector(`tr[data-id="${id}"]`);
          if (tr) tr.remove();
          rowCache.delete(id);
          state.total = Math.max(0, state.total - 1);
          if ($tbody.querySelector('tr[data-id]') == null) {
            state.page = 1;
            await load();
          }
          loadStats();
        } catch (err) {
          delBtn.disabled = false;
          window.alert('Delete failed: ' + err.message);
        }
      }
    });
  }

  if ($addForm) $addForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    if ($addErr) $addErr.textContent = '';
    if ($addSubmit) $addSubmit.disabled = true;
    const fd = new FormData($addForm);
    const payload = {};
    for (const [k, v] of fd.entries()) {
      const s = String(v).trim();
      if (s) payload[k] = s;
    }
    if (payload.plate) payload.plate = payload.plate.replace(/\s+/g, '').toUpperCase();
    if (payload.customer_name) payload.customer_name = payload.customer_name.trim();
    // phones_extra → phones[] array (dedup + include primary phone first)
    const phones = [];
    const seen = new Set();
    if (payload.phone && !seen.has(payload.phone)) { phones.push(payload.phone); seen.add(payload.phone); }
    if (payload.phones_extra) {
      for (const tok of payload.phones_extra.split(/[,;]+/)) {
        const t = tok.trim();
        if (t && !seen.has(t)) { phones.push(t); seen.add(t); }
      }
    }
    // Always send phones[] on both add and edit — an edit that clears the
    // phones field should persist as an empty array (not skip the column).
    payload.phones = phones;
    delete payload.phones_extra;

    // Add-mode → POST /api/customer_registry
    // Edit-mode → PATCH /api/customer_registry/<id>
    const url    = editModeRegId
      ? `/api/customer_registry/${editModeRegId}`
      : '/api/customer_registry';
    const method = editModeRegId ? 'PATCH' : 'POST';

    try {
      const r = await fetch(url, {
        method,
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || r.statusText);
      $addForm.reset();
      closeAddModal();
      state.page = 1;
      await Promise.all([load(), loadStats()]);
    } catch (err) {
      if ($addErr) $addErr.textContent = 'Save failed: ' + err.message;
    } finally {
      if ($addSubmit) $addSubmit.disabled = false;
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

  // ── URL query bootstrap ────────────────────────────────────────────
  // Pre-set the type dropdown so the sidebar sub-nav (?customer_type=boda)
  // lands in a pre-filtered view.
  if (state.type && $type) {
    $type.value = state.type;
  }
  // Auto-open the Add-Customer modal when the sidebar's "Create new
  // customer" link is clicked (routes to ?open=add). Wait a tick so
  // Metronic's core.bundle.js has finished wiring data-kt-modal-toggle.
  if (urlParams.get('open') === 'add') {
    setTimeout(() => {
      const trigger = document.querySelector('[data-kt-modal-toggle="#reg_add_modal"]');
      if (trigger) trigger.click();
    }, 100);
  }

  // ── Summary-cards toggle ──────────────────────────────────────────
  // Default state = hidden so the table is the first thing the user
  // sees. Preference persisted in localStorage — remembered across
  // sessions per browser.
  const $summary  = $('reg_summary_container');
  const $btnSum   = $('btn_toggle_summary');
  const SUMMARY_KEY = 'reg.summary.visible';

  const applySummary = (visible) => {
    if ($summary) $summary.style.display = visible ? '' : 'none';
    if ($btnSum)  $btnSum.innerHTML = visible
      ? '<i class="ki-filled ki-eye-slash"></i> Hide summary'
      : '<i class="ki-filled ki-chart-simple"></i> Show summary';
  };
  const savedSummary = localStorage.getItem(SUMMARY_KEY);
  applySummary(savedSummary === '1');
  if ($btnSum) {
    $btnSum.addEventListener('click', () => {
      const nowVisible = ($summary && $summary.style.display === 'none');
      applySummary(nowVisible);
      localStorage.setItem(SUMMARY_KEY, nowVisible ? '1' : '0');
    });
  }

  loadStats();
  load();
  setInterval(loadStats, 60_000);
})();
