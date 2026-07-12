// Transactions page — product tabs (All / Boda / iPhone / Iliyopata),
// bank + status filters, search, sort, date range, rescue modal.

(function () {
  const PRODUCT_TABS = {
    'boda':      ['CRDBPASSED', 'CRDBFAILED', 'NMBPASSED', 'NMBFAILED', 'BODAILIYOPATA'],
    'iphone':    ['IPHONEPASSED', 'IPHONEFAILED', 'IPHONEILIYOPATA'],
    'iliyopata': ['BODAILIYOPATA', 'IPHONEILIYOPATA'],
  };

  const STATUS_MATCH = {
    'passed': ['CRDBPASSED', 'NMBPASSED', 'IPHONEPASSED', 'CRDBSAVCOM', 'NMBSAVCOM'],
    'failed': ['CRDBFAILED', 'NMBFAILED', 'IPHONEFAILED'],
  };

  const FAILED_TABS = new Set(['CRDBFAILED', 'NMBFAILED', 'IPHONEFAILED']);
  const ILIYOPATA_TABS = new Set(['BODAILIYOPATA', 'IPHONEILIYOPATA']);

  const state = {
    page: 1, size: 25, search: '',
    product: '', bank: '', status: '',
    // Primary sort on transaction_day (real DATE column, always ISO)
    // handles cross-day ordering uniformly regardless of how the sheet
    // stored the raw date. transaction_date (text) is the tiebreaker
    // within the same day so time-of-day still orders correctly.
    // Text-sort of transaction_date alone was unreliable because NMB
    // rows store '31-May-2026' while CRDB rows store '2026-05-31 …',
    // and text compares "3" > "2" so NMB rows floated above CRDB.
    sort: 'transaction_day.desc.nullslast,transaction_date.desc.nullslast',
    dayFrom: '', dayTo: '',
    total: 0,
  };

  const $tbody   = document.getElementById('txn_tbody');
  const $thead   = document.getElementById('txn_thead');
  const $showing = document.getElementById('txn_showing');
  const $info    = document.getElementById('txn_info');
  const $pager   = document.getElementById('txn_pager');
  const $search  = document.getElementById('txn_search');
  const $bank    = document.getElementById('txn_bank');
  const $status  = document.getElementById('txn_status');
  const $sort    = document.getElementById('txn_sort');
  const $perpage = document.getElementById('txn_perpage');
  const $tabs    = document.getElementById('txn_producttabs');
  const $quick   = document.getElementById('txn_quickfilters');
  const $from    = document.getElementById('txn_from');
  const $to      = document.getElementById('txn_to');
  const $clear   = document.getElementById('txn_range_clear');

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const fmtMoney = (n) => Number(n || 0).toLocaleString('en-US', { maximumFractionDigits: 0 });
  const pad2 = (n) => String(n).padStart(2, '0');

  const MONTHS = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
  const MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  const parseTxnDate = (raw) => {
    if (!raw) return null;
    const s = String(raw).trim();
    let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (m) return { y: +m[1], mo: +m[2] - 1, d: +m[3], h: m[4] ? +m[4] : null, mi: m[5] ? +m[5] : 0 };
    m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (m) {
      const mo = MONTHS[m[2].toLowerCase()];
      if (mo == null) return null;
      let y = +m[3]; if (y < 100) y += 2000;
      return { y, mo, d: +m[1], h: m[4] ? +m[4] : null, mi: m[5] ? +m[5] : 0 };
    }
    return null;
  };

  const timeFromDescription = (desc) => {
    if (!desc) return null;
    const s = String(desc);
    let m = s.match(/\b([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?\b/);
    if (m) return { h: +m[1], mi: +m[2] };
    m = s.match(/\b\d{4}\s+([01]?\d|2[0-3])\s+([0-5]\d)\s+([0-5]\d)\b/);
    if (m) return { h: +m[1], mi: +m[2] };
    m = s.match(/\b([01]?\d|2[0-3])\s+([0-5]\d)\s+([0-5]\d)\b/);
    if (m) return { h: +m[1], mi: +m[2] };
    return null;
  };

  const fmtDate = (rawDate, desc) => {
    const p = parseTxnDate(rawDate);
    if (!p) return esc(rawDate || '—');
    if (p.h == null) {
      const t = timeFromDescription(desc);
      if (t) { p.h = t.h; p.mi = t.mi; }
    }
    const datePart = `${pad2(p.d)} ${MONTH_ABBR[p.mo]} ${p.y}`;
    if (p.h == null) return datePart;
    return `${datePart} <span class="text-secondary-foreground">·</span> ${pad2(p.h)}:${pad2(p.mi)}`;
  };

  const statusPill = (src) => {
    const s = String(src || '').toUpperCase();
    if (s.endsWith('FAILED'))    return { label: 'Failed',    cls: 'kt-badge-destructive' };
    if (s.endsWith('ILIYOPATA')) return { label: 'Iliyopata', cls: 'kt-badge-info' };
    if (s.endsWith('PASSED') || s.endsWith('SAVCOM')) return { label: 'Passed', cls: 'kt-badge-success' };
    return { label: s || '—', cls: 'kt-badge-secondary' };
  };

  const bankPill = (b) => {
    const bb = String(b || '').toUpperCase();
    if (bb === 'CRDB') return { label: 'CRDB', cls: 'kt-badge-primary' };
    if (bb === 'NMB')  return { label: 'NMB',  cls: 'kt-badge-warning' };
    return { label: bb || '—', cls: 'kt-badge-secondary' };
  };

  // Column layout differs on the Iliyopata tab: two dates + rescued-by.
  // No Actions column — the whole row is clickable and opens the details
  // drawer; Rescue lives inside the drawer for FAILED rows.
  const columnsForCurrent = () => {
    if (state.product === 'iliyopata') {
      return ['sel', 'ref', 'status', 'newdate', 'olddate', 'bank', 'customer', 'description', 'amount', 'rescuedby'];
    }
    return ['sel', 'ref', 'status', 'date', 'bank', 'customer', 'description', 'amount'];
  };

  const HEADERS = {
    sel:       { label: '', width: 'w-14' },
    ref:       { label: 'Ref Number',  width: 'min-w-[180px]' },
    status:    { label: 'Status',      width: 'w-[130px]' },
    date:      { label: 'Date',        width: 'min-w-[160px]' },
    newdate:   { label: 'New Date',    width: 'min-w-[160px]' },
    olddate:   { label: 'Old Date',    width: 'min-w-[160px]' },
    bank:      { label: 'Bank',        width: 'w-[110px]' },
    customer:  { label: 'Customer',    width: 'min-w-[200px]' },
    description:{ label: 'Description',width: 'min-w-[280px]' },
    amount:    { label: 'Amount',      width: 'w-[170px] text-end' },
    rescuedby: { label: 'Rescued by',  width: 'min-w-[150px]' },
    actions:   { label: '',            width: 'w-[100px]' },
  };

  const renderHead = () => {
    const cols = columnsForCurrent();
    $thead.innerHTML = '<tr>' + cols.map(c => {
      const h = HEADERS[c];
      if (c === 'sel')     return `<th class="${h.width}"><input class="kt-checkbox kt-checkbox-sm" type="checkbox"/></th>`;
      if (c === 'actions') return `<th class="${h.width}"></th>`;
      return `<th class="${h.width}"><span class="kt-table-col"><span class="kt-table-col-label">${h.label}</span></span></th>`;
    }).join('') + '</tr>';
  };

  const cellFor = (col, r) => {
    switch (col) {
      case 'sel':
        return `<td><input class="kt-checkbox kt-checkbox-sm" type="checkbox" value="${r.id}"/></td>`;
      case 'ref':
        return `<td class="text-foreground font-medium">${esc(r.ref_number || '—')}</td>`;
      case 'status': {
        const st = statusPill(r.source_tab);
        return `<td><span class="kt-badge kt-badge-sm kt-badge-outline ${st.cls}">${esc(st.label)}</span></td>`;
      }
      case 'date':
      case 'newdate':
        return `<td class="text-foreground font-normal">${fmtDate(r.transaction_date, r.description)}</td>`;
      case 'olddate':
        return `<td class="text-secondary-foreground text-sm">${fmtDate(r.old_transaction_date, r.description)}</td>`;
      case 'bank': {
        const bk = bankPill(r.bank);
        return `<td><span class="kt-badge kt-badge-sm kt-badge-outline ${bk.cls}">${esc(bk.label)}</span></td>`;
      }
      case 'customer': {
        const isFailed = FAILED_TABS.has(r.source_tab);
        const cell = r.customer_name
          ? esc(r.customer_name)
          : (isFailed && r.fail_reason
              ? `<span class="text-destructive text-xs">${esc(r.fail_reason)}</span>`
              : '<span class="text-muted-foreground">—</span>');
        return `<td class="text-foreground font-normal">${cell}</td>`;
      }
      case 'description':
        return `<td class="text-secondary-foreground text-sm align-top py-2" title="${esc(r.description || '')}">
                  <div style="max-width:420px;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden;white-space:normal;line-height:1.3;">${esc(r.description || '—')}</div>
                </td>`;
      case 'amount':
        return `<td class="text-foreground font-semibold text-end">${fmtMoney(r.credit_amount)}<span class="text-secondary-foreground font-normal"> TZS</span></td>`;
      case 'rescuedby':
        return `<td class="text-secondary-foreground text-sm">${esc(r.moved_by_username || '—')}</td>`;
    }
    return '<td></td>';
  };

  const renderRow = (r) => {
    const cols = columnsForCurrent();
    // No data-kt-drawer-toggle on the tr — ktui only binds hooks that were
    // in the DOM at init time. We handle the open by clicking a persistent
    // hidden trigger button (#td_trigger) after populating the fields, so
    // Metronic's ktui gets the overlay+blur exactly like the demo.
    return `<tr data-id="${r.id}" style="cursor:pointer">
      ${cols.map(c => cellFor(c, r)).join('')}
    </tr>`;
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

  const activeSourceTabs = () => {
    let productSet = null;
    if (state.product && PRODUCT_TABS[state.product]) {
      productSet = new Set(PRODUCT_TABS[state.product]);
    }
    let statusSet = null;
    if (state.status && STATUS_MATCH[state.status]) {
      statusSet = new Set(STATUS_MATCH[state.status]);
    }
    if (productSet && statusSet) {
      return [...productSet].filter(t => statusSet.has(t));
    }
    return productSet ? [...productSet] : (statusSet ? [...statusSet] : null);
  };

  const load = async () => {
    renderHead();
    const cols = columnsForCurrent();
    const colspan = cols.length;

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
    let fi = 0;
    const tabs = activeSourceTabs();
    if (tabs && tabs.length) {
      params.set(`filter[${fi}][field]`, 'source_tab');
      params.set(`filter[${fi}][value]`, tabs.join(','));
      params.set(`filter[${fi}][type]`,  'in');
      fi++;
    }
    if (state.bank) {
      params.set(`filter[${fi}][field]`, 'bank');
      params.set(`filter[${fi}][value]`, state.bank);
      params.set(`filter[${fi}][type]`,  'eq');
      fi++;
    }
    if (state.dayFrom) {
      params.set(`filter[${fi}][field]`, 'transaction_day');
      params.set(`filter[${fi}][value]`, state.dayFrom);
      params.set(`filter[${fi}][type]`,  'gte');
      fi++;
    }
    if (state.dayTo) {
      params.set(`filter[${fi}][field]`, 'transaction_day');
      params.set(`filter[${fi}][value]`, state.dayTo);
      params.set(`filter[${fi}][type]`,  'lte');
      fi++;
    }

    $tbody.innerHTML =
      `<tr><td class="text-center text-secondary-foreground py-6" colspan="${colspan}">Loading…</td></tr>`;

    let json;
    try {
      const r = await fetch('/api/transactions?' + params.toString(),
                            { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML =
        `<tr><td class="text-center text-destructive py-6" colspan="${colspan}">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }

    const rows = json.data || [];
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;

    if (!rows.length) {
      $tbody.innerHTML =
        `<tr><td class="text-center text-secondary-foreground py-6" colspan="${colspan}">No transactions found.</td></tr>`;
    } else {
      rowsById = {};
      for (const r of rows) rowsById[r.id] = r;
      $tbody.innerHTML = rows.map(renderRow).join('');
      wireRescueButtons();
      wireDetailButtons();
    }

    const from = state.total ? (state.page - 1) * state.size + 1 : 0;
    const to   = Math.min(state.page * state.size, state.total);
    $showing.textContent = state.total
      ? `Showing ${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()} transactions`
      : 'No transactions';
    $info.textContent = state.total
      ? `${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()}`
      : '';
    renderPager(state.page, lastPage);
  };

  // ── Rescue modal ────────────────────────────────────────────────────────
  const $modal    = document.getElementById('rescue_backdrop');
  const $rSearch  = document.getElementById('rescue_search');
  const $rResults = document.getElementById('rescue_results');
  const $rConfirm = document.getElementById('rescue_confirm');
  const $rCancel  = document.getElementById('rescue_cancel');
  const $rClose   = document.getElementById('rescue_close');
  const $rSub     = document.getElementById('rescue_subtitle');

  const rescueState = { txnId: null, customerId: null };
  let rowsById = {};

  // ── Details drawer — populate fields for the picked row ─────────────────
  const $td = {
    heroName:     document.getElementById('td_hero_name'),
    heroVerified: document.getElementById('td_hero_verified'),
    heroBankWrap: document.getElementById('td_hero_bank_wrap'),
    heroDate:     document.getElementById('td_hero_date'),
    heroStatusWrap: document.getElementById('td_hero_status_wrap'),
    avatarWrap:   document.getElementById('td_avatar_wrap'),
    avatarIcon:   document.getElementById('td_avatar_icon'),
    amount:       document.getElementById('td_amount'),
    description:  document.getElementById('td_description'),
    ref:          document.getElementById('td_ref'),
    identifier:   document.getElementById('td_identifier'),
    customerId:      document.getElementById('td_customer_id'),
    customerIdRow:   document.getElementById('td_customer_id_row'),
    failReason:      document.getElementById('td_fail_reason'),
    failReasonRow:   document.getElementById('td_fail_reason_row'),
    oldDate:      document.getElementById('td_old_date'),
    oldDateRow:   document.getElementById('td_old_date_row'),
    movedBy:      document.getElementById('td_moved_by'),
    movedByRow:   document.getElementById('td_moved_by_row'),
    movedAt:      document.getElementById('td_moved_at'),
    movedAtRow:   document.getElementById('td_moved_at_row'),
    sourceTab:    document.getElementById('td_source_tab'),
    originalId:   document.getElementById('td_original_id'),
    createdAt:    document.getElementById('td_created_at'),
    footer:       document.getElementById('td_footer'),
    rescueBtn:    document.getElementById('td_rescue_btn'),
  };

  // Avatar ring colour + icon key off the transaction state so the hero card
  // reads at a glance (green tick for passed, red cross for failed, blue
  // shield for iliyopata).
  const avatarStyleFor = (source_tab) => {
    const s = String(source_tab || '').toUpperCase();
    if (s.endsWith('FAILED'))    return { ring: 'border-destructive/40',        icon: 'ki-cross-circle',    tint: 'text-destructive' };
    if (s.endsWith('ILIYOPATA')) return { ring: 'border-info/40',               icon: 'ki-shield-tick',     tint: 'text-info' };
    if (s.endsWith('PASSED') || s.endsWith('SAVCOM'))
                                 return { ring: 'border-success/40',            icon: 'ki-check-circle',    tint: 'text-success' };
    return { ring: 'border-border', icon: 'ki-financial-schedule', tint: 'text-secondary-foreground' };
  };

  const populateDetails = (r) => {
    if (!r) return;
    const st = statusPill(r.source_tab);
    const bk = bankPill(r.bank);
    const av = avatarStyleFor(r.source_tab);

    // Hero
    $td.heroName.textContent = r.customer_name || (FAILED_TABS.has(r.source_tab) ? (r.fail_reason || 'Unmatched') : '(no name)');
    $td.heroVerified.style.display = (r.customer_name && !FAILED_TABS.has(r.source_tab)) ? '' : 'none';
    $td.heroBankWrap.innerHTML   = `<span class="kt-badge kt-badge-sm kt-badge-outline ${bk.cls}">${esc(bk.label)}</span>`;
    $td.heroStatusWrap.innerHTML = `<span class="kt-badge kt-badge-sm kt-badge-outline ${st.cls}">${esc(st.label)}</span>`;
    $td.heroDate.innerHTML       = fmtDate(r.transaction_date, r.description);
    // Reset ring class then set the state colour
    $td.avatarWrap.className = `flex items-center justify-center rounded-full border-2 bg-background size-[92px] shrink-0 ${av.ring}`;
    $td.avatarIcon.className = `ki-filled ${av.icon} text-3xl ${av.tint}`;

    // Amount
    $td.amount.textContent = fmtMoney(r.credit_amount);
    // Body
    $td.description.textContent = r.description || '—';
    $td.ref.textContent = r.ref_number || '—';
    $td.identifier.textContent = r.identifier || '—';
    if (r.customer_id) {
      $td.customerIdRow.style.display = '';
      $td.customerId.textContent = r.customer_id;
    } else {
      $td.customerIdRow.style.display = 'none';
    }
    if (r.fail_reason) {
      $td.failReasonRow.style.display = '';
      $td.failReason.textContent = r.fail_reason;
    } else {
      $td.failReasonRow.style.display = 'none';
    }
    $td.sourceTab.textContent = r.source_tab || '—';
    $td.originalId.textContent = r.original_id != null ? String(r.original_id) : '—';
    $td.createdAt.textContent = r.created_at ? new Date(r.created_at).toLocaleString('en-GB') : '—';

    // Iliyopata-only rows
    const isIly = ILIYOPATA_TABS.has(r.source_tab);
    $td.oldDateRow.style.display = isIly ? '' : 'none';
    $td.movedByRow.style.display = isIly ? '' : 'none';
    $td.movedAtRow.style.display = isIly ? '' : 'none';
    if (isIly) {
      $td.oldDate.innerHTML   = fmtDate(r.old_transaction_date, r.description);
      $td.movedBy.textContent = r.moved_by_username || '—';
      $td.movedAt.textContent = r.moved_at ? new Date(r.moved_at).toLocaleString('en-GB') : '—';
    }

    // Rescue button — footer only shown for FAILED rows.
    if (FAILED_TABS.has(r.source_tab)) {
      $td.footer.style.display = '';
      $td.rescueBtn.dataset.rescueId = r.id;
      $td.rescueBtn.dataset.ref = r.ref_number || '';
      $td.rescueBtn.dataset.amount = r.credit_amount || 0;
    } else {
      $td.footer.style.display = 'none';
    }
  };

  const $trigger = document.getElementById('td_trigger');

  const wireDetailButtons = () => {
    document.querySelectorAll('#txn_tbody tr[data-id]').forEach(tr => {
      tr.addEventListener('click', (e) => {
        // Ignore clicks on the row-select checkbox
        if (e.target.closest('input[type="checkbox"]')) return;
        populateDetails(rowsById[tr.dataset.id]);
        // Fire ktui's toggle via the persistent trigger, so we inherit
        // Metronic's overlay + backdrop-blur + slide-in animation.
        if ($trigger) $trigger.click();
      });
    });
  };

  const openRescue = (btn) => {
    rescueState.txnId = btn.dataset.rescueId;
    rescueState.customerId = null;
    const ref = btn.dataset.ref || '—';
    const amt = fmtMoney(btn.dataset.amount);
    $rSub.textContent = `Ref ${ref} · ${amt} TZS · pick the customer, transaction date will be stamped to now`;
    $rSearch.value = '';
    $rResults.innerHTML = '<div class="text-sm text-secondary-foreground p-3 text-center">Start typing to search…</div>';
    $rConfirm.disabled = true;
    $modal.classList.remove('hidden');
    $modal.style.display = 'flex';
    setTimeout(() => $rSearch.focus(), 40);
  };

  const closeRescue = () => {
    $modal.classList.add('hidden');
    $modal.style.display = 'none';
  };

  const wireRescueButtons = () => {
    // Legacy inline row buttons (kept for future); currently only the
    // drawer's Rescue button surfaces this action.
    document.querySelectorAll('button[data-rescue-id]').forEach(b => {
      b.addEventListener('click', () => openRescue(b));
    });
  };

  // Wire the drawer's Rescue button once (persistent element).
  {
    const $btn = document.getElementById('td_rescue_btn');
    if ($btn) {
      $btn.addEventListener('click', (e) => {
        e.stopPropagation();
        openRescue($btn);
      });
    }
  }

  const PRODUCT_LABEL_SHORT = {
    'BODA_RECORDS':   'Boda',
    'SAVCOM_RECORDS': 'Savcom',
    'IPHONE_RECORDS': 'iPhone',
  };

  const renderCustResults = (list) => {
    if (!list.length) {
      $rResults.innerHTML = '<div class="text-sm text-secondary-foreground p-3 text-center">No matches.</div>';
      return;
    }
    $rResults.innerHTML = list.map(c => `
      <label class="flex items-start gap-2 px-3 py-2 border-b border-border last:border-b-0 cursor-pointer hover:bg-accent/40">
        <input type="radio" name="rescue_cust" class="kt-radio mt-1" value="${c.id}"/>
        <div class="flex-1 min-w-0">
          <div class="text-sm font-medium text-mono truncate">${esc(c.name || '(no name)')}</div>
          <div class="text-xs text-secondary-foreground truncate">
            ${esc(c.phone || '—')} · ${esc(c.plate || c.customer_id || '—')}
            <span class="ms-2 kt-badge kt-badge-xs kt-badge-outline kt-badge-secondary">${esc(PRODUCT_LABEL_SHORT[c.source_tab] || c.source_tab)}</span>
          </div>
        </div>
      </label>
    `).join('');
    $rResults.querySelectorAll('input[name="rescue_cust"]').forEach(r => {
      r.addEventListener('change', () => {
        rescueState.customerId = r.value;
        $rConfirm.disabled = false;
      });
    });
  };

  let searchAbort = null;
  let searchTimerR = null;
  const runSearch = async (q) => {
    if (!q.trim()) {
      $rResults.innerHTML = '<div class="text-sm text-secondary-foreground p-3 text-center">Start typing to search…</div>';
      return;
    }
    if (searchAbort) searchAbort.abort();
    searchAbort = new AbortController();
    $rResults.innerHTML = '<div class="text-sm text-secondary-foreground p-3 text-center">Searching…</div>';
    try {
      const r = await fetch(`/api/customers/search?q=${encodeURIComponent(q)}`,
                            { credentials: 'same-origin', signal: searchAbort.signal });
      const j = await r.json();
      if (r.ok) renderCustResults(j.data || []);
      else $rResults.innerHTML = `<div class="text-sm text-destructive p-3 text-center">${esc(j.error || 'Search failed')}</div>`;
    } catch (e) {
      if (e.name !== 'AbortError') {
        $rResults.innerHTML = `<div class="text-sm text-destructive p-3 text-center">${esc(e.message)}</div>`;
      }
    }
  };

  $rSearch.addEventListener('input', (e) => {
    clearTimeout(searchTimerR);
    searchTimerR = setTimeout(() => runSearch(e.target.value), 200);
  });

  $rCancel.addEventListener('click', closeRescue);
  $rClose.addEventListener('click', closeRescue);
  $modal.addEventListener('click', (e) => { if (e.target === $modal) closeRescue(); });

  $rConfirm.addEventListener('click', async () => {
    if (!rescueState.txnId || !rescueState.customerId) return;
    $rConfirm.disabled = true;
    $rConfirm.textContent = 'Moving…';
    try {
      const r = await fetch(`/api/transactions/${rescueState.txnId}/rescue`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ customer_id: Number(rescueState.customerId) }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || r.statusText);
      closeRescue();
      load();
    } catch (e) {
      $rConfirm.disabled = false;
      $rConfirm.textContent = 'Move';
      alert('Rescue failed: ' + e.message);
    }
  });

  // ── Tab switching ─────────────────────────────────────────────────────────
  const setActiveTab = (product) => {
    state.product = product;
    state.page = 1;
    $tabs.querySelectorAll('.txn-tab').forEach(el => {
      el.classList.toggle('is-active', el.dataset.product === product);
    });
    load();
  };

  $tabs.querySelectorAll('.txn-tab').forEach(el => {
    el.addEventListener('click', (e) => {
      e.preventDefault();
      setActiveTab(el.dataset.product || '');
    });
  });

  // ── Filters + search + sort + per-page ────────────────────────────────────
  let searchTimer = null;
  $search.addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = e.target.value.trim();
      state.page = 1;
      load();
    }, 250);
  });

  $bank.addEventListener('change', (e) => { state.bank = e.target.value; state.page = 1; load(); });
  $status.addEventListener('change', (e) => { state.status = e.target.value; state.page = 1; load(); });
  $sort.addEventListener('change', (e) => { state.sort = e.target.value; state.page = 1; load(); });
  $perpage.addEventListener('change', (e) => { state.size = Number(e.target.value) || 25; state.page = 1; load(); });

  // ── Quick date filters ────────────────────────────────────────────────────
  const toISODate = (d) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;

  const applyQuickRange = (range) => {
    const now = new Date();
    let from = '', to = '';
    if (range === 'today') { from = to = toISODate(now); }
    else if (range === 'yesterday') { const y = new Date(now); y.setDate(y.getDate() - 1); from = to = toISODate(y); }
    else if (range === '7d') { const s = new Date(now); s.setDate(s.getDate() - 6); from = toISODate(s); to = toISODate(now); }
    else if (range === 'month') { from = toISODate(new Date(now.getFullYear(), now.getMonth(), 1)); to = toISODate(now); }
    else if (range === 'last_month') { from = toISODate(new Date(now.getFullYear(), now.getMonth() - 1, 1)); to = toISODate(new Date(now.getFullYear(), now.getMonth(), 0)); }
    else if (range === 'all') { from = to = ''; }
    state.dayFrom = from; state.dayTo = to; state.page = 1;
    $from.value = from ? `${from}T00:00` : '';
    $to.value   = to   ? `${to}T23:59`   : '';
    $quick.querySelectorAll('button[data-range]').forEach(b => b.classList.toggle('kt-btn-primary', b.dataset.range === range));
    load();
  };

  $quick.querySelectorAll('button[data-range]').forEach(b => {
    b.addEventListener('click', () => applyQuickRange(b.dataset.range));
  });

  const onRangeInput = () => {
    state.dayFrom = $from.value ? $from.value.slice(0, 10) : '';
    state.dayTo   = $to.value   ? $to.value.slice(0, 10)   : '';
    state.page = 1;
    $quick.querySelectorAll('button[data-range]').forEach(b => b.classList.remove('kt-btn-primary'));
    load();
  };
  $from.addEventListener('change', onRangeInput);
  $to.addEventListener('change', onRangeInput);

  $clear.addEventListener('click', () => applyQuickRange('all'));

  load();
})();
