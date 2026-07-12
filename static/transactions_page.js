// Transactions page — /api/transactions, product tabs (Boda / iPhone / All),
// bank + status filters, search, sort, paging.

(function () {
  // Product → source_tab groupings
  const PRODUCT_TABS = {
    'boda':   ['CRDBPASSED', 'CRDBFAILED', 'NMBPASSED', 'NMBFAILED'],
    'iphone': ['IPHONEPASSED', 'IPHONEFAILED'],
  };

  const STATUS_MATCH = {
    'passed': ['CRDBPASSED', 'NMBPASSED', 'IPHONEPASSED', 'CRDBSAVCOM', 'NMBSAVCOM'],
    'failed': ['CRDBFAILED', 'NMBFAILED', 'IPHONEFAILED'],
  };

  const state = {
    page:    1,
    size:    25,
    search:  '',
    product: '',
    bank:    '',
    status:  '',
    sort:    'id.desc',
    // Date range — inclusive gte/lte on transaction_day. ISO date strings.
    dayFrom: '',
    dayTo:   '',
    total:   0,
  };

  const $tbody   = document.getElementById('txn_tbody');
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

  const fmtMoney = (n) => {
    const v = Number(n || 0);
    return v.toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  // Parse the sheet's transaction_date text. Common shapes:
  //   "9-Jul-26 07:35:12"        (unpadded day, 2-digit year, 24h time)
  //   "09-Jul-2026 07:35:12"     (padded day, 4-digit year)
  //   "2026-07-09 07:35:12"      (ISO-ish)
  //   "9-Jul-26"                 (date only)
  // Returns {date: 'DD MMM YYYY', time: 'HH:MM' | null} or null on failure.
  const MONTHS = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
  const MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  const parseTxnDate = (raw) => {
    if (!raw) return null;
    const s = String(raw).trim();
    // ISO YYYY-MM-DD[ HH:MM[:SS]]
    let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (m) return { y: +m[1], mo: +m[2] - 1, d: +m[3], h: m[4] ? +m[4] : null, mi: m[5] ? +m[5] : 0 };
    // DD-Mon-YY[YY] [HH:MM[:SS]]
    m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (m) {
      const mo = MONTHS[m[2].toLowerCase()];
      if (mo == null) return null;
      let y = +m[3]; if (y < 100) y += 2000;
      return { y, mo, d: +m[1], h: m[4] ? +m[4] : null, mi: m[5] ? +m[5] : 0 };
    }
    return null;
  };

  const pad2 = (n) => String(n).padStart(2, '0');

  // Show either transaction_date (with time) or transaction_day (date only).
  const fmtDateCell = (r) => {
    const p = parseTxnDate(r.transaction_date) || parseTxnDate(r.transaction_day);
    if (!p) return esc(r.transaction_date || r.transaction_day || '—');
    const datePart = `${pad2(p.d)} ${MONTH_ABBR[p.mo]} ${p.y}`;
    if (p.h == null) return datePart;
    return `${datePart} <span class="text-secondary-foreground">·</span> ${pad2(p.h)}:${pad2(p.mi)}`;
  };

  // source_tab → status pill
  const statusPill = (src) => {
    const s = String(src || '').toUpperCase();
    if (s.endsWith('FAILED'))  return { label: 'Failed',  cls: 'kt-badge-destructive' };
    if (s.endsWith('PASSED') || s.endsWith('SAVCOM')) return { label: 'Passed', cls: 'kt-badge-success' };
    return { label: s || '—', cls: 'kt-badge-secondary' };
  };

  const bankPill = (b) => {
    const bb = String(b || '').toUpperCase();
    if (bb === 'CRDB') return { label: 'CRDB', cls: 'kt-badge-primary' };
    if (bb === 'NMB')  return { label: 'NMB',  cls: 'kt-badge-warning' };
    return { label: bb || '—', cls: 'kt-badge-secondary' };
  };

  const renderRow = (r) => {
    const st = statusPill(r.source_tab);
    const bk = bankPill(r.bank);
    const isFailed = String(r.source_tab || '').endsWith('FAILED');
    const customerCell = r.customer_name
      ? esc(r.customer_name)
      : (isFailed && r.fail_reason
          ? `<span class="text-destructive text-xs">${esc(r.fail_reason)}</span>`
          : '<span class="text-muted-foreground">—</span>');
    return `
      <tr data-id="${r.id}">
        <td>
          <input class="kt-checkbox kt-checkbox-sm" type="checkbox" value="${r.id}"/>
        </td>
        <td class="text-foreground font-medium">${esc(r.ref_number || '—')}</td>
        <td>
          <span class="kt-badge kt-badge-sm kt-badge-outline ${st.cls}">${esc(st.label)}</span>
        </td>
        <td class="text-foreground font-normal">${fmtDateCell(r)}</td>
        <td>
          <span class="kt-badge kt-badge-sm kt-badge-outline ${bk.cls}">${esc(bk.label)}</span>
        </td>
        <td class="text-foreground font-normal">${customerCell}</td>
        <td class="text-foreground font-semibold text-end">${fmtMoney(r.credit_amount)}<span class="text-secondary-foreground font-normal"> TZS</span></td>
        <td class="text-center">
          <button class="kt-btn kt-btn-sm kt-btn-icon kt-btn-ghost">
            <i class="ki-filled ki-eye text-lg"></i>
          </button>
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

  // Combine product + status filters into a single source_tab IN(...) list.
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
    const params = new URLSearchParams({
      page:  String(state.page),
      size:  String(state.size),
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
      '<tr><td class="text-center text-secondary-foreground py-6" colspan="8">Loading…</td></tr>';

    let json;
    try {
      const r = await fetch('/api/transactions?' + params.toString(),
                            { credentials: 'same-origin' });
      json = await r.json();
      if (!r.ok) throw new Error(json.error || r.statusText);
    } catch (e) {
      $tbody.innerHTML =
        `<tr><td class="text-center text-destructive py-6" colspan="8">Failed to load: ${esc(e.message)}</td></tr>`;
      return;
    }

    const rows = json.data || [];
    state.total = json.total || 0;
    const lastPage = json.last_page || 1;

    if (!rows.length) {
      $tbody.innerHTML =
        '<tr><td class="text-center text-secondary-foreground py-6" colspan="8">No transactions found.</td></tr>';
    } else {
      $tbody.innerHTML = rows.map(renderRow).join('');
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

  // Tab switching — sets state.product and re-styles active tab.
  const setActiveTab = (product) => {
    state.product = product;
    state.page = 1;
    $tabs.querySelectorAll('.kt-menu-item').forEach(el => {
      el.classList.toggle('kt-menu-item-here', el.dataset.product === product);
    });
    load();
  };

  $tabs.querySelectorAll('.kt-menu-item').forEach(el => {
    el.addEventListener('click', (e) => {
      e.preventDefault();
      setActiveTab(el.dataset.product || '');
    });
  });

  let searchTimer = null;
  $search.addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = e.target.value.trim();
      state.page = 1;
      load();
    }, 250);
  });

  $bank.addEventListener('change', (e) => {
    state.bank = e.target.value;
    state.page = 1;
    load();
  });

  $status.addEventListener('change', (e) => {
    state.status = e.target.value;
    state.page = 1;
    load();
  });

  $sort.addEventListener('change', (e) => {
    state.sort = e.target.value;
    state.page = 1;
    load();
  });

  $perpage.addEventListener('change', (e) => {
    state.size = Number(e.target.value) || 25;
    state.page = 1;
    load();
  });

  // ── Quick date filters ────────────────────────────────────────────────────
  // Returns YYYY-MM-DD for a Date object.
  const toISODate = (d) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;

  const applyQuickRange = (range) => {
    const now = new Date();
    let from = '', to = '';
    if (range === 'today') {
      from = to = toISODate(now);
    } else if (range === 'yesterday') {
      const y = new Date(now); y.setDate(y.getDate() - 1);
      from = to = toISODate(y);
    } else if (range === '7d') {
      const s = new Date(now); s.setDate(s.getDate() - 6);
      from = toISODate(s); to = toISODate(now);
    } else if (range === 'month') {
      from = toISODate(new Date(now.getFullYear(), now.getMonth(), 1));
      to   = toISODate(now);
    } else if (range === 'last_month') {
      from = toISODate(new Date(now.getFullYear(), now.getMonth() - 1, 1));
      to   = toISODate(new Date(now.getFullYear(), now.getMonth(), 0));
    } else if (range === 'all') {
      from = to = '';
    }
    state.dayFrom = from;
    state.dayTo   = to;
    state.page    = 1;
    $from.value   = from ? `${from}T00:00` : '';
    $to.value     = to   ? `${to}T23:59`   : '';
    // Toggle active button styling
    $quick.querySelectorAll('button[data-range]').forEach(b => {
      b.classList.toggle('kt-btn-primary', b.dataset.range === range);
    });
    load();
  };

  $quick.querySelectorAll('button[data-range]').forEach(b => {
    b.addEventListener('click', () => applyQuickRange(b.dataset.range));
  });

  // Custom range inputs — datetime-local. We filter by transaction_day (date
  // only), so time is ignored server-side but kept visible for the user.
  const onRangeInput = () => {
    state.dayFrom = $from.value ? $from.value.slice(0, 10) : '';
    state.dayTo   = $to.value   ? $to.value.slice(0, 10)   : '';
    state.page    = 1;
    // Clear the quick-button active styling once user types a custom range.
    $quick.querySelectorAll('button[data-range]').forEach(b => b.classList.remove('kt-btn-primary'));
    load();
  };
  $from.addEventListener('change', onRangeInput);
  $to.addEventListener('change', onRangeInput);

  $clear.addEventListener('click', () => applyQuickRange('all'));

  load();
})();
