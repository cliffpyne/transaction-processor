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

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const fmtMoney = (n) => {
    const v = Number(n || 0);
    return v.toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  const fmtDate = (d) => {
    if (!d) return '—';
    const dt = new Date(d + (d.length === 10 ? 'T00:00:00Z' : ''));
    if (isNaN(dt)) return d;
    return dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
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
        <td class="text-foreground font-normal">${esc(fmtDate(r.transaction_day || r.transaction_date))}</td>
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

  load();
})();
