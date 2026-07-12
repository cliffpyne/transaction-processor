// Customers page — hits /api/customers/enriched, renders rows into the
// kt-datatable body, keeps the header stats + toolbar in sync.

(function () {
  const state = {
    page: 1,
    size: 25,
    search: '',
    product: '',
    sort: 'last_txn_day.desc.nullslast',
    total: 0,
  };

  const $tbody   = document.getElementById('cust_tbody');
  const $showing = document.getElementById('cust_showing');
  const $info    = document.getElementById('cust_info');
  const $pager   = document.getElementById('cust_pager');
  const $search  = document.getElementById('cust_search');
  const $product = document.getElementById('cust_product');
  const $sort    = document.getElementById('cust_sort');
  const $perpage = document.getElementById('cust_perpage');
  const $total   = document.getElementById('cust_total');
  const $paying  = document.getElementById('cust_paying_month');

  const PRODUCT_LABEL = {
    'pikipiki_records':  { label: 'Pikipiki Loan', badge: 'kt-badge-primary' },
    'pikipiki_records2': { label: 'Pikipiki SAV',  badge: 'kt-badge-success' },
    'IPHONE_RECORDS':    { label: 'iPhone',        badge: 'kt-badge-info' },
  };

  const fmtMoney = (n) => {
    const v = Number(n || 0);
    return v.toLocaleString('en-US', { maximumFractionDigits: 0 }) + ' TZS';
  };

  const fmtCount = (n) => Number(n || 0).toLocaleString('en-US');

  const fmtRelDay = (d) => {
    if (!d) return '—';
    const today = new Date();
    const then  = new Date(d + 'T00:00:00Z');
    const days  = Math.floor((today - then) / 86_400_000);
    if (days <= 0) return 'Today';
    if (days === 1) return 'Yesterday';
    if (days < 7)   return `${days} days ago`;
    if (days < 30)  return `${Math.floor(days / 7)} wk ago`;
    if (days < 365) return `${Math.floor(days / 30)} mo ago`;
    return `${Math.floor(days / 365)} yr ago`;
  };

  const initials = (name) => {
    if (!name) return '?';
    return name.trim().split(/\s+/).slice(0, 2).map(s => s[0]).join('').toUpperCase();
  };

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const renderRow = (r) => {
    const product = PRODUCT_LABEL[r.source_tab] || { label: r.source_tab, badge: 'kt-badge-secondary' };
    const idOrPlate = r.plate || r.customer_id || '—';
    return `
      <tr data-id="${r.id}">
        <td class="text-center">
          <input class="kt-checkbox kt-checkbox-sm" data-kt-datatable-row-check="true" type="checkbox" value="${r.id}"/>
        </td>
        <td>
          <div class="flex items-center gap-2.5">
            <div class="rounded-full size-7 shrink-0 bg-accent text-secondary-foreground text-xs font-semibold flex items-center justify-center">
              ${esc(initials(r.name))}
            </div>
            <div class="flex flex-col">
              <a class="text-sm font-medium text-mono hover:text-primary mb-px" href="#" data-cust-view="${r.id}">
                ${esc(r.name || '(no name)')}
              </a>
              <span class="text-sm text-secondary-foreground font-normal">
                ${esc(r.phone || '—')}
              </span>
            </div>
          </div>
        </td>
        <td class="text-foreground font-normal">${esc(idOrPlate)}</td>
        <td>
          <span class="kt-badge kt-badge-outline ${product.badge}">${esc(product.label)}</span>
        </td>
        <td class="text-foreground font-medium">${esc(fmtMoney(r.total_paid_tzs))}</td>
        <td class="text-foreground font-normal">${esc(fmtRelDay(r.last_txn_day))}</td>
        <td class="text-center text-foreground font-medium">${esc(fmtCount(r.txn_count))}</td>
        <td>
          <div class="kt-menu" data-kt-menu="true">
            <div class="kt-menu-item"
                 data-kt-menu-item-offset="0, 10px"
                 data-kt-menu-item-placement="bottom-end"
                 data-kt-menu-item-toggle="dropdown"
                 data-kt-menu-item-trigger="click">
              <button class="kt-menu-toggle kt-btn kt-btn-sm kt-btn-icon kt-btn-ghost">
                <i class="ki-filled ki-dots-vertical text-lg"></i>
              </button>
              <div class="kt-menu-dropdown kt-menu-default w-full max-w-[175px]" data-kt-menu-dismiss="true">
                <div class="kt-menu-item">
                  <a class="kt-menu-link" href="#" data-cust-view="${r.id}">
                    <span class="kt-menu-icon"><i class="ki-filled ki-search-list"></i></span>
                    <span class="kt-menu-title">View</span>
                  </a>
                </div>
                <div class="kt-menu-item">
                  <a class="kt-menu-link" href="#" data-cust-edit="${r.id}">
                    <span class="kt-menu-icon"><i class="ki-filled ki-pencil"></i></span>
                    <span class="kt-menu-title">Edit</span>
                  </a>
                </div>
                <div class="kt-menu-separator"></div>
                <div class="kt-menu-item">
                  <a class="kt-menu-link" href="/api/customers/${r.id}/txns" target="_blank">
                    <span class="kt-menu-icon"><i class="ki-filled ki-file-up"></i></span>
                    <span class="kt-menu-title">Transactions</span>
                  </a>
                </div>
              </div>
            </div>
          </div>
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
      page:  String(state.page),
      size:  String(state.size),
      search: state.search,
    });
    if (state.sort) {
      const [field, dir, ...rest] = state.sort.split('.');
      params.set('sort[0][field]', field);
      params.set('sort[0][dir]', [dir, ...rest].join('.'));
    }
    if (state.product) {
      params.set('filter[0][field]', 'source_tab');
      params.set('filter[0][value]', state.product);
      params.set('filter[0][type]',  'eq');
    }

    $tbody.innerHTML =
      '<tr><td class="text-center text-secondary-foreground py-6" colspan="8">Loading…</td></tr>';

    let json;
    try {
      const r = await fetch('/api/customers/enriched?' + params.toString(),
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
        '<tr><td class="text-center text-secondary-foreground py-6" colspan="8">No customers found.</td></tr>';
    } else {
      $tbody.innerHTML = rows.map(renderRow).join('');
    }

    const from = (state.page - 1) * state.size + 1;
    const to   = Math.min(state.page * state.size, state.total);
    $showing.textContent = `Showing ${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()} customers`;
    $info.textContent    = `${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()}`;
    renderPager(state.page, lastPage);
  };

  const loadStats = async () => {
    try {
      const r = await fetch('/api/customers/stats', { credentials: 'same-origin' });
      const j = await r.json();
      if (r.ok) {
        $total.textContent  = (j.total || 0).toLocaleString();
        $paying.textContent = (j.paying_this_month || 0).toLocaleString();
      }
    } catch (e) { /* silent */ }
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

  $product.addEventListener('change', (e) => {
    state.product = e.target.value;
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

  loadStats();
  load();
})();
