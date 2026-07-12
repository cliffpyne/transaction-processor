// Customers page — reads /api/customers (plain customers table), no aggregates.

(function () {
  const state = {
    page: 1,
    size: 25,
    search: '',
    product: '',
    sort: 'id.desc',
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

  const PRODUCT_LABEL = {
    'pikipiki_records':  { label: 'Pikipiki Loan', badge: 'kt-badge-primary' },
    'pikipiki_records2': { label: 'Pikipiki SAV',  badge: 'kt-badge-success' },
    'IPHONE_RECORDS':    { label: 'iPhone',        badge: 'kt-badge-info' },
  };

  const fmtDate = (iso) => {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
  };

  // Demo ships 30 stock avatars (300-1.png … 300-30.png). We don't have real
  // customer photos, so pick a stable one per id — same customer always gets
  // the same avatar, spread evenly across the 30 files.
  const avatarFor = (id) => {
    const n = ((Number(id) || 0) % 30) + 1;
    return `/static/demo/assets/media/avatars/300-${n}.png`;
  };

  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  const renderRow = (r) => {
    const product = PRODUCT_LABEL[r.source_tab] || { label: r.source_tab || '—', badge: 'kt-badge-secondary' };
    return `
      <tr data-id="${r.id}">
        <td class="text-center">
          <input class="kt-checkbox kt-checkbox-sm" type="checkbox" value="${r.id}"/>
        </td>
        <td>
          <div class="flex items-center gap-2.5">
            <img alt="" class="rounded-full size-7 shrink-0" src="${avatarFor(r.id)}"/>
            <span class="text-sm font-medium text-mono">
              ${esc(r.name || '(no name)')}
            </span>
          </div>
        </td>
        <td class="text-foreground font-normal">${esc(r.phone || '—')}</td>
        <td class="text-foreground font-normal">${esc(r.plate || '—')}</td>
        <td class="text-foreground font-normal">${esc(r.customer_id || '—')}</td>
        <td>
          <span class="kt-badge kt-badge-outline ${product.badge}">${esc(product.label)}</span>
        </td>
        <td class="text-foreground font-normal">${esc(fmtDate(r.created_at))}</td>
        <td class="text-end">
          <button class="kt-btn kt-btn-sm kt-btn-icon kt-btn-ghost">
            <i class="ki-filled ki-dots-vertical text-lg"></i>
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
    if (state.product) {
      params.set('filter[0][field]', 'source_tab');
      params.set('filter[0][value]', state.product);
      params.set('filter[0][type]',  'eq');
    }

    $tbody.innerHTML =
      '<tr><td class="text-center text-secondary-foreground py-6" colspan="8">Loading…</td></tr>';

    let json;
    try {
      const r = await fetch('/api/customers?' + params.toString(),
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

    const from = state.total ? (state.page - 1) * state.size + 1 : 0;
    const to   = Math.min(state.page * state.size, state.total);
    $showing.textContent = state.total
      ? `Showing ${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()} customers`
      : 'No customers';
    $info.textContent = state.total
      ? `${from.toLocaleString()}–${to.toLocaleString()} of ${state.total.toLocaleString()}`
      : '';
    $total.textContent = state.total.toLocaleString();
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

  load();
})();
