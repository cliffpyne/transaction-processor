/* records.js — Tabulator SPA glue.
   All data goes through /api/<table>. Auth is via session cookies (handled by the browser).
*/
'use strict';

document.body.classList.add('role-' + window.__ME__.role);

const ROLE = window.__ME__.role;
const isAdmin  = ROLE === 'admin';
const isEditor = ROLE === 'admin' || ROLE === 'editor';

// ── View configuration ──────────────────────────────────────────────────────
const VIEWS = {
  customers: {
    title:    'Customers',
    endpoint: '/api/customers',
    columns: [
      { title: 'ID',           field: 'id',           width: 70,  sorter: 'number', headerFilter: 'input' },
      { title: 'Plate',        field: 'plate',                    headerFilter: 'input' },
      { title: 'Phone',        field: 'phone',                    headerFilter: 'input' },
      { title: 'Name',         field: 'name',                     headerFilter: 'input' },
      { title: 'Customer ID',  field: 'customer_id',              headerFilter: 'input' },
      { title: 'Source',       field: 'source_tab',   width: 160,
        headerFilter: 'list',
        headerFilterParams: { values: { '': 'All',
                                        'BODA_RECORDS':'BODA_RECORDS',
                                        'SAVCOM_RECORDS':'SAVCOM_RECORDS',
                                        'IPHONE_RECORDS':'IPHONE_RECORDS' } }
      },
      { title: 'Created',      field: 'created_at',   width: 175,
        formatter: fmtDateTime },
    ],
    canAdd:    isEditor,
    canEdit:   isEditor,
    canDelete: isAdmin,
    editFields: [
      { key: 'plate',       label: 'Plate' },
      { key: 'phone',       label: 'Phone' },
      { key: 'name',        label: 'Name' },
      { key: 'customer_id', label: 'Customer ID' },
      { key: 'source_tab',  label: 'Source', type: 'select',
        options: ['BODA_RECORDS','SAVCOM_RECORDS','IPHONE_RECORDS'] },
    ],
  },

  transactions: {
    title:    'Transactions',
    endpoint: '/api/transactions',
    columns: [
      { title: 'ID',            field: 'id',              width: 70,  sorter: 'number' },
      { title: 'Bank',          field: 'bank',            width: 90,
        headerFilter: 'list',
        headerFilterParams: { values: { '':'All','CRDB':'CRDB','NMB':'NMB' } } },
      { title: 'Source',        field: 'source_tab',      width: 160,
        headerFilter: 'list',
        headerFilterParams: { values: { '': 'All',
          'CRDBPASSED':'CRDBPASSED','CRDBSAVCOM':'CRDBSAVCOM','CRDBFAILED':'CRDBFAILED',
          'NMBPASSED':'NMBPASSED','NMBSAVCOM':'NMBSAVCOM','NMBFAILED':'NMBFAILED',
          'IPHONEPASSED':'IPHONEPASSED','IPHONEFAILED':'IPHONEFAILED' } } },
      { title: 'Date (sheet)',  field: 'transaction_date',width: 180 },
      { title: 'Day',           field: 'transaction_day', width: 110, headerFilter: 'input' },
      { title: 'Credit',        field: 'credit_amount',   width: 120, hozAlign: 'right',
        formatter: fmtMoney, sorter: 'number' },
      { title: 'Identifier',    field: 'identifier',      width: 160, headerFilter: 'input' },
      { title: 'Customer',      field: 'customer_name',              headerFilter: 'input' },
      { title: 'Ref',           field: 'ref_number',      width: 200, headerFilter: 'input' },
      { title: 'Fail reason',   field: 'fail_reason',     width: 220 },
      { title: 'Fuzzy',         field: 'is_fuzzy_rescued',width: 80,
        formatter: 'tickCross' },
      { title: 'Created',       field: 'created_at',      width: 175,
        formatter: fmtDateTime },
    ],
    canAdd: false, canEdit: false, canDelete: false,
  },

  dedup_alerts: {
    title:    'Dedup Alerts',
    endpoint: '/api/dedup_alerts',
    columns: [
      { title: 'ID',          field: 'id',          width: 70, sorter: 'number' },
      { title: 'Ref',         field: 'ref_number',  width: 220, headerFilter: 'input' },
      { title: 'Source',      field: 'source_tab',  width: 160, headerFilter: 'input' },
      { title: 'Description', field: 'description', headerFilter: 'input' },
      { title: 'Caught at',   field: 'caught_at',   width: 175, formatter: fmtDateTime },
    ],
    canAdd: false, canEdit: false, canDelete: false,
  },

  users: {
    title:    'Users',
    endpoint: '/api/users',
    columns: [
      { title: 'ID',         field: 'id',            width: 60,  sorter: 'number' },
      { title: 'Username',   field: 'username',      headerFilter: 'input' },
      { title: 'Full name',  field: 'full_name',     headerFilter: 'input' },
      { title: 'Role',       field: 'role',          width: 100,
        formatter: c => `<span class="role role-${c.getValue()}">${c.getValue()}</span>`,
        headerFilter: 'list',
        headerFilterParams: { values: { '':'All', admin:'admin', editor:'editor', viewer:'viewer' } } },
      { title: 'Created',    field: 'created_at',    width: 175, formatter: fmtDateTime },
      { title: 'Last login', field: 'last_login_at', width: 175, formatter: fmtDateTime },
    ],
    canAdd:    isAdmin,
    canEdit:   isAdmin,
    canDelete: isAdmin,
    editFields: [
      { key: 'username',  label: 'Username' },
      { key: 'full_name', label: 'Full name' },
      { key: 'role',      label: 'Role',     type: 'select',
        options: ['admin','editor','viewer'] },
      { key: 'password',  label: 'Password (leave blank to keep)', type: 'password' },
    ],
  },

  record_edits: {
    title:    'Audit Log',
    endpoint: '/api/record_edits',
    columns: [
      { title: 'ID',        field: 'id',         width: 70, sorter: 'number' },
      { title: 'When',      field: 'at',         width: 175, formatter: fmtDateTime },
      { title: 'User',      field: 'username',   width: 140 },
      { title: 'Action',    field: 'action',     width: 90,
        headerFilter: 'list',
        headerFilterParams: { values: {'':'All',INSERT:'INSERT',UPDATE:'UPDATE',DELETE:'DELETE'} } },
      { title: 'Table',     field: 'table_name', width: 140, headerFilter: 'input' },
      { title: 'Row',       field: 'row_id',     width: 90 },
      { title: 'Before',    field: 'before_json',formatter: c => jsonPreview(c.getValue()) },
      { title: 'After',     field: 'after_json', formatter: c => jsonPreview(c.getValue()) },
    ],
    canAdd: false, canEdit: false, canDelete: false,
  },
};

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtDateTime(cell) {
  const v = cell.getValue();
  if (!v) return '';
  const d = new Date(v);
  if (isNaN(+d)) return v;
  return d.toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}
function fmtMoney(cell) {
  const v = cell.getValue();
  if (v == null || v === '') return '';
  return Number(v).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
}
function jsonPreview(v) {
  if (!v) return '';
  try {
    const s = JSON.stringify(v);
    return `<code style="font-size:.72rem;color:#94a3b8">${s.length > 90 ? s.slice(0,88)+'…' : s}</code>`;
  } catch { return String(v); }
}

// ── State ────────────────────────────────────────────────────────────────────
let currentView = 'customers';
let table = null;

// ── Render a view ───────────────────────────────────────────────────────────
function loadView(name) {
  const cfg = VIEWS[name];
  if (!cfg) return;
  currentView = name;

  document.getElementById('viewTitle').textContent = cfg.title;
  document.querySelectorAll('.nav-item').forEach(a =>
    a.classList.toggle('active', a.dataset.view === name)
  );

  const btnAdd = document.getElementById('btnAdd');
  btnAdd.hidden = !cfg.canAdd;

  document.getElementById('searchBox').value = '';

  const columns = cfg.columns.slice();
  if (cfg.canEdit || cfg.canDelete) {
    columns.push({
      title: '', field: '__actions__', width: 90, hozAlign: 'center',
      headerSort: false, resizable: false,
      formatter: () => {
        const parts = [];
        if (cfg.canEdit)   parts.push('<button class="icon-btn" data-act="edit"   title="Edit">✎</button>');
        if (cfg.canDelete) parts.push('<button class="icon-btn danger" data-act="delete" title="Delete">🗑</button>');
        return parts.join('');
      },
      cellClick: (e, cell) => {
        const act = e.target.closest('[data-act]')?.dataset?.act;
        if (act === 'edit')   openEditModal(cell.getRow().getData(), cfg);
        if (act === 'delete') deleteRow(cell.getRow().getData(), cfg);
      },
    });
  }

  if (table) { table.destroy(); table = null; }
  table = new Tabulator('#grid', {
    layout: 'fitColumns',
    columns,
    ajaxURL: cfg.endpoint,
    ajaxConfig: 'GET',
    ajaxParamsFunc: () => {
      const q = document.getElementById('searchBox').value.trim();
      return q ? { search: q } : {};
    },
    ajaxContentType: 'form',
    pagination: true,
    paginationMode: 'remote',
    paginationSize: 50,
    paginationSizeSelector: [25, 50, 100, 200],
    filterMode: 'remote',
    sortMode: 'remote',
    ajaxURLGenerator: (url, config, params) => {
      const usp = new URLSearchParams();
      if (params.page) usp.append('page', params.page);
      if (params.size) usp.append('size', params.size);
      (params.sort || []).forEach((s, i) => {
        usp.append(`sort[${i}][field]`, s.field);
        usp.append(`sort[${i}][dir]`,   s.dir);
      });
      (params.filter || []).forEach((f, i) => {
        usp.append(`filter[${i}][field]`, f.field);
        usp.append(`filter[${i}][value]`, f.value);
        usp.append(`filter[${i}][type]`,  f.type || 'like');
      });
      const q = document.getElementById('searchBox').value.trim();
      if (q) usp.append('search', q);
      return `${url}?${usp.toString()}`;
    },
    ajaxResponse: (url, params, response) => response,
    dataLoaderLoading: '<div style="padding:20px;color:#94a3b8">Loading…</div>',
    placeholder: '<div style="padding:40px;color:#4b6080">No rows</div>',
  });
}

// ── Modal ───────────────────────────────────────────────────────────────────
let currentModal = { mode: 'add', row: null, cfg: null };

function openAddModal(cfg) {
  currentModal = { mode: 'add', row: null, cfg };
  document.getElementById('modalTitle').textContent = 'Add ' + cfg.title.slice(0, -1);
  renderModalForm(cfg.editFields || [], {});
  document.getElementById('modal').classList.remove('hidden');
}
function openEditModal(row, cfg) {
  currentModal = { mode: 'edit', row, cfg };
  document.getElementById('modalTitle').textContent = `Edit #${row.id}`;
  renderModalForm(cfg.editFields || [], row);
  document.getElementById('modal').classList.remove('hidden');
}
function closeModal() {
  document.getElementById('modal').classList.add('hidden');
}
function renderModalForm(fields, initial) {
  const form = document.getElementById('modalForm');
  form.innerHTML = fields.map(f => {
    const val = initial[f.key] ?? '';
    if (f.type === 'select') {
      const opts = f.options.map(o => `<option value="${o}" ${o===val?'selected':''}>${o}</option>`).join('');
      return `<div><label>${f.label}</label><select name="${f.key}">${opts}</select></div>`;
    }
    if (f.type === 'password') {
      return `<div><label>${f.label}</label><input name="${f.key}" type="password" placeholder="${initial.id ? '(leave blank to keep)' : ''}"></div>`;
    }
    return `<div><label>${f.label}</label><input name="${f.key}" type="text" value="${String(val).replace(/"/g,'&quot;')}"></div>`;
  }).join('');
}

document.getElementById('modalSave').addEventListener('click', async () => {
  const form = document.getElementById('modalForm');
  const body = {};
  new FormData(form).forEach((v, k) => { body[k] = v; });
  const { mode, row, cfg } = currentModal;
  const url = cfg.endpoint + (mode === 'edit' ? '/' + row.id : '');
  const method = mode === 'edit' ? 'PATCH' : 'POST';

  // Drop blank password on edit so it doesn't overwrite
  if (mode === 'edit' && body.password === '') delete body.password;

  try {
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      toast(err.error || 'Save failed', false);
      return;
    }
    closeModal();
    toast(mode === 'edit' ? 'Updated' : 'Added');
    table && table.replaceData();
  } catch (e) {
    toast('Network error', false);
  }
});

async function deleteRow(row, cfg) {
  if (!confirm(`Delete row #${row.id}? This cannot be undone.`)) return;
  try {
    const r = await fetch(cfg.endpoint + '/' + row.id, { method: 'DELETE' });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      toast(err.error || 'Delete failed', false);
      return;
    }
    toast('Deleted');
    table && table.replaceData();
  } catch (e) {
    toast('Network error', false);
  }
}

// ── Toast ───────────────────────────────────────────────────────────────────
function toast(msg, ok = true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = ok ? 'ok' : 'err';
  setTimeout(() => t.classList.add('hidden'), 2400);
  t.classList.remove('hidden');
}

// ── Wire up UI ──────────────────────────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(a => {
  a.addEventListener('click', e => {
    e.preventDefault();
    loadView(a.dataset.view);
  });
});
document.getElementById('btnAdd').addEventListener('click', () => {
  openAddModal(VIEWS[currentView]);
});
document.getElementById('btnReload').addEventListener('click', () => {
  table && table.replaceData();
});
// Debounced search
let searchTimer;
document.getElementById('searchBox').addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => table && table.replaceData(), 250);
});
// Escape closes modal
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeModal();
});
document.getElementById('modal').addEventListener('click', e => {
  if (e.target.id === 'modal') closeModal();
});

// Route from hash on first load
const initial = (location.hash.slice(1) || 'customers');
loadView(VIEWS[initial] ? initial : 'customers');
window.addEventListener('hashchange', () => {
  const h = location.hash.slice(1);
  if (VIEWS[h]) loadView(h);
});
