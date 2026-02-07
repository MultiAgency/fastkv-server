// NEAR Garden — Explorer
// Vanilla JS, no build step

const API = '';
const EXPLORER_URL = 'https://nearblocks.io/txns';

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

// ── API Client ──────────────────────────────────────────────

async function socialKeys(keys, contractId) {
  const body = { keys };
  if (contractId) body.contract_id = contractId;
  const res = await fetch(`${API}/v1/social/keys`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`socialKeys: ${res.status}`);
  return res.json();
}

async function socialGet(keys, contractId) {
  const body = { keys };
  if (contractId) body.contract_id = contractId;
  const res = await fetch(`${API}/v1/social/get`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`socialGet: ${res.status}`);
  return res.json();
}

async function kvGet(predecessorId, currentAccountId, key) {
  const params = new URLSearchParams({
    predecessor_id: predecessorId,
    current_account_id: currentAccountId,
    key,
  });
  const res = await fetch(`${API}/v1/kv/get?${params}`);
  if (!res.ok) throw new Error(`kvGet: ${res.status}`);
  const data = await res.json();
  return data;
}

// ── State ───────────────────────────────────────────────────

let currentAccount = 'root.near';
let contractId = 'social.near';
let viewMode = 'tree'; // 'tree' | 'json'
let treeData = null;
let rawData = null;
let breadcrumb = ['root.near'];
let loading = false;

// ── DOM refs ────────────────────────────────────────────────

const $ = (sel) => document.querySelector(sel);
const contractInput = $('#contract-input');
const queryInput = $('#query-input');
const exploreBtn = $('#explore-btn');
const exploreForm = $('#explore-form');
const quickPaths = $('#quick-paths');
const breadcrumbEl = $('#breadcrumb');
const viewTreeBtn = $('#view-tree');
const viewJsonBtn = $('#view-json');
const errorBar = $('#error-bar');
const errorMsg = $('#error-msg');
const retryBtn = $('#retry-btn');
const contentEl = $('#content');
const treePanel = $('#tree-panel');
const treeEl = $('#tree');
const detailPanel = $('#detail-panel');
const detailPath = $('#detail-path');
const detailValue = $('#detail-value');
const detailMeta = $('#detail-meta');
const jsonPanel = $('#json-panel');
const jsonView = $('#json-view');

// ── Explorer ────────────────────────────────────────────────

async function explore(pattern) {
  const q = pattern || queryInput.value || `${currentAccount}/profile/**`;
  contractId = contractInput.value || 'social.near';
  loading = true;
  exploreBtn.disabled = true;
  exploreBtn.textContent = '...';
  hideError();
  hideDetail();

  try {
    const keysResult = await socialKeys([q], contractId);
    if (Object.keys(keysResult).length === 0) {
      showError('No data found');
      treeData = null;
      rawData = null;
    } else {
      treeData = keysResult;
      const valResult = await socialGet([q], contractId);
      rawData = valResult;
    }
  } catch (e) {
    showError('Failed to fetch data');
    console.error(e);
  }

  loading = false;
  exploreBtn.disabled = false;
  exploreBtn.textContent = 'explore_';
  render();
}

function showError(msg) {
  errorMsg.textContent = msg;
  errorBar.hidden = false;
}

function hideError() {
  errorBar.hidden = true;
}

// ── Breadcrumb ──────────────────────────────────────────────

function renderBreadcrumb() {
  breadcrumbEl.innerHTML = '';

  const root = document.createElement('button');
  root.textContent = '~';
  root.type = 'button';
  root.onclick = () => navigateBreadcrumb(breadcrumb.slice(0, 1));
  breadcrumbEl.appendChild(root);

  breadcrumb.forEach((seg, i) => {
    const sep = document.createElement('span');
    sep.className = 'sep';
    sep.textContent = '/';
    breadcrumbEl.appendChild(sep);

    if (i < breadcrumb.length - 1) {
      const btn = document.createElement('button');
      btn.textContent = seg;
      btn.type = 'button';
      btn.onclick = () => navigateBreadcrumb(breadcrumb.slice(0, i + 1));
      breadcrumbEl.appendChild(btn);
    } else {
      const span = document.createElement('span');
      span.className = 'current';
      span.textContent = seg;
      breadcrumbEl.appendChild(span);
    }
  });
}

function navigateBreadcrumb(segments) {
  if (segments.length === 1) {
    currentAccount = segments[0];
    queryInput.value = '';
    breadcrumb = [currentAccount];
    explore(`${currentAccount}/profile/**`);
  } else {
    const path = segments.slice(1).join('/');
    const pattern = `${segments[0]}/${path}/**`;
    queryInput.value = pattern;
    breadcrumb = segments;
    explore(pattern);
  }
}

// ── Tree rendering ──────────────────────────────────────────

function renderTree() {
  treeEl.innerHTML = '';

  if (loading && !treeData) {
    treeEl.innerHTML = '<div class="tree-loading">loading_</div>';
    return;
  }

  if (!treeData) {
    treeEl.innerHTML = '<div class="tree-empty">no data</div>';
    return;
  }

  // Flatten: treeData = { "account.near": { "profile": {...}, ... } }
  const entries = Object.entries(treeData).flatMap(([_acct, val]) =>
    typeof val === 'object' && val !== null
      ? Object.entries(val)
      : []
  );

  if (entries.length === 0) {
    treeEl.innerHTML = '<div class="tree-empty">no data</div>';
    return;
  }

  entries.forEach(([key, val]) => {
    treeEl.appendChild(createTreeNode(key, val, key, 0));
  });
}

function createTreeNode(name, value, path, depth) {
  const container = document.createElement('div');
  const isBranch = typeof value === 'object' && value !== null;
  const isNearAccount = name.endsWith('.near') || name.endsWith('.tg');
  let expanded = false;
  let childrenLoaded = isBranch && Object.keys(value).length > 0;
  let children = isBranch ? value : null;

  // The clickable row
  const row = document.createElement('div');
  row.className = 'tree-item';
  row.tabIndex = 0;
  row.setAttribute('role', 'treeitem');

  // Icon
  const icon = document.createElement('span');
  icon.className = 'tree-icon' + (isBranch ? '' : ' leaf');
  icon.textContent = isBranch ? '\u25b6' : '=';
  row.appendChild(icon);

  // Name
  const nameEl = document.createElement('span');
  nameEl.className = 'tree-name' + (isBranch ? ' branch' : '') + (isNearAccount ? ' near-account' : '');
  nameEl.textContent = name;
  if (isNearAccount) {
    nameEl.onclick = (e) => {
      e.stopPropagation();
      navigateToAccount(name);
    };
  }
  row.appendChild(nameEl);

  // Leaf value preview
  if (!isBranch && value !== null && value !== undefined) {
    const preview = document.createElement('span');
    preview.className = 'tree-preview';
    const str = typeof value === 'string' ? value : JSON.stringify(value);
    preview.textContent = str.length > 60 ? str.slice(0, 60) + '...' : str;
    row.appendChild(preview);
  }

  container.appendChild(row);

  // Children container
  const childrenEl = document.createElement('div');
  childrenEl.className = 'tree-children';
  childrenEl.hidden = true;
  container.appendChild(childrenEl);

  function toggle() {
    if (!isBranch) {
      selectNode(path, value);
      return;
    }
    expanded = !expanded;
    icon.textContent = expanded ? '\u25bc' : '\u25b6';
    row.setAttribute('aria-expanded', expanded);

    if (expanded && !childrenLoaded) {
      loadChildren();
    } else {
      childrenEl.hidden = !expanded;
    }
  }

  async function loadChildren() {
    icon.textContent = '...';
    try {
      const data = await socialKeys([`${currentAccount}/${path}/*`], contractId);
      let node = data[currentAccount];
      for (const p of path.split('/')) {
        if (node && typeof node === 'object') node = node[p];
        else { node = null; break; }
      }
      children = node && typeof node === 'object' ? node : {};
      childrenLoaded = true;
      renderChildren();
      childrenEl.hidden = false;
    } catch (e) {
      console.error(`Failed to load children for ${path}:`, e);
      childrenEl.innerHTML = '<div class="tree-empty">failed_</div>';
      childrenEl.hidden = false;
    }
    icon.textContent = expanded ? '\u25bc' : '\u25b6';
  }

  function renderChildren() {
    childrenEl.innerHTML = '';
    const entries = Object.entries(children);
    if (entries.length === 0) {
      childrenEl.innerHTML = '<div class="tree-empty">(empty)</div>';
      return;
    }
    entries.forEach(([k, v]) => {
      childrenEl.appendChild(createTreeNode(k, v, `${path}/${k}`, depth + 1));
    });
  }

  // If branch already has children data, pre-render them
  if (isBranch && childrenLoaded) {
    renderChildren();
  }

  row.onclick = toggle;
  row.onkeydown = (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
  };

  return container;
}

// ── Detail panel ────────────────────────────────────────────

function selectNode(path, value) {
  detailPanel.hidden = false;
  detailPath.textContent = `${currentAccount}/${path}`;

  const formatted = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  detailValue.textContent = formatted;

  detailMeta.innerHTML = '<div class="tree-loading">loading metadata_</div>';

  kvGet(currentAccount, contractId, path)
    .then((entry) => {
      if (!entry) {
        detailMeta.innerHTML = '';
        return;
      }
      let html = '';
      if (entry.block_height) {
        html += `<div><span class="meta-label">block: </span><span class="meta-value">${esc(String(entry.block_height))}</span></div>`;
      }
      if (entry.tx_hash) {
        html += `<div><span class="meta-label">tx: </span><a href="${EXPLORER_URL}/${encodeURIComponent(entry.tx_hash)}" target="_blank" rel="noopener noreferrer">${esc(entry.tx_hash.slice(0, 12))}...</a></div>`;
      }
      if (entry.predecessor_id) {
        html += `<div><span class="meta-label">writer: </span><span class="meta-writer">${esc(entry.predecessor_id)}</span></div>`;
      }
      detailMeta.innerHTML = html;
    })
    .catch((e) => {
      console.error(`Failed to load detail for ${path}:`, e);
      detailMeta.innerHTML = '<div class="tree-empty">failed_</div>';
    });
}

function hideDetail() {
  detailPanel.hidden = true;
}

// ── Navigation ──────────────────────────────────────────────

function navigateToAccount(accountId) {
  currentAccount = accountId;
  queryInput.value = '';
  breadcrumb = [currentAccount];
  explore(`${currentAccount}/profile/**`);
}

function handleQuickPath(path) {
  const pattern = `${currentAccount}/${path}/**`;
  queryInput.value = pattern;
  breadcrumb = [currentAccount, path];
  explore(pattern);
}

// ── View mode ───────────────────────────────────────────────

function setViewMode(mode) {
  viewMode = mode;
  viewTreeBtn.classList.toggle('active', mode === 'tree');
  viewJsonBtn.classList.toggle('active', mode === 'json');
  viewTreeBtn.setAttribute('aria-pressed', mode === 'tree');
  viewJsonBtn.setAttribute('aria-pressed', mode === 'json');
  render();
}

// ── Render ──────────────────────────────────────────────────

function render() {
  renderBreadcrumb();

  if (viewMode === 'json') {
    contentEl.hidden = true;
    jsonPanel.hidden = false;
    jsonView.textContent = rawData ? JSON.stringify(rawData, null, 2) : '';
  } else {
    contentEl.hidden = false;
    jsonPanel.hidden = true;
    renderTree();
  }
}

// ── Event listeners ─────────────────────────────────────────

exploreForm.onsubmit = (e) => {
  e.preventDefault();
  const q = queryInput.value.trim();
  if (q) {
    const parts = q.replace(/\/?\*+$/, '').split('/');
    breadcrumb = parts;
    explore(q);
  } else {
    explore();
  }
};

retryBtn.onclick = () => explore();

viewTreeBtn.onclick = () => setViewMode('tree');
viewJsonBtn.onclick = () => setViewMode('json');

quickPaths.onclick = (e) => {
  const btn = e.target.closest('[data-path]');
  if (btn) handleQuickPath(btn.dataset.path);
};

contractInput.onchange = () => {
  contractId = contractInput.value;
};

// ── Init ────────────────────────────────────────────────────

breadcrumb = [currentAccount];
explore(`${currentAccount}/profile/**`);
