// NEAR Garden — Explorer
// Vanilla JS, no build step

const API = '';
const EXPLORER_URL = 'https://nearblocks.io/txns';

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function tryFormatJson(v) {
  if (typeof v !== 'string') return JSON.stringify(v, null, 2);
  try { return JSON.stringify(JSON.parse(v), null, 2); } catch { return v; }
}

function buildUrl(path, params) {
  const base = API || location.origin;
  const url = new URL(path, base);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }
  return url.toString();
}

function shQuote(s) {
  return "'" + String(s).replace(/'/g, "'\"'\"'") + "'";
}

function curlCmd(url) { return `curl -s ${shQuote(url)} | jq`; }

async function copyText(text, btn) {
  try {
    await navigator.clipboard.writeText(text);
    if (btn) {
      const orig = btn.textContent;
      btn.textContent = 'copied!';
      setTimeout(() => { btn.textContent = orig; }, 1500);
    }
  } catch { prompt('Copy:', text); }
}

// ── API Inspector state ────────────────────────────────────

let lastApiCall = null;

function logApiCall(method, url, body, status) {
  lastApiCall = { method, url, body, status, time: Date.now() };
  renderInspector();
}

function renderInspector() {
  const inspector = document.getElementById('api-inspector');
  const summary = document.getElementById('inspector-summary');
  const detail = document.getElementById('inspector-body');
  if (!inspector || !lastApiCall) return;

  inspector.hidden = false;
  const { method, url, body, status } = lastApiCall;
  const path = url.replace(window.location.origin, '');
  summary.textContent = `${method} ${path} — ${status}`;

  let bodyStr = '';
  if (method === 'POST' && body) {
    bodyStr = `\n${JSON.stringify(body, null, 2)}`;
  }
  detail.textContent = `${method} ${path}${bodyStr}`;
}

function toggleInspector() {
  const detail = document.getElementById('inspector-detail');
  const arrow = document.getElementById('inspector-arrow');
  if (!detail) return;
  const open = detail.hidden;
  detail.hidden = !open;
  arrow.classList.toggle('open', open);
}

function copyAsCurl() {
  if (!lastApiCall) return;
  const { method, url, body } = lastApiCall;
  let cmd;
  if (method === 'POST' && body) {
    cmd = `curl -s -X POST ${shQuote(url)} -H 'Content-Type: application/json' -d ${shQuote(JSON.stringify(body))} | jq`;
  } else {
    cmd = `curl -s ${shQuote(url)} | jq`;
  }
  copyText(cmd, document.getElementById('inspector-copy'));
}

// ── API Client ──────────────────────────────────────────────

async function kvAccounts(contractId, opts) {
  const params = { contractId };
  if (opts?.limit) params.limit = String(opts.limit);
  if (opts?.offset != null) params.offset = String(opts.offset);
  if (opts?.after_account) params.after_account = opts.after_account;
  const url = buildUrl('/v1/kv/accounts', params);
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvAccounts: ${res.status}`);
  const json = await res.json();
  return { accounts: json.data || [], meta: json.meta || {} };
}

async function kvGet(accountId, contractId, key) {
  const params = new URLSearchParams({ accountId, contractId, key, value_format: 'json' });
  const url = `${API}/v1/kv/get?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvGet: ${res.status}`);
  const json = await res.json();
  return json.data; // KvEntry | null
}

async function kvTimeline(accountId, contractId, limit) {
  const params = new URLSearchParams({
    accountId,
    contractId: contractId || 'contextual.near',
    limit: String(limit || 20),
    value_format: 'json',
  });
  const url = `${API}/v1/kv/timeline?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvTimeline: ${res.status}`);
  return res.json();
}

async function kvHistory(accountId, contractId, key, limit) {
  const params = new URLSearchParams({
    accountId,
    contractId,
    key,
    value_format: 'json',
    order: 'desc',
    limit: String(limit || 50),
  });
  const url = `${API}/v1/kv/history?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvHistory: ${res.status}`);
  const json = await res.json();
  return { entries: json.data || [], meta: json.meta || {} };
}

async function kvWriters(contractId, key, limit) {
  const params = new URLSearchParams({
    contractId,
    key,
    value_format: 'json',
    limit: String(limit || 20),
  });
  const url = `${API}/v1/kv/writers?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvWriters: ${res.status}`);
  return res.json();
}

async function kvDiff(accountId, contractId, key, blockA, blockB) {
  const params = new URLSearchParams({
    accountId, contractId, key,
    block_height_a: String(blockA),
    block_height_b: String(blockB),
    value_format: 'json',
  });
  const url = `${API}/v1/kv/diff?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvDiff: ${res.status}`);
  const json = await res.json();
  return json.data; // { a: KvEntry|null, b: KvEntry|null }
}

async function kvQueryTree(accountId, contractId, keyPrefix) {
  const params = new URLSearchParams({
    accountId, contractId,
    format: 'tree',
    value_format: 'json',
    exclude_null: 'true',
  });
  if (keyPrefix) params.set('key_prefix', keyPrefix);
  const url = `${API}/v1/kv/query?${params}`;
  const res = await fetch(url);
  logApiCall('GET', url, null, res.status);
  if (!res.ok) throw new Error(`kvQueryTree: ${res.status}`);
  const json = await res.json();
  return json.tree ?? json.data ?? null;
}

// ── State ───────────────────────────────────────────────────

let currentAccount = 'root.near';
let contractId = 'contextual.near';
let viewMode = 'tree'; // 'tree' | 'json' | 'plant' | 'feed'
let treeData = null;
let rawData = null;
let breadcrumb = ['root.near'];
let loading = false;
let multiAccountMode = false; // for cross-account tree grouping
let currentSelectedPath = null;
let currentHistoryEntries = [];

// ── DOM refs ────────────────────────────────────────────────

const $ = (sel) => document.querySelector(sel);
const contractInput = $('#contract-input');
const accountInput = $('#account-input');
const queryInput = $('#query-input');
const exploreBtn = $('#explore-btn');
const exploreForm = $('#explore-form');
const quickPaths = $('#quick-paths');
const breadcrumbEl = $('#breadcrumb');
const viewTreeBtn = $('#view-tree');
const viewJsonBtn = $('#view-json');
const viewPlantBtn = $('#view-plant');
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
const plantPanel = $('#plant-panel');
const feedPanel = $('#feed-panel');
const feedList = $('#feed-list');
const allAccountsCheck = $('#all-accounts-check');
const historyPanel = $('#history-panel');
const historyList = $('#history-list');
const diffSelectA = $('#diff-select-a');
const diffSelectB = $('#diff-select-b');
const diffBtn = $('#diff-btn');
const diffResult = $('#diff-result');
const diffCopyBar = $('#diff-copy-bar');

// ── Explorer ────────────────────────────────────────────────

const MULTI_ACCOUNT_CAP = 200;
const MULTI_ACCOUNT_CONCURRENCY = 5;

async function explore(keyPath) {
  currentAccount = accountInput.value || 'root.near';
  contractId = contractInput.value || 'contextual.near';
  multiAccountMode = allAccountsCheck && allAccountsCheck.checked;

  const keyPrefix = (keyPath || queryInput.value || 'profile/**').replace(/\/?\*+$/, '') || undefined;

  loading = true;
  exploreBtn.disabled = true;
  exploreBtn.textContent = '...';
  hideError();
  hideDetail();

  try {
    if (!multiAccountMode) {
      // ── Single-account: KV only, no fallback ──
      const tree = await kvQueryTree(currentAccount, contractId, keyPrefix);
      if (tree && Object.keys(tree).length > 0) {
        treeData = { [currentAccount]: tree };
        rawData = treeData;
      } else {
        showError('No data (KV)');
        treeData = null;
        rawData = null;
      }
    } else {
      // ── Cross-account: /v1/kv/accounts → parallel KV queries ──
      const { accounts } = await kvAccounts(contractId, { limit: MULTI_ACCOUNT_CAP });
      if (accounts.length === 0) {
        showError('No accounts found for this contract');
        treeData = null;
        rawData = null;
      } else {
        const merged = {};
        const truncated = accounts.length >= MULTI_ACCOUNT_CAP;

        // Parallel fetch with concurrency limit
        const queue = [...accounts];
        const inflight = [];
        while (queue.length > 0 || inflight.length > 0) {
          while (inflight.length < MULTI_ACCOUNT_CONCURRENCY && queue.length > 0) {
            const acct = queue.shift();
            const p = kvQueryTree(acct, contractId, keyPrefix)
              .then(tree => { if (tree && Object.keys(tree).length > 0) merged[acct] = tree; })
              .catch(err => console.warn(`kvQueryTree failed for ${acct}:`, err));
            p._done = false;
            p.finally(() => { p._done = true; });
            inflight.push(p);
          }
          await Promise.race(inflight);
          // Remove settled promises
          for (let i = inflight.length - 1; i >= 0; i--) {
            if (inflight[i]._done) inflight.splice(i, 1);
          }
        }

        if (Object.keys(merged).length === 0) {
          showError('No data (KV)');
          treeData = null;
          rawData = null;
        } else {
          treeData = merged;
          rawData = merged;
          if (truncated) {
            showError(`Showing first ${MULTI_ACCOUNT_CAP} accounts — more may exist`);
          }
        }
      }
    }
  } catch (e) {
    showError('Failed to fetch data');
    console.error(e);
  }

  loading = false;
  exploreBtn.disabled = false;
  exploreBtn.textContent = 'explore_';

  // Switch to tree view to show results
  if (viewMode === 'feed') setViewMode('tree');
  else render();
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
  setAccount(segments[0]);
  if (segments.length === 1) {
    queryInput.value = '';
    breadcrumb = [currentAccount];
    explore('profile/**');
  } else {
    const keyPath = segments.slice(1).join('/');
    queryInput.value = `${keyPath}/**`;
    breadcrumb = segments;
    explore(`${keyPath}/**`);
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

  if (multiAccountMode) {
    // Cross-account mode: show accounts as top-level nodes
    const accounts = Object.entries(treeData);
    if (accounts.length === 0) {
      treeEl.innerHTML = '<div class="tree-empty">no data</div>';
      return;
    }
    accounts.forEach(([acct, val]) => {
      treeEl.appendChild(createTreeNode(acct, val, acct, 0));
    });
    return;
  }

  // Single-account mode: flatten top-level account wrapper
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
      const tree = await kvQueryTree(currentAccount, contractId, path);
      children = tree && typeof tree === 'object' ? tree : {};
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

// ── Detail + History panels ─────────────────────────────────

function selectNode(path, value) {
  currentSelectedPath = path;
  detailPanel.hidden = false;
  if (historyPanel) historyPanel.hidden = false;

  // Path + copy buttons
  detailPath.innerHTML = '';
  const pathText = document.createElement('span');
  pathText.textContent = `${currentAccount}/${path}`;
  detailPath.appendChild(pathText);

  const getUrl = buildUrl('/v1/kv/get', {
    accountId: currentAccount, contractId, key: path, value_format: 'json',
  });
  const copyBar = document.createElement('div');
  copyBar.className = 'copy-bar';
  const cpUrl = document.createElement('button');
  cpUrl.className = 'copy-btn'; cpUrl.textContent = 'copy url';
  cpUrl.onclick = () => copyText(getUrl, cpUrl);
  const cpCurl = document.createElement('button');
  cpCurl.className = 'copy-btn'; cpCurl.textContent = 'copy curl';
  cpCurl.onclick = () => copyText(curlCmd(getUrl), cpCurl);
  copyBar.appendChild(cpUrl);
  copyBar.appendChild(cpCurl);
  detailPath.appendChild(copyBar);

  // Placeholder value from tree
  detailValue.textContent = tryFormatJson(value);
  detailMeta.innerHTML = '<div class="tree-loading">loading_</div>';
  if (historyList) historyList.innerHTML = '<div class="tree-loading">loading_</div>';
  if (diffResult) diffResult.textContent = '';

  // Fetch value + history in parallel
  Promise.all([
    kvGet(currentAccount, contractId, path),
    kvHistory(currentAccount, contractId, path, 50),
  ]).then(([entry, history]) => {
    // Detail panel
    if (entry) {
      detailValue.textContent = tryFormatJson(entry.value);
      renderDetailMeta(entry);
    } else {
      detailMeta.innerHTML = '';
    }
    // History panel
    currentHistoryEntries = history.entries;
    renderHistory(history.entries, history.meta);
  }).catch((e) => {
    console.error(`Failed to load detail for ${path}:`, e);
    detailMeta.innerHTML = '<div class="tree-empty">failed_</div>';
    if (historyList) historyList.innerHTML = '<div class="tree-empty">failed_</div>';
  });
}

function renderDetailMeta(entry) {
  let html = '';
  if (entry.block_height != null) {
    html += `<div><span class="meta-label">block: </span><span class="meta-value">${esc(String(entry.block_height))}</span></div>`;
  }
  if (entry.tx_hash) {
    html += `<div><span class="meta-label">tx: </span><a href="${EXPLORER_URL}/${encodeURIComponent(entry.tx_hash)}" target="_blank" rel="noopener noreferrer">${esc(entry.tx_hash.slice(0, 12))}...</a></div>`;
  }
  if (entry.receipt_id) {
    html += `<div><span class="meta-label">receipt: </span><span class="meta-value">${esc(entry.receipt_id.slice(0, 12))}...</span></div>`;
  }
  if (entry.accountId) {
    html += `<div><span class="meta-label">writer: </span><span class="meta-writer">${esc(entry.accountId)}</span></div>`;
  }
  if (entry.is_deleted) {
    html += `<div><span class="meta-label">deleted: </span><span style="color:var(--danger)">true</span></div>`;
  }
  detailMeta.innerHTML = html;
}

function renderHistory(entries, meta) {
  if (!historyList) return;
  historyList.innerHTML = '';
  if (diffSelectA) diffSelectA.innerHTML = '';
  if (diffSelectB) diffSelectB.innerHTML = '';

  if (entries.length === 0) {
    historyList.innerHTML = '<div class="tree-empty">no history</div>';
    return;
  }

  // Populate diff selects
  entries.forEach((e) => {
    if (diffSelectA) {
      const opt = document.createElement('option');
      opt.value = e.block_height;
      opt.textContent = `#${e.block_height}`;
      diffSelectA.appendChild(opt);
    }
    if (diffSelectB) {
      const opt = document.createElement('option');
      opt.value = e.block_height;
      opt.textContent = `#${e.block_height}`;
      diffSelectB.appendChild(opt);
    }
  });
  // Default A = 2nd-newest, B = newest
  if (entries.length >= 2 && diffSelectA) diffSelectA.selectedIndex = 1;

  // Render version list
  entries.forEach((e) => {
    const row = document.createElement('div');
    row.className = 'history-row';

    const block = document.createElement('span');
    block.className = 'history-block';
    block.textContent = `#${e.block_height}`;
    row.appendChild(block);

    if (e.is_deleted) {
      const del = document.createElement('span');
      del.className = 'history-deleted';
      del.textContent = 'deleted';
      row.appendChild(del);
    }

    const preview = document.createElement('span');
    preview.className = 'history-preview';
    const val = typeof e.value === 'string' ? e.value : JSON.stringify(e.value);
    preview.textContent = val.length > 40 ? val.slice(0, 40) + '...' : val;
    row.appendChild(preview);

    row.onclick = () => {
      detailValue.textContent = tryFormatJson(e.value);
      renderDetailMeta(e);
    };

    historyList.appendChild(row);
  });

  if (meta && meta.has_more) {
    const more = document.createElement('div');
    more.className = 'tree-empty';
    more.textContent = '+ more versions...';
    historyList.appendChild(more);
  }

  // History copy bar
  const hUrl = buildUrl('/v1/kv/history', {
    accountId: currentAccount, contractId, key: currentSelectedPath,
    value_format: 'json', order: 'desc', limit: '50',
  });
  const hBar = historyList.parentElement.querySelector('.history-copy-bar');
  if (hBar) {
    hBar.innerHTML = '';
    const cpU = document.createElement('button');
    cpU.className = 'copy-btn'; cpU.textContent = 'copy url';
    cpU.onclick = () => copyText(hUrl, cpU);
    const cpC = document.createElement('button');
    cpC.className = 'copy-btn'; cpC.textContent = 'copy curl';
    cpC.onclick = () => copyText(curlCmd(hUrl), cpC);
    hBar.appendChild(cpU);
    hBar.appendChild(cpC);
  }
}

function hideDetail() {
  detailPanel.hidden = true;
  if (historyPanel) historyPanel.hidden = true;
  currentSelectedPath = null;
  currentHistoryEntries = [];
}

// ── Feed / Timeline ────────────────────────────────────────

async function loadFeed(accountId) {
  if (!feedList) return;
  feedList.innerHTML = '<div class="tree-loading">loading_</div>';

  try {
    const result = await kvTimeline(accountId, contractId, 20);
    const items = result.data || result || [];

    if (items.length === 0) {
      feedList.innerHTML = '<div class="tree-empty">no recent activity</div>';
      return;
    }

    feedList.innerHTML = '';
    items.forEach(item => {
      const div = document.createElement('div');
      div.className = 'feed-item';
      div.onclick = () => feedNavigate(accountId, item.key || '');

      const header = document.createElement('div');
      header.className = 'feed-item-header';
      const keySpan = document.createElement('span');
      keySpan.className = 'feed-item-key';
      keySpan.textContent = item.key || '';
      const timeSpan = document.createElement('span');
      timeSpan.className = 'feed-item-time';
      timeSpan.textContent = item.block_timestamp
        ? timeAgo(Math.floor(Number(item.block_timestamp) / 1e6)) : '';
      header.appendChild(keySpan);
      header.appendChild(timeSpan);
      div.appendChild(header);

      const valDiv = document.createElement('div');
      valDiv.className = 'feed-item-value';
      const raw = typeof item.value === 'string' ? item.value : JSON.stringify(item.value || '');
      valDiv.textContent = raw.length > 80 ? raw.slice(0, 80) + '...' : raw;
      div.appendChild(valDiv);

      const blockDiv = document.createElement('div');
      blockDiv.className = 'feed-item-block';
      blockDiv.textContent = `block ${item.block_height || ''}`;
      div.appendChild(blockDiv);

      feedList.appendChild(div);
    });
  } catch (e) {
    feedList.innerHTML = '<div class="tree-empty">failed to load feed</div>';
    console.error(e);
  }
}

function feedNavigate(accountId, key) {
  setAccount(accountId);
  const parts = key.split('/');
  breadcrumb = [accountId, ...parts];
  queryInput.value = `${key}/**`;
  setViewMode('tree');
  explore(`${key}/**`);
}

function timeAgo(timestampMs) {
  const diff = Date.now() - timestampMs;
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

// ── Navigation ──────────────────────────────────────────────

function navigateToAccount(accountId) {
  setAccount(accountId);
  queryInput.value = '';
  breadcrumb = [currentAccount];
  explore('profile/**');
}

function handleQuickPath(path) {
  if (path === 'recent') {
    const acct = (typeof walletGetAccountId === 'function' && walletGetAccountId()) || currentAccount;
    setViewMode('feed');
    loadFeed(acct);
    return;
  }
  queryInput.value = `${path}/**`;
  currentAccount = accountInput.value || 'root.near';
  breadcrumb = [currentAccount, path];
  explore(`${path}/**`);
}

// ── View mode ───────────────────────────────────────────────

function setViewMode(mode) {
  viewMode = mode;
  viewTreeBtn.classList.toggle('active', mode === 'tree');
  viewJsonBtn.classList.toggle('active', mode === 'json');
  if (viewPlantBtn) viewPlantBtn.classList.toggle('active', mode === 'plant');
  viewTreeBtn.setAttribute('aria-pressed', mode === 'tree');
  viewJsonBtn.setAttribute('aria-pressed', mode === 'json');
  if (viewPlantBtn) viewPlantBtn.setAttribute('aria-pressed', mode === 'plant');
  render();
}

// ── Render ──────────────────────────────────────────────────

function render() {
  renderBreadcrumb();

  // Hide all panels first
  contentEl.hidden = true;
  jsonPanel.hidden = true;
  if (plantPanel) plantPanel.hidden = true;
  if (feedPanel) feedPanel.hidden = true;

  if (viewMode === 'json') {
    jsonPanel.hidden = false;
    jsonView.textContent = rawData ? JSON.stringify(rawData, null, 2) : '';
  } else if (viewMode === 'plant') {
    if (plantPanel) plantPanel.hidden = false;
  } else if (viewMode === 'feed') {
    if (feedPanel) feedPanel.hidden = false;
  } else {
    contentEl.hidden = false;
    renderTree();
  }
}

// ── Event listeners ─────────────────────────────────────────

exploreForm.onsubmit = (e) => {
  e.preventDefault();
  const q = queryInput.value.trim();
  currentAccount = accountInput.value || 'root.near';
  if (q) {
    const keyParts = q.replace(/\/?\*+$/, '').split('/');
    breadcrumb = [currentAccount, ...keyParts];
    explore(q);
  } else {
    breadcrumb = [currentAccount];
    explore();
  }
};

retryBtn.onclick = () => explore();

viewTreeBtn.onclick = () => setViewMode('tree');
viewJsonBtn.onclick = () => setViewMode('json');
if (viewPlantBtn) viewPlantBtn.onclick = () => setViewMode('plant');

quickPaths.onclick = (e) => {
  const btn = e.target.closest('[data-path]');
  if (btn) handleQuickPath(btn.dataset.path);
};

contractInput.onchange = () => {
  contractId = contractInput.value;
};

accountInput.onchange = () => {
  currentAccount = accountInput.value;
};

if (allAccountsCheck) {
  allAccountsCheck.onchange = () => {
    accountInput.disabled = allAccountsCheck.checked;
  };
}

function setAccount(accountId) {
  currentAccount = accountId;
  if (accountInput) { accountInput.value = accountId; accountInput.disabled = false; }
  if (allAccountsCheck) allAccountsCheck.checked = false;
}

// Diff button handler
if (diffBtn) {
  diffBtn.onclick = async () => {
    const blockA = parseInt(diffSelectA.value, 10);
    const blockB = parseInt(diffSelectB.value, 10);
    if (isNaN(blockA) || isNaN(blockB) || !currentSelectedPath) return;

    diffBtn.disabled = true;
    diffBtn.textContent = '...';
    diffResult.textContent = 'loading diff...';

    try {
      const diff = await kvDiff(currentAccount, contractId, currentSelectedPath, blockA, blockB);
      const aVal = diff.a ? tryFormatJson(diff.a.value) : '(not found)';
      const bVal = diff.b ? tryFormatJson(diff.b.value) : '(not found)';
      diffResult.textContent = `\u2500\u2500 block #${blockA} \u2500\u2500\n${aVal}\n\n\u2500\u2500 block #${blockB} \u2500\u2500\n${bVal}`;

      // Diff copy buttons
      if (diffCopyBar) {
        diffCopyBar.innerHTML = '';
        const dUrl = buildUrl('/v1/kv/diff', {
          accountId: currentAccount, contractId,
          key: currentSelectedPath,
          block_height_a: blockA, block_height_b: blockB,
          value_format: 'json',
        });
        const cpU = document.createElement('button');
        cpU.className = 'copy-btn'; cpU.textContent = 'copy url';
        cpU.onclick = () => copyText(dUrl, cpU);
        const cpC = document.createElement('button');
        cpC.className = 'copy-btn'; cpC.textContent = 'copy curl';
        cpC.onclick = () => copyText(curlCmd(dUrl), cpC);
        diffCopyBar.appendChild(cpU);
        diffCopyBar.appendChild(cpC);
      }
    } catch (e) {
      console.error('Diff failed:', e);
      diffResult.textContent = 'diff failed';
    }

    diffBtn.disabled = false;
    diffBtn.textContent = 'diff';
  };
}

// ── Init ────────────────────────────────────────────────────

currentAccount = accountInput.value || 'root.near';
breadcrumb = [currentAccount];
explore('profile/**');
