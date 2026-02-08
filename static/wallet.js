// NEAR Garden — Wallet + Plant
// Depends on near-api-js loaded via CDN (global nearApi)

const NEAR_NETWORK = 'mainnet';
const NEAR_NODE_URL = 'https://rpc.mainnet.near.org';
const NEAR_WALLET_URL = 'https://app.mynearwallet.com';
const SOCIAL_CONTRACT = 'social.near';
const APP_KEY_PREFIX = 'near-garden';

let nearConnection = null;
let walletConnection = null;

// ── Init ────────────────────────────────────────────────────

async function initNear() {
  const keyStore = new nearApi.keyStores.BrowserLocalStorageKeyStore();
  nearConnection = await nearApi.connect({
    networkId: NEAR_NETWORK,
    keyStore,
    nodeUrl: NEAR_NODE_URL,
    walletUrl: NEAR_WALLET_URL,
    headers: {},
  });
  walletConnection = new nearApi.WalletConnection(nearConnection, APP_KEY_PREFIX);
  renderWalletUI();

  // If just returned from wallet redirect, check for plant intent
  if (walletIsSignedIn()) {
    const pending = localStorage.getItem('near-garden-pending-plant');
    if (pending) {
      localStorage.removeItem('near-garden-pending-plant');
      try {
        const { keyPath, value } = JSON.parse(pending);
        setPlantFields(keyPath, value);
        setViewMode('plant');
      } catch (_) { /* ignore corrupt data */ }
    }
  }
}

function walletIsSignedIn() {
  return walletConnection && walletConnection.isSignedIn();
}

function walletGetAccountId() {
  return walletConnection ? walletConnection.getAccountId() : null;
}

function walletSignIn() {
  if (!walletConnection) return;
  walletConnection.requestSignIn({ contractId: SOCIAL_CONTRACT });
}

function walletSignOut() {
  if (!walletConnection) return;
  walletConnection.signOut();
  renderWalletUI();
  // If on plant tab, switch back to tree
  if (viewMode === 'plant') setViewMode('tree');
}

// ── Wallet UI ───────────────────────────────────────────────

function renderWalletUI() {
  const container = document.getElementById('wallet-area');
  if (!container) return;

  if (walletIsSignedIn()) {
    const accountId = walletGetAccountId();
    container.innerHTML =
      `<span class="wallet-account">${esc(accountId)}</span>` +
      `<button type="button" class="wallet-btn disconnect" onclick="walletSignOut()">disconnect</button>`;
    // Auto-fill account field only if still on the default value
    const acctInput = document.getElementById('account-input');
    if (acctInput && acctInput.value === 'root.near') {
      acctInput.value = accountId;
    }
    // Show the plant tab
    const plantBtn = document.getElementById('view-plant');
    if (plantBtn) plantBtn.hidden = false;
  } else {
    container.innerHTML =
      `<button type="button" class="wallet-btn connect" onclick="walletSignIn()">connect wallet</button>`;
    // Hide the plant tab
    const plantBtn = document.getElementById('view-plant');
    if (plantBtn) plantBtn.hidden = true;
  }
}

// ── Plant ───────────────────────────────────────────────────

const PLANT_TEMPLATES = {
  profile: {
    keyPath: 'profile',
    value: {
      name: 'Your Name',
      description: 'Built with NEAR Garden',
      linktree: { website: 'https://example.com' },
    },
  },
  post: {
    keyPath: 'post/main',
    value: JSON.stringify({ text: 'Hello from NEAR Garden!' }),
  },
  widget: {
    keyPath: 'widget/Greeting',
    value: { '': 'return <div><h3>Hello!</h3><p>Built in NEAR Garden</p></div>;' },
  },
  raw: {
    keyPath: '',
    value: {},
  },
};

function setPlantFields(keyPath, value) {
  const keyInput = document.getElementById('plant-key');
  const valueInput = document.getElementById('plant-value');
  if (keyInput) keyInput.value = keyPath || '';
  if (valueInput) {
    valueInput.value = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  }
  updatePlantPreview();
}

function applyTemplate(name) {
  const tpl = PLANT_TEMPLATES[name];
  if (tpl) setPlantFields(tpl.keyPath, tpl.value);
}

function updatePlantPreview() {
  const preview = document.getElementById('plant-preview');
  const keyInput = document.getElementById('plant-key');
  if (!preview || !keyInput) return;
  const accountId = walletGetAccountId() || 'you.near';
  const key = keyInput.value || '(key)';
  preview.textContent = `${accountId}/${key}`;
}

// Convert slash-delimited key path to nested object
// e.g. "recipes/carbonara" + value → { "recipes": { "carbonara": value } }
function keyPathToNested(keyPath, value) {
  const parts = keyPath.split('/').filter(Boolean);
  if (parts.length === 0) return value;

  let obj = value;
  for (let i = parts.length - 1; i >= 0; i--) {
    obj = { [parts[i]]: obj };
  }
  return obj;
}

async function plantData() {
  if (!walletIsSignedIn()) return;

  const keyInput = document.getElementById('plant-key');
  const valueInput = document.getElementById('plant-value');
  const statusEl = document.getElementById('plant-status');
  const plantBtn = document.getElementById('plant-btn');

  const keyPath = (keyInput.value || '').trim();
  if (!keyPath) {
    if (statusEl) statusEl.textContent = 'enter a key path';
    return;
  }

  let parsedValue;
  try {
    parsedValue = JSON.parse(valueInput.value);
  } catch (_) {
    // Treat as raw string
    parsedValue = valueInput.value;
  }

  const accountId = walletGetAccountId();
  const nested = keyPathToNested(keyPath, parsedValue);
  const args = { data: { [accountId]: nested } };

  // Save intent in case of redirect
  localStorage.setItem('near-garden-pending-plant', JSON.stringify({ keyPath, value: valueInput.value }));

  if (plantBtn) { plantBtn.disabled = true; plantBtn.textContent = '...'; }
  if (statusEl) statusEl.textContent = 'signing transaction...';

  try {
    const account = walletConnection.account();
    await account.functionCall({
      contractId: SOCIAL_CONTRACT,
      methodName: 'set',
      args,
      gas: '30000000000000',
    });

    // If we get here (unlikely with redirect wallet), plant succeeded
    localStorage.removeItem('near-garden-pending-plant');
    if (statusEl) statusEl.textContent = 'planted! waiting for indexer...';
    pollForIndexed(accountId, keyPath);
  } catch (e) {
    console.error('Plant failed:', e);
    localStorage.removeItem('near-garden-pending-plant');
    const msg = e.message || 'unknown error';
    const isRejected = msg.includes('User denied') || msg.includes('rejected') || msg.includes('cancelled');
    if (statusEl) statusEl.textContent = isRejected ? 'transaction cancelled' : `failed: ${msg}`;
    if (plantBtn) { plantBtn.disabled = false; plantBtn.textContent = 'plant_'; }
  }
}

async function pollForIndexed(accountId, keyPath) {
  const statusEl = document.getElementById('plant-status');
  const plantBtn = document.getElementById('plant-btn');
  const cId = contractInput ? contractInput.value : SOCIAL_CONTRACT;

  let delay = 2000; // start at 2s, backoff 1.5x each round
  for (let i = 0; i < 12; i++) {
    await new Promise(r => setTimeout(r, delay));
    try {
      const res = await fetch(`${API}/v1/kv/get?accountId=${encodeURIComponent(accountId)}&contractId=${encodeURIComponent(cId)}&key=${encodeURIComponent(keyPath)}`);
      if (res.ok) {
        const json = await res.json();
        if (json.data) {
          if (statusEl) {
            statusEl.innerHTML =
              `indexed! <button type="button" class="plant-view-btn" onclick="viewPlantedData('${esc(accountId)}', '${esc(keyPath)}')">view in explorer</button>`;
          }
          if (plantBtn) { plantBtn.disabled = false; plantBtn.textContent = 'plant_'; }
          return;
        }
      }
    } catch (_) { /* retry */ }
    delay = Math.min(delay * 1.5, 10000);
    if (statusEl) statusEl.textContent = `waiting for indexer... (${i + 1})`;
  }

  if (statusEl) statusEl.textContent = 'indexing may take a moment — try exploring in a few seconds';
  if (plantBtn) { plantBtn.disabled = false; plantBtn.textContent = 'plant_'; }
}

function viewPlantedData(accountId, keyPath) {
  const acctInput = document.getElementById('account-input');
  if (acctInput) acctInput.value = accountId;
  currentAccount = accountId;
  if (queryInput) queryInput.value = `${keyPath}/**`;
  breadcrumb = [accountId, ...keyPath.split('/')];
  setViewMode('tree');
  explore(`${keyPath}/**`);
}
