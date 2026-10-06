// actions.js — the sidebar's Actions tab: re-parsing a database in place,
// fetching remote SQLite files, reloading plugin plot types, and the
// Schema/Actions sidebar tab switcher itself.
import { G, tabEl, escHtml } from './state.js';
import { api, addLogEntry, clientLog } from './log.js';
import './builder.js'; // window.runShow, window.renderPlotChips, window.renderExtraOpts — see below
import './schema.js';  // window.renderSchemaTree

// --- Re-parse ------------------------------------------------------------
export function populateReparseSel() {
  const wrap = document.getElementById('reparse-db-wrap');
  const sel = document.getElementById('reparse-db-sel');
  sel.innerHTML = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
  if (G.singleDb) { wrap.style.display = 'none'; sel.value = G.defaultDb; }
  else { wrap.style.display = 'block'; }
}

export function populateAllDbSelects() {
  const opts = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
  const allIds = [...G.tabs.map(t => t.id), ...Object.keys(G.panelState)];
  allIds.forEach(id => {
    const sel = tabEl(id, 'db-select');
    if (sel) { const prev = sel.value; sel.innerHTML = opts; if (prev) sel.value = prev; }
    const dbWrap = document.getElementById(`db-select-wrap-${id}`);
    if (dbWrap) dbWrap.style.display = G.singleDb ? 'none' : 'block';
  });
}

async function onReparseClick() {
  const db = G.singleDb ? G.defaultDb : document.getElementById('reparse-db-sel').value;
  if (!db) return;
  clientLog(`Re-parsing database "${db}"…`);
  document.getElementById('reparse-result').textContent = 'Running…';
  // window.api, not the local import: no current addon wraps '/api/reparse',
  // but every other cross-cutting call in this split goes through window.*
  // for the same reason (see plotting.js's header), so this stays
  // consistent and future-proof rather than being a special case.
  const res = await window.api('POST', '/api/reparse', { database: db });
  document.getElementById('reparse-result').textContent = res.message || res.error;
  if (res.databases) {
    G.databases = res.databases;
    G.singleDb = !!res.single_db;
    G.defaultDb = res.default_database || '';
    window.renderSchemaTree(); populateReparseSel(); populateAllDbSelects();
    // Refresh every open tab/panel bound to this DB so newly added columns
    // actually show up (this used to require a server restart).
    const affectedIds = [
      ...G.tabs.filter(t => t.state.database === db).map(t => t.id),
      ...Object.keys(G.panelState).filter(pid => G.panelState[pid].database === db),
    ];
    for (const id of affectedIds) {
      const sqlVal = tabEl(id, 'sql-input')?.value.trim();
      // window.runShow: axis_addon.js wraps it to restore its own fields
      // once a fresh query resolves — see builder.js/config.js for the
      // fuller version of this comment; the same reasoning applies to any
      // call site that re-runs a query, including this bulk refresh.
      if (sqlVal) await window.runShow(id);
    }
    if (affectedIds.length) clientLog(`Refreshed columns for ${affectedIds.length} open panel(s) bound to "${db}"`);
  }
  if (res.log_entry) addLogEntry(res.log_entry);
}

// --- Remote fetch ----------------------------------------------------------
export function populateFetchSystems() {
  const sel = document.getElementById('fetch-system-sel');
  sel.innerHTML = '<option value="">— select system —</option>' +
    Object.keys(G.remoteSystems).map(s => `<option value="${s}">${s}</option>`).join('');
}

export function onFetchSystemChange() {
  const sys = document.getElementById('fetch-system-sel').value;
  const paths = G.remoteSystems[sys] || [];
  const list = document.getElementById('fetch-path-list');
  if (!paths.length) { list.innerHTML = '<span style="color:var(--text3);font-size:9px">No paths configured</span>'; return; }
  list.innerHTML = paths.map(p =>
    `<label class="fetch-path-item"><input type="checkbox" value="${escHtml(p)}"> <span>${escHtml(p)}</span></label>`
  ).join('');
}

export async function runFetch() {
  const sys = document.getElementById('fetch-system-sel').value;
  if (!sys) { clientLog('Select a remote system first', 'warn'); return; }
  const checked = [...document.querySelectorAll('#fetch-path-list input:checked')].map(i => i.value);
  if (!checked.length) { clientLog('No paths selected', 'warn'); return; }
  clientLog(`Fetching ${checked.length} path(s) from "${sys}"…`);
  document.getElementById('fetch-result').textContent = 'Running…';
  const res = await window.api('POST', '/api/fetch_remote', { system: sys, paths: checked });
  document.getElementById('fetch-result').textContent = res.message || res.error;
  if (res.databases) {
    G.databases = res.databases;
    G.singleDb = !!res.single_db;
    G.defaultDb = res.default_database || '';
    window.renderSchemaTree(); populateReparseSel(); populateAllDbSelects();
  }
  if (res.log_entry) addLogEntry(res.log_entry);
}

// --- Plugins ---------------------------------------------------------------
async function onReloadPluginsClick() {
  clientLog('Reloading plugins…');
  const res = await window.api('POST', '/api/reload_plugins', { dirs: [] });
  if (res.error) { clientLog(res.error, 'error'); return; }
  Object.assign(G.plotTypes, res.plot_types || {});
  const allIds = [...G.tabs.map(t => t.id), ...Object.keys(G.panelState)];
  // window.renderPlotChips/renderExtraOpts: no current addon wraps these
  // specifically, but kept consistent with the rest of this module — see
  // the comment on window.api above.
  allIds.forEach(id => { window.renderPlotChips(id); window.renderExtraOpts(id); });
  document.getElementById('plugin-list').textContent = res.reloaded.length ? res.reloaded.join(', ') : 'No plugins found';
  if (res.log_entry) addLogEntry(res.log_entry);
}

// --- Sidebar Schema/Actions tab switcher -----------------------------------
function wireSidebarTabs() {
  document.querySelectorAll('.sb-tab').forEach(t => t.addEventListener('click', () => {
    const name = t.dataset.sbtab;
    document.querySelectorAll('.sb-tab').forEach(x => x.classList.toggle('active', x.dataset.sbtab === name));
    document.querySelectorAll('.sb-pane').forEach(p => p.classList.toggle('active', p.id === `sbtab-${name}`));
  }));
}

export function wireActionsSidebar() {
  document.getElementById('btn-reparse').addEventListener('click', onReparseClick);
  document.getElementById('btn-reload-plugins').addEventListener('click', onReloadPluginsClick);
  wireSidebarTabs();
}

// onFetchSystemChange and runFetch are invoked from inline onchange="..."/
// onclick="..." HTML attributes in the static webapp.html shell (same
// pattern as closeTab/renameTab in tabs.js), so they must be reachable as
// bare globals rather than only as module exports.
Object.assign(window, {
  populateReparseSel, populateAllDbSelects, populateFetchSystems,
  onFetchSystemChange, runFetch, wireActionsSidebar,
});
