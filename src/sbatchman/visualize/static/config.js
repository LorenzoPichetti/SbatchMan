// config.js — turning a node/tab's live UI state into a saved config object
// (and back), and the workspace-level JSON import/export this all serves.
//
// This module is patched more heavily by the addon files than any other
// split so far: axis_addon.js wraps window.getNodeConfig/applyNodeConfig to
// carry its axis-range/y2/error-bar fields; grid_addon.js wraps
// window.getTabConfig/applyTabConfig to carry share_x/share_y/panel-labels/
// legend; style_addon.js wraps window.download/applyWorkspace to carry the
// global style. Every internal call site below that could reach one of
// those six functions goes through window.* rather than a local reference —
// see plotting.js and grid.js's module headers for the fuller reasoning
// behind this rule; getting it wrong here is exactly how three class of
// regression already slipped into earlier chunks before being caught.
import { G, getState, toArray } from './state.js';
import { clientLog } from './log.js';
import './builder.js';
import './tabs.js';
import './grid.js';

function isEmptyVal(v) {
  return v === '' || v === null || v === undefined ||
    (Array.isArray(v) && v.length === 0) ||
    (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0);
}

function pruneConfig(obj, alwaysKeep) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (alwaysKeep.includes(k)) { out[k] = v; continue; }
    if (isEmptyVal(v)) continue;
    out[k] = v;
  }
  return out;
}

export function getNodeConfig(id) {
  const st = getState(id);
  if (!st) return null;
  const pt = G.plotTypes[st.plotType] || {};
  const extra = {};
  for (const [key, defVal] of Object.entries(pt.defaults || {})) {
    const inp = document.getElementById(`extra-${id}-${key}`);
    if (!inp) continue;
    const val = isNaN(inp.value) ? inp.value : Number(inp.value);
    if (String(val) !== String(defVal)) extra[key] = val; // only non-default values
  }
  for (const field of G.plotFields[st.plotType] || []) {
    const input = document.getElementById(`plot-field-${id}-${field.name}`);
    if (!input) continue;
    const val = input.type === 'checkbox' ? input.checked
      : input.type === 'number' ? (input.value === '' ? '' : Number(input.value)) : input.value;
    const defaultValue = input.type === 'checkbox' ? field.default === 'true'
      : input.type === 'number' && field.default !== '' ? Number(field.default) : field.default || '';
    if (val !== defaultValue) extra[field.name] = val;
  }
  const raw = {
    database: window.currentDb ? window.currentDb(id) : (G.singleDb ? G.defaultDb : document.getElementById(`db-select-${id}`)?.value) || '',
    sql: document.getElementById(`sql-input-${id}`)?.value || '',
    plotType: st.plotType,
    columns: st.columns,
    chartTitle: document.getElementById(`chart-title-${id}`)?.value || '',
    x: document.getElementById(`x-col-${id}`)?.value || '',
    y: st.yCols,
    group: st.groupCols,
    markerBy: document.getElementById(`marker-by-${id}`)?.value || '',
    dashBy: document.getElementById(`dash-by-${id}`)?.value || '',
    z: document.getElementById(`plot-field-${id}-z`)?.value || st.z || '',
    xLabel: document.getElementById(`x-label-${id}`)?.value || '',
    yLabel: document.getElementById(`y-label-${id}`)?.value || '',
    xScale: (document.getElementById(`x-scale-${id}`)?.value || 'linear') === 'linear' ? '' : document.getElementById(`x-scale-${id}`).value,
    yScale: (document.getElementById(`y-scale-${id}`)?.value || 'linear') === 'linear' ? '' : document.getElementById(`y-scale-${id}`).value,
    xTickFmt: document.getElementById(`x-tickfmt-${id}`)?.value || '',
    yTickFmt: document.getElementById(`y-tickfmt-${id}`)?.value || '',
    tickFormatter: document.getElementById(`tick-formatter-${id}`)?.value || '',
    legendPosition: (document.getElementById(`legend-position-${id}`)?.value || 'right') === 'right' ? '' : document.getElementById(`legend-position-${id}`).value,
    legendTitle: document.getElementById(`legend-title-${id}`)?.value || '',
    legendOrientation: document.getElementById(`legend-orientation-${id}`)?.value || '',
    legendRows: document.getElementById(`legend-rows-${id}`)?.value ? Number(document.getElementById(`legend-rows-${id}`).value) : '',
    legendColumns: document.getElementById(`legend-columns-${id}`)?.value ? Number(document.getElementById(`legend-columns-${id}`).value) : '',
    legendMarkerSize: document.getElementById(`legend-marker-size-${id}`)?.value ? Number(document.getElementById(`legend-marker-size-${id}`).value) : '',
    showLegend: document.getElementById(`show-legend-${id}`)?.checked !== false,
    extra,
    customScript: document.getElementById(`script-${id}`)?.value || '',
    transformScript: document.getElementById(`transform-${id}`)?.value || '',
    layoutScript: document.getElementById(`layout-script-${id}`)?.value || '',
  };
  return pruneConfig(raw, ['database', 'sql', 'plotType']);
}

export function applyNodeConfig(id, cfg) {
  const st = getState(id);
  if (!st || !cfg) return;
  const f = (sid, val) => { const el = document.getElementById(`${sid}-${id}`); if (el && val !== undefined) el.value = val; };
  f('db-select', cfg.database);
  f('sql-input', cfg.sql);
  f('chart-title', cfg.chartTitle || '');
  f('x-label', cfg.xLabel || '');
  f('y-label', cfg.yLabel || '');
  f('x-scale', cfg.xScale || 'linear');
  f('y-scale', cfg.yScale || 'linear');
  f('x-tickfmt', cfg.xTickFmt || '');
  f('y-tickfmt', cfg.yTickFmt || '');
  f('tick-formatter', cfg.tickFormatter || '');
  f('legend-position', cfg.legendPosition || 'right');
  f('legend-title', cfg.legendTitle || '');
  f('legend-orientation', cfg.legendOrientation || '');
  f('legend-rows', cfg.legendRows ?? '');
  f('legend-columns', cfg.legendColumns ?? '');
  f('legend-marker-size', cfg.legendMarkerSize ?? '');
  { const el = document.getElementById(`show-legend-${id}`); if (el) el.checked = cfg.showLegend !== false; }
  f('script', cfg.customScript || '');
  f('transform', cfg.transformScript || '');
  f('layout-script', cfg.layoutScript || '');

  st.database = cfg.database || st.database;
  st.plotType = cfg.plotType || 'line';
  st.columns = toArray(cfg.columns);
  st.x = cfg.x || '';
  st.z = cfg.z || '';
  st.yCols = toArray(cfg.y);
  st.groupCols = toArray(cfg.group);
  // Keep column-dependent selections in state while the imported SQL runs;
  // their <select> options do not exist yet at this point.
  st.markerBy = cfg.markerBy || '';
  st.dashBy = cfg.dashBy || '';
  st.extra = cfg.extra || {};
  if (cfg.z && st.extra.z === undefined) st.extra.z = cfg.z;
  st.customScript = cfg.customScript || '';
  st.transformScript = cfg.transformScript || '';
  st.layoutScript = cfg.layoutScript || '';

  // window.*, not local: axis_addon.js doesn't wrap these two, but they DO
  // need to reflect st.plotType having just changed above, and staying
  // consistent with the window.* rule here avoids relying on load-order
  // subtleties if a future addon ever does wrap them.
  window.renderPlotChips(id);
  window.renderExtraOpts(id);
  window.renderPlotSpecificFields(id);
  if (st.columns.length) {
    window.updateAxisControls(id, st.columns);
    window.updateFacetGroupColumns?.(id, st.columns);
  }
  f('x-col', st.x);
  for (const [key, val] of Object.entries(cfg.extra || {})) {
    const inp = document.getElementById(`extra-${id}-${key}`);
    if (inp) inp.value = val;
  }
  setTimeout(() => { f('marker-by', st.markerBy); f('dash-by', st.dashBy); }, 0);

  if (cfg.sql && cfg.sql.trim()) {
    // window.runShow: axis_addon.js wraps it to restore its own
    // error-y/-low/-high selects once this query actually resolves (see
    // builder.js's wireBuilderColumns for the fuller version of this
    // comment — this is the other call site that reasoning applies to).
    setTimeout(() => window.runShow(id).then(() => {
      setTimeout(() => {
        f('x-col', cfg.x || '');
        f('marker-by', st.markerBy);
        f('dash-by', st.dashBy);
      }, 0);
    }), 0);
  }
}

export function getTabConfig(tabId) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return null;
  const mode = tab.state.mode || 'single';
  const cfg = { version: 3, label: tab.label, mode,
    rendererBackend: tab.state.rendererBackend || 'matplotlib' };
  if (mode === 'grid' || mode === 'facet') {
    // Keep the tab's own builder too: it remains configured while the grid
    // panel builders are active and can be selected again later.
    cfg.single = window.getNodeConfig(tabId);
  } else {
    Object.assign(cfg, window.getNodeConfig(tabId));
  }
  if (tab.state.grid) {
    cfg.grid = {
      rows: tab.state.grid.rows, cols: tab.state.grid.cols,
      activePanelIndex: Math.max(0, tab.state.grid.panelIds.indexOf(tab.state.grid.activePanel)),
      // window.getNodeConfig: axis_addon.js wraps it to add xMin/y2Cols/
      // errorY/etc; a local call here would silently export plain configs
      // with none of that for every panel.
      panels: tab.state.grid.panelIds.map(pid => window.getNodeConfig(pid)),
    };
  }
  return cfg;
}

function restoreGridConfig(tabId, grid, mode) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab || !grid) return;
  // Construct panels through the wrapped API so addon controls are wired for
  // every imported grid, including a grid saved while Single plot was active.
  window.setFigureMode(tabId, mode === 'facet' ? 'facet' : 'grid');
  document.getElementById(`grid-rows-${tabId}`).value = grid.rows || 1;
  document.getElementById(`grid-cols-${tabId}`).value = grid.cols || 1;
  window.applyGrid(tabId);
  tab.state.grid.panelIds.forEach((pid, i) => {
    if (grid.panels?.[i]) window.applyNodeConfig(pid, grid.panels[i]);
  });
  const activeIndex = Math.min(tab.state.grid.panelIds.length - 1,
    Math.max(0, Number(grid.activePanelIndex) || 0));
  if (tab.state.grid.panelIds[activeIndex]) window.showBuilderFor(tabId, tab.state.grid.panelIds[activeIndex]);
  if (mode === 'single') window.setFigureMode(tabId, 'single');
}

export function applyTabConfig(tabId, cfg) {
  if (!cfg || cfg.version < 2) { alert('Invalid or incompatible config (expected version 2 or later).'); return; }
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  tab.label = cfg.label || tab.label;
  tab.state.rendererBackend = cfg.rendererBackend === 'plotly' ? 'plotly' : 'matplotlib';
  const backendEl = document.getElementById(`renderer-backend-${tabId}`);
  if (backendEl) backendEl.value = tab.state.rendererBackend;
  const lbl = document.querySelector(`#tabbtn-${tabId} .tab-label`);
  if (lbl) lbl.textContent = tab.label;

  const mode = ['grid', 'facet'].includes(cfg.mode) ? cfg.mode : 'single';
  if (cfg.grid) restoreGridConfig(tabId, cfg.grid, mode);
  const radios = document.getElementsByName(`mode-${tabId}`);
  radios.forEach(r => { r.checked = (r.value === mode); });
  if (mode === 'single') {
    window.setFigureMode(tabId, 'single');
    window.applyNodeConfig(tabId, cfg.single || cfg);
  } else if (cfg.single) {
    window.applyNodeConfig(tabId, cfg.single);
  }
  window.setStatus(tabId, 'Config loaded — click Run to render', 'warn');
  clientLog(`Config imported into tab "${tab.label}"`);
}

export function applyWorkspace(ws, sourceLabel) {
  if (!ws?.tabs?.length) { alert('Invalid workspace file.'); return; }
  [...G.tabs].forEach(t => window.closeTab(t.id));
  G.tabs = [];
  document.querySelectorAll('.plot-tab').forEach(el => el.remove());
  document.querySelectorAll('.workspace').forEach(el => el.remove());
  ws.tabs.forEach(cfg => {
    const newId = window.createTab(cfg.label || `Plot ${G.nextTabId}`, {});
    // window.applyTabConfig: grid_addon.js wraps it to restore its own
    // grid-level settings — same reasoning as applyNodeConfig above.
    window.applyTabConfig(newId, cfg);
  });
  const activeIndex = Math.min(ws.tabs.length - 1,
    Math.max(0, Number(ws.activeTabIndex) || 0));
  if (G.tabs[activeIndex]) window.activateTab(G.tabs[activeIndex].id);
  clientLog(`Workspace ${sourceLabel}: ${ws.tabs.length} tab(s)`);
}

// --- small utilities shared with export_image.js's PNG/SVG export (chunk 9) --
export function download(text, filename) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([text], { type: 'application/json' }));
  a.download = filename; a.click(); URL.revokeObjectURL(a.href);
}

export function readJson(file, cb) {
  const r = new FileReader();
  r.onload = ev => { try { cb(JSON.parse(ev.target.result)); } catch (e) { alert('Could not parse JSON: ' + e.message); } };
  r.readAsText(file);
}

async function copyText(text) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
  } catch (e) { /* try the textarea fallback below */ }
  const field = document.createElement('textarea');
  field.value = text;
  field.setAttribute('readonly', '');
  field.style.cssText = 'position:fixed;left:-9999px;top:0';
  document.body.appendChild(field);
  field.select();
  const copied = document.execCommand('copy');
  field.remove();
  if (!copied) throw new Error('Clipboard access was denied by the browser.');
}

function openWorkspaceExportMenu(button, json, filename) {
  if (button._workspaceExportClose) {
    button._workspaceExportClose();
    return;
  }
  const menu = document.createElement('div');
  menu.setAttribute('role', 'menu');
  menu.style.cssText = 'position:fixed;z-index:1000;display:flex;flex-direction:column;gap:4px;padding:6px;background:var(--bg2);border:1px solid var(--border2);border-radius:6px;box-shadow:0 8px 24px rgba(0,0,0,.35);min-width:190px';
  const rect = button.getBoundingClientRect();
  menu.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - 205))}px`;
  menu.style.top = `${rect.bottom + 4}px`;

  const close = () => {
    menu.remove();
    button.setAttribute('aria-expanded', 'false');
    delete button._workspaceExportClose;
    document.removeEventListener('pointerdown', dismiss);
    document.removeEventListener('keydown', onKey);
  };
  const dismiss = e => { if (!menu.contains(e.target) && e.target !== button) close(); };
  const onKey = e => { if (e.key === 'Escape') close(); };
  const addChoice = (label, action) => {
    const choice = document.createElement('button');
    choice.type = 'button'; choice.className = 'btn'; choice.textContent = label;
    choice.setAttribute('role', 'menuitem');
    choice.style.cssText = 'justify-content:flex-start;width:100%;padding:7px 9px';
    choice.addEventListener('click', async () => {
      close();
      try { await action(); }
      catch (e) { clientLog(`Workspace export failed: ${e.message}`, 'error'); alert(`Could not export workspace: ${e.message}`); }
    });
    menu.appendChild(choice);
  };
  addChoice('Download JSON file', () => {
    window.download(json, filename);
    clientLog('Workspace JSON downloaded.');
  });
  addChoice('Copy JSON to clipboard', async () => {
    await copyText(json);
    clientLog('Workspace JSON copied to clipboard.');
  });

  document.body.appendChild(menu);
  button.setAttribute('aria-expanded', 'true');
  button._workspaceExportClose = close;
  document.addEventListener('pointerdown', dismiss);
  document.addEventListener('keydown', onKey);
}

export function wireConfigControls() {
  document.getElementById('btn-export-config').addEventListener('click', () => {
    // window.getTabConfig/window.download: grid_addon.js wraps the former,
    // style_addon.js wraps the latter (though style_addon's wrap only
    // touches objects shaped like a workspace, {tabs:[...]}, so a single
    // tab's config export is passed through untouched by design there).
    const cfg = window.getTabConfig(G.activeTab);
    if (!cfg) return;
    window.download(JSON.stringify(cfg, null, 2), `plot-config-${cfg.label.replace(/\s+/g, '-')}.json`);
    clientLog(`Config exported for tab "${cfg.label}"`);
  });
  document.getElementById('btn-import-config').addEventListener('click', () => document.getElementById('import-file-input').click());
  document.getElementById('import-file-input').addEventListener('change', e => {
    const f = e.target.files[0]; if (!f) return;
    readJson(f, cfg => window.applyTabConfig(G.activeTab, cfg));
    e.target.value = '';
  });

  document.getElementById('btn-export-all').addEventListener('click', event => {
    // window.getTabConfig here too, and for the same reason: this is a
    // *workspace* export, exactly the shape style_addon.js's window.download
    // wrap looks for to inject the global style.
    const ws = { version: 3,
      activeTabIndex: Math.max(0, G.tabs.findIndex(t => t.id === G.activeTab)),
      tabs: G.tabs.map(t => window.getTabConfig(t.id)), style: window.G_STYLE || {} };
    const json = JSON.stringify(ws, null, 2);
    openWorkspaceExportMenu(event.currentTarget, json, `workspace-${Date.now()}.json`);
  });
  document.getElementById('btn-import-all').addEventListener('click', () => document.getElementById('import-ws-file-input').click());
  document.getElementById('import-ws-file-input').addEventListener('change', e => {
    const f = e.target.files[0]; if (!f) return;
    // window.applyWorkspace: style_addon.js wraps it to restore G_STYLE.
    readJson(f, ws => window.applyWorkspace(ws, 'imported'));
    e.target.value = '';
  });
}

Object.assign(window, {
  getNodeConfig, applyNodeConfig, getTabConfig, applyTabConfig, applyWorkspace,
  download, readJson, wireConfigControls,
});
