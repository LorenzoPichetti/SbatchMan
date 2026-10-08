// state.js — the single mutable G object every other module reads/writes,
// plus the generic "node" (tab-or-grid-panel) state helpers and a couple of
// small formatting utilities used everywhere. No DOM wiring lives here.

export const G = {
  databases:  {},
  plotTypes:  {},
  plotFields: {}, // plot type -> XML-declared UI field definitions
  remoteSystems: {},
  tabs:       [],     // top-level tabs: [{id, label, state}]
  panelState: {},     // panelId -> state (same shape as tab.state), for grid-mode sub-panels
  activeTab:  null,
  nextTabId:  1,
  nextPanelSeq: {},   // tabId -> next panel sequence number
  logCount:   0,
  singleDb:   false,
  defaultDb:  '',
};

export function toArray(v) {
  if (Array.isArray(v)) return v;
  if (v === null || v === undefined || v === '') return [];
  return [v]; // tolerate older configs that stored a single string instead of an array
}

export function escHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// A "node" is either a top-level tab or a grid sub-panel — both share the
// exact same state shape, so everything downstream (builder, plotting,
// config import/export) can treat an id as "some node" without caring
// which kind it is.
export function getState(id) {
  const tab = G.tabs.find(t => t.id === id);
  if (tab) return tab.state;
  return G.panelState[id];
}

export function tabEl(id, suffix) {
  return document.getElementById(`${suffix}-${id}`);
}

export function currentDb(id) {
  return G.singleDb ? G.defaultDb : (tabEl(id, 'db-select')?.value);
}

export function defaultNodeState(base) {
  const state = Object.assign({
    database: G.defaultDb || Object.keys(G.databases)[0] || '',
    sql: '', plotType: 'line', columns: [], yCols: [], groupCols: [],
    x: '', markerBy: '', dashBy: '', z: '', extra: {},
    chartTitle: '', xLabel: '', yLabel: '', xScale: 'linear', yScale: 'linear',
    plotFields: {},
    xTickFmt: '', yTickFmt: '', tickFormatter: '', customScript: '', transformScript: '', layoutScript: '',
    rendererBackend: 'matplotlib',
    legendPosition: 'right', legendTitle: '', showLegend: true,
  }, base || {});
  if (!state.sql?.trim()) {
    const firstTable = G.databases[state.database]?.[0]?.name;
    if (firstTable) state.sql = `SELECT * FROM "${String(firstTable).replaceAll('"', '""')}"`;
  }
  return state;
}

// Transitional: expose on window so the existing style/axis/grid addons
// (which still monkey-patch window.*) keep working unmodified while the
// rest of app.js is split across later chunks. Removed in the final chunk
// once those addons are rewritten as real imports.
Object.assign(window, { G, toArray, escHtml, getState, tabEl, currentDb, defaultNodeState });
