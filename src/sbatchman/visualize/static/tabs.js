// tabs.js — creating/activating/closing/renaming top-level plot tabs, the
// per-tab workspace container markup (single-plot toolbar + plot area +
// statusbar), and resizing Plotly's canvas when its container changes size
// for reasons Plotly itself won't notice (a CSS-driven layout change, e.g.
// the data drawer opening, or a hidden tab becoming visible again).
import { G, escHtml, getState } from './state.js';
import { clientLog } from './log.js';
// builderColumnsHTML/wireBuilderColumns are deliberately NOT imported and
// called directly below — axis_addon.js wraps both (window.builderColumnsHTML
// to append its extra UI columns, window.wireBuilderColumns to wire them up),
// and every real call site has to go through window.* for that to take
// effect. Importing builder.js here is still needed so it's loaded (and
// its window.* assignments run) before any tab is created.
import './builder.js';

export function activeTabObj() {
  return G.tabs.find(t => t.id === G.activeTab);
}

// Must match BASE_PLOT_HEIGHT in utils/styles.py — both sides use this as
// the floor for the plot area before a tall legend grows it further.
const BASE_PLOT_HEIGHT = 520;

export function resizePlot(id) {
  const el = document.getElementById(`plot-${id}`);
  if (el && el.data) {
    requestAnimationFrame(() => { try { Plotly.Plots.resize(el); } catch (e) { /* plot not drawn yet */ } });
  }
}

export function workspaceHTML(id) {
  return `
<div class="figure-toolbar">
  <label class="mode-toggle" id="mode-label-grid-${id}">
    <input type="radio" name="mode-${id}" value="grid" checked> Plot grid
  </label>
  <label class="mode-toggle" id="mode-label-facet-${id}">
    <input type="radio" name="mode-${id}" value="facet"> Data-driven grid
  </label>
  <label style="display:inline-flex;align-items:center;gap:5px;margin-left:10px">Renderer
    <select id="renderer-backend-${id}" style="width:auto;padding:3px 6px">
      <option value="matplotlib">Matplotlib / Seaborn</option><option value="plotly">Plotly</option>
    </select>
  </label>
  <span id="grid-controls-wrap-${id}" style="display:none;align-items:center;gap:6px">
     rows <input type="number" id="grid-rows-${id}" min="1" max="6" value="1">
     cols <input type="number" id="grid-cols-${id}" min="1" max="6" value="1">
     <button class="btn sm" id="grid-apply-${id}">Apply grid</button>
  </span>
  <span id="facet-controls-${id}" style="display:none;align-items:center;gap:6px;margin-left:8px">
    <span>Group by</span><div id="facet-group-columns-${id}" class="multi-col-list"></div>
    <span>Arrange by</span><select id="facet-axis-${id}" style="width:auto;padding:3px 6px"><option value="cols">columns</option><option value="rows">rows</option></select>
    <input type="number" id="facet-count-${id}" min="1" max="6" value="2" title="Number of grid rows or columns" style="width:52px">
    <label style="display:flex;align-items:center;gap:4px;margin:0">Title format
      <input type="text" id="facet-label-format-${id}" placeholder="{column}={value}" title="Placeholders: {column} and {value}" style="width:150px;padding:3px 5px">
    </label>
    <!-- <span style="font-size:10px;color:var(--text3)" title="Legend rows, columns, and orientation come from Panel 1's Appearance &amp; legend section">Legend layout: Panel 1 → Appearance &amp; legend (uncheck share legend for per-facet legends)</span> -->
  </span>
  <span id="grid-shared-controls-${id}" style="display:none;align-items:center;gap:6px"></span>
</div>
<div class="panel-tabs" id="panel-tabs-${id}"></div>
<div class="builder" id="builder-wrap-${id}">
  <div class="builder-inner active" id="builder-inner-${id}">${window.builderColumnsHTML(id)}</div>
</div>
<div class="panel-resizer workspace-resizer" data-resize="builder" role="separator" aria-orientation="horizontal" aria-label="Resize plot controls" title="Drag to resize plot controls"></div>

<div class="plot-area">
  <div class="plotly-wrap">
    <div class="plotly-div" id="plot-${id}"></div>
    <img class="rendered-image" id="plot-image-${id}" alt="Rendered figure" style="display:none;width:100%;height:100%;object-fit:contain">
    <div class="plot-overlay" id="overlay-${id}">
      <div class="overlay-icon">⬡</div>
      <div>Write a SQL query, configure axes, then click <strong>Run</strong></div>
    </div>
  </div>
  <div class="panel-resizer preview-resizer" data-resize="preview" role="separator" aria-orientation="horizontal" aria-label="Resize data preview" title="Drag to resize data preview"></div>
  <div class="data-preview" id="preview-${id}"></div>
  <div class="statusbar">
    <span id="status-${id}" class="s-ok">Ready</span>
    <button class="btn sm" id="preview-toggle-${id}" data-tip="Show the exact rows behind the current figure">Data ▾</button>
    <span id="status-rows-${id}" style="margin-left:auto;color:var(--text3)"></span>
  </div>
</div>`;
}

function wireWorkspace(id) {
  // window.wireBuilderColumns: see the header comment above. This would
  // actually still resolve correctly even written bare (wireBuilderColumns
  // is never locally declared in this module, so JS falls back to the
  // global/window property) — written explicitly anyway so the behavior
  // doesn't depend on that fallback, which a later local import here would
  // silently break by shadowing it.
  window.wireBuilderColumns(id);
  wireWorkspaceResizers(id);
  const backend = document.getElementById(`renderer-backend-${id}`);
  if (backend) {
    backend.value = getState(id).rendererBackend || 'matplotlib';
    backend.addEventListener('change', () => {
      const st = getState(id);
      st.rendererBackend = backend.value;
      if (st.plotted) document.getElementById('btn-run')?.click();
    });
  }

  document.getElementById(`preview-toggle-${id}`).addEventListener('click', () => {
    const pv = document.getElementById(`preview-${id}`);
    const open = pv.classList.toggle('open');
    if (open) window.renderPendingDataPreview?.(pv);
    document.querySelector(`#ws-${id} .plot-area`)?.classList.toggle('preview-open', open);
    document.getElementById(`preview-toggle-${id}`).textContent = open ? 'Data ▴' : 'Data ▾';
    resizePlot(id);
  });

  // Single/grid mode toggle and "Apply grid" are wired by grid.js's
  // wireGridControls(id), called right after this from createTab — kept
  // separate so tabs.js doesn't need to know anything about grid internals.
  if (window.wireGridControls) window.wireGridControls(id);
}

export function createTab(label, stateOverride) {
  const id = G.nextTabId++;
  const state = window.defaultNodeState(stateOverride);
  // A single plot is represented by a 1×1 static grid so figure size and
  // grid-level style controls are available from the start.
  if (!state.mode || state.mode === 'single') state.mode = 'grid';
  const tab = { id, label: label || `Plot ${id}`, state };
  G.tabs.push(tab);
  G.nextPanelSeq[id] = 1;

  const bar = document.getElementById('plot-tabs-bar');
  const btn = document.createElement('div');
  btn.className = 'plot-tab';
  btn.id = `tabbtn-${id}`;
  btn.innerHTML = `<span class="tab-label" ondblclick="renameTab(${id})">${escHtml(tab.label)}</span><span class="plot-tab-close" onclick="closeTab(${id},event)">×</span>`;
  const clone = document.createElement('span');
  clone.className = 'plot-tab-clone';
  clone.textContent = '⧉';
  clone.title = `Clone ${tab.label}`;
  clone.setAttribute('role', 'button');
  clone.setAttribute('aria-label', `Clone ${tab.label}`);
  clone.addEventListener('click', event => {
    event.stopPropagation();
    cloneTab(id);
  });
  btn.insertBefore(clone, btn.querySelector('.plot-tab-close'));
  btn.addEventListener('click', () => activateTab(id));
  bar.insertBefore(btn, document.getElementById('btn-add-tab'));

  const ws = document.createElement('div');
  ws.className = 'workspace';
  ws.id = `ws-${id}`;
  ws.innerHTML = workspaceHTML(id);
  document.getElementById('workspaces').appendChild(ws);

  wireWorkspace(id);
  if (window.setFigureMode) window.setFigureMode(id, state.mode);
  activateTab(id);
  return id;
}

export function activateTab(id) {
  G.activeTab = id;
  document.querySelectorAll('.plot-tab').forEach(b => b.classList.toggle('active', b.id === `tabbtn-${id}`));
  document.querySelectorAll('.workspace').forEach(w => w.classList.toggle('active', w.id === `ws-${id}`));
  resizePlot(id);
}

export function closeTab(id, ev) {
  ev?.stopPropagation();
  if (G.tabs.length <= 1) return;
  G.tabs = G.tabs.filter(t => t.id !== id);
  document.getElementById(`tabbtn-${id}`)?.remove();
  document.getElementById(`ws-${id}`)?.remove();
  if (G.activeTab === id) activateTab(G.tabs[G.tabs.length - 1].id);
}

export function renameTab(id) {
  const tab = G.tabs.find(t => t.id === id);
  if (!tab) return;
  const label = prompt('Rename tab:', tab.label);
  if (label && label.trim()) {
    tab.label = label.trim();
    const el = document.querySelector(`#tabbtn-${id} .tab-label`);
    if (el) el.textContent = tab.label;
  }
}

export function cloneTab(id) {
  const source = G.tabs.find(t => t.id === id);
  if (!source) return;
  const config = window.getTabConfig(id);
  const label = `${source.label} copy`;
  config.label = label;
  const newId = createTab(label, {});
  window.applyTabConfig(newId, config);
  activateTab(newId);
  clientLog(`Cloned tab "${source.label}" as "${label}"`);
}

export function wireTabBar() {
  restoreResizerPreferences();
  wireGlobalResizers();
  document.getElementById('btn-add-tab').addEventListener('click', () => {
    const src = activeTabObj();
    const baseState = src ? JSON.parse(JSON.stringify(src.state)) : {};
    delete baseState.renderedImage;
    baseState.plotted = false;
    baseState.mode = 'grid'; baseState.grid = undefined;
    const newId = createTab(`Plot ${G.nextTabId}`, baseState);
    clientLog(`New tab created (cloned from "${src?.label}")`);
  });
  window.addEventListener('resize', () => { if (G.activeTab) resizePlot(G.activeTab); });
}

const RESIZER_KEYS = {
  sidebar: ['--sidebar-width', 'visualize.sidebarWidth'],
  logs: ['--log-width', 'visualize.logWidth'],
  builder: ['--builder-height', 'visualize.builderHeight'],
  preview: ['--preview-height', 'visualize.previewHeight'],
};

function restoreResizerPreferences() {
  for (const [property, key] of Object.values(RESIZER_KEYS)) {
    const value = Number(localStorage.getItem(key));
    if (Number.isFinite(value) && value > 0) document.documentElement.style.setProperty(property, `${value}px`);
  }
}

function wireWorkspaceResizers(id) {
  const workspace = document.getElementById(`ws-${id}`);
  workspace?.querySelectorAll('[data-resize]').forEach(handle => {
    const kind = handle.dataset.resize;
    const target = kind === 'builder'
      ? document.getElementById(`builder-wrap-${id}`)
      : document.getElementById(`preview-${id}`);
    if (!target) return;
    bindResizer(handle, {
      axis: 'y',
      read: () => target.getBoundingClientRect().height,
      move: (start, event, pointerStart) => {
        const delta = event.clientY - pointerStart;
        if (kind === 'builder') {
          const available = workspace.getBoundingClientRect().bottom - target.getBoundingClientRect().top - 150;
          return clamp(start + delta, 150, Math.max(150, available));
        }
        const available = target.parentElement.getBoundingClientRect().height - 150;
        return clamp(start - delta, 90, Math.max(90, available));
      },
      write: value => document.documentElement.style.setProperty(RESIZER_KEYS[kind][0], `${value}px`),
      key: RESIZER_KEYS[kind][1],
      after: () => resizePlot(id),
    });
  });
}

function wireGlobalResizers() {
  const sidebar = document.getElementById('sidebar');
  const logPanel = document.getElementById('log-panel');
  const sidebarHandle = document.getElementById('sidebar-resizer');
  const logHandle = document.getElementById('log-resizer');
  if (sidebar && sidebarHandle) bindResizer(sidebarHandle, {
    axis: 'x', read: () => sidebar.getBoundingClientRect().width,
    move: (_start, event) => clamp(event.clientX, 190, Math.min(520, window.innerWidth * .48)),
    write: value => document.documentElement.style.setProperty(RESIZER_KEYS.sidebar[0], `${value}px`),
    key: RESIZER_KEYS.sidebar[1], after: () => G.activeTab && resizePlot(G.activeTab),
  });
  if (logPanel && logHandle) bindResizer(logHandle, {
    axis: 'x', read: () => logPanel.getBoundingClientRect().width,
    move: (_start, event) => clamp(window.innerWidth - event.clientX, 260, Math.min(900, window.innerWidth * .85)),
    write: value => document.documentElement.style.setProperty(RESIZER_KEYS.logs[0], `${value}px`),
    key: RESIZER_KEYS.logs[1], after: () => G.activeTab && resizePlot(G.activeTab),
  });
}

function bindResizer(handle, {axis, read, move, write, key, after}) {
  let start = 0, pointerStart = 0, frame = 0;
  const pointerDown = event => {
    if (event.button !== undefined && event.button !== 0) return;
    event.preventDefault();
    start = read();
    pointerStart = axis === 'x' ? event.clientX : event.clientY;
    handle.classList.add('dragging');
    document.body.classList.add('panel-resizing', axis === 'x' ? 'col-resize' : 'row-resize');
    document.addEventListener('pointermove', pointerMove);
    document.addEventListener('pointerup', pointerUp, {once:true});
    document.addEventListener('pointercancel', pointerUp, {once:true});
  };
  const pointerMove = event => {
    const size = move(start, event, pointerStart);
    write(size);
    cancelAnimationFrame(frame);
    frame = requestAnimationFrame(() => { after?.(); });
  };
  const pointerUp = () => {
    document.removeEventListener('pointermove', pointerMove);
    document.removeEventListener('pointercancel', pointerUp);
    handle.classList.remove('dragging');
    document.body.classList.remove('panel-resizing', axis === 'x' ? 'col-resize' : 'row-resize');
    const property = kindPropertyFromHandle(handle);
    const propertyName = property && RESIZER_KEYS[property][0];
    const current = propertyName && getComputedStyle(document.documentElement).getPropertyValue(propertyName).trim();
    const currentNumber = parseFloat(current);
    if (key && Number.isFinite(currentNumber)) localStorage.setItem(key, String(currentNumber));
    after?.();
  };
  handle.addEventListener('pointerdown', pointerDown);
}

function kindPropertyFromHandle(handle) {
  if (handle.id === 'sidebar-resizer') return 'sidebar';
  if (handle.id === 'log-resizer') return 'logs';
  return handle.dataset.resize;
}

function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }

Object.assign(window, {
  activeTabObj, resizePlot, workspaceHTML, createTab, activateTab, closeTab, renameTab, cloneTab, wireTabBar,
});
