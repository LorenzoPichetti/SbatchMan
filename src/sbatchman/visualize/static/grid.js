// grid.js — the single-plot/subplot-grid toggle for a tab, building and
// resizing the grid of panel builders, and the panel-tab chips used to
// switch which panel's builder is currently shown.
import { G } from './state.js';
import { clientLog } from './log.js';
// builderColumnsHTML/wireBuilderColumns are deliberately NOT imported here,
// for the same reason as plotting.js and tabs.js: axis_addon.js wraps the
// window.* versions to add its own UI fields to every new panel, and a
// static import would silently skip them for every grid panel created from
// here on. This side-effect import just guarantees load order.
import './builder.js';

export function showBuilderFor(tabId, nodeId) {
  document.querySelectorAll(`#builder-wrap-${tabId} .builder-inner`).forEach(el => el.classList.remove('active'));
  const el = document.getElementById(`builder-inner-${nodeId}`);
  if (el) el.classList.add('active');
  const tab = G.tabs.find(t => t.id === tabId);
  if (tab && tab.state.grid?.panelIds?.includes(nodeId)) tab.state.grid.activePanel = nodeId;
  document.querySelectorAll(`#panel-tabs-${tabId} .panel-chip`).forEach(c => c.classList.toggle('active', c.dataset.node === String(nodeId)));
}

export function renderPanelTabs(tabId) {
  const wrap = document.getElementById(`panel-tabs-${tabId}`);
  const tab = G.tabs.find(t => t.id === tabId);
  wrap.innerHTML = '';
  (tab.state.grid.panelIds || []).forEach((pid, i) => {
    const chip = document.createElement('div');
    chip.className = 'panel-chip';
    chip.dataset.node = pid;
    chip.addEventListener('click', () => showBuilderFor(tabId, pid));
    const label = document.createElement('span');
    label.textContent = `Panel ${i + 1}`;
    chip.appendChild(label);
    const clone = document.createElement('button');
    clone.type = 'button';
    clone.className = 'panel-clone-btn';
    clone.textContent = '⧉';
    clone.title = `Clone Panel ${i + 1}`;
    clone.setAttribute('aria-label', `Clone Panel ${i + 1}`);
    clone.addEventListener('click', event => {
      event.stopPropagation();
      clonePanel(tabId, pid);
    });
    chip.appendChild(clone);
    wrap.appendChild(chip);
  });
}

export function clonePanel(tabId, sourceId) {
  const tab = G.tabs.find(t => t.id === tabId);
  const grid = tab?.state.grid;
  if (!tab || !grid || !grid.panelIds.includes(sourceId)) return;
  if (grid.panelIds.length >= 36) {
    clientLog('Cannot clone this panel: the subplot grid is already at its 6×6 limit.', 'warn');
    return;
  }

  const config = window.getNodeConfig(sourceId);
  if (grid.cols < 6) grid.cols += 1;
  else grid.rows += 1;
  document.getElementById(`grid-rows-${tabId}`).value = grid.rows;
  document.getElementById(`grid-cols-${tabId}`).value = grid.cols;
  window.applyGrid(tabId);

  const cloneId = grid.panelIds[grid.panelIds.length - 1];
  if (!cloneId) return;
  window.applyNodeConfig(cloneId, config);
  showBuilderFor(tabId, cloneId);
  clientLog(`Cloned panel configuration into Panel ${grid.panelIds.length} for tab "${tab.label}"`);
}

export function applyGrid(tabId) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  const rows = Math.max(1, parseInt(document.getElementById(`grid-rows-${tabId}`).value) || 1);
  const cols = Math.max(1, parseInt(document.getElementById(`grid-cols-${tabId}`).value) || 1);
  const needed = rows * cols;
  if (!tab.state.grid) tab.state.grid = { rows, cols, panelIds: [], activePanel: null };
  tab.state.grid.rows = rows; tab.state.grid.cols = cols;

  let ids = tab.state.grid.panelIds;
  // Add panels if we need more
  while (ids.length < needed) {
    const seq = G.nextPanelSeq[tabId]++;
    const pid = `${tabId}_p${seq}`;
    G.panelState[pid] = window.defaultNodeState({ database: tab.state.database });
    const wrap = document.getElementById(`builder-wrap-${tabId}`);
    const div = document.createElement('div');
    div.className = 'builder-inner';
    div.id = `builder-inner-${pid}`;
    div.innerHTML = window.builderColumnsHTML(pid);
    wrap.appendChild(div);
    window.wireBuilderColumns(pid);
    ids.push(pid);
  }
  // Remove panels if we need fewer
  while (ids.length > needed) {
    const pid = ids.pop();
    document.getElementById(`builder-inner-${pid}`)?.remove();
    delete G.panelState[pid];
  }

  renderPanelTabs(tabId);
  showBuilderFor(tabId, ids[0]);
  clientLog(`Grid set to ${rows}×${cols} (${needed} panel(s)) for tab "${tab.label}"`);
}

export function setFigureMode(tabId, mode) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  tab.state.mode = mode;
  document.getElementById(`grid-controls-wrap-${tabId}`).style.display = mode === 'grid' ? 'inline-flex' : 'none';
  document.getElementById(`facet-controls-${tabId}`).style.display = mode === 'facet' ? 'inline-flex' : 'none';
  document.getElementById(`grid-shared-controls-${tabId}`).style.display = (mode === 'grid' || mode === 'facet') ? 'inline-flex' : 'none';
  document.getElementById(`panel-tabs-${tabId}`).style.display = mode === 'grid' ? 'flex' : 'none';
  if (mode === 'grid' || mode === 'facet') {
    if (!tab.state.grid) tab.state.grid = { rows: 1, cols: 1, panelIds: [], activePanel: null };
    document.getElementById(`grid-rows-${tabId}`).value = tab.state.grid.rows;
    document.getElementById(`grid-cols-${tabId}`).value = tab.state.grid.cols;
    // window.applyGrid, not the local function: grid_addon.js wraps it to
    // inject the share_x/share_y/panel-label/legend controls right after a
    // grid is (re)built, and a self-call to the local binding here would
    // skip that — the controls would never appear on a tab's first switch
    // into grid mode.
    if (!tab.state.grid.panelIds.length) window.applyGrid(tabId);
    else showBuilderFor(tabId, mode === 'facet' ? tab.state.grid.panelIds[0] : (tab.state.grid.activePanel || tab.state.grid.panelIds[0]));
  } else {
    showBuilderFor(tabId, tabId);
  }
}

// Wires the per-tab mode radios and "Apply grid" button. Called from
// tabs.js's wireWorkspace(id) via window.wireGridControls, since tabs.js
// must not import grid.js directly (grid.js builds on a tab's DOM, which
// only exists once tabs.js has created it — keeping the dependency as a
// runtime lookup here avoids a real circular ES import between the two).
export function wireGridControls(id) {
  const radios = document.getElementsByName(`mode-${id}`);
  radios.forEach(r => r.addEventListener('change', () => setFigureMode(id, r.value)));
  document.getElementById(`grid-apply-${id}`).addEventListener('click', () => window.applyGrid(id)); // see the comment in setFigureMode above
}

Object.assign(window, { showBuilderFor, renderPanelTabs, applyGrid, clonePanel, setFigureMode, wireGridControls });
