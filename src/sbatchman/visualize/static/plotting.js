// plotting.js — turning a builder's fields into a plot request, running it
// (single plot or grid), and the legend/resize bookkeeping Plotly's canvas
// needs help with that it won't do on its own.
import { G, getState, tabEl, currentDb } from './state.js';
import { api, addLogEntries, clientLog } from './log.js';
import { renderDataTable, renderMultiPreview } from './builder.js';
// updateAxisControls is deliberately NOT imported here: axis_addon.js wraps
// window.updateAxisControls to refresh the y2/error-column selects whenever
// fresh columns arrive, and a static import would bypass that wrap (see the
// same reasoning, worked out in detail, in builder.js's own runShow). This
// side-effect import just ensures builder.js has run (and set window.*)
// before any plot runs.
import './builder.js';

// Must match BASE_PLOT_HEIGHT in utils/styles.py.
const BASE_PLOT_HEIGHT = 520;

// --- Gather a plot payload (single node) — shared by single-plot Run and
// each panel of a grid figure ------------------------------------------
export function gatherPlotPayload(id) {
  const st = getState(id);
  const pt = G.plotTypes[st.plotType] || {};
  const extra = {};
  for (const key of Object.keys(pt.defaults || {})) {
    const inp = document.getElementById(`extra-${id}-${key}`);
    if (inp) extra[key] = isNaN(inp.value) ? inp.value : Number(inp.value);
  }
  const config = {
    title: tabEl(id, 'chart-title')?.value || '',
    x: tabEl(id, 'x-col')?.value || '',
    y: st.yCols,
    group: st.groupCols,
    marker_by: tabEl(id, 'marker-by')?.value || '',
    dash_by: tabEl(id, 'dash-by')?.value || '',
    z: st.z || '',
    x_label: tabEl(id, 'x-label')?.value || '',
    y_label: tabEl(id, 'y-label')?.value || '',
    x_scale: tabEl(id, 'x-scale')?.value || 'linear',
    y_scale: tabEl(id, 'y-scale')?.value || 'linear',
    x_tickformat: tabEl(id, 'x-tickfmt')?.value || '',
    y_tickformat: tabEl(id, 'y-tickfmt')?.value || '',
    tick_formatter: tabEl(id, 'tick-formatter')?.value || '',
    legend_position: tabEl(id, 'legend-position')?.value || 'right',
    legend_title: tabEl(id, 'legend-title')?.value || '',
    legend_orientation: tabEl(id, 'legend-orientation')?.value || '',
    legend_rows: Number(tabEl(id, 'legend-rows')?.value || 0),
    legend_columns: Number(tabEl(id, 'legend-columns')?.value || 0),
    legend_marker_size: Number(tabEl(id, 'legend-marker-size')?.value || 0),
    show_legend: tabEl(id, 'show-legend')?.checked !== false,
    ...extra,
  };
  for (const field of G.plotFields[st.plotType] || []) {
    const input = document.getElementById(`plot-field-${id}-${field.name}`);
    if (!input) continue;
    config[field.name] = input.type === 'checkbox' ? input.checked
      : input.type === 'number' ? (input.value === '' ? '' : Number(input.value)) : input.value;
  }
  return {
    database: currentDb(id),
    sql: tabEl(id, 'sql-input')?.value.trim() || '',
    plot_type: st.plotType,
    config,
    custom_script: tabEl(id, 'script')?.value || '',
    transform_script: tabEl(id, 'transform')?.value || '',
    layout_script: tabEl(id, 'layout-script')?.value || '',
  };
}

export function applyLegendHeightReservation(tabId, extraPx) {
  const wrap = document.getElementById(`plot-${tabId}`)?.parentElement;
  if (!wrap) return;
  wrap.style.minHeight = extraPx > 0 ? `${BASE_PLOT_HEIGHT + extraPx}px` : '';
  // Force the browser to apply the new size synchronously before Plotly
  // measures the container — reading a layout property flushes any pending
  // style/layout recalculation, otherwise Plotly can read the *previous*,
  // smaller size and lay the plot out squeezed into it.
  void wrap.offsetHeight;
}

export function resizeAfterRender(tabId) {
  requestAnimationFrame(() => {
    const gd = document.getElementById(`plot-${tabId}`);
    if (gd) Plotly.Plots.resize(gd);
  });
}

export function setStatus(tabId, msg, level) {
  const el = document.getElementById(`status-${tabId}`);
  if (!el) return;
  el.textContent = msg;
  el.className = level === 'err' ? 's-err' : level === 'warn' ? 's-warn' : 's-ok';
}

export async function runPlot() {
  const tab = window.activeTabObj();
  if (!tab) return;
  if (tab.state.mode === 'grid' || tab.state.mode === 'facet') return runGridPlot(tab.id);

  const tabId = tab.id;
  // Addons (axis_addon.js, etc.) extend this payload by wrapping the global.
  const payload = window.gatherPlotPayload(tabId);
  if (!payload.database || !payload.sql) { setStatus(tabId, 'Set a database and SQL query first', 'err'); return; }

  setStatus(tabId, 'Running…', '');
  document.getElementById('btn-run').disabled = true;
  try {
    // Deliberately window.api, not the statically-imported `api`: style_addon.js
    // and grid_addon.js monkey-patch window.api to inject the global style
    // and grid share_x/share_y/etc fields into this exact call, and a direct
    // import would bypass those patches entirely. See the module header.
    const res = await window.api('POST', '/api/plot', {...payload, backend: tab.state.rendererBackend || 'matplotlib'});
    document.getElementById('btn-run').disabled = false;
    addLogEntries(res.log_entries);
    if (res.error) { setStatus(tabId, res.error, 'err'); clientLog(res.error, 'error'); return; }
    if (res.columns?.length) window.updateAxisControls(tabId, res.columns);

    document.getElementById(`overlay-${tabId}`).style.display = 'none';
    if (res.image_png) {
      showMatplotlibImage(tabId, res, 1);
      tab.state.renderedImage = res;
      tab.state.plotted = true;
      const hint = res.truncated ? ' (truncated to 10k rows)' : '';
      setStatus(tabId, `OK — Matplotlib rendered${hint}`, 'ok');
      clientLog(`Matplotlib plot rendered: "${tab.label}"${hint}`);
      renderDataTable(document.getElementById(`preview-${tabId}`), res.preview);
      return;
    }
    showPlotlyFigure(tabId);
    tab.state.renderedImage = null;
    applyLegendHeightReservation(tabId, res.legend_extra_height || 0);
    // Figure inch dimensions are export-oriented. A fixed Plotly width/height
    // (for example the 3.4in single-column preset) otherwise leaves the live
    // chart stuck at a small size inside this responsive workspace.
    const viewLayout = { ...res.layout, autosize: true };
    delete viewLayout.width;
    delete viewLayout.height;
    Plotly.react(`plot-${tabId}`, res.traces, viewLayout, { responsive: true, displaylogo: false, modeBarButtonsToRemove: ['sendDataToCloud'] });
    resizeAfterRender(tabId);
    tab.state.plotted = true;
    tab.state.legendExtraHeight = res.legend_extra_height || 0;
    tab.state.legendExtraWidth = res.legend_extra_width || 0;
    tab.state.legendCenterRequiredWidth = res.legend_center_required_width || 0;
    const hint = res.truncated ? ' (truncated to 10k rows)' : '';
    setStatus(tabId, `OK — ${res.traces.length} trace(s), ${res.columns?.length || 0} column(s)${hint}`, 'ok');
    clientLog(`Plot rendered: "${tab.label}" — ${res.traces.length} trace(s)${hint}`);
    renderDataTable(document.getElementById(`preview-${tabId}`), res.preview);
  } catch (e) {
    document.getElementById('btn-run').disabled = false;
    setStatus(tabId, e.message, 'err');
    clientLog(e.message, 'error');
  }
}

export async function runGridPlot(tabId) {
  const tab = G.tabs.find(t => t.id === tabId);
  const grid = tab.state.grid;
  if (!grid || !grid.panelIds.length) { setStatus(tabId, 'Configure the grid first', 'err'); return; }

  const isFacet = tab.state.mode === 'facet';
  const panelIds = isFacet ? grid.panelIds.slice(0, 1) : grid.panelIds;
  // Use the wrapped global so addon fields such as y2 columns are included
  // for both ordinary panels and the source panel of a data-driven grid.
  const panels = panelIds.map(pid => window.gatherPlotPayload(pid));
  const missing = panels.findIndex(p => !p.database || !p.sql);
  if (missing !== -1) { setStatus(tabId, `Panel ${missing + 1}: set a database and SQL query`, 'err'); return; }

  setStatus(tabId, 'Running…', '');
  document.getElementById('btn-run').disabled = true;
  try {
    // window.api here too — see runPlot's comment; grid_addon.js's share_x/
    // share_y/legend injection specifically targets '/api/plot_grid'.
    const res = await window.api('POST', '/api/plot_grid', {
      rows: grid.rows, cols: grid.cols, panels, backend: tab.state.rendererBackend || 'matplotlib',
    });
    document.getElementById('btn-run').disabled = false;
    addLogEntries(res.log_entries);
    if (res.error) { setStatus(tabId, res.error, 'err'); clientLog(res.error, 'error'); return; }

    document.getElementById(`overlay-${tabId}`).style.display = 'none';
    if (res.image_png) {
      const rows = res.grid_rows || grid.rows || 1;
      showMatplotlibImage(tabId, res, rows);
      tab.state.renderedImage = res;
      tab.state.plotted = true;
      setStatus(tabId, `OK — ${res.panel_count || panels.length} panel(s), Matplotlib rendered`, 'ok');
      clientLog(`Matplotlib grid rendered: "${tab.label}" — ${res.panel_count || panels.length} panel(s)`);
      const labeled = (res.previews || []).map((df, i) => ({ label: `Panel ${i + 1}`, df }));
      renderMultiPreview(document.getElementById(`preview-${tabId}`), labeled);
      return;
    }
    showPlotlyFigure(tabId);
    tab.state.renderedImage = null;
    const plotWrap = document.getElementById(`plot-${tabId}`)?.parentElement;
    if (plotWrap) {
      // Give every grid row enough vertical room for its subplot title, axis
      // titles, and tick labels. The plot area scrolls when the figure is
      // taller than the available viewport.
      const rows = res.grid_rows || grid.rows || 1;
      const legendHeight = res.legend_extra_height || 0;
      plotWrap.style.minHeight = `${Math.max(BASE_PLOT_HEIGHT, rows * 320) + legendHeight}px`;
      void plotWrap.offsetHeight;
    }
    const viewLayout = { ...res.layout, autosize: true };
    delete viewLayout.width;
    delete viewLayout.height;
    Plotly.react(`plot-${tabId}`, res.traces, viewLayout, { responsive: true, displaylogo: false, modeBarButtonsToRemove: ['sendDataToCloud'] });
    resizeAfterRender(tabId);
    tab.state.plotted = true;
    tab.state.legendExtraHeight = 0;
    tab.state.legendExtraWidth = 0;
    tab.state.legendCenterRequiredWidth = 0;
    const panelCount = res.panel_count || panels.length;
    setStatus(tabId, `OK — ${panelCount} panel(s), ${res.traces.length} trace(s) total`, 'ok');
    clientLog(`Grid figure rendered: "${tab.label}" — ${panelCount} panel(s)`);

    const labeled = (res.previews || []).map((df, i) => ({ label: `Panel ${i + 1}`, df }));
    renderMultiPreview(document.getElementById(`preview-${tabId}`), labeled);
  } catch (e) {
    document.getElementById('btn-run').disabled = false;
    setStatus(tabId, e.message, 'err');
    clientLog(e.message, 'error');
  }
}

export function wireRunControls() {
  document.getElementById('btn-run').addEventListener('click', runPlot);
  document.addEventListener('keydown', e => { if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); runPlot(); } });
}

function showMatplotlibImage(tabId, result, rows) {
  const plot = document.getElementById(`plot-${tabId}`);
  const image = document.getElementById(`plot-image-${tabId}`);
  if (!plot || !image) return;
  plot.style.display = 'none';
  image.src = `data:image/png;base64,${result.image_png}`;
  image.style.display = 'block';
  const wrap = plot.parentElement;
  if (wrap) {
    wrap.style.minHeight = `${Math.max(BASE_PLOT_HEIGHT, rows * 320) + (result.legend_extra_height || 0)}px`;
    void wrap.offsetHeight;
  }
}

function showPlotlyFigure(tabId) {
  const plot = document.getElementById(`plot-${tabId}`);
  const image = document.getElementById(`plot-image-${tabId}`);
  if (image) { image.style.display = 'none'; image.removeAttribute('src'); }
  if (plot) plot.style.display = '';
}

Object.assign(window, {
  gatherPlotPayload, applyLegendHeightReservation, resizeAfterRender, setStatus,
  runPlot, runGridPlot, wireRunControls,
});
