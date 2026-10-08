// Loaded after app.js. Wires up UI for render_grid's share_x/share_y, xgap/
// ygap, panel_labels and legend options — all already supported server-side
// but never reachable from the grid toolbar. Same additive pattern as the
// other addons: wraps applyGrid (to inject controls) and runGridPlot (to
// read them into the payload), rather than editing app.js.
(function () {
  function gridState(tabId) {
    const tab = G.tabs.find(t => t.id === tabId);
    if (!tab) return null;
    tab.state.grid = tab.state.grid || {};
    const g = tab.state.grid;
    if (g.shareX === undefined) g.shareX = false;
    if (g.shareY === undefined) g.shareY = false;
    if (g.xgap === undefined) g.xgap = 0.20;
    if (g.ygap === undefined) g.ygap = 0.24;
    if (g.panelLabels === undefined) g.panelLabels = false;
    if (g.facetByGroup === undefined) g.facetByGroup = false;
    if (!Array.isArray(g.facetGroupCols)) g.facetGroupCols = [];
    if (!g.facetAxis) g.facetAxis = 'cols';
    if (!g.facetCount) g.facetCount = 2;
    if (g.figureWidth === undefined) g.figureWidth = 0;
    if (g.figureHeight === undefined) g.figureHeight = 0;
    if (g.subplotWidth === undefined) g.subplotWidth = 4;
    if (g.subplotHeight === undefined) g.subplotHeight = 3.3;
    if (g.facetLabelFormat === undefined) g.facetLabelFormat = '{column}={value}';
    if (g.legendPosition === undefined) g.legendPosition = 'right';
    if (g.legendTitle === undefined) g.legendTitle = '';
    if (g.showLegend === undefined) g.showLegend = true;
    if (g.shareLegend === undefined) g.shareLegend = true;
    return g;
  }

  // getTabConfig(tabId) (app.js) is what "Export workspace" and the
  // plots.json auto-load format both serialize; for grid tabs it only
  // writes {rows, cols, panels}, so our share_x/share_y/gap/panel-labels/
  // legend settings were being silently dropped on every save. Wrap it (and
  // applyTabConfig, the import-side counterpart) the same way axis_addon.js
  // wraps getNodeConfig/applyNodeConfig.
  const _getTabConfig = window.getTabConfig;
  window.getTabConfig = function (tabId) {
    const cfg = _getTabConfig(tabId);
    if (!cfg || !cfg.grid) return cfg;
    const g = gridState(tabId);
    Object.assign(cfg.grid, {
      shareX: !!g.shareX, shareY: !!g.shareY, xgap: g.xgap, ygap: g.ygap,
      panelLabels: !!g.panelLabels, facetByGroup: !!g.facetByGroup, legendPosition: g.legendPosition,
      legendTitle: g.legendTitle, showLegend: g.showLegend !== false,
      shareLegend: g.shareLegend !== false,
      facetGroupCols: g.facetGroupCols, facetAxis: g.facetAxis, facetCount: g.facetCount,
      groupBy: g.facetGroupCols,
      figureWidth: g.figureWidth, figureHeight: g.figureHeight,
      subplotWidth: g.subplotWidth, subplotHeight: g.subplotHeight,
      facetLabelFormat: g.facetLabelFormat,
    });
    return cfg;
  };
  const _applyTabConfig = window.applyTabConfig;
  window.applyTabConfig = function (tabId, cfg) {
    _applyTabConfig(tabId, cfg);
    if (!cfg || !cfg.grid) return;
    const g = gridState(tabId);
    Object.assign(g, {
      shareX: !!cfg.grid.shareX, shareY: !!cfg.grid.shareY,
      xgap: cfg.grid.xgap ?? g.xgap, ygap: cfg.grid.ygap ?? g.ygap,
      panelLabels: !!cfg.grid.panelLabels, facetByGroup: !!cfg.grid.facetByGroup,
      legendPosition: cfg.grid.legendPosition || 'right',
      legendTitle: cfg.grid.legendTitle || '', showLegend: cfg.grid.showLegend !== false,
      shareLegend: cfg.grid.shareLegend !== false,
      facetGroupCols: Array.isArray(cfg.grid.groupBy) ? cfg.grid.groupBy :
        (Array.isArray(cfg.grid.facetGroupCols) ? cfg.grid.facetGroupCols : []),
      facetAxis: cfg.grid.facetAxis === 'rows' ? 'rows' : 'cols',
      facetCount: Math.max(1, Math.min(6, parseInt(cfg.grid.facetCount, 10) || 2)),
      figureWidth: Math.max(0, Number(cfg.grid.figureWidth) || 0),
      figureHeight: Math.max(0, Number(cfg.grid.figureHeight) || 0),
      subplotWidth: Math.max(.5, Number(cfg.grid.subplotWidth) || 4),
      subplotHeight: Math.max(.5, Number(cfg.grid.subplotHeight) || 3.3),
      facetLabelFormat: cfg.grid.facetLabelFormat || '{column}={value}',
    });
    // applyGrid() (called by applyTabConfig -> setFigureMode -> applyGrid)
    // already ran and built the controls with the OLD (default) state, since
    // this assignment happens after; push the restored values into the DOM.
    const f = (id, val, isCheckbox) => { const el = document.getElementById(id); if (!el) return;
      isCheckbox ? (el.checked = val) : (el.value = val); };
    f(`grid-share-x-${tabId}`, g.shareX, true); f(`grid-share-y-${tabId}`, g.shareY, true);
    f(`grid-panel-labels-${tabId}`, g.panelLabels, true);
    f(`grid-share-legend-${tabId}`, g.shareLegend, true);
    f(`grid-xgap-${tabId}`, g.xgap); f(`grid-ygap-${tabId}`, g.ygap);
    f(`grid-legend-title-${tabId}`, g.legendTitle);
    f(`grid-legend-pos-${tabId}`, g.showLegend === false ? 'none' : g.legendPosition);
    f(`facet-axis-${tabId}`, g.facetAxis); f(`facet-count-${tabId}`, g.facetCount);
    f(`grid-figure-width-${tabId}`, g.figureWidth); f(`grid-figure-height-${tabId}`, g.figureHeight);
    f(`grid-subplot-width-${tabId}`, g.subplotWidth); f(`grid-subplot-height-${tabId}`, g.subplotHeight);
    f(`facet-label-format-${tabId}`, g.facetLabelFormat);
    refreshFacetColumns(tabId, g.facetColumns || []);
  };

  const _applyGrid = window.applyGrid;
  window.applyGrid = function (tabId) {
    _applyGrid(tabId);
    const wrap = document.getElementById(`grid-shared-controls-${tabId}`);
    if (!wrap || wrap.querySelector('.grid-extra')) return; // already injected
    const g = gridState(tabId);
    const box = document.createElement('span');
    box.className = 'grid-extra';
    box.style.cssText = 'display:inline-flex;gap:10px;align-items:center;margin-left:10px';
    box.innerHTML = `
      <label style="display:flex;gap:3px;align-items:center;font-size:10px"><input type="checkbox" id="grid-share-x-${tabId}" style="width:auto"> share x</label>
      <label style="display:flex;gap:3px;align-items:center;font-size:10px"><input type="checkbox" id="grid-share-y-${tabId}" style="width:auto"> share y</label>
      <label style="display:flex;gap:3px;align-items:center;font-size:10px" title="Use one deduplicated legend for the whole grid"><input type="checkbox" id="grid-share-legend-${tabId}" style="width:auto"> share legend</label>
      <label style="display:flex;gap:3px;align-items:center;font-size:10px"><input type="checkbox" id="grid-panel-labels-${tabId}" style="width:auto">(a)(b)... labels</label>
      <span style="font-size:10px">Figure W/H (in)</span>
      <input class="grid-dimension" type="number" id="grid-figure-width-${tabId}" min="0" step="0.1" title="Figure width in inches; 0 uses subplot width × columns" placeholder="auto">
      <input class="grid-dimension" type="number" id="grid-figure-height-${tabId}" min="0" step="0.1" title="Figure height in inches; 0 uses subplot height × rows" placeholder="auto">
      <span style="font-size:10px">Panel W/H (in)</span>
      <input class="grid-dimension" type="number" id="grid-subplot-width-${tabId}" min="0.5" step="0.1" title="Subplot width in inches">
      <input class="grid-dimension" type="number" id="grid-subplot-height-${tabId}" min="0.5" step="0.1" title="Subplot height in inches">
      gap x <input class="grid-gap" type="number" id="grid-xgap-${tabId}" min="0" max="0.5" step="0.02">
      y <input class="grid-gap" type="number" id="grid-ygap-${tabId}" min="0" max="0.5" step="0.02">
      legend <select id="grid-legend-pos-${tabId}" style="width:7rem">
        <option value="right">right</option><option value="top">top</option>
        <option value="bottom">bottom</option><option value="none">hidden</option>
      </select>
      <input type="text" id="grid-legend-title-${tabId}" placeholder="legend title" style="width:10rem">`;
    wrap.appendChild(box);

    const bind = (id, key, isCheckbox) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.value !== undefined && !isCheckbox && (el.value = g[key]);
      isCheckbox && (el.checked = !!g[key]);
      el.addEventListener('change', () => { g[key] = isCheckbox ? el.checked : el.value; });
    };
    bind(`grid-share-x-${tabId}`, 'shareX', true);
    bind(`grid-share-y-${tabId}`, 'shareY', true);
    bind(`grid-share-legend-${tabId}`, 'shareLegend', true);
    bind(`grid-panel-labels-${tabId}`, 'panelLabels', true);
    bind(`grid-figure-width-${tabId}`, 'figureWidth', false);
    bind(`grid-figure-height-${tabId}`, 'figureHeight', false);
    bind(`grid-subplot-width-${tabId}`, 'subplotWidth', false);
    bind(`grid-subplot-height-${tabId}`, 'subplotHeight', false);
    bind(`grid-xgap-${tabId}`, 'xgap', false);
    bind(`grid-ygap-${tabId}`, 'ygap', false);
    bind(`grid-legend-title-${tabId}`, 'legendTitle', false);
    const posSel = document.getElementById(`grid-legend-pos-${tabId}`);
    if (posSel) {
      posSel.value = g.showLegend === false ? 'none' : g.legendPosition;
      posSel.addEventListener('change', () => {
        g.showLegend = posSel.value !== 'none';
        if (posSel.value !== 'none') g.legendPosition = posSel.value;
      });
    }
  };

  function refreshFacetColumns(tabId, columns) {
    const tab = G.tabs.find(t => t.id === tabId);
    const g = tab && tab.state.grid;
    const host = document.getElementById(`facet-group-columns-${tabId}`);
    if (!g || !host) return;
    g.facetColumns = Array.from(columns || []);
    // During workspace import, keep the serialized selections until the
    // source panel's saved/query columns are available. An empty refresh is
    // a loading state, not evidence that every saved facet column vanished.
    if (g.facetColumns.length) {
      g.facetGroupCols = g.facetGroupCols.filter(c => g.facetColumns.includes(c));
    }
    host.innerHTML = '';
    if (!g.facetColumns.length) {
      host.innerHTML = '<span style="font-size:10px;color:var(--text3)">Run &amp; Show first</span>';
    } else {
      g.facetColumns.forEach(col => {
        const pill = document.createElement('button');
        pill.type = 'button'; pill.className = 'col-pill'; pill.textContent = col;
        pill.classList.toggle('selected', g.facetGroupCols.includes(col));
        pill.addEventListener('click', () => {
          g.facetGroupCols = g.facetGroupCols.includes(col)
            ? g.facetGroupCols.filter(c => c !== col) : [...g.facetGroupCols, col];
          pill.classList.toggle('selected', g.facetGroupCols.includes(col));
        });
        host.appendChild(pill);
      });
    }
    const axis = document.getElementById(`facet-axis-${tabId}`);
    const count = document.getElementById(`facet-count-${tabId}`);
    if (axis) { axis.value = g.facetAxis; axis.onchange = () => { g.facetAxis = axis.value; }; }
    if (count) { count.value = g.facetCount; count.onchange = () => { g.facetCount = Math.max(1, Math.min(6, parseInt(count.value, 10) || 1)); count.value = g.facetCount; }; }
    const format = document.getElementById(`facet-label-format-${tabId}`);
    if (format) { format.value = g.facetLabelFormat; format.onchange = () => { g.facetLabelFormat = format.value || '{column}={value}'; }; }
  }
  window.updateFacetGroupColumns = function (panelId, columns) {
    const tab = G.tabs.find(t => t.state.grid?.panelIds?.[0] === panelId);
    if (tab) refreshFacetColumns(tab.id, columns);
  };

  // runGridPlot(tabId) (app.js) builds {rows, cols, panels} and POSTs
  // /api/plot_grid; wrap it to add the grid-level fields the backend
  // accepts. We can't easily wrap the internal api('POST', '/api/plot_grid',
  // payload) call itself without editing app.js, so instead wrap the global
  // api() function the same way style_addon.js already does for '/api/plot',
  // and both addons compose fine since each only touches requests to its
  // own endpoint.
  const _api = window.api;
  window.api = (m, p, b) => {
    if (b && p === '/api/plot_grid') {
      const tab = G.tabs.find(t => t.id === G.activeTab);
      const g = tab && tab.state.grid;
      if (g) {
        b = {
          ...b,
          share_x: !!g.shareX, share_y: !!g.shareY,
          share_legend: g.shareLegend !== false,
          xgap: g.xgap !== undefined ? +g.xgap : undefined,
          ygap: g.ygap !== undefined ? +g.ygap : undefined,
          panel_labels: !!g.panelLabels,
          legend: { position: g.legendPosition || 'right', title: g.legendTitle || '', show: g.showLegend !== false },
          figure_width_in: +g.figureWidth || 0, figure_height_in: +g.figureHeight || 0,
          subplot_width_in: +g.subplotWidth || 4, subplot_height_in: +g.subplotHeight || 3.3,
        };
        if (tab.state.mode === 'facet') {
          const facetColumns = g.facetGroupCols || [];
          if (!facetColumns.length) throw new Error('Choose one or more columns in Group by for the data-driven grid.');
          b = { ...b, group_by: facetColumns, facet_axis: g.facetAxis || 'cols',
            facet_count: Math.max(1, Math.min(6, parseInt(g.facetCount, 10) || 1)),
            facet_label_format: g.facetLabelFormat || '{column}={value}', panels: b.panels.slice(0, 1) };
        } else if (g.facetByGroup) {
          const sourceId = g.panelIds?.[0];
          const facetColumns = (sourceId && G.panelState[sourceId]?.groupCols) || [];
          if (!facetColumns.length) {
            throw new Error('Select one or more Group by (colour) columns in Panel 1 to split it into grid panels.');
          } else {
            b = { ...b, group_by: facetColumns, panels: b.panels.slice(0, 1) };
          }
        }
      }
    }
    return _api(m, p, b);
  };
})();
