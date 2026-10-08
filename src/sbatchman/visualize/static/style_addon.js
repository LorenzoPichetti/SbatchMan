// Loaded AFTER app.js. Adds: global Style tab, focus mode (big preview),
// and "Sync panels" for subplot grids. No edits to app.js needed except the
// export-scale line (see notes).
(function () {
  const FIELDS = [
    ['font_family', 'Font family', 'text'],
    ['title_size', 'Title size (pt)', 'number'], ['label_size', 'Axis label size (pt)', 'number'],
    ['tick_size', 'Tick label size (pt)', 'number'], ['legend_size', 'Legend font size (pt)', 'number'],
    ['legend_orientation', 'Legend orientation', ['auto', 'vertical', 'horizontal']],
    ['legend_rows', 'Legend rows (horizontal, 0=auto)', 'number'], ['legend_columns', 'Legend columns (horizontal, 0=auto)', 'number'],
    ['line_width', 'Line width', 'number'], ['marker_size', 'Marker size (plot + legend)', 'number'],
    ['palette', 'Palette (comma-separated hex)', 'list'],
    ['tick_dir', 'Tick direction', ['outside', 'inside']], ['tick_len', 'Tick length', 'number'],
    ['minor_ticks', 'Minor ticks', ['auto', 'on', 'off']], ['exponent', 'Exponent format', ['power', 'e', 'SI']],
    ['tick_angle', 'X tick angle', 'number'],
    ['grid', 'Grid', 'bool'], ['mirror', 'Mirror axes (box)', 'bool'],
    ['width_in', 'Figure width (in, 0=fill)', 'number'], ['height_in', 'Figure height (in)', 'number'],
    ['dpi', 'Export DPI', 'number'],
  ];
  window.G_STYLE = JSON.parse(localStorage.getItem('plotStyle') || '{}');
  let presets = {}, timer;

  const _api = window.api;
  window.api = (m, p, b) => {
    if (b && (p === '/api/plot' || p === '/api/plot_grid')) b = {...b, style: G_STYLE};
    return _api(m, p, b);
  };

  function save() {
    localStorage.setItem('plotStyle', JSON.stringify(G_STYLE));
    clearTimeout(timer);
    timer = setTimeout(() => { if (activeTabObj()?.state.plotted) runPlot(); }, 400);
  }

  function build(pane) {
    const box = document.createElement('div'); box.className = 'sb-section-body'; box.style.padding = '10px 12px';
    box.innerHTML = '<label>Preset</label><select id="style-preset"><option value="">— custom —</option>' +
      Object.keys(presets).map(k => `<option>${k}</option>`).join('') + '</select>';
    FIELDS.forEach(([k, label, type]) => {
      const w = document.createElement('div'); w.style.marginTop = '5px';
      let el;
      if (Array.isArray(type)) { el = document.createElement('select'); type.forEach(o => el.add(new Option(o))); }
      else { el = document.createElement('input'); el.type = type === 'bool' ? 'checkbox' : type === 'number' ? 'number' : 'text'; el.step = 'any'; }
      if (k === 'legend_rows' || k === 'legend_columns') { el.min = '0'; el.step = '1'; }
      el.id = 'st-' + k;
      if (type === 'bool') { el.style.width = 'auto'; w.innerHTML = `<label style="display:flex;gap:6px;align-items:center"></label>`; w.firstChild.append(el, label); }
      else { w.innerHTML = `<label>${label}</label>`; w.append(el); }
      el.addEventListener('change', () => {
        G_STYLE[k] = type === 'bool' ? el.checked : type === 'number' ? (el.value === '' ? '' : +el.value)
          : type === 'list' ? el.value.split(',').map(x => x.trim()).filter(Boolean) : el.value;
        save();
      });
      box.append(w);
    });
    const io = document.createElement('div'); io.style.cssText = 'display:flex;gap:6px;margin-top:10px';
    io.innerHTML = '<button class="btn sm" id="st-exp">Export style</button><button class="btn sm" id="st-imp">Import style</button>';
    box.append(io); pane.append(box);
    box.querySelector('#style-preset').addEventListener('change', e => {
      if (!e.target.value) return;
      G_STYLE = {...presets[e.target.value]}; fill(); save();
    });
    box.querySelector('#st-exp').onclick = () => download(JSON.stringify(G_STYLE, null, 2), 'plot-style.json');
    box.querySelector('#st-imp').onclick = () => {
      const f = document.createElement('input'); f.type = 'file'; f.accept = '.json';
      f.onchange = () => readJson(f.files[0], s => { G_STYLE = s; fill(); save(); }); f.click();
    };
    fill();
  }
  function fill() {
    FIELDS.forEach(([k, , type]) => {
      const el = document.getElementById('st-' + k); if (!el) return;
      const v = G_STYLE[k] ?? (k === 'legend_orientation' ? 'auto' : undefined);
      if (type === 'bool') el.checked = v !== false; else el.value = Array.isArray(v) ? v.join(', ') : (v ?? '');
    });
    const presetSelect = document.getElementById('style-preset');
    if (presetSelect) {
      const match = Object.entries(presets).find(([, preset]) =>
        Object.entries(preset).every(([key, value]) => JSON.stringify(G_STYLE[key]) === JSON.stringify(value)));
      presetSelect.value = match?.[0] || '';
    }
  }

  // Focus mode: hide the builder so the figure gets the whole window
  const focusBtn = document.createElement('button');
  focusBtn.className = 'btn'; focusBtn.textContent = 'Focus plot';
  focusBtn.onclick = () => {
    document.body.classList.toggle('focus');
    focusBtn.textContent = document.body.classList.contains('focus') ? 'Show controls' : 'Focus plot';
    requestAnimationFrame(() => {
      if (G.activeTab) window.resizePlot(G.activeTab);
    });
  };
  document.querySelector('.topbar-mid').append(focusBtn);
  const css = document.createElement('style');
  css.textContent = `.plotly-wrap{min-height:0;min-width:0;overflow:hidden}.builder{max-height:40%}
    body.focus .builder,body.focus .panel-tabs{display:none}body.focus #sidebar{display:none}
    body.focus #app{grid-template-columns:minmax(0,1fr)}`;
  document.head.append(css);

  // Sync panels: copy the active panel's settings to every other panel in the grid
  const _apply = window.applyGrid;
  window.applyGrid = function (tabId) {
    _apply(tabId);
    const wrap = document.getElementById(`grid-controls-wrap-${tabId}`);
    if (wrap && !wrap.querySelector('.sync-btn')) {
      const b = document.createElement('button'); b.className = 'btn sm sync-btn';
      b.textContent = 'Sync panels'; b.title = 'Copy settings of the current panel to all others';
      b.onclick = () => syncPanels(tabId); wrap.append(b);
    }
  };
  async function syncPanels(tabId) {
    const g = getState(tabId).grid, src = g.activePanel;
    const withSql = confirm('Also copy SQL / transform scripts? (Cancel = keep each panel\'s own query)');
    const ids = ['x-label', 'y-label', 'x-scale', 'y-scale', 'x-tickfmt', 'y-tickfmt', 'tick-formatter', 'legend-position', 'legend-title',
      'marker-by', 'dash-by', 'layout-script', 'script', ...(withSql ? ['sql-input', 'transform'] : [])];
    const s0 = getState(src);
    for (const pid of g.panelIds.filter(p => p !== src)) {
      const st = getState(pid);
      ids.forEach(i => { const a = document.getElementById(`${i}-${src}`), b = document.getElementById(`${i}-${pid}`); if (a && b) b.value = a.value; });
      const c = document.getElementById(`show-legend-${pid}`); if (c) c.checked = document.getElementById(`show-legend-${src}`).checked;
      Object.assign(st, {plotType: s0.plotType, yCols: [...s0.yCols], groupCols: [...s0.groupCols], extra: {...s0.extra}});
      renderPlotChips(pid); renderExtraOpts(pid);
      window.renderPlotSpecificFields(pid);
      const x = document.getElementById(`x-col-${src}`).value;
      await runShow(pid); const xs = document.getElementById(`x-col-${pid}`); if (xs) xs.value = x;
    }
    clientLog(`Synced settings from panel to ${g.panelIds.length - 1} other panel(s)`);
  }

  // Persist G_STYLE inside the workspace JSON itself (not just localStorage)
  // so plots.json carries the style when shared or committed. download(text,
  // filename) and applyWorkspace(ws, label) are both in app.js; wrap them
  // rather than editing app.js's own 'Export/Import workspace' handlers.
  const _download = window.download;
  window.download = function (text, filename) {
    try {
      const obj = JSON.parse(text);
      if (obj && Array.isArray(obj.tabs) && !obj.style) { obj.style = G_STYLE; text = JSON.stringify(obj, null, 2); }
    } catch (e) { /* not JSON, or not a workspace object: pass through untouched */ }
    return _download(text, filename);
  };
  const _applyWorkspace = window.applyWorkspace;
  window.applyWorkspace = function (ws, label) {
    if (ws && ws.style) { G_STYLE = ws.style; fill(); save(); }
    return _applyWorkspace(ws, label);
  };

  // Boot
  fetch('/api/style_presets').then(r => r.json()).then(r => {
    presets = r.presets;
    const tabs = document.querySelector('.sb-tabs'), t = document.createElement('div');
    t.className = 'sb-tab'; t.dataset.sbtab = 'style'; t.textContent = 'Style';
    const pane = document.createElement('div'); pane.className = 'sb-pane'; pane.id = 'sbtab-style';
    tabs.append(t); document.getElementById('sidebar').append(pane); build(pane);
    t.addEventListener('click', () => {
      document.querySelectorAll('.sb-tab').forEach(x => x.classList.toggle('active', x === t));
      document.querySelectorAll('.sb-pane').forEach(p => p.classList.toggle('active', p === pane));
    });
  });
})();
