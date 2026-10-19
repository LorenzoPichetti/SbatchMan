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
  css.textContent = `.plotly-wrap{min-height:0;min-width:0;overflow:hidden}.builder{max-height:none}
    body.focus .builder,body.focus .panel-tabs{display:none}body.focus #sidebar{display:none}
    body.focus #sidebar-resizer{display:none}
    body.focus #app{grid-template-columns:0 0 minmax(0,1fr)}
    body.focus #centre{grid-column:3}`;
  document.head.append(css);

  // Sync panels: copy the active panel's complete saved config to all others.
  const _apply = window.applyGrid;
  window.applyGrid = function (tabId) {
    _apply(tabId);
    const wrap = document.getElementById(`grid-controls-wrap-${tabId}`);
    if (wrap && !wrap.querySelector('.sync-btn')) {
      const b = document.createElement('button'); b.className = 'btn sm sync-btn';
      b.textContent = 'Sync from Panel 1…';
      b.onclick = () => syncPanels(tabId); wrap.append(b);
    }
    const chips = document.getElementById(`panel-tabs-${tabId}`);
    if (chips && !chips._syncSourceListener) {
      chips._syncSourceListener = true;
      chips.addEventListener('click', () => requestAnimationFrame(() => refreshSyncButton(tabId)));
    }
    refreshSyncButton(tabId);
  };
  function refreshSyncButton(tabId) {
    const grid = getState(tabId)?.grid;
    const button = document.querySelector(`#grid-controls-wrap-${tabId} .sync-btn`);
    if (!grid || !button) return;
    const source = Math.max(0, grid.panelIds.indexOf(grid.activePanel));
    const targets = grid.panelIds.map((_, i) => i + 1).filter(n => n !== source + 1);
    button.textContent = `Sync from Panel ${source + 1}…`;
    button.title = targets.length
      ? `Copy Panel ${source + 1} settings to Panels ${targets.join(', ')}`
      : 'Add another panel to enable syncing';
    button.disabled = targets.length === 0;
  }
  function syncPanels(tabId) {
    const grid = getState(tabId)?.grid;
    if (!grid || grid.panelIds.length < 2) return;
    const sourceId = grid.activePanel;
    const sourceIndex = grid.panelIds.indexOf(sourceId) + 1;
    const targets = grid.panelIds.map((id, index) => ({id, index: index + 1}))
      .filter(panel => panel.id !== sourceId);
    const targetLabels = targets.map(panel => `Panel ${panel.index}`).join(', ');
    const confirmed = confirm(
      `Sync from Panel ${sourceIndex} to ${targetLabels}?\n\n` +
      `This overwrites each target's database, SQL query, transform script, plot type, data mappings, appearance and legend, axis settings, plot-specific fields, and custom plot/layout scripts. The source panel is unchanged.`
    );
    if (!confirmed) return;
    const sourceConfig = window.getNodeConfig(sourceId);
    targets.forEach(panel => window.applyNodeConfig(panel.id, sourceConfig));
    clientLog(`Copied all settings from Panel ${sourceIndex} to ${targetLabels}`);
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

  // Standalone tab-config JSON files also carry the active style profile.
  // Restore it on import just as workspace import restores workspace.style.
  const _applyTabConfig = window.applyTabConfig;
  window.applyTabConfig = function (tabId, cfg) {
    if (cfg && cfg.style && typeof cfg.style === 'object') {
      G_STYLE = cfg.style;
      fill();
      save();
    }
    return _applyTabConfig(tabId, cfg);
  };

  // A standalone tab-config export is also a JSON handoff, so include the
  // current global style there just as workspace exports do. Without this,
  // importing a tab config into another browser/session silently falls back
  // to that session's local style even though the source figure was styled.
  const _getTabConfig = window.getTabConfig;
  window.getTabConfig = function (tabId) {
    const cfg = _getTabConfig(tabId);
    if (cfg) cfg.style = {...G_STYLE};
    return cfg;
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
