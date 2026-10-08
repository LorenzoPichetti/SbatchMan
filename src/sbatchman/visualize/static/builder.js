// builder.js — the per-node (tab or grid panel) query/plot-type/axis
// builder: the plot-type chip picker, the SQL + transform script boxes, the
// axis/column controls, the inline data preview, and runShow (the single
// "Run & show" entry point that replaced the old separate load-columns /
// test-transform buttons).
import { G, getState, tabEl, currentDb, toArray, escHtml } from './state.js';
import { api, addLogEntries, clientLog } from './log.js';

// --- Data preview table (shared renderer) ----------------------------
export function renderDataTable(container, dfData, emptyMsg) {
  if (!dfData || !dfData.columns || !dfData.columns.length) {
    container.innerHTML = `<span style="color:var(--text3);font-size:10px;padding:8px;display:block">${emptyMsg || 'No data yet'}</span>`;
    return;
  }
  let html = '<table class="data-table"><thead><tr>';
  dfData.columns.forEach(c => html += `<th>${escHtml(c)}</th>`);
  html += '</tr></thead><tbody>';
  for (const row of dfData.rows) {
    html += '<tr>' + row.map(v => `<td>${v === null || v === undefined ? '' : escHtml(v)}</td>`).join('') + '</tr>';
  }
  html += '</tbody></table>';
  if (dfData.truncated) html += `<div style="font-size:9px;color:var(--text3);padding:4px 8px">Preview truncated — showing first ${dfData.rows.length} row(s)</div>`;
  container.innerHTML = html;
}

export function renderMultiPreview(container, labeledTables) {
  container.innerHTML = '';
  labeledTables.forEach(({ label, df }) => {
    const lbl = document.createElement('div');
    lbl.className = 'preview-section-label';
    lbl.textContent = label;
    container.appendChild(lbl);
    const box = document.createElement('div');
    renderDataTable(box, df);
    container.appendChild(box);
  });
}

// --- Plot type chips / extra options ----------------------------------
export function renderPlotChips(id) {
  const wrap = document.getElementById(`chips-${id}`);
  if (!wrap) return;
  wrap.innerHTML = '';
  const st = getState(id);
  if (!st) return;
  for (const [key, pt] of Object.entries(G.plotTypes)) {
    const ch = document.createElement('div');
    ch.className = 'plot-chip' + (pt.is_plugin ? ' plugin' : '') + (key === st.plotType ? ' active' : '');
    ch.textContent = pt.label;
    ch.title = pt.description;
    ch.dataset.type = key;
    ch.addEventListener('click', () => {
      st.plotType = key;
      wrap.querySelectorAll('.plot-chip').forEach(c => c.classList.toggle('active', c.dataset.type === key));
      renderExtraOpts(id);
      renderPlotSpecificFields(id);
    });
    wrap.appendChild(ch);
  }
}

export function renderExtraOpts(id) {
  const st = getState(id);
  const area = document.getElementById(`extra-opts-${id}`);
  if (!st || !area) return;
  area.innerHTML = '';
  const pt = G.plotTypes[st.plotType];
  if (!pt) return;
  for (const [key, val] of Object.entries(pt.defaults || {})) {
    const wrap = document.createElement('div');
    const lbl = document.createElement('label'); lbl.textContent = key;
    if (key === 'mode') {
      lbl.title = 'For line charts: choose lines, markers, or both. Supported by Plotly and Matplotlib.';
    }
    const inp = document.createElement('input');
    inp.id = `extra-${id}-${key}`;
    inp.value = st.extra?.[key] ?? val;
    inp.type = typeof val === 'number' ? 'number' : 'text';
    wrap.appendChild(lbl); wrap.appendChild(inp);
    if (key === 'mode') {
      const hint = document.createElement('small');
      hint.textContent = 'Line, markers, or both (Plotly and Matplotlib)';
      hint.style.cssText = 'display:block;color:var(--text3);font-size:9px';
      wrap.appendChild(hint);
    }
    area.appendChild(wrap);
  }
}

export function renderPlotSpecificFields(id) {
  const panel = document.getElementById(`plot-specific-fields-${id}`);
  const controls = document.getElementById(`plot-field-controls-${id}`);
  const st = getState(id);
  if (!panel || !controls || !st) return;
  const fields = G.plotFields[st.plotType] || [];
  panel.hidden = fields.length === 0;
  controls.replaceChildren();

  for (const field of fields) {
    const wrap = document.createElement('div');
    wrap.className = 'plot-specific-field';
    const label = document.createElement('label');
    label.htmlFor = `plot-field-${id}-${field.name}`;
    label.textContent = `${field.label}${field.required ? ' *' : ''}`;
    if (field.help) label.title = field.help;
    wrap.appendChild(label);

    let input;
    if (field.type === 'column' || field.type === 'select') {
      input = document.createElement('select');
      const empty = document.createElement('option');
      empty.value = '';
      empty.textContent = field.required ? '— select a column —' : '— none —';
      input.appendChild(empty);
      const options = field.type === 'column'
        ? (st.columns || []).map(value => ({value, label: value}))
        : field.options;
      options.forEach(option => {
        const el = document.createElement('option');
        el.value = option.value;
        el.textContent = option.label;
        input.appendChild(el);
      });
    } else if (field.type === 'textarea') {
      input = document.createElement('textarea');
    } else {
      input = document.createElement('input');
      input.type = field.type === 'number' ? 'number' : field.type === 'checkbox' ? 'checkbox' : 'text';
      if (field.min !== null) input.min = field.min;
      if (field.max !== null) input.max = field.max;
      if (field.step !== null) input.step = field.step;
    }
    input.id = `plot-field-${id}-${field.name}`;
    input.required = !!field.required;
    if (field.placeholder && input.type !== 'checkbox') input.placeholder = field.placeholder;
    const legacyValue = field.name === 'z' ? st.z : undefined;
    const value = st.extra?.[field.name] ?? legacyValue ?? field.default ?? '';
    if (input.type === 'checkbox') input.checked = value === true || value === 'true';
    else input.value = value;
    input.addEventListener('change', () => {
      const current = getState(id);
      if (!current.extra) current.extra = {};
      current.extra[field.name] = input.type === 'checkbox' ? input.checked
        : input.type === 'number' ? (input.value === '' ? '' : Number(input.value)) : input.value;
      if (field.name === 'z') current.z = input.value;
    });
    wrap.appendChild(input);
    controls.appendChild(wrap);
  }
}

// --- Axis / column controls --------------------------------------------
export function updateAxisControls(id, columns) {
  const st = getState(id);
  if (!st) return;
  st.columns = columns;
  st.yCols = toArray(st.yCols).filter(c => columns.includes(c));
  st.groupCols = toArray(st.groupCols).filter(c => columns.includes(c));
  renderPlotSpecificFields(id);

  const xSel = tabEl(id, 'x-col');
  const markerSel = tabEl(id, 'marker-by');
  const dashSel = tabEl(id, 'dash-by');
  const yPills = document.getElementById(`y-pills-${id}`);
  const grpPills = document.getElementById(`group-pills-${id}`);
  if (!xSel || !yPills) return;

  const prevX = xSel.value, prevMarker = markerSel?.value, prevDash = dashSel?.value;
  const mkOpt = (v, sel) => { const o = document.createElement('option'); o.value = v; o.textContent = v; if (v === sel) o.selected = true; return o; };

  xSel.innerHTML = '<option value="">— x column —</option>';
  if (markerSel) markerSel.innerHTML = '<option value="">— none —</option>';
  if (dashSel) dashSel.innerHTML = '<option value="">— none —</option>';
  yPills.innerHTML = '';
  if (grpPills) grpPills.innerHTML = '';

  for (const col of columns) {
    xSel.appendChild(mkOpt(col, prevX || st.x));
    if (markerSel) markerSel.appendChild(mkOpt(col, prevMarker || st.markerBy));
    if (dashSel) dashSel.appendChild(mkOpt(col, prevDash || st.dashBy));

    const yPill = document.createElement('div');
    yPill.className = 'col-pill' + (st.yCols.includes(col) ? ' selected' : '');
    yPill.textContent = col; yPill.dataset.col = col;
    yPill.addEventListener('click', () => {
      if (st.yCols.includes(col)) { st.yCols = st.yCols.filter(c => c !== col); yPill.classList.remove('selected'); }
      else { st.yCols.push(col); yPill.classList.add('selected'); }
    });
    yPills.appendChild(yPill);

    if (grpPills) {
      const gPill = document.createElement('div');
      gPill.className = 'col-pill' + (st.groupCols.includes(col) ? ' selected' : '');
      gPill.textContent = col; gPill.dataset.col = col;
      gPill.addEventListener('click', () => {
        if (st.groupCols.includes(col)) { st.groupCols = st.groupCols.filter(c => c !== col); gPill.classList.remove('selected'); }
        else { st.groupCols.push(col); gPill.classList.add('selected'); }
      });
      grpPills.appendChild(gPill);
    }
  }
}

// --- Builder markup (shared by tabs in "single" mode and by grid panels) --
export function builderColumnsHTML(id) {
  const dbOpts = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
  return `
    <div class="b-col" style="grid-column:1 / -1;min-width:240px;flex:1.2">
      <div class="b-col-title">Data source</div>
      <div id="db-select-wrap-${id}" style="margin-bottom:5px">
        <select id="db-select-${id}">${dbOpts}</select>
      </div>
      <textarea id="sql-input-${id}" class="code-editor sql-editor" spellcheck="false" aria-label="SQL query" placeholder="SELECT * FROM results"></textarea>

      <div style="margin-top:8px">
        <div class="script-toggle" id="transform-toggle-${id}" data-tip="Modify the query result with pandas before it's plotted">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor"><path d="M2 2h5v5H2V2zm7 0h5v5H9V2zM2 9h5v5H2V9zm7 0h5v5H9V9z"/></svg>
          Data transform (pandas)
          <svg id="transform-arrow-${id}" width="9" height="9" viewBox="0 0 16 16" fill="currentColor" style="margin-left:auto;transition:transform .2s"><path d="M4 6l4 4 4-4"/></svg>
        </div>
        <div class="script-box" id="transform-box-${id}">
          <div class="script-hint">Runs after the SQL query. <b>data['result']</b> starts with exactly the query's selected columns, including <code>AS</code> aliases. Source tables are available for lookups, but source columns omitted from SQL are removed before plotting; select any original fields you want to plot. New transform columns that are not existing source-table fields are kept. <b>agg</b> exposes summarize()/speedup()/efficiency() for collapsing repeated runs into mean+error rows. Call <b>log(msg)</b> to print to the Logs panel while you iterate.</div>
          <textarea id="transform-${id}" class="code-editor python-editor" spellcheck="false" aria-label="Python pandas transform" placeholder="df = data['result']&#10;df['flags'] = df['flags'].map(map_compiler_flags_names)&#10;data['result'] = df"></textarea>
        </div>
      </div>

      <div style="display:flex;gap:6px;margin-top:8px;align-items:center">
        <button class="btn sm" id="run-show-${id}" data-tip="Run the SQL query + transform and show the resulting table below">Run &amp; show</button>
        <span id="preview-status-${id}" style="font-size:9px;color:var(--text3)"></span>
      </div>
      <div class="data-preview" id="inline-preview-${id}" style="max-height:160px;margin-top:5px;border:1px solid var(--border);border-radius:var(--radius)"></div>
    </div>

    <div class="b-col" style="grid-column:1 / -1;min-width:200px;max-width:240px">
      <div class="b-col-title" data-tip="Choose how data is rendered">Plot type</div>
      <div class="plot-type-grid" id="chips-${id}"></div>
      <div style="margin-top:6px">
        <div class="script-toggle" id="script-toggle-${id}" data-tip="Define a custom plot() function in Python — overrides the plot type above">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor"><path d="M9.5 2l4 6-4 6h-3l4-6-4-6h3zm-6 0l4 6-4 6H0l4-6L0 2h3.5z"/></svg>
          Custom plot script
          <svg id="script-arrow-${id}" width="9" height="9" viewBox="0 0 16 16" fill="currentColor" style="margin-left:auto;transition:transform .2s"><path d="M4 6l4 4 4-4"/></svg>
        </div>
        <div class="script-box" id="script-box-${id}">
          <div class="script-hint">Define <code>plot(df_data, config)</code> to return Plotly traces, or <code>plot_matplotlib(ax, df_data, config)</code> to draw directly with Matplotlib. You can define both; each backend uses its matching function. <code>df_data</code> has <code>columns</code> and <code>rows</code>.</div>
          <textarea id="script-${id}" class="code-editor python-editor" spellcheck="false" aria-label="Custom Python plot script" placeholder="def plot_matplotlib(ax, df_data, config):&#10;    x = df_data['columns'].index(config['x'])&#10;    y = df_data['columns'].index(config['y'][0])&#10;    ax.scatter([r[x] for r in df_data['rows']],&#10;               [r[y] for r in df_data['rows']])"></textarea>
        </div>
      </div>
      <div class="extra-opts" id="extra-opts-${id}"></div>
      <div class="plot-specific-fields" id="plot-specific-fields-${id}" hidden>
        <div class="b-col-title">Plot-specific fields</div>
        <div id="plot-field-controls-${id}"></div>
      </div>
    </div>

    <div class="b-col" style="min-width:180px">
      <div class="b-col-title">Data mappings</div>
      <label data-tip="Column mapped to the X axis">X column</label>
      <select id="x-col-${id}"><option value="">— x column —</option></select>
      <label style="margin-top:5px" data-tip="One or more columns mapped to Y — click to toggle">Y column(s)</label>
      <div class="multi-col-list" id="y-pills-${id}"></div>
      <label style="margin-top:5px" data-tip="Group / colour by one or more columns — combinations become separate traces">Group by (colour) — multi-column</label>
      <div class="multi-col-list" id="group-pills-${id}"></div>
      <label style="margin-top:5px" data-tip="Assign a distinct marker shape to each value of this column (line/scatter charts)">Marker by</label>
      <select id="marker-by-${id}"><option value="">— none —</option></select>
      <label style="margin-top:5px" data-tip="Assign a distinct linestyle (solid/dash/dot/…) to each value of this column (line charts)">Linestyle by</label>
      <select id="dash-by-${id}"><option value="">— none —</option></select>
    </div>

    <div class="b-col" style="min-width:180px">
      <div class="b-col-title">Appearance &amp; legend</div>
      <label>Chart / subplot title</label>
      <input type="text" id="chart-title-${id}" placeholder="My benchmark">
      <label style="margin-top:5px">X axis label</label>
      <input type="text" id="x-label-${id}" placeholder="(auto from column)">
      <label style="margin-top:5px">Y axis label</label>
      <input type="text" id="y-label-${id}" placeholder="(auto from column)">
      <label style="margin-top:5px" data-tip="linear, log (base 10), log2 (base 2), date, category">X scale</label>
      <select id="x-scale-${id}">
        <option value="linear">linear</option><option value="log">log10</option><option value="log2">log2</option>
        <option value="date">date</option><option value="category">category</option>
      </select>
      <label style="margin-top:5px" data-tip="linear, log (base 10), log2 (base 2), date, category">Y scale</label>
      <select id="y-scale-${id}">
        <option value="linear">linear</option><option value="log">log10</option><option value="log2">log2</option>
        <option value="date">date</option><option value="category">category</option>
      </select>
      <label style="margin-top:5px" data-tip="Plotly d3 tick format string, e.g. .2f or %Y-%m">X tick format</label>
      <input type="text" id="x-tickfmt-${id}" placeholder=".2f">
      <label style="margin-top:5px">Y tick format</label>
      <input type="text" id="y-tickfmt-${id}" placeholder=".2f">
      <label style="margin-top:7px" data-tip="Optional Python formatter. When set, it takes precedence over the X/Y tick format strings above.">Custom tick formatter (Python)</label>
      <textarea id="tick-formatter-${id}" class="tick-formatter-editor code-editor python-editor" spellcheck="false" aria-label="Python tick formatter" placeholder="def format_tick(value, axis):&#10;    return f'{value:g}'"></textarea>
      <div class="script-hint">Define <code>format_tick(value, axis)</code>; <code>axis</code> is <code>'x'</code> or <code>'y'</code>. Return the label as a string. It is used for both plotting backends.</div>
      <label style="margin-top:5px" data-tip="Where the legend sits. The 'inside' options give the compact, journal-figure look when there's an empty corner to put it in.">Legend position</label>
      <select id="legend-position-${id}">
        <option value="right">outside right</option>
        <option value="top">top (horizontal)</option>
        <option value="bottom">bottom (horizontal)</option>
        <option value="inside-top-right">inside — top right</option>
        <option value="inside-top-left">inside — top left</option>
        <option value="inside-bottom-right">inside — bottom right</option>
        <option value="inside-bottom-left">inside — bottom left</option>
      </select>
      <label style="margin-top:5px">Legend title</label>
      <input type="text" id="legend-title-${id}" placeholder="(none)">
      <label style="margin-top:5px" data-tip="Override the global legend layout for this panel. In a grid, explicit layout settings give this panel its own legend.">Panel legend orientation</label>
      <select id="legend-orientation-${id}">
        <option value="">inherit global</option><option value="auto">auto</option>
        <option value="vertical">vertical</option><option value="horizontal">horizontal</option>
      </select>
      <div style="display:flex;gap:5px;margin-top:5px">
        <div style="flex:1"><label data-tip="Horizontal legend rows; 0 means automatic">Rows</label><input type="number" min="0" step="1" id="legend-rows-${id}" placeholder="auto"></div>
        <div style="flex:1"><label data-tip="Horizontal legend columns; 0 means automatic">Columns</label><input type="number" min="0" step="1" id="legend-columns-${id}" placeholder="auto"></div>
      </div>
      <label style="margin-top:5px" data-tip="Optional marker size override for this panel, in pixels">Panel marker size</label>
      <input type="number" min="1" step="1" id="legend-marker-size-${id}" placeholder="global">
      <label style="margin-top:5px;display:flex;align-items:center;gap:6px">
        <input type="checkbox" id="show-legend-${id}" checked style="width:auto;margin:0"> Show legend
      </label>

      <div style="margin-top:8px">
        <div class="script-toggle" id="layout-toggle-${id}" data-tip="A short Python snippet to fine-tune this plot's appearance beyond the fields above">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor"><path d="M8 1l1.8 3.6L14 5.2l-3 2.9.7 4.1L8 10.3l-3.7 1.9.7-4.1-3-2.9 4.2-.6z"/></svg>
          Layout script
          <svg id="layout-arrow-${id}" width="9" height="9" viewBox="0 0 16 16" fill="currentColor" style="margin-left:auto;transition:transform .2s"><path d="M4 6l4 4 4-4"/></svg>
        </div>
        <div class="script-box" id="layout-box-${id}">
          <div class="script-hint">
            <b>Plotly:</b> edit the supplied <code>layout</code> dictionary, for example <code>layout['xaxis']['tickangle'] = 45</code>.<br>
            <b>Matplotlib:</b> optionally define <code>customize_matplotlib(fig, ax, config)</code> to edit the figure or axes directly. In a subplot grid, <code>ax</code> is the current panel's axes. <code>log(msg)</code> writes to the Logs panel.
          </div>
          <textarea id="layout-script-${id}" class="code-editor python-editor" spellcheck="false" aria-label="Python layout script" placeholder="layout['xaxis']['tickangle'] = 45&#10;&#10;def customize_matplotlib(fig, ax, config):&#10;    ax.grid(True, alpha=0.25)"></textarea>
        </div>
      </div>
    </div>`;
}

// --- Wire up a single builder's controls (shared by tabs and panels) -----
export function wireBuilderColumns(id) {
  const st = getState(id);
  if (!st) return;

  renderPlotChips(id);
  renderExtraOpts(id);
  renderPlotSpecificFields(id);

  const f = (sid, val) => { const el = document.getElementById(sid); if (el && val !== undefined) el.value = val; };
  f(`db-select-${id}`, st.database);
  f(`sql-input-${id}`, st.sql);
  f(`chart-title-${id}`, st.chartTitle);
  f(`x-label-${id}`, st.xLabel);
  f(`y-label-${id}`, st.yLabel);
  f(`x-scale-${id}`, st.xScale);
  f(`y-scale-${id}`, st.yScale);
  f(`x-tickfmt-${id}`, st.xTickFmt);
  f(`y-tickfmt-${id}`, st.yTickFmt);
  f(`tick-formatter-${id}`, st.tickFormatter);
  f(`legend-position-${id}`, st.legendPosition || 'right');
  f(`legend-title-${id}`, st.legendTitle);
  f(`legend-orientation-${id}`, st.legendOrientation || '');
  f(`legend-rows-${id}`, st.legendRows);
  f(`legend-columns-${id}`, st.legendColumns);
  f(`legend-marker-size-${id}`, st.legendMarkerSize);
  { const el = document.getElementById(`show-legend-${id}`); if (el) el.checked = st.showLegend !== false; }
  f(`script-${id}`, st.customScript);
  f(`transform-${id}`, st.transformScript);
  f(`layout-script-${id}`, st.layoutScript);

  const dbWrap = document.getElementById(`db-select-wrap-${id}`);
  if (dbWrap) dbWrap.style.display = G.singleDb ? 'none' : 'block';
  if (G.singleDb && G.defaultDb) {
    const sel = tabEl(id, 'db-select');
    if (sel) sel.value = G.defaultDb;
    st.database = G.defaultDb;
  }

  // window.updateAxisControls (see the fuller comment in runShow below): this
  // re-syncs a node's axis-dependent selects from state it already has (e.g.
  // a cloned tab, or a grid panel just created with a database already set),
  // not from a fresh query — same reasoning applies.
  if (st.columns && st.columns.length) window.updateAxisControls(id, st.columns);
  setTimeout(() => { f(`marker-by-${id}`, st.markerBy); f(`dash-by-${id}`, st.dashBy); }, 0);

  const bindToggle = (toggleId, boxId, arrowId) => {
    const t = document.getElementById(toggleId);
    if (!t) return;
    t.addEventListener('click', () => {
      const box = document.getElementById(boxId);
      const arrow = document.getElementById(arrowId);
      const open = box.classList.toggle('open');
      if (arrow) arrow.style.transform = open ? 'rotate(180deg)' : '';
    });
  };
  bindToggle(`script-toggle-${id}`, `script-box-${id}`, `script-arrow-${id}`);
  bindToggle(`transform-toggle-${id}`, `transform-box-${id}`, `transform-arrow-${id}`);
  bindToggle(`layout-toggle-${id}`, `layout-box-${id}`, `layout-arrow-${id}`);

  // window.runShow, not the local import: axis_addon.js wraps it to restore
  // the error-y/-low/-high selects once a post-import query actually
  // resolves (see axis_addon.js's runShow wrap and its comment). Calling the
  // bare import here would make that restore unreachable from real clicks.
  const runShowBtn = document.getElementById(`run-show-${id}`);
  if (runShowBtn) runShowBtn.addEventListener('click', () => window.runShow(id));
  const sqlBox = document.getElementById(`sql-input-${id}`);
  if (sqlBox) sqlBox.addEventListener('blur', () => { if (sqlBox.value.trim()) window.runShow(id); });
}

// --- Run & show (SQL + transform preview) --------------------------------
export async function runShow(id) {
  const db = currentDb(id);
  const sql = tabEl(id, 'sql-input')?.value.trim();
  const transformScript = tabEl(id, 'transform')?.value || '';
  const statusEl = document.getElementById(`preview-status-${id}`);
  const previewEl = document.getElementById(`inline-preview-${id}`);
  if (!db || !sql) { if (statusEl) statusEl.textContent = 'Set a database and SQL query first'; return; }
  if (statusEl) statusEl.textContent = 'Running…';
  if (previewEl) previewEl.classList.add('open');
  try {
    const res = await api('POST', '/api/preview', { database: db, sql, transform_script: transformScript });
    addLogEntries(res.log_entries);
    if (!res.ok) {
      if (statusEl) statusEl.textContent = res.error || 'Failed';
      if (previewEl) renderDataTable(previewEl, null, 'Query/transform failed — see Logs');
      return;
    }
    // window.updateAxisControls, not the local import: axis_addon.js wraps
    // it to refresh the y2/error-column selects whenever fresh columns come
    // back from a query — this is the one call site that actually drives
    // that refresh on an ordinary run (not just a post-import run).
    window.updateAxisControls(id, res.columns);
    window.updateFacetGroupColumns?.(id, res.columns);
    if (statusEl) statusEl.textContent = `OK — ${res.preview.rows.length} row(s), ${res.columns.length} column(s)`;
    if (previewEl) renderDataTable(previewEl, res.preview);
    clientLog(`Run & show: ${res.preview.rows.length} row(s)`);
  } catch (e) {
    if (statusEl) statusEl.textContent = e.message;
    clientLog(e.message, 'error');
  }
}

// Keep Tab inside code editors and insert real indentation. This listener is
// delegated because grid panels create their textareas dynamically.
document.addEventListener('keydown', event => {
  const editor = event.target.closest?.('textarea.code-editor');
  if (!editor || event.key !== 'Tab') return;
  event.preventDefault();
  const start = editor.selectionStart;
  const end = editor.selectionEnd;
  const value = editor.value;
  const lineStart = value.lastIndexOf('\n', Math.max(0, start - 1)) + 1;
  const lineEndAt = value.indexOf('\n', end);
  const lineEnd = lineEndAt < 0 ? value.length : lineEndAt;

  if (event.shiftKey && start === end) {
    const currentLine = value.slice(lineStart, lineEnd);
    const spaces = currentLine.match(/^ {1,4}/)?.[0].length || 0;
    if (spaces) editor.setRangeText('', lineStart, lineStart + spaces, 'end');
    if (spaces && start > lineStart + spaces) editor.setSelectionRange(start - spaces, start - spaces);
    return;
  }

  if (!event.shiftKey && start === end) {
    editor.setRangeText('    ', start, end, 'end');
    return;
  }

  const selectedLines = value.slice(lineStart, lineEnd).split('\n');
  if (!event.shiftKey) {
    const indented = selectedLines.map(line => `    ${line}`).join('\n');
    editor.setRangeText(indented, lineStart, lineEnd, 'select');
    editor.setSelectionRange(start + 4, end + selectedLines.length * 4);
    return;
  }

  let removedBeforeStart = 0;
  let removedBeforeEnd = 0;
  let cursor = lineStart;
  const unindented = selectedLines.map(line => {
    const spaces = line.match(/^ {1,4}/)?.[0].length || 0;
    if (cursor < start) removedBeforeStart += spaces;
    if (cursor < end) removedBeforeEnd += spaces;
    cursor += line.length + 1;
    return line.slice(spaces);
  }).join('\n');
  editor.setRangeText(unindented, lineStart, lineEnd, 'select');
  editor.setSelectionRange(Math.max(lineStart, start - removedBeforeStart), Math.max(lineStart, end - removedBeforeEnd));
});

Object.assign(window, {
  renderDataTable, renderMultiPreview, renderPlotChips, renderExtraOpts, renderPlotSpecificFields,
  updateAxisControls, builderColumnsHTML, wireBuilderColumns, runShow,
});
