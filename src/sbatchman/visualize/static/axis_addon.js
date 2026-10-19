// Loaded AFTER app.js (and after style_addon.js, order doesn't matter between
// the two). Adds UI for the backend fields render.py/layout.py already
// support but the builder never exposed: axis min/max + reversed, a
// secondary y-axis, and error-bar/band columns. Follows the same additive
// pattern as style_addon.js: inject DOM into the existing "Labels & scales"
// and "Axes" columns, extend getState()'s defaults, and wrap
// gatherPlotPayload/getNodeConfig/applyNodeConfig rather than editing them.
(function () {
  const NUM_FIELDS = ['xMin', 'xMax', 'yMin', 'yMax', 'y2Min', 'y2Max'];
  const BOOL_FIELDS = ['xReversed', 'yReversed'];
  const SEL_FIELDS = ['errorY', 'errorYLow', 'errorYHigh'];   // column pickers, populated from st.columns
  const TEXT_FIELDS = ['y2Label'];
  const LINE_FIELDS = ['xLines', 'yLines'];

  // --- state defaults --------------------------------------------------
  // defaultNodeState() (in app.js) is called once per new tab/panel; wrap it
  // so every node state has these keys without editing app.js itself.
  const _defaultNodeState = window.defaultNodeState;
  window.defaultNodeState = function (base) {
    const st = _defaultNodeState(base);
    NUM_FIELDS.forEach(k => { if (!(k in st)) st[k] = ''; });
    BOOL_FIELDS.forEach(k => { if (!(k in st)) st[k] = false; });
    SEL_FIELDS.forEach(k => { if (!(k in st)) st[k] = ''; });
    TEXT_FIELDS.forEach(k => { if (!(k in st)) st[k] = ''; });
    LINE_FIELDS.forEach(k => { if (!Array.isArray(st[k])) st[k] = []; });
    if (!('y2Cols' in st)) st.y2Cols = [];
    if (!('errorStyle' in st)) st.errorStyle = 'bars';
    return st;
  };

  // --- DOM injection -----------------------------------------------------
  // builderColumnsHTML(id) (app.js) returns the static per-node markup;
  // append our fields to it rather than reconstructing the whole thing.
  const _builderHTML = window.builderColumnsHTML;
  window.builderColumnsHTML = function (id) {
    return _builderHTML(id) + `
    <div class="b-col" style="min-width:200px">
      <div class="b-col-title" data-tip="Set axis bounds, a secondary axis, or error display">Axis details</div>
      <div style="display:flex;gap:4px">
        <input type="text" id="x-min-${id}" placeholder="x min">
        <input type="text" id="x-max-${id}" placeholder="x max">
      </div>
      <label style="display:flex;gap:6px;align-items:center;margin-top:4px">
        <input type="checkbox" id="x-reversed-${id}" style="width:auto;margin:0"> Reverse x axis
      </label>
      <div style="display:flex;gap:4px;margin-top:6px">
        <input type="text" id="y-min-${id}" placeholder="y min">
        <input type="text" id="y-max-${id}" placeholder="y max">
      </div>
      <label style="display:flex;gap:6px;align-items:center;margin-top:4px">
        <input type="checkbox" id="y-reversed-${id}" style="width:auto;margin:0"> Reverse y axis
      </label>

      <div class="b-col-title" style="margin-top:10px" data-tip="Plot some Y columns against a second, independently-scaled y axis on the right">Secondary y-axis</div>
      <label>Columns on y2 (multi-select)</label>
      <div style="font-size:9px;color:var(--text3);margin:2px 0">Choose columns already selected under Y column(s); they move to the right axis.</div>
      <div class="multi-col-list" id="y2-pills-${id}"></div>
      <input type="text" id="y2-label-${id}" placeholder="y2 axis label" style="margin-top:4px">
      <div style="display:flex;gap:4px;margin-top:4px">
        <input type="text" id="y2-min-${id}" placeholder="y2 min">
        <input type="text" id="y2-max-${id}" placeholder="y2 max">
      </div>

      <div class="b-col-title" style="margin-top:10px" data-tip="Add reference lines at data coordinates">Reference lines</div>
      <label>Horizontal lines (Y values)</label>
      <div id="reference-y-lines-${id}"></div>
      <button type="button" class="btn sm" id="reference-y-add-${id}">Add horizontal line</button>
      <label style="margin-top:5px">Vertical lines (X values)</label>
      <div id="reference-x-lines-${id}"></div>
      <button type="button" class="btn sm" id="reference-x-add-${id}">Add vertical line</button>

      <div class="b-col-title" style="margin-top:10px" data-tip="Requires a column with the error magnitude, e.g. from agg.summarize() in a transform script">Error bars / band</div>
      <label>Symmetric error column</label>
      <select id="error-y-${id}"><option value="">— none —</option></select>
      <div style="font-size:9px;color:var(--text3);margin:3px 0">— or, for asymmetric error (e.g. min/max) —</div>
      <select id="error-y-low-${id}"><option value="">— low bound col —</option></select>
      <select id="error-y-high-${id}" style="margin-top:3px"><option value="">— high bound col —</option></select>
      <label style="margin-top:5px;display:flex;align-items:center;gap:10px">
        <span style="font-size:10px;color:var(--text2)">Style</span>
        <label style="display:flex;gap:4px;align-items:center;font-size:10px">
          <input type="radio" name="error-style-${id}" value="bars" checked style="width:auto"> Whiskers
        </label>
        <label style="display:flex;gap:4px;align-items:center;font-size:10px">
          <input type="radio" name="error-style-${id}" value="band" style="width:auto"> Shaded band
        </label>
      </label>
    </div>`;
  };

  // --- wiring: populate fields from state, bind selects to available columns --
  const _wire = window.wireBuilderColumns;
  window.wireBuilderColumns = function (id) {
    _wire(id);
    const st = getState(id);
    if (!st) return;
    const f = (sid, val) => { const el = document.getElementById(sid); if (el && val !== undefined) el.value = val; };
    f(`x-min-${id}`, st.xMin); f(`x-max-${id}`, st.xMax);
    f(`y-min-${id}`, st.yMin); f(`y-max-${id}`, st.yMax);
    f(`y2-min-${id}`, st.y2Min); f(`y2-max-${id}`, st.y2Max); f(`y2-label-${id}`, st.y2Label);
    st.yLines = normalizeLines(st.yLines, 'y');
    st.xLines = normalizeLines(st.xLines, 'x');
    renderReferenceLines(id, 'y');
    renderReferenceLines(id, 'x');
    ['x', 'y'].forEach(axis => {
      document.getElementById(`reference-${axis}-add-${id}`)?.addEventListener('click', () => {
        st[axis + 'Lines'] = readReferenceLines(id, axis);
        st[axis + 'Lines'].push(defaultLine(axis));
        renderReferenceLines(id, axis);
      });
    });
    const xr = document.getElementById(`x-reversed-${id}`); if (xr) xr.checked = !!st.xReversed;
    const yr = document.getElementById(`y-reversed-${id}`); if (yr) yr.checked = !!st.yReversed;
    const styleRadio = document.querySelector(`input[name="error-style-${id}"][value="${st.errorStyle || 'bars'}"]`);
    if (styleRadio) styleRadio.checked = true;
    if (st.columns && st.columns.length) populateAxisAddonSelects(id);
    setTimeout(() => { f(`error-y-${id}`, st.errorY); f(`error-y-low-${id}`, st.errorYLow); f(`error-y-high-${id}`, st.errorYHigh); }, 0);
  };

  // runShow(id) (app.js) re-runs the SQL query and, on success, calls
  // updateAxisControls(id, columns) — which we already hook to refresh our
  // selects. applyNodeConfig (below) can't just setTimeout a fixed delay to
  // restore error-y/-low/-high, because that races runShow's async query:
  // if the fetch is slow, our restore runs before the columns (and thus the
  // <option> elements) exist, and a <select>.value assignment to a value
  // with no matching <option> silently no-ops in a real browser. Instead,
  // stash the values to restore on the state object and apply them the
  // moment runShow's promise actually resolves for that id.
  const _runShow = window.runShow;
  window.runShow = function (id) {
    return _runShow(id).then(result => {
      const st = getState(id);
      if (st && st._pendingErrorRestore) {
        const { errorY, errorYLow, errorYHigh } = st._pendingErrorRestore;
        populateAxisAddonSelects(id);
        const f = (sid, val) => { const el = document.getElementById(`${sid}-${id}`); if (el && val) el.value = val; };
        f('error-y', errorY); f('error-y-low', errorYLow); f('error-y-high', errorYHigh);
        delete st._pendingErrorRestore;
      }
      return result;
    });
  };

  function populateAxisAddonSelects(id) {
    const st = getState(id);
    const opt = (val, label) => { const o = document.createElement('option'); o.value = val; o.textContent = label ?? val; return o; };
    [`error-y-${id}`, `error-y-low-${id}`, `error-y-high-${id}`].forEach(sid => {
      const sel = document.getElementById(sid); if (!sel) return;
      const prev = sel.value;
      sel.innerHTML = '';
      sel.appendChild(opt('', '— none —'));
      (st.columns || []).forEach(c => sel.appendChild(opt(c)));
      if ((st.columns || []).includes(prev)) sel.value = prev;
    });
    const pills = document.getElementById(`y2-pills-${id}`);
    if (pills) {
      pills.innerHTML = '';
      (st.columns || []).forEach(col => {
        const p = document.createElement('div');
        p.className = 'col-pill' + ((st.y2Cols || []).includes(col) ? ' selected' : '');
        p.textContent = col; p.dataset.col = col;
        p.addEventListener('click', () => {
          st.y2Cols = st.y2Cols || [];
          if (st.y2Cols.includes(col)) { st.y2Cols = st.y2Cols.filter(c => c !== col); p.classList.remove('selected'); }
          else { st.y2Cols.push(col); p.classList.add('selected'); }
        });
        pills.appendChild(p);
      });
    }
  }

  // updateAxisControls(id, columns) (app.js) is called every time a query
  // returns new columns; hook it so our selects/pills refresh alongside x/y/group.
  const _updateAxisControls = window.updateAxisControls;
  window.updateAxisControls = function (id, columns) {
    _updateAxisControls(id, columns);
    const st = getState(id);
    if (st) st.y2Cols = (st.y2Cols || []).filter(c => columns.includes(c));
    populateAxisAddonSelects(id);
  };

  // --- payload / config: merge our fields into config / saved config -------
  const _gather = window.gatherPlotPayload;
  window.gatherPlotPayload = function (id) {
    const payload = _gather(id);
    const st = getState(id);
    const g = sid => document.getElementById(`${sid}-${id}`);
    const numOrUndef = el => (el && el.value !== '' ? el.value : undefined);
    if (st) {
      st.xLines = readReferenceLines(id, 'x').filter(line => Number.isFinite(line.value));
      st.yLines = readReferenceLines(id, 'y').filter(line => Number.isFinite(line.value));
    }
    Object.assign(payload.config, {
      x_min: numOrUndef(g('x-min')), x_max: numOrUndef(g('x-max')),
      y_min: numOrUndef(g('y-min')), y_max: numOrUndef(g('y-max')),
      x_reversed: !!(g('x-reversed') && g('x-reversed').checked),
      y_reversed: !!(g('y-reversed') && g('y-reversed').checked),
      y2: st && st.y2Cols && st.y2Cols.length ? st.y2Cols : undefined,
      y2_label: numOrUndef(g('y2-label')), y2_min: numOrUndef(g('y2-min')), y2_max: numOrUndef(g('y2-max')),
      x_lines: st?.xLines || [], y_lines: st?.yLines || [],
      error_y: numOrUndef(g('error-y')), error_y_low: numOrUndef(g('error-y-low')), error_y_high: numOrUndef(g('error-y-high')),
      error_style: (document.querySelector(`input[name="error-style-${id}"]:checked`) || {}).value || 'bars',
    });
    return payload;
  };

  // getNodeConfig(id) (app.js) builds the JSON saved to plots.json / exported
  // config files; extend it the same way, pruned like the rest of that object
  // (empty values dropped) so saved configs stay small and diffable.
  const _getNodeConfig = window.getNodeConfig;
  window.getNodeConfig = function (id) {
    const cfg = _getNodeConfig(id);
    if (!cfg) return cfg;
    const st = getState(id);
    const g = sid => document.getElementById(`${sid}-${id}`);
    const v = el => (el && el.value !== '' ? el.value : undefined);
    const extra = {
      xMin: v(g('x-min')), xMax: v(g('x-max')), yMin: v(g('y-min')), yMax: v(g('y-max')),
      xReversed: (g('x-reversed') && g('x-reversed').checked) || undefined,
      yReversed: (g('y-reversed') && g('y-reversed').checked) || undefined,
      y2Cols: st && st.y2Cols && st.y2Cols.length ? st.y2Cols : undefined,
      y2Label: v(g('y2-label')), y2Min: v(g('y2-min')), y2Max: v(g('y2-max')),
      xLines: readReferenceLines(id, 'x').filter(line => Number.isFinite(line.value)),
      yLines: readReferenceLines(id, 'y').filter(line => Number.isFinite(line.value)),
      errorY: v(g('error-y')), errorYLow: v(g('error-y-low')), errorYHigh: v(g('error-y-high')),
      errorStyle: (document.querySelector(`input[name="error-style-${id}"]:checked`) || {}).value,
    };
    if (!extra.xLines.length) delete extra.xLines;
    if (!extra.yLines.length) delete extra.yLines;
    Object.entries(extra).forEach(([k, val]) => { if (val !== undefined) cfg[k] = val; });
    return cfg;
  };

  const LINE_STYLES = ['solid', 'dash', 'dot', 'dashdot'];
  const LINE_POSITIONS = ['start', 'center', 'end'];
  function defaultLine(axis) {
    return {value: '', color: axis === 'y' ? '#d62728' : '#1f77b4', linestyle: 'dash',
      width: 1.5, label: '', label_orientation: 'auto', label_position: 'end'};
  }
  function normalizeLines(lines, axis) {
    return (Array.isArray(lines) ? lines : []).map(line => {
      const base = defaultLine(axis);
      return line && typeof line === 'object' ? {...base, ...line} : {...base, value: line};
    });
  }
  function renderReferenceLines(id, axis) {
    const st = getState(id), host = document.getElementById(`reference-${axis}-lines-${id}`);
    if (!st || !host) return;
    const key = axis + 'Lines';
    st[key] = normalizeLines(st[key], axis);
    host.replaceChildren();
    st[key].forEach((line, index) => {
      const row = document.createElement('div');
      row.dataset.referenceLine = String(index);
      row.style.cssText = 'display:grid;grid-template-columns:1fr 30px 1fr;gap:3px;margin:4px 0;padding:4px;border:1px solid var(--border);border-radius:4px';
      const input = (field, placeholder, type='text') => {
        const el = document.createElement('input'); el.type = type; el.dataset.field = field;
        el.placeholder = placeholder; el.value = line[field] ?? ''; el.style.minWidth = '0';
        if (field === 'width') { el.min = '0.1'; el.step = '0.1'; }
        return el;
      };
      const coord = input('value', axis === 'y' ? 'Y value' : 'X value', 'number');
      const color = input('color', 'color', 'color');
      const width = input('width', 'width', 'number');
      const style = document.createElement('select'); style.dataset.field = 'linestyle';
      LINE_STYLES.forEach(value => style.add(new Option(value, value))); style.value = line.linestyle;
      const label = input('label', 'label');
      const orient = document.createElement('select'); orient.dataset.field = 'label_orientation';
      [['auto','auto'],['horizontal','horizontal'],['vertical','vertical'],['diagonal','diagonal']].forEach(([v,l]) => orient.add(new Option(l,v)));
      orient.value = line.label_orientation;
      const position = document.createElement('select'); position.dataset.field = 'label_position';
      LINE_POSITIONS.forEach(value => position.add(new Option(value, value))); position.value = line.label_position;
      const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = '×';
      remove.className = 'btn sm'; remove.title = 'Remove reference line';
      remove.addEventListener('click', () => {
        row.remove();
        st[key] = readReferenceLines(id, axis);
        renderReferenceLines(id, axis);
      });
      [[coord, 'Coordinate'], [color, 'Line color'], [style, 'Line style'], [width, 'Line width'],
        [label, 'Label'], [orient, 'Label orientation'], [position, 'Label position']].forEach(([el, title]) => {
        el.title = title; el.addEventListener('input', () => { st[key] = readReferenceLines(id, axis); });
        el.addEventListener('change', () => { st[key] = readReferenceLines(id, axis); });
      });
      row.append(coord, color, remove, style, width, label, orient, position);
      host.appendChild(row);
    });
  }
  function readReferenceLines(id, axis) {
    const host = document.getElementById(`reference-${axis}-lines-${id}`);
    if (!host) return [];
    return [...host.querySelectorAll('[data-reference-line]')].map(row => {
      const value = {};
      row.querySelectorAll('[data-field]').forEach(el => { value[el.dataset.field] = el.value; });
      value.value = value.value === '' ? '' : Number(value.value);
      value.width = Number(value.width) || 1.5;
      return value;
    });
  }

  // applyNodeConfig(id, cfg) (app.js) is the inverse of getNodeConfig, run on
  // import; restore our fields into both the state object and the DOM.
  const _applyNodeConfig = window.applyNodeConfig;
  window.applyNodeConfig = function (id, cfg) {
    _applyNodeConfig(id, cfg);
    const st = getState(id);
    if (!st || !cfg) return;
    ['xMin', 'xMax', 'yMin', 'yMax', 'y2Label', 'y2Min', 'y2Max', 'errorY', 'errorYLow', 'errorYHigh', 'errorStyle']
      .forEach(k => { st[k] = cfg[k] ?? st[k]; });
    st.xReversed = !!cfg.xReversed; st.yReversed = !!cfg.yReversed;
    st.y2Cols = cfg.y2Cols || [];
    st.xLines = normalizeLines(cfg.xLines, 'x');
    st.yLines = normalizeLines(cfg.yLines, 'y');
    const f = (sid, val) => { const el = document.getElementById(`${sid}-${id}`); if (el && val !== undefined) el.value = val; };
    f('x-min', cfg.xMin || ''); f('x-max', cfg.xMax || ''); f('y-min', cfg.yMin || ''); f('y-max', cfg.yMax || '');
    f('y2-label', cfg.y2Label || ''); f('y2-min', cfg.y2Min || ''); f('y2-max', cfg.y2Max || '');
    renderReferenceLines(id, 'x'); renderReferenceLines(id, 'y');
    const xr = document.getElementById(`x-reversed-${id}`); if (xr) xr.checked = !!cfg.xReversed;
    const yr = document.getElementById(`y-reversed-${id}`); if (yr) yr.checked = !!cfg.yReversed;
    const styleRadio = document.querySelector(`input[name="error-style-${id}"][value="${cfg.errorStyle || 'bars'}"]`);
    if (styleRadio) styleRadio.checked = true;
    // Column-dependent selects (error-y/-low/-high) can't be restored until
    // the columns exist, which only happens once runShow's query resolves —
    // see the runShow wrapper above, which applies this the moment it does.
    // If there's no SQL to run (so runShow never fires), populate immediately
    // from whatever columns the state already has.
    if (cfg.sql && cfg.sql.trim()) {
      st._pendingErrorRestore = { errorY: cfg.errorY, errorYLow: cfg.errorYLow, errorYHigh: cfg.errorYHigh };
    } else {
      populateAxisAddonSelects(id);
      f('error-y', cfg.errorY || ''); f('error-y-low', cfg.errorYLow || ''); f('error-y-high', cfg.errorYHigh || '');
    }
  };
})();
