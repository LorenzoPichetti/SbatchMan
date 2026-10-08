// schema.js — the Databases/Tables tree in the sidebar, and the "which
// builder is currently visible" helper the tree's click handler uses to
// know which x/sql fields to fill in when a table is clicked.
import { G, getState, tabEl } from './state.js';
import { clientLog } from './log.js';

async function copySchemaName(name, kind) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(name);
    } else {
      const input = document.createElement('textarea');
      input.value = name;
      input.style.position = 'fixed';
      input.style.opacity = '0';
      document.body.appendChild(input);
      input.select();
      const copied = document.execCommand('copy');
      input.remove();
      if (!copied) throw new Error('Clipboard access is unavailable');
    }
    clientLog(`Copied ${kind} name: ${name}`);
  } catch (error) {
    clientLog(`Could not copy ${kind} name: ${error.message}`, 'warn');
  }
}

function makeCopyableName(name, kind) {
  const el = document.createElement('span');
  el.className = 'schema-copy-name';
  el.textContent = name;
  el.title = `Click to copy ${kind} name`;
  el.setAttribute('role', 'button');
  el.setAttribute('tabindex', '0');
  el.addEventListener('click', event => {
    event.stopPropagation();
    copySchemaName(name, kind);
  });
  el.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      event.stopPropagation();
      copySchemaName(name, kind);
    }
  });
  return el;
}

export function renderTableList(container, dbName, tables) {
  for (const [tname, cols] of Object.entries(tables || {})) {
    const ti = document.createElement('div');
    ti.className = 'table-item';
    ti.innerHTML = '<svg width="9" height="9" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="1" y="1" width="14" height="14" rx="1"/><line x1="1" y1="5.5" x2="15" y2="5.5"/><line x1="6" y1="5.5" x2="6" y2="15"/></svg>';
    const expandIcon = document.createElement('span');
    expandIcon.className = 'table-expand-icon';
    expandIcon.textContent = '▾';
    ti.append(expandIcon);
    ti.append(makeCopyableName(tname, 'table'));
    const badge = document.createElement('span');
    badge.className = 'badge';
    badge.textContent = cols.length;
    ti.append(badge);
    let colsOpen = false;
    const colsDiv = document.createElement('div');
    colsDiv.className = 'table-cols';
    colsDiv.style.display = 'none';
    cols.forEach(c => {
      const ci = document.createElement('div');
      ci.className = 'col-item';
      const marker = document.createElement('span');
      marker.textContent = '▸';
      ci.append(marker, makeCopyableName(c.name, 'column'));
      const type = document.createElement('span');
      type.className = 'col-type';
      type.textContent = c.type || '';
      ci.append(type);
      colsDiv.appendChild(ci);
    });
    ti.addEventListener('click', e => {
      e.stopPropagation();
      colsOpen = !colsOpen;
      colsDiv.style.display = colsOpen ? 'block' : 'none';
      expandIcon.classList.toggle('expanded', colsOpen);
      ti.setAttribute('aria-expanded', String(colsOpen));
      const id = activeBuilderId();
      if (id) {
        const sel = tabEl(id, 'db-select');
        if (sel) sel.value = dbName;
        const st = getState(id);
        if (st) st.database = dbName;
        const sql = tabEl(id, 'sql-input');
        if (sql && !sql.value.trim()) sql.value = `SELECT * FROM ${tname}`;
      }
    });
    container.appendChild(ti);
    container.appendChild(colsDiv);
  }
}

export function renderSchemaTree() {
  const el = document.getElementById('schema-tree');
  const hdr = document.getElementById('schema-hdr-label');
  const dbs = G.databases;
  const names = Object.keys(dbs);
  document.getElementById('db-count').textContent = names.length;
  if (!names.length) { el.innerHTML = '<span style="color:var(--text3)">No databases loaded</span>'; return; }
  el.innerHTML = '';

  if (G.singleDb) {
    hdr.textContent = 'Tables';
    const flat = document.createElement('div');
    flat.className = 'table-list flat';
    renderTableList(flat, names[0], dbs[names[0]]);
    el.appendChild(flat);
    return;
  }

  hdr.textContent = 'Databases';
  for (const [dbName, tables] of Object.entries(dbs)) {
    const dbDiv = document.createElement('div');
    dbDiv.className = 'db-item';
    const nameEl = document.createElement('div');
    nameEl.className = 'db-name';
    nameEl.innerHTML = `<svg width="9" height="9" viewBox="0 0 16 16" fill="currentColor"><ellipse cx="8" cy="4" rx="6" ry="2.5"/><path d="M2 4v4c0 1.38 2.69 2.5 6 2.5S14 9.38 14 8V4"/><path d="M2 8v4c0 1.38 2.69 2.5 6 2.5S14 13.38 14 12V8"/></svg> ${dbName}`;
    let open = false;
    const tableList = document.createElement('div');
    tableList.className = 'table-list';
    tableList.style.display = 'none';
    renderTableList(tableList, dbName, tables);
    nameEl.addEventListener('click', () => { open = !open; tableList.style.display = open ? 'block' : 'none'; });
    dbDiv.appendChild(nameEl); dbDiv.appendChild(tableList);
    el.appendChild(dbDiv);
  }
}

// activeTabObj() lives in tabs.js, which (per the chunk plan) lands later
// than this module. Resolved through window.* for now, like the addons do
// — this is the one spot in this chunk with a genuine forward dependency;
// swap it for `import { activeTabObj } from './tabs.js'` once that chunk
// is in and drop the window fallback.
export function activeBuilderId() {
  const tab = window.activeTabObj ? window.activeTabObj() : null;
  if (!tab) return null;
  if (tab.state.mode === 'grid') return tab.state.grid?.activePanel || null;
  if (tab.state.mode === 'facet') return tab.state.grid?.panelIds?.[0] || null;
  return tab.id;
}

Object.assign(window, { renderTableList, renderSchemaTree, activeBuilderId });
