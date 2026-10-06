// log.js — the fetch wrapper every module uses to talk to the backend, plus
// the log panel (both the in-page one and mirroring entries to the server).
import { G, escHtml } from './state.js';

export async function api(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  return r.json();
}

export function addLogEntry(e) {
  const body = document.getElementById('log-body');
  const div = document.createElement('div');
  div.className = `log-entry ${e.level}`;
  div.innerHTML = `<span class="log-ts">${e.ts}</span><span class="log-msg">${escHtml(e.msg)}</span>`;
  body.appendChild(div);
  body.scrollTop = body.scrollHeight;
  G.logCount++;
  const badge = document.getElementById('log-badge');
  const panel = document.getElementById('log-panel');
  if (!panel.classList.contains('open')) {
    badge.textContent = G.logCount;
    badge.style.display = 'block';
  }
}

export function addLogEntries(entries) {
  (entries || []).forEach(e => addLogEntry(e));
}

export function clientLog(msg, level = 'info') {
  addLogEntry({ ts: new Date().toLocaleTimeString('en', { hour12: false }), level, msg });
  api('POST', '/api/log', { message: msg, level });
}

export async function fetchServerLogs() {
  const res = await api('GET', '/api/logs');
  (res.logs || []).forEach(e => addLogEntry(e));
}

export function wireLogPanel() {
  document.getElementById('btn-log-toggle').addEventListener('click', () => {
    const p = document.getElementById('log-panel');
    p.classList.toggle('open');
    if (p.classList.contains('open')) {
      G.logCount = 0;
      document.getElementById('log-badge').style.display = 'none';
    }
  });
  document.getElementById('log-close').addEventListener('click', () =>
    document.getElementById('log-panel').classList.remove('open'));
  document.getElementById('log-clear').addEventListener('click', () => {
    document.getElementById('log-body').innerHTML = '';
    G.logCount = 0;
    document.getElementById('log-badge').style.display = 'none';
  });
}

// Transitional, see state.js's note.
Object.assign(window, { api, addLogEntry, addLogEntries, clientLog, fetchServerLogs, wireLogPanel });
