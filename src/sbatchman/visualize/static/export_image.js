// export_image.js — turning the live Plotly figure into a downloadable PNG
// or SVG that matches exactly what's on screen, including text rendered as
// real vector paths (so the SVG has no font dependency when opened
// elsewhere) and a fix for Plotly's own legend-width-clipping bug.
//
// Depends on two globals from non-module <script> tags loaded earlier in
// webapp.html: `opentype` (opentype.js, vendored) and
// `EMBEDDED_SERIF_FONT_TTF_B64` (font.js, the embedded font's base64). Both
// are referenced bare (not via `window.`) exactly like `Plotly` is
// elsewhere in this split — see tabs.js's resizePlot for the same pattern.
import { G } from './state.js';
import { clientLog } from './log.js';

// --- Embedded font loading (cached) --------------------------------------
let _cachedOpentypeFont = null;
let _fontLoadAttempted = false;
function getEmbeddedOpentypeFont() {
  if (_fontLoadAttempted) return _cachedOpentypeFont;
  _fontLoadAttempted = true;
  try {
    if (typeof EMBEDDED_SERIF_FONT_TTF_B64 !== 'string' || !EMBEDDED_SERIF_FONT_TTF_B64) return null;
    const binary = atob(EMBEDDED_SERIF_FONT_TTF_B64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    _cachedOpentypeFont = opentype.parse(bytes.buffer);
  } catch (e) {
    clientLog(`Embedded font could not be loaded; exporting text as SVG text instead (${e.message}).`, 'warn');
  }
  return _cachedOpentypeFont;
}

// --- Text -> real vector paths, so the exported SVG has no font dependency --
function convertTextToPaths(svgRoot) {
  const font = getEmbeddedOpentypeFont();
  // Keep Plotly's native SVG text if the optional font data is unavailable.
  if (!font) return;
  const texts = Array.from(svgRoot.querySelectorAll('text'));
  texts.forEach(textEl => {
    const content = textEl.textContent;
    if (!content) { textEl.remove(); return; }

    const style = textEl.getAttribute('style') || '';
    const sizeMatch = style.match(/font-size:\s*([\d.]+)px/);
    const fillMatch = style.match(/(?:^|;)\s*fill:\s*([^;]+)/);
    const fontSize = sizeMatch ? parseFloat(sizeMatch[1]) : parseFloat(textEl.getAttribute('font-size')) || 11;
    const fill = fillMatch ? fillMatch[1].trim() : (textEl.getAttribute('fill') || '#222');
    const x = parseFloat(textEl.getAttribute('x')) || 0;
    const y = parseFloat(textEl.getAttribute('y')) || 0;

    let path;
    try {
      path = font.getPath(content, x, y, fontSize, { kerning: true });
    } catch (e) {
      return; // leave this element as text (e.g. a character our subset lacks)
    }
    const d = path.toPathData(2);
    if (!d) return;

    const ns = 'http://www.w3.org/2000/svg';
    const pathEl = document.createElementNS(ns, 'path');
    pathEl.setAttribute('d', d);
    pathEl.setAttribute('fill', fill);
    if (textEl.getAttribute('class')) pathEl.setAttribute('class', textEl.getAttribute('class'));
    textEl.replaceWith(pathEl);
  });
}

// --- Multi-layer SVG combining (Plotly draws several stacked <svg> layers) --
function cloneSvgLayerWithUniqueIds(svg, layerIndex) {
  const clone = svg.cloneNode(true);
  // Rename every id in this layer to be layer-specific, then fix up every
  // attribute (and inline style) in the layer that pointed at the old id,
  // so references stay correct after the rename.
  const idMap = new Map();
  clone.querySelectorAll('[id]').forEach(el => {
    const newId = `L${layerIndex}-${el.id}`;
    idMap.set(el.id, newId);
    el.id = newId;
  });
  if (idMap.size === 0) return clone;
  const urlRefAttrs = ['clip-path', 'fill', 'stroke', 'mask', 'filter'];
  const hrefAttrs = ['href', 'xlink:href'];
  clone.querySelectorAll('*').forEach(el => {
    urlRefAttrs.forEach(attr => {
      const val = el.getAttribute(attr);
      const m = val && val.match(/^url\(#(.+)\)$/);
      if (m && idMap.has(m[1])) el.setAttribute(attr, `url(#${idMap.get(m[1])})`);
    });
    hrefAttrs.forEach(attr => {
      const val = el.getAttribute(attr);
      if (val && val.startsWith('#') && idMap.has(val.slice(1))) {
        el.setAttribute(attr, `#${idMap.get(val.slice(1))}`);
      }
    });
    const style = el.getAttribute('style');
    if (style && style.includes('url(#')) {
      const newStyle = style.replace(/url\(#([^)]+)\)/g, (m0, id) => idMap.has(id) ? `url(#${idMap.get(id)})` : m0);
      if (newStyle !== style) el.setAttribute('style', newStyle);
    }
  });
  return clone;
}

function stripLegendClipping(svgRoot) {
  svgRoot.querySelectorAll('g.legend').forEach(legend => {
    if (legend.hasAttribute('clip-path')) legend.removeAttribute('clip-path');
    legend.querySelectorAll('[clip-path]').forEach(el => el.removeAttribute('clip-path'));
  });
}

function buildCombinedSvgClone(plotDiv) {
  const svgs = plotDiv.querySelectorAll('svg.main-svg');
  if (!svgs.length) return null;
  const ns = 'http://www.w3.org/2000/svg';
  const first = svgs[0];
  const combined = document.createElementNS(ns, 'svg');
  combined.setAttribute('xmlns', ns);
  const width = first.getAttribute('width') || String(first.getBoundingClientRect().width);
  const height = first.getAttribute('height') || String(first.getBoundingClientRect().height);
  combined.setAttribute('width', width);
  combined.setAttribute('height', height);
  const viewBox = first.getAttribute('viewBox');
  if (viewBox) combined.setAttribute('viewBox', viewBox);
  svgs.forEach((svg, i) => {
    const layerClone = cloneSvgLayerWithUniqueIds(svg, i);
    while (layerClone.firstChild) combined.appendChild(layerClone.firstChild);
  });
  stripLegendClipping(combined);

  // Known gap, not fixed in this pass: the exported SVG always uses the one
  // embedded serif font below, regardless of what G_STYLE.font_family the
  // Style tab has set. A real fix needs multiple bundled fonts selected by
  // family, which is a feature in its own right — out of scope for this
  // split. Surfacing the mismatch instead of hiding it:
  if (window.G_STYLE?.font_family && !/times|serif/i.test(window.G_STYLE.font_family)) {
    clientLog(`Note: exported image uses the built-in serif font; your Style tab is set to "${window.G_STYLE.font_family}" (font-matched export isn't implemented yet).`, 'warn');
  }
  convertTextToPaths(combined);
  return combined;
}

// --- Plotly truncates an overflowing legend label with an ellipsis instead
// of growing its box; this re-measures the real text width from the live
// (on-screen) legend and widens the box in the clone to fit it exactly. ---
function widenLegendBoxToFitText(plotDiv, svgClone, legendPosition) {
  const liveLegend = plotDiv.querySelector('.legend');
  const cloneLegend = svgClone.querySelector('.legend');
  if (!liveLegend || !cloneLegend) return;

  let maxRight = 0;
  liveLegend.querySelectorAll('.traces').forEach(trace => {
    try {
      const bbox = trace.getBBox();
      maxRight = Math.max(maxRight, bbox.x + bbox.width);
    } catch (e) { /* an empty/degenerate group can throw; skip it */ }
  });
  if (maxRight <= 0) return;

  const PADDING = 20;
  const neededWidth = Math.ceil(maxRight + PADDING);

  const bgRect = cloneLegend.querySelector('rect.bg');
  const currentWidth = bgRect ? parseFloat(bgRect.getAttribute('width')) || 0 : 0;
  const delta = neededWidth - currentWidth;
  if (delta <= 0) return; // already wide enough, nothing to fix

  if (bgRect) bgRect.setAttribute('width', String(currentWidth + delta));
  cloneLegend.querySelectorAll('rect.legendtoggle').forEach(r => {
    const w = parseFloat(r.getAttribute('width')) || 0;
    r.setAttribute('width', String(w + delta));
  });

  const isCentered = legendPosition === 'top' || legendPosition === 'bottom';
  const transform = cloneLegend.getAttribute('transform') || '';
  const m = transform.match(/translate\(([-\d.]+)\s*,\s*([-\d.]+)\)/);
  let newX = m ? parseFloat(m[1]) : 0;
  const y = m ? m[2] : '0';
  if (isCentered) newX -= delta / 2;
  if (newX < 0) newX = 0;
  if (m) cloneLegend.setAttribute('transform', `translate(${newX},${y})`);

  const rootWidth = parseFloat(svgClone.getAttribute('width')) || 0;
  const boxRight = newX + currentWidth + delta;
  const shortfall = Math.ceil(boxRight + PADDING - rootWidth);
  if (shortfall > 0) svgClone.setAttribute('width', String(rootWidth + shortfall));
}

function svgCloneToPngBlob(svgEl, scale) {
  return new Promise((resolve, reject) => {
    const width = parseFloat(svgEl.getAttribute('width')) || 1600;
    const height = parseFloat(svgEl.getAttribute('height')) || 900;
    const svgStr = new XMLSerializer().serializeToString(svgEl);
    const svgUrl = URL.createObjectURL(new Blob([svgStr], { type: 'image/svg+xml;charset=utf-8' }));
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = Math.round(width * scale);
      canvas.height = Math.round(height * scale);
      const ctx = canvas.getContext('2d');
      ctx.fillStyle = '#ffffff'; // Plotly's own PNG export is opaque white, match it
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      URL.revokeObjectURL(svgUrl);
      canvas.toBlob(blob => blob ? resolve(blob) : reject(new Error('canvas.toBlob returned null')), 'image/png');
    };
    img.onerror = (err) => { URL.revokeObjectURL(svgUrl); reject(err); };
    img.src = svgUrl;
  });
}

export function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function activeFigureBlob(format) {
  const tab = window.activeTabObj();
  if (!tab?.state?.plotted) throw new Error('No plot rendered yet.');
  const plotDiv = document.getElementById(`plot-${G.activeTab}`);
  if (!plotDiv) throw new Error('Plot element not found.');
  if (tab.state.rendererBackend === 'matplotlib') {
    const encoded = tab.state.renderedImage?.[`image_${format}`];
    if (!encoded) throw new Error(`No ${format.toUpperCase()} image is available; render the figure again first.`);
    const binary = atob(encoded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new Blob([bytes], { type: format === 'svg' ? 'image/svg+xml' : 'image/png' });
  }
  const svgClone = buildCombinedSvgClone(plotDiv);
  if (!svgClone) throw new Error("Could not find the plot's SVG to export.");
  const legendPosition = document.getElementById(`legend-position-${G.activeTab}`)?.value || 'right';
  widenLegendBoxToFitText(plotDiv, svgClone, legendPosition);
  if (format === 'svg') {
    const svgStr = '<?xml version="1.0" standalone="no"?>\r\n' + new XMLSerializer().serializeToString(svgClone);
    return new Blob([svgStr], { type: 'image/svg+xml;charset=utf-8' });
  }
  return svgCloneToPngBlob(svgClone, 2); // 2x for a crisp PNG
}

export async function exportActiveFigure(selectedFormat) {
  const tab = window.activeTabObj();
  if (!tab) { clientLog('No active figure', 'warn'); return; }
  const format = selectedFormat || document.getElementById('export-format')?.value || 'png';
  const filename = `hpc-plot-${tab.label.replace(/\s+/g, '-')}`;
  try {
    const blob = await activeFigureBlob(format);
    downloadBlob(blob, `${filename}.${format}`);
    clientLog(`${format.toUpperCase()} saved for tab "${tab.label}"`);
  } catch (err) { clientLog(`Export failed: ${err.message || err}`, 'error'); }
}

async function copyActiveFigure() {
  const tab = window.activeTabObj();
  try {
    if (!navigator.clipboard?.write || typeof ClipboardItem === 'undefined') {
      throw new Error('Image clipboard access is unavailable. Use a secure browser context and try again.');
    }
    const png = await activeFigureBlob('png');
    await navigator.clipboard.write([new ClipboardItem({'image/png': png})]);
    clientLog(`PNG copied to clipboard for tab "${tab?.label || ''}"`);
  } catch (err) {
    clientLog(`Copy image failed: ${err.message || err}`, 'error');
  }
}

export function wireImageExport() {
  const button = document.getElementById('btn-export-png');
  button.addEventListener('click', event => {
    event.stopPropagation();
    const old = document.getElementById('image-export-menu');
    if (old) { old._close?.(); return; }
    const menu = document.createElement('div');
    menu.id = 'image-export-menu'; menu.setAttribute('role', 'menu');
    menu.style.cssText = 'position:fixed;z-index:1000;display:flex;flex-direction:column;gap:4px;padding:6px;background:var(--bg2);color:var(--text);border:1px solid var(--border2);border-radius:6px;box-shadow:0 8px 24px rgba(0,0,0,.35);min-width:190px';
    const rect = button.getBoundingClientRect();
    menu.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - 205))}px`;
    menu.style.top = `${rect.bottom + 4}px`;
    let dismiss;
    menu._close = () => {
      menu.remove();
      if (dismiss) document.removeEventListener('pointerdown', dismiss);
    };
    const add = (label, action) => {
      const choice = document.createElement('button');
      choice.type = 'button'; choice.className = 'btn'; choice.textContent = label;
      choice.style.cssText = 'justify-content:flex-start;width:100%;padding:7px 9px';
      choice.addEventListener('click', async () => { menu._close(); await action(); });
      menu.appendChild(choice);
    };
    const format = document.getElementById('export-format')?.value || 'png';
    add(`Download ${format.toUpperCase()}`, () => exportActiveFigure(format));
    add('Copy PNG to clipboard', copyActiveFigure);
    document.body.appendChild(menu);
    dismiss = e => {
      if (!menu.contains(e.target) && e.target !== button) {
        menu._close();
      }
    };
    setTimeout(() => document.addEventListener('pointerdown', dismiss), 0);
  });
}

Object.assign(window, { downloadBlob, exportActiveFigure, wireImageExport });
