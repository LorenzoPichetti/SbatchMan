// main.js — imports every module (so their window.* assignments exist),
// wires up every control that isn't wired by a tab/panel being created, and
// boots the app.
//
// IMPORTANT, read before wiring this into webapp.html: this module exports
// `boot()` instead of calling it automatically at the bottom, unlike the
// original app.js's trailing `init();`. Reason: boot() creates the first
// tab (or restores a saved plots.json workspace), and axis_addon.js/
// style_addon.js/grid_addon.js patch window.builderColumnsHTML/applyGrid/
// applyWorkspace/etc to inject their own fields into exactly that work. If
// boot() ran at the end of this module's own top-level code, it would run
// BEFORE the addon <script> tags (which come after this one in the HTML)
// have executed — so the very first tab, and every tab restored from a
// saved workspace, would silently be missing every addon field. This was
// already true of the pre-split app.js (its `init();` has the identical
// ordering problem against the exact same addon files), just never
// surfaced because nothing before now exercised it end-to-end.
//
// The fix belongs in webapp.html's script ordering, not in this file:
//   <script type="module" src="js/main.js"></script>
//   <script type="module" src="style_addon.js"></script>   <!-- type="module" added -->
//   <script type="module" src="axis_addon.js"></script>    <!-- so these defer and -->
//   <script type="module" src="grid_addon.js"></script>    <!-- execute in document -->
//   <script type="module">                                  <!-- order, same as -->
//     import { boot } from './js/main.js'; boot();           <!-- main.js itself -->
//   </script>
// Module scripts execute in relative document order (the same deferred-
// execution guarantee classic `defer` scripts have), so by the time that
// last inline script runs, every addon above it has already patched
// window.*. The addon files need no code changes for this — they're plain
// IIFEs with no import/export, so `type="module"` changes only their
// scheduling, not their behavior.
import { G } from './state.js';
import { api, clientLog, fetchServerLogs, wireLogPanel } from './log.js';
import './schema.js';
import './builder.js';
import './tabs.js';
import './grid.js';
import './plotting.js';
import './config.js';
import './export_image.js';
import './actions.js';

function wireAll() {
  wireLogPanel();
  window.wireTabBar();
  window.wireRunControls();
  window.wireConfigControls();
  window.wireImageExport();
  window.wireActionsSidebar();
}

export async function init() {
  const [dbRes, ptRes, rsRes, wsRes] = await Promise.all([
    window.api('GET', '/api/databases'),
    window.api('GET', '/api/plot_types'),
    window.api('GET', '/api/remote_systems'),
    window.api('GET', '/api/initial_workspace'),
  ]);
  G.databases = dbRes.databases || {};
  G.singleDb = !!dbRes.single_db;
  G.defaultDb = dbRes.default_database || '';
  G.plotTypes = ptRes.plot_types || {};
  G.remoteSystems = rsRes.systems || {};

  window.renderSchemaTree();
  window.populateReparseSel();
  window.populateFetchSystems();

  if (wsRes.workspace?.tabs?.length) {
    // window.applyWorkspace: style_addon.js wraps it to restore the saved
    // global style — this is exactly the call the header comment above is
    // about getting the timing right for.
    window.applyWorkspace(wsRes.workspace, 'auto-loaded from plots.json');
  } else {
    window.createTab('Plot 1');
  }

  await fetchServerLogs();
  clientLog('Application ready');
}

export async function boot() {
  wireAll();
  await init();
}
