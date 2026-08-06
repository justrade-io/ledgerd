package com.adbe.risk.http;

/**
 * Static, self-contained dashboard for the risk service (ADR 0012). Plain HTML +
 * JavaScript with no external dependencies: it polls {@code /risk/scores} and
 * {@code /risk/graph} and renders a score heatmap table and an SVG money-flow
 * graph. Served verbatim from {@code GET /}.
 */
final class DashboardHtml {

    static final String HTML =
            """
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>ADBE Risk Substrate</title>
            <style>
              body { font-family: system-ui, sans-serif; margin: 0; background: #0f1419; color: #e6e6e6; }
              header { padding: 12px 20px; background: #1b2430; border-bottom: 1px solid #2b3542; }
              header h1 { margin: 0; font-size: 18px; }
              header .meta { font-size: 12px; color: #8aa0b4; margin-top: 4px; }
              main { display: flex; flex-wrap: wrap; gap: 20px; padding: 20px; }
              section { background: #161c24; border: 1px solid #2b3542; border-radius: 8px; padding: 16px; }
              section h2 { margin: 0 0 12px; font-size: 14px; color: #8aa0b4; text-transform: uppercase; }
              table { border-collapse: collapse; width: 100%; font-size: 13px; }
              th, td { text-align: right; padding: 6px 10px; border-bottom: 1px solid #232c38; }
              th { color: #8aa0b4; font-weight: 600; }
              td.acct { text-align: left; font-variant-numeric: tabular-nums; }
              .flag { color: #ff6b6b; font-weight: 700; }
              svg { background: #0f1419; border-radius: 6px; }
              .node { fill: #4c9aff; }
              .node.hot { fill: #ff6b6b; }
              .edge { stroke: #3a4a5c; stroke-width: 1; }
              .label { fill: #8aa0b4; font-size: 10px; }
            </style>
            </head>
            <body>
            <header>
              <h1>ADBE Risk Substrate</h1>
              <div class="meta" id="meta">connecting...</div>
            </header>
            <main>
              <section style="flex: 1 1 380px;">
                <h2>Account risk scores</h2>
                <table>
                  <thead><tr><th class="acct">account</th><th>score</th><th>z-score</th>
                  <th>centrality</th><th>tx</th></tr></thead>
                  <tbody id="scores"></tbody>
                </table>
              </section>
              <section style="flex: 2 1 520px;">
                <h2>Money-flow graph</h2>
                <svg id="graph" width="560" height="440" viewBox="0 0 560 440"></svg>
              </section>
            </main>
            <script>
            const SVG_NS = "http://www.w3.org/2000/svg";
            function heat(score) {
              const r = Math.round(76 + score * 179);
              const g = Math.round(154 - score * 87);
              const b = Math.round(255 - score * 187);
              return `rgb(${r},${g},${b})`;
            }
            async function refreshScores() {
              const res = await fetch("risk/scores");
              const data = await res.json();
              const body = document.getElementById("scores");
              body.innerHTML = "";
              for (const s of data.scores) {
                const tr = document.createElement("tr");
                const flag = s.flagged ? " <span class='flag'>&#9650;</span>" : "";
                tr.innerHTML = `<td class='acct'>${s.account}${flag}</td>`
                  + `<td style='color:${heat(s.score)}'>${s.score.toFixed(3)}</td>`
                  + `<td>${s.zScore.toFixed(2)}</td>`
                  + `<td>${s.centrality.toFixed(0)}</td>`
                  + `<td>${s.txCount}</td>`;
                body.appendChild(tr);
              }
            }
            async function refreshGraph() {
              const res = await fetch("risk/graph");
              const data = await res.json();
              const svg = document.getElementById("graph");
              svg.innerHTML = "";
              const nodes = data.nodes;
              const n = nodes.length;
              const cx = 280, cy = 220, radius = 170;
              const pos = {};
              nodes.forEach((node, i) => {
                const a = (2 * Math.PI * i) / Math.max(1, n);
                pos[node.account] = { x: cx + radius * Math.cos(a), y: cy + radius * Math.sin(a), c: node.centrality };
              });
              for (const e of data.edges) {
                const from = pos[e.from], to = pos[e.to];
                if (!from || !to) continue;
                const line = document.createElementNS(SVG_NS, "line");
                line.setAttribute("class", "edge");
                line.setAttribute("x1", from.x); line.setAttribute("y1", from.y);
                line.setAttribute("x2", to.x); line.setAttribute("y2", to.y);
                svg.appendChild(line);
              }
              const maxC = nodes.reduce((m, x) => Math.max(m, x.centrality), 1);
              for (const node of nodes) {
                const p = pos[node.account];
                const circle = document.createElementNS(SVG_NS, "circle");
                circle.setAttribute("class", node.centrality > maxC * 0.6 ? "node hot" : "node");
                circle.setAttribute("cx", p.x); circle.setAttribute("cy", p.y);
                circle.setAttribute("r", 5 + 12 * (node.centrality / maxC));
                svg.appendChild(circle);
                const label = document.createElementNS(SVG_NS, "text");
                label.setAttribute("class", "label");
                label.setAttribute("x", p.x + 8); label.setAttribute("y", p.y + 3);
                label.textContent = node.account;
                svg.appendChild(label);
              }
            }
            async function refreshMeta() {
              const res = await fetch("metrics");
              const m = await res.json();
              document.getElementById("meta").textContent =
                `events ${m.eventsProcessed} | transfers ${m.transfers} | accounts ${m.scoredAccounts}`
                + ` | follower ${m.healthy ? "healthy" : "degraded"} | failovers ${m.failovers}`;
            }
            async function tick() {
              try { await Promise.all([refreshScores(), refreshGraph(), refreshMeta()]); }
              catch (e) { document.getElementById("meta").textContent = "error: " + e; }
            }
            tick();
            setInterval(tick, 2000);
            </script>
            </body>
            </html>
            """;

    private DashboardHtml() {}
}
