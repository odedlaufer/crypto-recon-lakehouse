// Renders the live numbers from data/*.json (written by scripts/export_site_data.py).
const COLORS = { raw: "#8b95ad", repaired: "#4fd1a5", official: "#f6b54a", grid: "#22305a", text: "#9aa6c2", note: "#e8ecf6" };
const PRODUCTS = { "BTC-USD": "#f6b54a", "ETH-USD": "#7aa2ff", "SOL-USD": "#4fd1a5" };
const FIRST_FULL_DAY = "2026-11-11"; // capture began 2026-11-10 12:53 UTC
const EVENTS = [
  { day: "2026-11-11", text: "capture still being made reliable", align: "left" },
  { day: "2026-11-19", text: "CH-2: producer killed" },
  { day: "2026-11-22", text: "10-hour outage", align: "right" },
  { day: "2026-11-23", text: "CH-1: 5% dropped", align: "right" },
];

const fmtCompact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const pct = (v, digits = 2) => (v == null ? "n/a" : `${v.toFixed(digits)}%`);

async function load(name) {
  const response = await fetch(`data/${name}.json`, { cache: "no-store" });
  if (!response.ok) throw new Error(`${name}: HTTP ${response.status}`);
  return response.json();
}

// Draws short notes under chosen points of one dataset (chart annotations).
const annotate = {
  id: "annotate",
  afterDatasetsDraw(chart, _args, options) {
    if (!options.items) return;
    const meta = chart.getDatasetMeta(options.dataset);
    const { ctx } = chart;
    ctx.save();
    ctx.fillStyle = COLORS.note;
    ctx.font = "12px Inter, system-ui, sans-serif";
    for (const item of options.items) {
      const index = chart.data.labels.indexOf(item.day);
      if (index < 0 || !meta.data[index]) continue;
      const point = meta.data[index];
      ctx.textAlign = item.align || "center";
      ctx.fillText(item.text, point.x, point.y + 20);
    }
    ctx.restore();
  },
};

function renderKpis(summary) {
  const slo = Object.fromEntries(summary.slos.map((s) => [s.key, s]));
  const totals = summary.totals;
  const values = {
    repaired: pct(slo.repaired_completeness_pct.value, 1),
    raw: pct(slo.raw_completeness_pct.value, 1),
    trades: fmtCompact.format(totals.stream_trades + totals.backfilled_trades),
    recovered: fmtCompact.format(totals.trades_recovered),
  };
  for (const [key, value] of Object.entries(values)) {
    document.querySelector(`[data-kpi="${key}"]`).textContent = value;
  }
  const generated = new Date(summary.generated_at);
  document.getElementById("generated").textContent =
    `data ${totals.first_day} to ${totals.last_day}, updated ${generated.toUTCString().replace(" GMT", " UTC")}`;
}

function renderSlos(summary) {
  const container = document.getElementById("slos");
  for (const s of summary.slos) {
    const digits = s.unit === "%" ? (s.value != null && s.value < 1 ? 3 : 2) : 1;
    const value = s.value == null ? "n/a" : `${s.value.toFixed(digits)} ${s.unit}`;
    const badge = s.met === null
      ? '<span class="badge info">tracked</span>'
      : s.met ? '<span class="badge met">met</span>' : '<span class="badge missed">missed</span>';
    const target = s.op ? `target ${s.op} ${s.target} ${s.unit}` : "tracked, no target";
    const card = document.createElement("div");
    card.className = "slo";
    card.innerHTML = `<div class="target">${s.label}</div><div class="value">${value}${badge}</div><div class="target">${target}</div>`;
    container.appendChild(card);
  }
}

function renderCompleteness(allRows) {
  const rows = allRows.filter((r) => r.day >= FIRST_FULL_DAY);
  const lowest = Math.min(...rows.map((r) => r.raw_pct));
  new Chart(document.getElementById("completenessChart"), {
    type: "line",
    data: {
      labels: rows.map((r) => r.day),
      datasets: [
        { label: "Repaired", data: rows.map((r) => r.repaired_pct), borderColor: COLORS.repaired, backgroundColor: "rgba(79,209,165,.12)", fill: true, tension: 0.2, borderWidth: 3 },
        { label: "Raw (live stream only)", data: rows.map((r) => r.raw_pct), borderColor: COLORS.raw, tension: 0.2, borderDash: [6, 4] },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: { y: { min: Math.max(0, Math.floor(lowest / 10) * 10 - 10), max: 100, ticks: { callback: (v) => `${v}%` } } },
      plugins: {
        annotate: { dataset: 1, items: EVENTS },
        tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y.toFixed(2)}%` } },
      },
    },
    plugins: [annotate],
  });
}

function renderBars(id, rows, field, label, color) {
  new Chart(document.getElementById(id), {
    type: "bar",
    data: { labels: rows.map((r) => r.day), datasets: [{ label, data: rows.map((r) => r[field]), backgroundColor: color, borderRadius: 4 }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: (v) => fmtCompact.format(v) } } } },
  });
}

function renderVolatility(rows) {
  const days = [...new Set(rows.map((r) => r.day))];
  const datasets = Object.entries(PRODUCTS).map(([product, color]) => ({
    label: product,
    data: days.map((d) => rows.find((r) => r.day === d && r.product_id === product)?.realized_vol_pct ?? null),
    borderColor: color,
    tension: 0.25,
  }));
  new Chart(document.getElementById("volChart"), {
    type: "line",
    data: { labels: days, datasets },
    options: { responsive: true, scales: { y: { ticks: { callback: (v) => `${v}%` } } } },
  });
}

async function showDemoIfPresent() {
  try {
    const response = await fetch("media/demo.mp4", { method: "HEAD" });
    if (response.ok) {
      document.getElementById("demo").hidden = false;
      document.getElementById("demoButton").hidden = false;
    }
  } catch {
    // no demo video published yet
  }
}

async function main() {
  mermaid.initialize({ startOnLoad: true, theme: "dark" });
  Chart.defaults.color = COLORS.text;
  Chart.defaults.borderColor = COLORS.grid;
  Chart.defaults.font.family = "Inter, system-ui, sans-serif";
  showDemoIfPresent();
  try {
    const [summary, completeness, gaps, market] = await Promise.all(
      ["summary", "completeness_daily", "gaps_daily", "market_daily"].map(load),
    );
    renderKpis(summary);
    renderSlos(summary);
    renderCompleteness(completeness);
    renderBars("gapsChart", gaps.filter((r) => r.day >= FIRST_FULL_DAY), "missing_trades", "Missing trades (all repaired)", COLORS.repaired);
    renderBars("officialChart", completeness.filter((r) => r.day >= FIRST_FULL_DAY), "official_mismatch_minutes", "Minutes", COLORS.official);
    renderVolatility(market.filter((r) => r.day >= FIRST_FULL_DAY));
  } catch (error) {
    document.getElementById("generated").textContent = `live data unavailable (${error.message})`;
  }
}

main();
