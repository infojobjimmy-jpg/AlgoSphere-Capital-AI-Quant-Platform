type Pt = { equity_usd?: number };

export default function PortfolioCurve({ points }: { points: Pt[] }) {
  const pts = points.filter((p) => typeof p.equity_usd === "number").slice(-120);
  if (pts.length < 2) {
    return <div className="tl-muted tl-small">No portfolio_snapshots yet (curve fills as execution persists).</div>;
  }
  const vals = pts.map((p) => Number(p.equity_usd));
  const lo = Math.min(...vals);
  const hi = Math.max(...vals);
  const span = Math.max(hi - lo, 1e-6);
  const w = 280;
  const h = 72;
  const d = pts
    .map((p, i) => {
      const v = Number(p.equity_usd);
      const x = (i / (pts.length - 1)) * w;
      const y = h - ((v - lo) / span) * (h - 8) - 4;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <div className="tl-spark">
      <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="tl-spark-svg" aria-hidden>
        <path d={d} fill="none" stroke="#7af8d6" strokeWidth="1.8" opacity={0.95} />
      </svg>
      <div className="tl-spark-legend tl-small tl-muted">Equity (portfolio_snapshots · DB)</div>
    </div>
  );
}
