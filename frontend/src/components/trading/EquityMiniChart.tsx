import { useRef } from "react";

type FlowRow = { cumulative_notional_usd?: number };

export default function EquityMiniChart({ rows, equityNow }: { rows: FlowRow[]; equityNow: number | null }) {
  const uid = useRef<string | null>(null);
  if (uid.current === null) uid.current = Math.random().toString(36).slice(2, 7);
  const gradId = `tlGrad-${uid.current}`;
  const pts = rows.slice(-96);
  if (pts.length < 2) {
    return (
      <div className="tl-muted tl-small">
        {equityNow != null ? `Equity now: ${equityNow.toFixed(2)} — add more orders for a cumulative timeline.` : "No order timeline yet."}
      </div>
    );
  }
  const vals = pts.map((p) => Number(p.cumulative_notional_usd ?? 0));
  const lo = Math.min(...vals);
  const hi = Math.max(...vals);
  const span = Math.max(hi - lo, 1e-6);
  const w = 280;
  const h = 72;
  const d = pts
    .map((p, i) => {
      const v = Number(p.cumulative_notional_usd ?? 0);
      const x = (i / (pts.length - 1)) * w;
      const y = h - ((v - lo) / span) * (h - 8) - 4;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <div className="tl-spark">
      <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="tl-spark-svg" aria-hidden>
        <path d={d} fill="none" stroke={`url(#${gradId})`} strokeWidth="1.6" />
        <defs>
          <linearGradient id={gradId} x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="#4fd1ff" />
            <stop offset="100%" stopColor="#c084fc" />
          </linearGradient>
        </defs>
      </svg>
      <div className="tl-spark-legend tl-small tl-muted">
        Cumulative notional (ledger orders) · peak {hi.toFixed(0)}
        {equityNow != null ? ` · equity ${equityNow.toFixed(2)}` : ""}
      </div>
    </div>
  );
}
