import type { TradingSnapshot } from "../../state/tradingStore";

export default function TradingInsights({ snap }: { snap: TradingSnapshot }) {
  const r = snap.risk as Record<string, unknown> | null;
  const ri = (r?.risk_intel as Record<string, unknown> | undefined) ?? {};
  const dd = typeof r?.drawdown_pct === "number" ? r.drawdown_pct.toFixed(2) : String(r?.drawdown_pct ?? "—");
  const last = (snap.orders[0] as Record<string, unknown> | undefined) ?? null;
  const lastS = (snap.signals[0] as Record<string, unknown> | undefined) ?? null;
  return (
    <div className="tl-card">
      <div className="tl-card-h">Insights</div>
      <div className="tl-insight-row">
        <span className="tl-tag">DD</span>
        <span>{dd}%</span>
      </div>
      <div className="tl-insight-row">
        <span className="tl-tag">risk</span>
        <span>{String(ri.risk_state ?? "—")}</span>
      </div>
      <div className="tl-muted">{String(ri.reason ?? "")}</div>
      <div className="tl-card-h" style={{ marginTop: 10 }}>
        Latest signal
      </div>
      <div className="tl-mono tl-small">
        {lastS
          ? `${String(lastS.side)} ${String(lastS.symbol)} · ${Number(lastS.strength).toFixed(3)}`
          : "—"}
      </div>
      <div className="tl-card-h" style={{ marginTop: 10 }}>
        Latest order
      </div>
      <div className="tl-mono tl-small">
        {last
          ? `${String(last.side)} ${String(last.symbol)} · ${String(last.status)}`
          : "—"}
      </div>
    </div>
  );
}
