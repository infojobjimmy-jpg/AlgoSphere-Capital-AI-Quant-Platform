import type { TradingSnapshot } from "../../state/tradingStore";

export default function TradingRegime({ snap }: { snap: TradingSnapshot }) {
  const r = snap.risk as Record<string, unknown> | null;
  const reg = (r?.regime as Record<string, unknown> | undefined) ?? {};
  const name = String(reg.regime ?? "—");
  const conf = typeof reg.confidence === "number" ? reg.confidence.toFixed(2) : String(reg.confidence ?? "—");
  return (
    <div className="tl-card tl-neon">
      <div className="tl-card-h">Regime</div>
      <div className="tl-regime-name">{name}</div>
      <div className="tl-muted">confidence {conf}</div>
    </div>
  );
}
