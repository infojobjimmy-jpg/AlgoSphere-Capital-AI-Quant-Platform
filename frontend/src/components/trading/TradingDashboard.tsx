import { type ReactNode, useEffect, useMemo, useSyncExternalStore } from "react";
import { startTradingStream } from "../../services/tradingStream";
import { tradingStore } from "../../state/tradingStore";
import AiInsightsPanel from "./AiInsightsPanel";
import DraggableCard from "./DraggableCard";
import EquityMiniChart from "./EquityMiniChart";
import PortfolioCurve from "./PortfolioCurve";
import TradingAI from "./TradingAI";
import TradingInsights from "./TradingInsights";
import TradingRegime from "./TradingRegime";

function tickLine(ticks: unknown[]): string {
  return ticks
    .slice(0, 6)
    .map((t) => {
      const o = t as Record<string, unknown>;
      const sym = String(o.symbol ?? o.pair ?? "?");
      const p = o.price;
      const pr = typeof p === "number" ? p.toFixed(p < 2 ? 4 : 2) : String(p ?? "—");
      return `${sym} ${pr}`;
    })
    .join(" · ");
}

function venueBlob(blob: unknown): { ticks: unknown[]; hint?: string } {
  if (!blob || typeof blob !== "object") return { ticks: [] };
  const o = blob as Record<string, unknown>;
  const ticks = Array.isArray(o.ticks) ? o.ticks : [];
  const hint =
    o.hint != null
      ? String(o.hint)
      : o.error != null
        ? String(o.error)
        : o.note != null
          ? String(o.note)
          : o.configured === false
            ? "Not configured"
            : "";
  return { ticks, hint };
}

function VenueRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="tl-mh-venue">
      <span>{label}</span>
      <div className="tl-mono tl-small">{children}</div>
    </div>
  );
}

export default function TradingDashboard() {
  const snap = useSyncExternalStore(tradingStore.subscribe, tradingStore.getSnapshot);

  useEffect(() => {
    const stop = startTradingStream(2500);
    return () => stop();
  }, []);

  const r = snap.risk as Record<string, unknown> | null;
  const paper = (snap.portfolio?.paper as Record<string, unknown> | undefined) ?? {};
  const eq = String(r?.equity ?? paper.equity ?? "—");
  const cash = String(paper.cash ?? "—");
  const ks = String(snap.portfolio?.kill_switch ?? r?.kill_switch ?? "—");
  const hub = snap.marketHub;
  const crypto = (hub?.crypto as Record<string, unknown> | undefined) ?? {};
  const forex = (hub?.forex as Record<string, unknown> | undefined) ?? {};
  const fusionFx = (forex.fusion_markets as Record<string, unknown> | undefined) ?? {};
  const equities = (hub?.equities as Record<string, unknown> | undefined) ?? {};
  const broker = (hub?.broker as Record<string, unknown> | undefined) ?? {};

  const regimeLabel = String((r?.regime as Record<string, unknown> | undefined)?.regime ?? "—");

  const orderFlow = useMemo(() => {
    const rows = snap.equitySeries?.order_flow;
    return Array.isArray(rows) ? (rows as Array<{ cumulative_notional_usd?: number }>) : [];
  }, [snap.equitySeries]);

  const equityNowNum =
    typeof snap.equitySeries?.equity_now === "number"
      ? (snap.equitySeries.equity_now as number)
      : typeof r?.equity === "number"
        ? (r.equity as number)
        : null;

  const hm = snap.hubMetrics;
  const legPnl = (hm?.leg_pnl as Record<string, unknown> | undefined) ?? {};
  const legs = Array.isArray(legPnl.legs) ? (legPnl.legs as Record<string, unknown>[]) : [];
  const portfolioCurve = useMemo(() => {
    const c = hm?.portfolio_curve;
    return Array.isArray(c) ? (c as Array<{ equity_usd?: number }>) : [];
  }, [hm]);

  const symbolUniverse = useMemo(() => {
    const u = hm?.symbol_universe;
    return Array.isArray(u) ? (u as Record<string, unknown>[]) : [];
  }, [hm]);

  const pendingOrders = useMemo(() => {
    const pend = new Set(["open", "pending", "submitted", "new", "partial"]);
    return snap.orders.filter((o) => {
      const x = o as Record<string, unknown>;
      return pend.has(String(x.status ?? "").toLowerCase());
    });
  }, [snap.orders]);

  const tickPulseLine = useMemo(() => {
    const pulse = (hm?.market_tick_pulse as Record<string, unknown> | undefined)?.by_asset_class;
    if (!pulse || typeof pulse !== "object") return "—";
    const parts: string[] = [];
    for (const [ac, arr] of Object.entries(pulse)) {
      if (!Array.isArray(arr) || arr.length === 0) continue;
      const head = arr[0] as Record<string, unknown>;
      const sym = String(head.symbol ?? "?");
      const pr = typeof head.price === "number" ? head.price.toFixed(head.price < 2 ? 4 : 2) : String(head.price ?? "—");
      parts.push(`${ac}:${sym}@${pr}`);
    }
    return parts.length ? parts.join(" · ") : "—";
  }, [hm]);

  const setMode = async (m: "paper" | "live") => {
    try {
      await fetch(`/api/trading/mode?mode=${m}`, { method: "POST" });
      tradingStore.patch({ mode: m });
    } catch {
      /* ignore */
    }
  };

  return (
    <div className="tl-dash">
      <div className="tl-mh-ribbon">
        <div className="tl-mh-title">Market Hub · Trading Lab</div>
        <div className="tl-muted tl-small">
          Live venues + ledger (read-only UI) · hub {String(hub?.ts ?? "—")} · bridge{" "}
          {broker.any_bridge_url ? "configured" : "off"} · {String(broker.live_broker_setting ?? "—")}
        </div>
        <div className="tl-mh-panels">
          <div className="tl-mh-panel">
            <div className="tl-mh-panel-h">Crypto · Binance / Kraken / Coinbase</div>
            <VenueRow label="Binance">{tickLine(Array.isArray(crypto.binance) ? (crypto.binance as unknown[]) : [])}</VenueRow>
            <VenueRow label="Kraken">{tickLine(venueBlob(crypto.kraken).ticks) || venueBlob(crypto.kraken).hint || "—"}</VenueRow>
            <VenueRow label="Coinbase">{tickLine(venueBlob(crypto.coinbase).ticks) || "—"}</VenueRow>
          </div>
          <div className="tl-mh-panel">
            <div className="tl-mh-panel-h">Forex · Twelve / OANDA / Fusion</div>
            <VenueRow label="Twelve Data">{tickLine(Array.isArray(forex.twelve_data) ? (forex.twelve_data as unknown[]) : [])}</VenueRow>
            <VenueRow label="OANDA">{tickLine(venueBlob(forex.oanda).ticks) || venueBlob(forex.oanda).hint || "—"}</VenueRow>
            <VenueRow label="Fusion / MT5 / cTrader">
              positions={String(fusionFx.live_positions_count ?? "—")} · fields=
              {(fusionFx.live_account_fields as unknown[] | undefined)?.join(", ") || "—"}
            </VenueRow>
          </div>
          <div className="tl-mh-panel">
            <div className="tl-mh-panel-h">Equities · Finnhub / Alpha / Yahoo / broker</div>
            <VenueRow label="Finnhub">{tickLine(Array.isArray(equities.finnhub) ? (equities.finnhub as unknown[]) : [])}</VenueRow>
            <VenueRow label="Alpha Vantage">{tickLine(venueBlob(equities.alpha_vantage).ticks) || venueBlob(equities.alpha_vantage).hint || "—"}</VenueRow>
            <VenueRow label="Yahoo">{tickLine(venueBlob(equities.yahoo_finance).ticks) || venueBlob(equities.yahoo_finance).hint || "—"}</VenueRow>
            <VenueRow label="Live broker book">
              open legs (Redis)={String((broker.live_positions as unknown[] | undefined)?.length ?? 0)}
            </VenueRow>
          </div>
          <div className="tl-mh-panel">
            <div className="tl-mh-panel-h">Trading Lab · regime & flow</div>
            <VenueRow label="Regime">{regimeLabel}</VenueRow>
            <VenueRow label="Risk">{String((r?.risk_intel as Record<string, unknown> | undefined)?.risk_state ?? "—")}</VenueRow>
            <VenueRow label="Pending orders (DB)">{String(pendingOrders.length)}</VenueRow>
            <VenueRow label="Symbols in DB (48h)">{String(symbolUniverse.length)}</VenueRow>
          </div>
        </div>
        <div className="tl-mh-pulse">
          <div className="tl-mh-feed-h">Ingest pulse (market_ticks · DB)</div>
          <div className="tl-mh-feed-body tl-mono tl-small">{tickPulseLine}</div>
        </div>
        <div className="tl-mh-equity">
          <div className="tl-mh-feed-h">Ledger timeline (order notional)</div>
          <EquityMiniChart rows={orderFlow} equityNow={equityNowNum} />
          <div className="tl-mh-feed-h" style={{ marginTop: 12 }}>
            Equity history (portfolio_snapshots · DB)
          </div>
          <PortfolioCurve points={portfolioCurve} />
          <div className="tl-muted tl-small">{String(snap.equitySeries?.disclaimer ?? "")}</div>
        </div>
      </div>

      <div className="tl-col tl-left">
        <DraggableCard title="Portfolio" resizable>
          <div className="tl-stat">
            <label>Equity</label>
            <strong>{eq}</strong>
          </div>
          <div className="tl-stat">
            <label>Cash</label>
            <strong>{cash}</strong>
          </div>
          <div className="tl-stat">
            <label>Drawdown</label>
            <strong>{typeof r?.drawdown_pct === "number" ? `${r.drawdown_pct.toFixed(2)}%` : "—"}</strong>
          </div>
          <div className="tl-stat">
            <label>Kill switch</label>
            <strong>{ks}</strong>
          </div>
        </DraggableCard>
        <DraggableCard title="Execution mode" resizable>
          <div className="tl-mode">
            <button
              type="button"
              className={snap.mode === "paper" ? "tl-btn tl-btn-on" : "tl-btn"}
              onClick={() => void setMode("paper")}
            >
              PAPER
            </button>
            <button
              type="button"
              className={snap.mode === "live" ? "tl-btn tl-btn-on" : "tl-btn"}
              onClick={() => void setMode("live")}
            >
              LIVE
            </button>
          </div>
          <div className="tl-muted tl-small">Same API pipeline; execution mode only.</div>
        </DraggableCard>
      </div>

      <div className="tl-col tl-center">
        <DraggableCard title="Leg PnL · aggregate" resizable>
          <div className="tl-stat">
            <label>Aggregate unrealized (marks)</label>
            <strong>{legPnl.aggregate_pnl_usd != null ? String(legPnl.aggregate_pnl_usd) : "—"}</strong>
          </div>
          <div className="tl-stat">
            <label>Worst leg (fraction of notional)</label>
            <strong>
              {legPnl.worst_leg_symbol != null ? String(legPnl.worst_leg_symbol) : "—"} ·{" "}
              {legPnl.worst_leg_pnl_frac != null ? String(legPnl.worst_leg_pnl_frac) : "—"}
            </strong>
          </div>
          <div className="tl-muted tl-small">
            {String(legPnl.marks_source ?? "")}
            {legPnl.aggregate_partial ? " · partial (missing marks for some legs)" : ""}
          </div>
          <div className="tl-scroll" style={{ maxHeight: 140 }}>
            {legs.map((row, i) => (
              <div key={`${String(row.symbol)}-${String(row.entry)}`} className="tl-row tl-mono tl-small">
                {String(row.symbol)} qty={String(row.qty)} entry={String(row.entry)} mark=
                {row.mark != null ? String(row.mark) : "—"} pnl=
                {row.pnl_usd != null ? String(row.pnl_usd) : "—"}
              </div>
            ))}
            {legs.length === 0 ? <div className="tl-muted">No open ledger legs.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Signals (live)" resizable>
          <div className="tl-scroll">
            {snap.signals.slice(0, 12).map((s, i) => {
              const o = s as Record<string, unknown>;
              return (
                <div key={`${String(o.symbol)}-${String(o.side)}-${i}`} className="tl-row">
                  <span className="tl-pill">{String(o.side)}</span>
                  <span className="tl-mono tl-small">
                    {String(o.symbol)} · {Number(o.strength).toFixed(3)}
                  </span>
                </div>
              );
            })}
            {snap.signals.length === 0 ? <div className="tl-muted">No rows.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Positions (ledger)" resizable>
          <div className="tl-scroll">
            {snap.positions.slice(0, 8).map((p, i) => (
              <div key={`pos-${JSON.stringify(p).slice(0, 30)}`} className="tl-row tl-mono tl-small">
                {JSON.stringify(p).slice(0, 180)}
              </div>
            ))}
            {snap.positions.length === 0 ? <div className="tl-muted">Flat.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Pending orders (live DB)" resizable>
          <div className="tl-scroll" style={{ maxHeight: 120 }}>
            {pendingOrders.slice(0, 12).map((o, i) => {
              const x = o as Record<string, unknown>;
              return (
                <div key={`${String(x.time)}-${String(x.symbol)}-${String(x.side)}`} className="tl-row tl-mono tl-small">
                  {String(x.time)?.slice(11, 19)} {String(x.side)} {String(x.symbol)} {String(x.status)}
                </div>
              );
            })}
            {pendingOrders.length === 0 ? <div className="tl-muted">No pending rows.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Executed / all orders" resizable>
          <div className="tl-scroll">
            {snap.orders.slice(0, 12).map((o, i) => {
              const x = o as Record<string, unknown>;
              return (
                <div key={`ord-${String(x.time)}-${String(x.symbol)}-${String(x.side)}`} className="tl-row tl-mono tl-small">
                  {String(x.time)?.slice(11, 19)} {String(x.side)} {String(x.symbol)} {String(x.status)}
                </div>
              );
            })}
            {snap.orders.length === 0 ? <div className="tl-muted">No rows.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Live broker positions (Redis)" resizable>
          <div className="tl-scroll" style={{ maxHeight: 140 }}>
            {snap.livePositions.slice(0, 10).map((p, i) => (
              <div key={`lp-${JSON.stringify(p).slice(0, 30)}`} className="tl-row tl-mono tl-small">
                {JSON.stringify(p).slice(0, 200)}
              </div>
            ))}
            {snap.livePositions.length === 0 ? <div className="tl-muted">No live_positions (paper mode typical).</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Symbol universe (market_ticks)" resizable>
          <div className="tl-scroll" style={{ maxHeight: 160 }}>
            {symbolUniverse.slice(0, 40).map((row, i) => (
              <div key={`${String(row.symbol)}-${String(row.venue)}`} className="tl-row tl-mono tl-small">
                {String(row.symbol)} · {String(row.asset_class)} · {String(row.venue)} @ {String(row.price)}
              </div>
            ))}
            {symbolUniverse.length === 0 ? <div className="tl-muted">No ticks in DB window.</div> : null}
          </div>
        </DraggableCard>
        <DraggableCard title="Execution log" resizable>
          <div className="tl-scroll">
            {((snap.portfolio?.recent_orders as unknown[]) ?? []).slice(0, 12).map((o, i) => (
              <div key={`ro-${JSON.stringify(o).slice(0, 30)}`} className="tl-row tl-mono tl-small">
                {JSON.stringify(o).slice(0, 200)}
              </div>
            ))}
            {(((snap.portfolio?.recent_orders as unknown[]) ?? []).length ?? 0) === 0 ? (
              <div className="tl-muted">No recent_orders in portfolio payload.</div>
            ) : null}
          </div>
        </DraggableCard>
        <DraggableCard title="EXECUTION SAFE FINAL" resizable>
          <div className="tl-muted tl-small">
            Multi-line <code className="tl-mono">EXECUTION SAFE FINAL</code> blocks are emitted by the executor to the
            backend logger (stdout in Docker). This UI shows persisted orders above and portfolio snapshots in the
            equity strip—wire log shipping (Loki, CloudWatch, file tail) for verbatim log replay.
          </div>
        </DraggableCard>
      </div>

      <div className="tl-col tl-right">
        <AiInsightsPanel />
        <DraggableCard title="AI · Market Hub (advisory)" resizable className="tl-neon">
          <TradingAI embed />
        </DraggableCard>
        <TradingInsights snap={snap} />
        <div className="tl-card">
          <div className="tl-card-h">Risk state</div>
          <div className="tl-muted tl-small">
            {String((r?.risk_intel as Record<string, unknown> | undefined)?.risk_state ?? "—")}
          </div>
        </div>
        <TradingRegime snap={snap} />
        {snap.lastError ? <div className="tl-err">{snap.lastError}</div> : null}
      </div>
    </div>
  );
}
