import { useEffect, useState } from "react";

import NewsGuardPanel from "./NewsGuardPanel";

type Opp = {
  symbol: string;
  action: string;
  confidence: number;
  reason: string;
  risk_level: string;
};

type Insights = {
  opportunities: Opp[];
  warnings: string[];
  summary: string;
};

export default function AiInsightsPanel() {
  const [data, setData] = useState<Insights | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const r = await fetch("/api/trading/ai-insights");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const j = (await r.json()) as Insights;
        if (!cancelled) {
          setData(j);
          setErr(null);
        }
      } catch (e) {
        if (!cancelled) {
          setErr(e instanceof Error ? e.message : "load_failed");
        }
      }
    };
    void load();
    const t = window.setInterval(() => void load(), 10_000);
    return () => {
      cancelled = true;
      window.clearInterval(t);
    };
  }, []);

  return (
    <>
      <NewsGuardPanel />
      <div className="tl-card tl-ai-insights tl-holo">
        <div className="tl-card-h">AI Insights · advisory</div>
        {err ? <div className="tl-muted tl-small">Insights unavailable ({err}).</div> : null}
        {data?.summary ? <div className="tl-ai-ins-summary">{data.summary}</div> : null}
        {data?.warnings?.length ? (
          <div className="tl-ai-ins-warn">
            {data.warnings.map((w, i) => (
              <div key={`${w.slice(0, 30)}-${i}`} className="tl-ai-ins-warn-line">
                ⚠ {w}
              </div>
            ))}
          </div>
        ) : null}
        <div className="tl-ai-ins-list">
          {(data?.opportunities ?? []).slice(0, 8).map((o, i) => (
            <div key={`${o.symbol}-${i}`} className="tl-ai-ins-opp">
              <div className="tl-ai-ins-opp-head">
                <span className="tl-mono">{o.symbol}</span>
                <span className="tl-pill">{o.action}</span>
                <span className="tl-ai-ins-conf">{(o.confidence * 100).toFixed(0)}%</span>
                <span className="tl-ai-ins-risk">{o.risk_level}</span>
              </div>
              <div className="tl-muted tl-small">{o.reason}</div>
            </div>
          ))}
          {!data?.opportunities?.length && !err ? (
            <div className="tl-muted tl-small">Waiting for signal history…</div>
          ) : null}
        </div>
      </div>
    </>
  );
}
