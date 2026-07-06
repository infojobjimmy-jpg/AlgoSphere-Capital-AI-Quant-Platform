import { useCallback, useState } from "react";

type Props = {
  /** Globe / fusion snapshot for advisory context only (no execution). */
  snapshot: Record<string, unknown> | null;
};

export default function GlobeAdvisoryPanel({ snapshot }: Props) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [out, setOut] = useState("");

  const ask = useCallback(async () => {
    setBusy(true);
    setOut("");
    try {
      const layers = (snapshot?.layers as Record<string, unknown[]> | undefined | null) ?? {};
      const meta = (snapshot?.meta as Record<string, unknown> | undefined) ?? {};
      const ctx = {
        globe: {
          aircraft: (layers.aircraft ?? []).length,
          satellites: (layers.satellites ?? []).length,
          ships: (layers.ships ?? []).length,
          cameras: (layers.cameras ?? []).length,
          weather: (layers.weather ?? []).length,
          crypto_ticks: (layers.market_crypto ?? []).length,
          forex_ticks: (layers.market_forex ?? []).length,
          equity_ticks: (layers.market_equities ?? []).length,
          updated_at: meta.updated_at,
        },
        alerts: (snapshot?.alerts as unknown[] | undefined)?.slice(0, 6) ?? [],
      };
      const r = await fetch("/api/trading/ai", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message:
            "Brief advisory: given this Globe layer snapshot, what should the operator watch next? No trade instructions.",
          context: ctx,
        }),
      });
      const j = (await r.json()) as { summary?: string; risk?: string; market?: string; action?: string };
      setOut([j.summary, j.risk, j.market, j.action].filter(Boolean).join("\n\n") || "No advisory text.");
    } catch {
      setOut("Advisory service unreachable.");
    } finally {
      setBusy(false);
    }
  }, [snapshot]);

  return (
    <div className="gaios-globe-ai">
      <button type="button" className="gaios-globe-ai-toggle" onClick={() => setOpen((v) => !v)}>
        {open ? "Close Globe AI" : "Globe AI"}
      </button>
      {open ? (
        <div className="gaios-globe-ai-panel tl-holo">
          <div className="tl-muted tl-small" style={{ marginBottom: 8 }}>
            Contextual to Globe layers only. Advisory — not execution.
          </div>
          <button type="button" className="tl-btn" disabled={busy} onClick={() => void ask()}>
            {busy ? "…" : "Refresh insight"}
          </button>
          <pre className="tl-ai-out" style={{ marginTop: 8, minHeight: 80 }}>
            {out || "Tap refresh for a live summary from /api/trading/ai."}
          </pre>
        </div>
      ) : null}
    </div>
  );
}
