import { useCallback, useState } from "react";
import { tradingStore } from "../../state/tradingStore";

type AiResp = {
  summary?: string;
  risk?: string;
  market?: string;
  action?: string;
};

export default function TradingAI({ embed }: { embed?: boolean }) {
  const [msg, setMsg] = useState("Explain current trading state and risks.");
  const [typing, setTyping] = useState("");
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState<AiResp | null>(null);

  const ask = useCallback(async () => {
    setBusy(true);
    setTyping("");
    const snap = tradingStore.getSnapshot();
    try {
      const r = await fetch("/api/trading/ai", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: msg,
          context: {
            positions: snap.positions,
            signals: snap.signals.slice(0, 20),
            risk: snap.risk,
            ticks: snap.ticks,
          },
        }),
      });
      const j = (await r.json()) as AiResp;
      setLast(j);
      const full = [j.summary, j.risk, j.market, j.action].filter(Boolean).join("\n\n");
      setTyping("");
      for (let i = 0; i <= full.length; i++) {
        setTyping(full.slice(0, i));
        // eslint-disable-next-line no-await-in-loop
        await new Promise((q) => window.setTimeout(q, 6));
      }
    } catch {
      setTyping("Unable to reach /api/trading/ai.");
    } finally {
      setBusy(false);
    }
  }, [msg]);

  const shell = embed ? "tl-ai tl-ai-embed tl-holo" : "tl-card tl-ai tl-holo";
  return (
    <div className={shell}>
      {!embed ? (
        <div className="tl-card-h">
          <span className="tl-robot">🤖</span> AI Assistant
        </div>
      ) : null}
      <textarea className="tl-input" value={msg} onChange={(e) => setMsg(e.target.value)} rows={3} />
      <button type="button" className="tl-btn" disabled={busy} onClick={() => void ask()}>
        {busy ? "Thinking…" : "Ask (advisory)"}
      </button>
      <pre className="tl-ai-out">
        {typing || last?.summary || "Ask a question (advisory only, uses live API data)."}
      </pre>
    </div>
  );
}
