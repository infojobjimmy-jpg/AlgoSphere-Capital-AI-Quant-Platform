import { useCallback, useEffect, useRef, useState } from "react";
import { tradingStore } from "../../state/tradingStore";

type ChatLine = { role: "user" | "assistant"; text: string };

function wsMarketHubUrl(): string {
  const env = import.meta.env.VITE_MARKET_HUB_WS as string | undefined;
  if (env) return env;
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws/market-hub`;
}

export default function FloatingAiRobot() {
  const [open, setOpen] = useState(false);
  const [lines, setLines] = useState<ChatLine[]>([
    { role: "assistant", text: "Tap the core to open chat. I use Market Hub WS + REST (advisory only)." },
  ]);
  const [input, setInput] = useState("Summarize risk and tape in one paragraph.");
  const [busy, setBusy] = useState(false);
  const [hubLine, setHubLine] = useState("Market Hub: connecting…");
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const unsub = tradingStore.subscribe(() => {
      const s = tradingStore.getSnapshot();
      const ts = String(s.marketHub?.ts ?? "—");
      const sigN = s.signals.length;
      setHubLine(`Hub ts=${ts} · signals=${sigN} · mode=${s.mode}`);
    });
    return unsub;
  }, []);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let dead = false;
    const connect = () => {
      try {
        ws = new WebSocket(wsMarketHubUrl());
      } catch {
        return;
      }
      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(String(ev.data)) as Record<string, unknown>;
          if (data.type !== "market_hub") return;
          if (dead) return;
          const sigs = (data.signals as unknown[])?.length ?? 0;
          const mh = data.marketHub as Record<string, unknown> | undefined;
          const ts = mh && mh.ts != null ? String(mh.ts) : "tick";
          setHubLine(`WS market-hub · ${ts} · signals=${sigs}`);
        } catch {
          /* ignore */
        }
      };
      ws.onclose = () => {
        if (dead) return;
        window.setTimeout(connect, 4000);
      };
    };
    connect();
    return () => {
      dead = true;
      ws?.close();
    };
  }, []);

  useEffect(() => {
    if (!open) return;
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [lines, open]);

  const send = useCallback(async () => {
    const text = input.trim();
    if (!text || busy) return;
    setBusy(true);
    setInput("");
    setLines((prev) => [...prev, { role: "user", text }, { role: "assistant", text: "" }]);
    const snap = tradingStore.getSnapshot();
    try {
      const r = await fetch("/api/trading/ai", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: text,
          context: {
            positions: snap.positions,
            signals: snap.signals.slice(0, 24),
            risk: snap.risk,
            ticks: snap.ticks,
            marketHub: snap.marketHub,
            hubMetrics: snap.hubMetrics,
          },
        }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = (await r.json()) as Record<string, string | undefined>;
      const full = [j.summary, j.risk, j.market, j.action].filter(Boolean).join("\n\n") || "No advisory text returned.";
      for (let i = 0; i <= full.length; i += 1) {
        const acc = full.slice(0, i);
        setLines((prev) => {
          const next = [...prev];
          next[next.length - 1] = { role: "assistant", text: acc };
          return next;
        });
        // eslint-disable-next-line no-await-in-loop
        await new Promise((q) => window.setTimeout(q, 3));
      }
    } catch {
      setLines((prev) => {
        const next = [...prev];
        if (next[next.length - 1]?.role === "assistant" && next[next.length - 1]?.text === "") {
          next[next.length - 1] = { role: "assistant", text: "Request failed (network or API)." };
        } else next.push({ role: "assistant", text: "Request failed (network or API)." });
        return next;
      });
    } finally {
      setBusy(false);
    }
  }, [busy, input]);

  return (
    <div className={`gaios-float-ai ${open ? "is-open" : ""}`}>
      <button
        type="button"
        className="gaios-float-ai-orb"
        aria-label="Open AI assistant"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="gaios-float-ai-core" />
      </button>
      {open ? (
        <div className="gaios-float-ai-panel">
          <div className="gaios-float-ai-head">
            <span>AC · Advisory</span>
            <button type="button" className="gaios-float-ai-close" onClick={() => setOpen(false)}>
              ×
            </button>
          </div>
          <div className="gaios-float-ai-src tl-small tl-muted">
            {hubLine}
            <div>REST: /trading/market-hub/live · /trading/hub-metrics</div>
          </div>
          <div ref={scrollRef} className="gaios-float-ai-chat">
            {lines.map((ln, i) => (
              <div key={i} className={`gaios-float-ai-line gaios-float-ai-line-${ln.role}`}>
                {ln.text}
              </div>
            ))}
          </div>
          <div className="gaios-float-ai-compose">
            <input
              className="gaios-float-ai-input"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  void send();
                }
              }}
              placeholder="Ask (advisory)…"
            />
            <button type="button" className="gaios-float-ai-send" disabled={busy} onClick={() => void send()}>
              {busy ? "…" : "Send"}
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
