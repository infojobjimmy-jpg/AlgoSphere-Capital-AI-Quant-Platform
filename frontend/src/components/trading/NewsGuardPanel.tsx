import { useEffect, useMemo, useState } from "react";

import DraggableCard from "./DraggableCard";

type NewsEvent = {
  id?: string;
  title?: string;
  at?: string;
  risk?: string;
  currencies?: string[];
  symbols?: string[];
  off_before_min?: number;
  off_after_min?: number;
};

type NewsGuardState = {
  enabled?: boolean;
  status?: "ON" | "OFF" | string;
  risk?: string;
  active_events?: NewsEvent[];
  next_event?: NewsEvent | null;
  checked_at?: string;
  fail_safe?: boolean;
  reason?: string;
};

function formatMontreal(value?: string): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return new Intl.DateTimeFormat("fr-CA", {
    timeZone: "America/Montreal",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(d);
}

function countdown(value?: string): string {
  if (!value) return "—";
  const delta = new Date(value).getTime() - Date.now();
  if (!Number.isFinite(delta)) return "—";
  if (delta <= 0) return "en cours / passée";
  const mins = Math.floor(delta / 60000);
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return h > 0 ? `${h} h ${m} min` : `${m} min`;
}

function eventTargets(event?: NewsEvent | null): string {
  if (!event) return "—";
  const symbols = event.symbols ?? [];
  const currencies = event.currencies ?? [];
  const values = symbols.length ? symbols : currencies;
  return values.length ? values.join(", ") : "global";
}

export default function NewsGuardPanel() {
  const [state, setState] = useState<NewsGuardState | null>(null);
  const [error, setError] = useState<string>("");
  const [, setClock] = useState(0);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const res = await fetch("/api/news-guard/status", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = (await res.json()) as NewsGuardState;
        if (!cancelled) {
          setState(payload);
          setError("");
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "News Guard indisponible");
      }
    };

    void load();
    const poller = window.setInterval(() => void load(), 15000);
    const clock = window.setInterval(() => setClock((v) => v + 1), 30000);
    return () => {
      cancelled = true;
      window.clearInterval(poller);
      window.clearInterval(clock);
    };
  }, []);

  const next = state?.next_event ?? null;
  const active = state?.active_events ?? [];
  const status = state?.status ?? (error ? "OFF" : "—");
  const risk = state?.risk ?? (error ? "CRITICAL" : "—");
  const headline = useMemo(() => {
    if (status === "OFF") return "NOUVELLES ENTRÉES BLOQUÉES";
    if (status === "ON") return "TRADING AUTORISÉ";
    return "ÉTAT INCONNU";
  }, [status]);

  return (
    <DraggableCard title="AlgoSphere News Guard" resizable className="tl-neon">
      <div className="tl-stat">
        <label>État</label>
        <strong>{status === "OFF" ? "🔴 OFF" : status === "ON" ? "🟢 ON" : "⚪ —"}</strong>
      </div>
      <div className="tl-stat">
        <label>Risque</label>
        <strong>{risk}</strong>
      </div>
      <div className="tl-muted tl-small">{headline}</div>

      {active.length > 0 ? (
        <div className="tl-scroll" style={{ maxHeight: 130, marginTop: 10 }}>
          {active.map((event, index) => (
            <div key={event.id ?? `${event.title}-${index}`} className="tl-row tl-mono tl-small">
              {event.title ?? "Événement"} · {eventTargets(event)} · {formatMontreal(event.at)} Québec
            </div>
          ))}
        </div>
      ) : null}

      <div className="tl-stat" style={{ marginTop: 10 }}>
        <label>Prochaine nouvelle</label>
        <strong>{next?.title ?? "—"}</strong>
      </div>
      <div className="tl-stat">
        <label>Heure Québec</label>
        <strong>{formatMontreal(next?.at)}</strong>
      </div>
      <div className="tl-stat">
        <label>Compte à rebours</label>
        <strong>{countdown(next?.at)}</strong>
      </div>
      <div className="tl-stat">
        <label>Touchés</label>
        <strong>{eventTargets(next)}</strong>
      </div>
      {next ? (
        <div className="tl-muted tl-small">
          Fenêtre OFF: -{next.off_before_min ?? 0} min / +{next.off_after_min ?? 0} min
        </div>
      ) : null}
      {state?.fail_safe ? <div className="tl-muted tl-small">Fail-safe actif</div> : null}
      {error || state?.reason ? <div className="tl-err">{error || state?.reason}</div> : null}
    </DraggableCard>
  );
}
