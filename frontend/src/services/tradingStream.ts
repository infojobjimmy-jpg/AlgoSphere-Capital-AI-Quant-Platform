import { tradingStore } from "../state/tradingStore";

const api = "/api";

async function j<T>(r: Response): Promise<T | null> {
  if (!r.ok) return null;
  try {
    return (await r.json()) as T;
  } catch {
    return null;
  }
}

function wsMarketHubUrl(): string {
  const env = import.meta.env.VITE_MARKET_HUB_WS as string | undefined;
  if (env) return env;
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const host = window.location.host;
  return `${proto}//${host}/ws/market-hub`;
}

function patchTradingFromServer(p: Record<string, unknown>) {
  const signals = (p.signals as unknown[]) ?? [];
  const orders = (p.orders as unknown[]) ?? [];
  const positions = (p.positions as unknown[]) ?? [];
  const livePositions = (p.livePositions as unknown[]) ?? [];
  const risk = (p.risk as Record<string, unknown> | null) ?? null;
  const portfolio = (p.portfolio as Record<string, unknown> | null) ?? null;
  const marketHub = (p.marketHub as Record<string, unknown> | null) ?? null;
  const equitySeries = (p.equitySeries as Record<string, unknown> | null) ?? null;
  const hubMetrics = (p.hubMetrics as Record<string, unknown> | null) ?? null;
  const modeRaw = String(p.mode ?? (risk as Record<string, unknown> | null)?.mode ?? portfolio?.mode ?? "paper").toLowerCase();
  const mode = modeRaw === "live" ? "live" : "paper";
  const ticks = signals.slice(0, 24).map((s) => {
    const o = s as Record<string, unknown>;
    return { t: o.time, sym: o.symbol, side: o.side, st: o.strength };
  });

  tradingStore.patch({
    signals,
    orders,
    positions,
    livePositions,
    risk,
    portfolio,
    ticks,
    mode,
    marketHub,
    equitySeries,
    hubMetrics,
    lastError: null,
  });
}

async function pollTradingRest() {
  try {
    const [sig, ord, pos, risk, port, hub, eqs, hm] = await Promise.all([
      fetch(`${api}/trading/signals?limit=60`),
      fetch(`${api}/trading/orders?limit=40`),
      fetch(`${api}/trading/positions`),
      fetch(`${api}/trading/risk`),
      fetch(`${api}/trading/portfolio`),
      fetch(`${api}/trading/market-hub/live`),
      fetch(`${api}/trading/equity-series?limit=500`),
      fetch(`${api}/trading/hub-metrics`),
    ]);
    const sj = await j<{ signals: unknown[] }>(sig);
    const oj = await j<{ orders: unknown[] }>(ord);
    const pj = await j<{ positions?: unknown[]; live_positions?: unknown[]; mode?: string }>(pos);
    const rj = await j<Record<string, unknown>>(risk);
    const portj = await j<Record<string, unknown>>(port);
    const hubj = await j<Record<string, unknown>>(hub);
    const eqj = await j<Record<string, unknown>>(eqs);
    const hmj = await j<Record<string, unknown>>(hm);

    const signals = sj?.signals ?? [];
    const orders = oj?.orders ?? [];
    const positions = pj?.positions ?? [];
    const livePositions = pj?.live_positions ?? [];
    const modeRaw = String(pj?.mode ?? rj?.mode ?? portj?.mode ?? "paper").toLowerCase();
    const mode = modeRaw === "live" ? "live" : "paper";
    const ticks = signals.slice(0, 24).map((s) => {
      const o = s as Record<string, unknown>;
      return { t: o.time, sym: o.symbol, side: o.side, st: o.strength };
    });

    tradingStore.patch({
      signals,
      orders,
      positions,
      livePositions,
      risk: rj,
      portfolio: portj,
      ticks,
      mode,
      marketHub: hubj,
      equitySeries: eqj,
      hubMetrics: hmj,
      lastError: null,
    });
  } catch (e) {
    tradingStore.patch({
      lastError: e instanceof Error ? e.message : "stream_error",
    });
  }
}

export function startTradingStream(intervalMs = 2500) {
  let pollId: ReturnType<typeof setInterval> | undefined;
  let ws: WebSocket | null = null;
  let stopped = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let backoffMs = 2000;

  const stopPoll = () => {
    if (pollId) {
      window.clearInterval(pollId);
      pollId = undefined;
    }
  };

  const startPoll = () => {
    stopPoll();
    void pollTradingRest();
    pollId = window.setInterval(() => void pollTradingRest(), intervalMs);
  };

  const stopReconnect = () => {
    if (reconnectTimer) window.clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  };

  const scheduleWsReconnect = () => {
    stopReconnect();
    if (stopped) return;
    reconnectTimer = window.setTimeout(() => {
      connectWs();
    }, Math.min(backoffMs, 30_000));
    backoffMs = Math.min(30_000, backoffMs + 1500);
  };

  const connectWs = () => {
    if (stopped) return;
    try {
      ws = new WebSocket(wsMarketHubUrl());
    } catch {
      startPoll();
      scheduleWsReconnect();
      return;
    }

    ws.onopen = () => {
      stopPoll();
      backoffMs = 2000;
    };

    ws.onmessage = (ev) => {
      try {
        const data = JSON.parse(String(ev.data)) as Record<string, unknown>;
        if (data.type === "market_hub") {
          const { type: _t, ...rest } = data;
          patchTradingFromServer(rest);
        }
      } catch {
        /* ignore malformed */
      }
    };

    ws.onerror = () => {
      ws?.close();
    };

    ws.onclose = () => {
      ws = null;
      startPoll();
      scheduleWsReconnect();
    };
  };

  startPoll();
  connectWs();

  return () => {
    stopped = true;
    stopReconnect();
    stopPoll();
    ws?.close();
    ws = null;
  };
}
