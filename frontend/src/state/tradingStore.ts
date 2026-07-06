export type TradingMode = "paper" | "live";

export type TradingSnapshot = {
  positions: unknown[];
  livePositions: unknown[];
  signals: unknown[];
  orders: unknown[];
  risk: Record<string, unknown> | null;
  portfolio: Record<string, unknown> | null;
  ticks: unknown[];
  mode: TradingMode;
  marketHub: Record<string, unknown> | null;
  equitySeries: Record<string, unknown> | null;
  hubMetrics: Record<string, unknown> | null;
  lastError: string | null;
  updatedAt: number;
};

const initial: TradingSnapshot = {
  positions: [],
  livePositions: [],
  signals: [],
  orders: [],
  risk: null,
  portfolio: null,
  ticks: [],
  mode: "paper",
  marketHub: null,
  equitySeries: null,
  hubMetrics: null,
  lastError: null,
  updatedAt: 0,
};

let snap: TradingSnapshot = { ...initial };
const listeners = new Set<() => void>();

export const tradingStore = {
  subscribe(cb: () => void) {
    listeners.add(cb);
    return () => {
      listeners.delete(cb);
    };
  },
  getSnapshot(): TradingSnapshot {
    return snap;
  },
  reset() {
    snap = { ...initial };
    listeners.forEach((l) => l());
  },
  patch(p: Partial<TradingSnapshot>) {
    snap = { ...snap, ...p, updatedAt: Date.now() };
    listeners.forEach((l) => l());
  },
};
