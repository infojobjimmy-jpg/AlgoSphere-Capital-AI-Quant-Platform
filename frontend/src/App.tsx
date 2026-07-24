import * as Cesium from "cesium";
import "cesium/Build/Cesium/Widgets/widgets.css";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import FloatingAiRobot from "./components/ai/FloatingAiRobot";
import GlobeAdvisoryPanel from "./components/globe/GlobeAdvisoryPanel";

type StrategySnap = {
  goals_active?: Array<{ id: number; description: string; priority: number }>;
  global_priority?: number;
  attention_weights?: Record<string, number>;
  goal_bias?: Record<string, number>;
  execution?: Record<string, unknown>;
  active_plan?: {
    plan_id?: number;
    goal_id?: number;
    title?: string;
    steps?: Array<Record<string, unknown>>;
    step_index?: number;
  };
  plan_created?: number;
};

type Snapshot = {
  meta?: Record<string, unknown> & {
    strategy?: StrategySnap;
    cortex?: { loop?: string; agents?: string[]; phases?: Array<Record<string, unknown>> };
    evolution?: { guidance?: Record<string, unknown> };
  };
  layers?: Record<string, Array<Record<string, unknown>>>;
  alerts?: Array<Record<string, unknown>>;
  correlations?: Array<Record<string, unknown>>;
  heatmaps?: Record<string, Array<Record<string, unknown>>>;
  events?: Array<Record<string, unknown>>;
  investigations?: Array<Record<string, unknown>>;
  insights?: Array<Record<string, unknown>>;
  decisions?: Array<Record<string, unknown>>;
};

const apiBase = "/api";

function wsUrl(): string {
  const env = import.meta.env.VITE_WS_URL;
  if (env) return env;
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  return `${proto}://${host}/ws/live`;
}

function colorForAltKm(altKm: number): Cesium.Color {
  const t = Cesium.Math.clamp((altKm + 1) / 900, 0, 1);
  return Cesium.Color.fromHsl(0.55 + 0.22 * t, 0.95, 0.55, 0.92);
}

function colorForTempC(t: number | undefined): Cesium.Color {
  if (t === undefined || Number.isNaN(t)) return Cesium.Color.fromCssColorString("#4fd1ff");
  const u = Cesium.Math.clamp((t + 20) / 60, 0, 1);
  return Cesium.Color.fromHsl(0.58 - 0.35 * u, 0.9, 0.55, 0.95);
}

function availabilityCount(value: number): React.ReactNode {
  return value > 0 ? value : <span className="source-unavailable">Unavailable</span>;
}

export default function App() {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const viewerRef = useRef<Cesium.Viewer | null>(null);
  const layersRef = useRef<{
    aircraft: Cesium.PointPrimitiveCollection;
    ships: Cesium.PointPrimitiveCollection;
    sats: Cesium.PointPrimitiveCollection;
    wx: Cesium.PointPrimitiveCollection;
    cams: Cesium.PointPrimitiveCollection;
    trails: Cesium.PolylineCollection;
    heat: Cesium.EntityCollection;
  } | null>(null);

  const trailMemRef = useRef<Map<string, Cesium.Cartesian3[]>>(new Map());
  const lastFocusSigRef = useRef<string>("");

  const [liveSnap, setLiveSnap] = useState<Snapshot | null>(null);
  const [replayFrames, setReplayFrames] = useState<Snapshot[]>([]);
  const [replayEnabled, setReplayEnabled] = useState(false);
  const [replayIdx, setReplayIdx] = useState(0);

  const [showAircraft, setShowAircraft] = useState(true);
  const [showSats, setShowSats] = useState(true);
  const [showShips, setShowShips] = useState(true);
  const [showWeather, setShowWeather] = useState(true);
  const [showCams, setShowCams] = useState(true);
  const [showHeat, setShowHeat] = useState(true);

  const [historyCompare, setHistoryCompare] = useState<{
    fusion_events: Array<Record<string, unknown>>;
    decisions: Array<Record<string, unknown>>;
    adaptive_threshold_trend: Record<string, unknown>;
  } | null>(null);

  const [goalsApi, setGoalsApi] = useState<Array<Record<string, unknown>>>([]);
  const [plansApi, setPlansApi] = useState<{
    active_bundle: Record<string, unknown> | null;
    recent_plans: Array<Record<string, unknown>>;
  } | null>(null);

  const workspaceRef = useRef<HTMLDivElement | null>(null);
  const [leftPct, setLeftPct] = useState(40);
  const [fullscreen, setFullscreen] = useState<null | "globe" | "lab">(null);
  const dragRef = useRef<{ startX: number; startPct: number; width: number } | null>(null);

  const snap = useMemo(() => {
    if (replayEnabled && replayFrames.length) {
      const idx = Cesium.Math.clamp(replayIdx, 0, replayFrames.length - 1);
      return replayFrames[idx] ?? null;
    }
    return liveSnap;
  }, [liveSnap, replayFrames, replayEnabled, replayIdx]);

  const counts = useMemo(() => {
    const layers = snap?.layers ?? {};
    return {
      aircraft: (layers.aircraft ?? []).length,
      satellites: (layers.satellites ?? []).length,
      ships: (layers.ships ?? []).length,
      cameras: (layers.cameras ?? []).length,
      weather: (layers.weather ?? []).length,
      updated: String(snap?.meta?.updated_at ?? "—"),
    };
  }, [snap]);

  const strategy = useMemo(() => (snap?.meta?.strategy ?? {}) as StrategySnap, [snap]);

  useEffect(() => {
    const token = import.meta.env.VITE_CESIUM_TOKEN || import.meta.env.VITE_CESIUM_ION_TOKEN;
    if (token) Cesium.Ion.defaultAccessToken = String(token);
  }, []);

  useEffect(() => {
    const el = hostRef.current;
    if (!el) return;

    const viewer = new Cesium.Viewer(el, {
      baseLayerPicker: false,
      geocoder: false,
      animation: false,
      timeline: false,
      fullscreenButton: true,
      homeButton: true,
      navigationHelpButton: false,
      sceneModePicker: true,
      selectionIndicator: true,
      infoBox: true,
      terrainProvider: new Cesium.EllipsoidTerrainProvider(),
    });

    viewer.scene.globe.depthTestAgainstTerrain = false;
    viewer.scene.requestRenderMode = true;
    viewer.scene.maximumRenderTimeChange = Infinity;

    viewer.imageryLayers.removeAll();
    viewer.imageryLayers.addImageryProvider(
      new Cesium.UrlTemplateImageryProvider({
        url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        credit: new Cesium.Credit("© OpenStreetMap contributors", false),
      }),
    );

    const primitives = viewer.scene.primitives;
    const aircraft = primitives.add(new Cesium.PointPrimitiveCollection());
    const ships = primitives.add(new Cesium.PointPrimitiveCollection());
    const sats = primitives.add(new Cesium.PointPrimitiveCollection());
    const wx = primitives.add(new Cesium.PointPrimitiveCollection());
    const cams = primitives.add(new Cesium.PointPrimitiveCollection());
    const trails = primitives.add(new Cesium.PolylineCollection());

    layersRef.current = { aircraft, ships, sats, wx, cams, trails, heat: viewer.entities };

    viewer.camera.setView({
      destination: Cesium.Cartesian3.fromDegrees(-15, 25, 18_000_000),
    });

    viewerRef.current = viewer;
    return () => {
      layersRef.current = null;
      viewerRef.current = null;
      viewer.destroy();
    };
  }, []);

  useEffect(() => {
    let stopped = false;
    const load = async () => {
      try {
        const r = await fetch(`${apiBase}/intel/timeline?limit=40`);
        const j = await r.json();
        if (!stopped) setReplayFrames(Array.isArray(j.frames) ? j.frames : []);
      } catch {
        if (!stopped) setReplayFrames([]);
      }
    };
    void load();
    const t = window.setInterval(load, 60_000);
    return () => {
      stopped = true;
      window.clearInterval(t);
    };
  }, []);

  useEffect(() => {
    let ping: number | undefined;
    const url = wsUrl();
    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";
    ws.onmessage = async (ev) => {
      try {
        if (typeof ev.data === "string") {
          setLiveSnap(JSON.parse(ev.data) as Snapshot);
          return;
        }
        const buf = ev.data as ArrayBuffer;
        const u8 = new Uint8Array(buf);
        if (u8.byteLength > 2 && u8[0] === 0x02) {
          const stream = new Blob([u8.slice(1)]).stream().pipeThrough(new DecompressionStream("gzip"));
          const txt = await new Response(stream).text();
          setLiveSnap(JSON.parse(txt) as Snapshot);
          return;
        }
        const txt = new TextDecoder().decode(u8);
        setLiveSnap(JSON.parse(txt) as Snapshot);
      } catch {
        // ignore
      }
    };
    const wantCompress = import.meta.env.VITE_LIVE_WS_COMPRESS === "true";
    ws.onopen = () => {
      try {
        ws.send(JSON.stringify({ type: "hello", compress: wantCompress }));
      } catch {
        // ignore
      }
      ping = window.setInterval(() => {
        try {
          ws.send(JSON.stringify({ type: "ping" }));
        } catch {
          // ignore
        }
      }, 25_000);
    };
    return () => {
      if (ping) window.clearInterval(ping);
      ws.close();
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const boot = async () => {
      try {
        const r = await fetch(`${apiBase}/layers/snapshot`);
        const j = (await r.json()) as Snapshot;
        if (!cancelled) setLiveSnap(j);
      } catch {
        // ignore
      }
    };
    void boot();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const r = await fetch(`${apiBase}/intel/history-compare?events_limit=25&decisions_limit=25`);
        const j = await r.json();
        if (!cancelled) setHistoryCompare(j);
      } catch {
        if (!cancelled) setHistoryCompare(null);
      }
    };
    void load();
    const t = window.setInterval(load, 120_000);
    return () => {
      cancelled = true;
      window.clearInterval(t);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [g, p] = await Promise.all([
          fetch(`${apiBase}/goals?status=all`),
          fetch(`${apiBase}/goals/plans/summary`),
        ]);
        const gj = await g.json();
        const pj = await p.json();
        if (!cancelled) {
          setGoalsApi(Array.isArray(gj.goals) ? gj.goals : []);
          setPlansApi(
            pj && typeof pj === "object"
              ? {
                  active_bundle: (pj.active_bundle as Record<string, unknown> | null) ?? null,
                  recent_plans: Array.isArray(pj.recent_plans) ? pj.recent_plans : [],
                }
              : null,
          );
        }
      } catch {
        if (!cancelled) {
          setGoalsApi([]);
          setPlansApi(null);
        }
      }
    };
    void load();
    const t = window.setInterval(load, 45_000);
    return () => {
      cancelled = true;
      window.clearInterval(t);
    };
  }, []);

  useEffect(() => {
    const L = layersRef.current;
    const viewer = viewerRef.current;
    if (!L || !viewer || !snap) return;

    L.aircraft.removeAll();
    L.ships.removeAll();
    L.sats.removeAll();
    L.wx.removeAll();
    L.cams.removeAll();
    L.trails.removeAll();
    L.heat.removeAll();

    const layers = snap.layers ?? {};

    if (showAircraft) {
      const mem = trailMemRef.current;
      const alive = new Set<string>();
      for (const a of layers.aircraft ?? []) {
        const id = String(a.id ?? "");
        if (id) alive.add(id);
      }
      if (mem.size > 14000) {
        for (const k of [...mem.keys()]) {
          if (!alive.has(k)) mem.delete(k);
        }
      }

      for (const a of layers.aircraft ?? []) {
        const lat = Number(a.lat);
        const lon = Number(a.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        const alt = Number(a.alt_m ?? 10_000);
        const altKm = alt / 1000;
        const imp = Cesium.Math.clamp(Number(a.importance ?? 0), 0, 1);
        const id = String(a.id ?? "");
        if (id) {
          const p = Cesium.Cartesian3.fromDegrees(lon, lat, Math.max(200, alt));
          const arr = mem.get(id) ?? [];
          arr.push(p);
          while (arr.length > 14) arr.shift();
          mem.set(id, arr);
        }
        const glow = Cesium.Color.fromHsl(0.56, 0.95, 0.55 + 0.12 * imp, 0.15 + 0.55 * imp);
        const base = colorForAltKm(altKm);
        const tint = base.withAlpha(Cesium.Math.clamp(0.72 + 0.22 * imp, 0.55, 0.98));
        L.aircraft.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, Math.max(200, alt)),
          color: tint,
          pixelSize: 2.0 + imp * 7.0,
          outlineColor: glow,
          outlineWidth: imp > 0.62 ? 3.0 : 1.0,
        });
      }

      const scored = [...(layers.aircraft ?? [])]
        .map((a) => ({ a, imp: Number(a.importance ?? 0) }))
        .sort((x, y) => y.imp - x.imp)
        .slice(0, 1800);
      for (const { a } of scored) {
        const id = String(a.id ?? "");
        const pts = mem.get(id);
        if (!pts || pts.length < 2) continue;
        L.trails.add({
          positions: pts,
          width: 1.1 + 2.4 * Cesium.Math.clamp(Number(a.importance ?? 0), 0, 1),
          material: Cesium.Material.fromType("Color", {
            color: Cesium.Color.fromCssColorString("#4fd1ff").withAlpha(0.35),
          }),
        });
      }
    } else {
      trailMemRef.current.clear();
    }

    if (showShips) {
      for (const s of layers.ships ?? []) {
        const lat = Number(s.lat);
        const lon = Number(s.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.ships.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 5),
          color: Cesium.Color.fromCssColorString("#7af8d6").withAlpha(0.9),
          pixelSize: 3,
        });
      }
    }

    if (showSats) {
      for (const s of layers.satellites ?? []) {
        const lat = Number(s.lat);
        const lon = Number(s.lon);
        const altKm = Number(s.alt_km ?? 400);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        const h = Math.max(250_000, altKm * 1000);
        L.sats.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, h),
          color: Cesium.Color.fromCssColorString("#c7b6ff").withAlpha(0.95),
          pixelSize: 4,
        });
      }
    }

    if (showWeather) {
      for (const w of layers.weather ?? []) {
        const lat = Number(w.lat);
        const lon = Number(w.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        const t = typeof w.temperature_c === "number" ? w.temperature_c : undefined;
        L.wx.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 25_000),
          color: colorForTempC(t),
          pixelSize: 10,
        });
      }
    }

    if (showCams) {
      for (const c of layers.cameras ?? []) {
        const lat = Number(c.lat);
        const lon = Number(c.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.cams.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 1200),
          color: Cesium.Color.fromCssColorString("#ffd166").withAlpha(0.95),
          pixelSize: 7,
        });
      }
    }

    if (showHeat) {
      const acHeat = snap.heatmaps?.aircraft ?? [];
      for (const h of acHeat) {
        const lat = Number(h.lat);
        const lon = Number(h.lon);
        const intensity = Number(h.intensity ?? 0.5);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.heat.add({
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 0),
          ellipse: {
            semiMinorAxis: 180_000 * intensity + 40_000,
            semiMajorAxis: 220_000 * intensity + 60_000,
            material: Cesium.Color.fromCssColorString("#ff5c7a").withAlpha(0.22),
            outline: true,
            outlineColor: Cesium.Color.fromCssColorString("#ff5c7a").withAlpha(0.35),
            heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
          },
        });
      }
    }

    viewer.scene.requestRender();
  }, [snap, showAircraft, showSats, showShips, showWeather, showCams, showHeat]);

  useEffect(() => {
    const viewer = viewerRef.current;
    const f = snap?.meta?.focus as { lat?: number; lon?: number; alt?: number } | undefined;
    if (!viewer || !f || typeof f.lat !== "number" || typeof f.lon !== "number") return;
    const sig = `${f.lat.toFixed(3)},${f.lon.toFixed(3)}`;
    const alerts = snap?.alerts ?? [];
    const critical = alerts.some((a) => String(a.severity) === "critical");
    const hi = (snap?.events ?? []).some((e) => String(e.severity) === "high");
    if (!critical && !hi) return;
    if (sig === lastFocusSigRef.current) return;
    lastFocusSigRef.current = sig;
    viewer.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(f.lon, f.lat, f.alt ?? 1_850_000),
      duration: 2.35,
    });
    viewer.scene.requestRender();
  }, [snap]);

  const globeSnap = snap as unknown as Record<string, unknown> | null;

  const onSplitPointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const root = workspaceRef.current;
      if (!root) return;
      const rect = root.getBoundingClientRect();
      const inner = Math.max(1, rect.width - 6);
      dragRef.current = { startX: e.clientX, startPct: leftPct, width: inner };
      const el = e.currentTarget;
      const pid = e.pointerId;
      el.setPointerCapture(pid);
      const onMove = (ev: PointerEvent) => {
        const d = dragRef.current;
        if (!d) return;
        const dx = ev.clientX - d.startX;
        const deltaPct = (dx / d.width) * 100;
        setLeftPct(Math.min(78, Math.max(22, d.startPct + deltaPct)));
      };
      const cleanup = () => {
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", end);
        window.removeEventListener("pointercancel", end);
        dragRef.current = null;
        try {
          el.releasePointerCapture(pid);
        } catch {
          /* ignore */
        }
      };
      const end = () => {
        cleanup();
      };
      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", end);
      window.addEventListener("pointercancel", end);
    },
    [leftPct],
  );

  const globeColStyle =
    fullscreen === "lab"
      ? undefined
      : fullscreen === "globe"
        ? { flex: "1 1 100%", minWidth: 0 }
        : { flex: `0 0 calc((100% - 6px) * ${leftPct / 100})`, minWidth: 220, maxWidth: "85%" };
  const rightColStyle =
    fullscreen === "globe"
      ? undefined
      : fullscreen === "lab"
        ? { flex: "1 1 100%", minWidth: 0 }
        : { flex: "1 1 0", minWidth: 0 };

  return (
    <div
      ref={workspaceRef}
      className={`gaios-shell gaios-workspace ${fullscreen ? "is-fullscreen" : ""}`}
    >
      <div
        className={`gaios-col-globe glass-depth ${fullscreen === "lab" ? "is-hidden" : ""} ${fullscreen === "globe" ? "is-full" : ""}`}
        style={globeColStyle}
      >
        <div ref={hostRef} className="gaios-cesium" />
        {fullscreen !== "lab" ? <GlobeAdvisoryPanel snapshot={globeSnap} /> : null}
      </div>
      {fullscreen === null ? (
        <div
          className="gaios-splitter"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize Globe and Hub columns"
          onPointerDown={onSplitPointerDown}
        />
      ) : null}
      <div
        className={`gaios-col-right glass-depth ${fullscreen === "globe" ? "is-hidden" : ""} ${fullscreen === "lab" ? "is-full" : ""}`}
        style={rightColStyle}
      >
        <div className="gaios-workspace-toolbar">
          <button
            type="button"
            className={fullscreen === "globe" ? "gaios-fs-btn gaios-fs-on" : "gaios-fs-btn"}
            onClick={() => setFullscreen((f) => (f === "globe" ? null : "globe"))}
          >
            {fullscreen === "globe" ? "Exit Globe fullscreen" : "Globe fullscreen"}
          </button>
          <button
            type="button"
            className={fullscreen === "lab" ? "gaios-fs-btn gaios-fs-on" : "gaios-fs-btn"}
            onClick={() => setFullscreen((f) => (f === "lab" ? null : "lab"))}
          >
            {fullscreen === "lab" ? "Exit Intelligence fullscreen" : "Intelligence fullscreen"}
          </button>
        </div>
        <div className="gaios-panel gaios-panel--dock">
          <div className="gaios-title">
            <h1>COPIE PALANTIR</h1>
            <span>Unified geospatial intelligence · verified global data</span>
          </div>

          <div className="grid">
            <div className="stat">
              <label>Aircraft</label>
              <strong>{availabilityCount(counts.aircraft)}</strong>
            </div>
            <div className="stat">
              <label>Satellites</label>
              <strong>{availabilityCount(counts.satellites)}</strong>
            </div>
            <div className="stat">
              <label>Ships</label>
              <strong>{availabilityCount(counts.ships)}</strong>
            </div>
            <div className="stat">
              <label>Weather cells</label>
              <strong>{availabilityCount(counts.weather)}</strong>
            </div>
            <div className="stat">
              <label>Cameras</label>
              <strong>{availabilityCount(counts.cameras)}</strong>
            </div>
            <div className="stat">
              <label>Updated</label>
              <strong style={{ fontSize: 12, color: "var(--muted)" }}>{counts.updated}</strong>
            </div>
          </div>

          <div className="row">
            <label className="toggle">
              <input type="checkbox" checked={showAircraft} onChange={(e) => setShowAircraft(e.target.checked)} />
              Aircraft
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showSats} onChange={(e) => setShowSats(e.target.checked)} />
              Satellites
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showShips} onChange={(e) => setShowShips(e.target.checked)} />
              Ships
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showWeather} onChange={(e) => setShowWeather(e.target.checked)} />
              Weather
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showCams} onChange={(e) => setShowCams(e.target.checked)} />
              Cameras
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showHeat} onChange={(e) => setShowHeat(e.target.checked)} />
              Heat clusters
            </label>
          </div>

          <div className="alerts">
            {(snap?.alerts ?? []).slice(0, 6).map((a, idx) => {
              const sev = String(a.severity ?? "info");
              const cls = sev === "critical" ? "critical" : sev === "warning" ? "warning" : "";
              return (
                <div key={`${String(a.title ?? idx)}-${sev}`} className={`alert ${cls}`}>
                  <div className="t">{String(a.title ?? "Alert")}</div>
                  {a.body ? <div className="b">{String(a.body)}</div> : null}
                </div>
              );
            })}
            {(snap?.alerts ?? []).length === 0 ? (
              <div style={{ color: "var(--muted)", fontSize: 12 }}>No active anomaly alerts.</div>
            ) : null}
          </div>

          <div className="intelblock">
            <label>Fusion events</label>
            <div className="intelitems">
              {(snap?.events ?? []).slice(0, 4).map((e, idx) => (
                <div key={`${String(e.event_type ?? idx)}-${String(e.summary ?? "").slice(0, 20)}`} className="intelrow">
                  <span className={`pill sev_${String(e.severity ?? "info")}`}>{String(e.severity ?? "info")}</span>
                  <span className="inteltxt">{String(e.summary ?? e.event_type ?? "")}</span>
                </div>
              ))}
              {(snap?.events ?? []).length === 0 ? (
                <div style={{ color: "var(--muted)", fontSize: 12 }}>No fused events in this frame.</div>
              ) : null}
            </div>
          </div>

          <div className="intelblock">
            <label>Operator insights</label>
            <div className="intelitems">
              {(snap?.insights ?? []).slice(0, 5).map((it, idx) => (
                <div key={String(it.text ?? idx).slice(0, 40) || String(idx)} className="intelrow">
                  <span className="pill sev_info">AI</span>
                  <span className="inteltxt">{String(it.text ?? "")}</span>
                </div>
              ))}
              {(snap?.insights ?? []).length === 0 ? (
                <div style={{ color: "var(--muted)", fontSize: 12 }}>Awaiting narrator output.</div>
              ) : null}
            </div>
          </div>

          <div className="intelblock">
            <label>Strategic decisions</label>
            <div className="intelitems">
              {(snap?.decisions ?? []).slice(0, 6).map((d, idx) => (
                <div key={`${String(d.action ?? idx)}-${String(d.severity ?? "")}`} className="intelrow">
                  <span className={`pill sev_${String(d.severity ?? "info")}`}>{String(d.severity ?? "—")}</span>
                  <span className="inteltxt">
                    <span className="mono">{String(d.action ?? "")}</span>
                    <div className="sub">{String(d.rationale ?? "").slice(0, 220)}</div>
                  </span>
                </div>
              ))}
              {(snap?.decisions ?? []).length === 0 ? (
                <div style={{ color: "var(--muted)", fontSize: 12 }}>No strategic decisions this frame.</div>
              ) : null}
            </div>
          </div>

          <div className="intelblock">
            <label>Goal-driven strategy</label>
            {(snap?.meta?.cortex as Record<string, unknown> | undefined)?.loop ? (
              <div className="sub" style={{ marginTop: 6 }}>
                <span className="mono">Cortex</span> · {String((snap?.meta?.cortex as Record<string, unknown>).loop)}
                <div className="sub" style={{ marginTop: 4 }}>
                  {(
                    ((snap?.meta?.cortex as Record<string, unknown>).phases as Array<Record<string, unknown>>) ?? []
                  ).map((p, i) => (
                    <span key={String(p.name ?? i)} style={{ marginRight: 8 }}>
                      {String(p.name)} {typeof p.ms === "number" ? `${p.ms.toFixed(0)}ms` : ""}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
            {(snap?.meta?.evolution as Record<string, unknown> | undefined)?.guidance ? (
              <div className="sub" style={{ marginTop: 6 }}>
                <span className="mono">Meta-learning</span> guidance on tick:{" "}
                <span className="mono">
                  {JSON.stringify((snap?.meta?.evolution as Record<string, unknown>).guidance).slice(0, 220)}
                </span>
              </div>
            ) : null}
            <div className="sub" style={{ marginTop: 6 }}>
              Live snapshot bias plus REST-backed goals. Global priority rises with goal weight, fused events, and
              high-severity alerts.
            </div>
            <div className="sub" style={{ marginTop: 6 }}>
              Global stress score:{" "}
              <span className="mono">
                {typeof strategy.global_priority === "number" ? strategy.global_priority.toFixed(3) : "—"}
              </span>
            </div>
            <div className="strat-priority" title="Relative strategic load (0–1)">
              <span style={{ width: `${Math.round(100 * (strategy.global_priority ?? 0))}%` }} />
            </div>
            {strategy.goal_bias && Object.keys(strategy.goal_bias).length ? (
              <div className="sub" style={{ marginTop: 6 }}>
                Threshold bias:{" "}
                <span className="mono">
                  {Object.entries(strategy.goal_bias)
                    .map(([k, v]) => `${k}=${Number(v).toFixed(3)}`)
                    .join(" · ")}
                </span>
              </div>
            ) : null}
            <div className="attention-row">
              {(strategy.attention_weights ? Object.entries(strategy.attention_weights) : []).map(([k, v]) => (
                <span key={k} className="att-chip">
                  {k}: {(Number(v) * 100).toFixed(0)}%
                </span>
              ))}
              {!strategy.attention_weights ? (
                <span className="att-chip">Attention map loads when active goals exist</span>
              ) : null}
            </div>
            <div style={{ marginTop: 10, fontSize: 10, color: "var(--muted)", letterSpacing: "0.08em" }}>
              ACTIVE GOALS (SNAPSHOT)
            </div>
            {(strategy.goals_active ?? []).map((g) => (
              <div key={g.id} className="goal-row">
                <span className="mono">p={g.priority.toFixed(1)}</span> {g.description}
              </div>
            ))}
            {(strategy.goals_active ?? []).length === 0 ? (
              <div className="sub">No active goals in this frame (bridge may be offline or DB empty).</div>
            ) : null}
            <div style={{ marginTop: 10, fontSize: 10, color: "var(--muted)", letterSpacing: "0.08em" }}>
              GOALS (API · ALL STATUSES)
            </div>
            {goalsApi.slice(0, 8).map((g) => (
              <div key={String(g.id)} className="goal-row">
                <span className="mono">
                  {String(g.status)} · p={Number(g.priority).toFixed(1)}
                </span>{" "}
                {String(g.description ?? "").slice(0, 200)}
              </div>
            ))}
            {goalsApi.length === 0 ? <div className="sub">No goals returned from API.</div> : null}
            <div style={{ marginTop: 10, fontSize: 10, color: "var(--muted)", letterSpacing: "0.08em" }}>
              PLAN EXECUTION (SNAPSHOT)
            </div>
            <div className="intelrow" style={{ marginTop: 6 }}>
              <span className="pill sev_info">{String(strategy.execution?.status ?? "idle")}</span>
              <span className="inteltxt">
                {strategy.active_plan?.title ? (
                  <>
                    <div>{String(strategy.active_plan.title)}</div>
                    <div className="sub">
                      plan #{strategy.active_plan.plan_id} · goal #{strategy.active_plan.goal_id} · step{" "}
                      {Number(strategy.active_plan.step_index ?? 0) + 1} /{" "}
                      {(strategy.active_plan.steps ?? []).length || "?"}
                    </div>
                  </>
                ) : (
                  <span className="sub">No active plan in this tick.</span>
                )}
              </span>
            </div>
            <div className="plan-steps">
              {((strategy.active_plan?.steps ?? []) as Array<Record<string, unknown>>).map((st, i) => {
                const cur = Number(strategy.active_plan?.step_index ?? 0);
                const cls = i < cur ? "plan-step done" : i === cur ? "plan-step current" : "plan-step";
                return (
                  <div key={String(st.id ?? i)} className={cls}>
                    <div>
                      <span className="mono">{String(st.condition ?? "")}</span>
                    </div>
                    <div>{String(st.title ?? "")}</div>
                    <div className="sub">fallback: {String(st.fallback_action ?? "—")}</div>
                  </div>
                );
              })}
            </div>
            {typeof strategy.plan_created === "number" ? (
              <div className="sub" style={{ marginTop: 6 }}>
                New plan queued: #{strategy.plan_created}
              </div>
            ) : null}
            <div style={{ marginTop: 10, fontSize: 10, color: "var(--muted)", letterSpacing: "0.08em" }}>
              PLANS (API · RECENT)
            </div>
            <div className="plan-steps">
              {(plansApi?.recent_plans ?? []).slice(0, 5).map((p) => (
                <div key={String(p.id)} className="plan-step">
                  <span className="mono">
                    {String(p.status)} · gid {String(p.goal_id ?? "—")}
                  </span>
                  <div>{String(p.title ?? "")}</div>
                  {p.execution ? (
                    <div className="sub">
                      exec step {Number((p.execution as Record<string, unknown>).step_index ?? 0)} ·{" "}
                      {String((p.execution as Record<string, unknown>).last_tick_at ?? "—")}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
            {(plansApi?.recent_plans ?? []).length === 0 ? <div className="sub">No plans in database yet.</div> : null}
          </div>

          <div className="intelblock">
            <label>AI reasoning trace</label>
            <div className="trace">
              {((snap?.meta?.reasoning_trace as string[] | undefined) ?? []).map((ln, idx) => (
                <div key={`${idx}-${ln.slice(0, 20)}`} className="traceln">
                  {ln}
                </div>
              ))}
              {((snap?.meta?.reasoning_trace as string[] | undefined) ?? []).length === 0 ? (
                <div style={{ color: "var(--muted)", fontSize: 12 }}>Trace warming…</div>
              ) : null}
            </div>
          </div>

          <div className="intelblock">
            <label>Adaptive learning</label>
            <div className="learngrid">
              <div className="stat">
                <label>Air z-threshold</label>
                <strong>{String((snap?.meta?.learning as Record<string, unknown> | undefined)?.air_density_z ?? "—")}</strong>
              </div>
              <div className="stat">
                <label>Iso contamination</label>
                <strong>{String((snap?.meta?.learning as Record<string, unknown> | undefined)?.iso_contamination ?? "—")}</strong>
              </div>
            </div>
          </div>

          <div className="intelblock">
            <label>Event vs decision history</label>
            <div className="hist2">
              <div>
                <div className="histcap">Fusion (DB)</div>
                <ul>
                  {(historyCompare?.fusion_events ?? []).slice(0, 6).map((e, i) => (
                    <li key={`${String(e.severity)}-${String(e.summary ?? "").slice(0, 20)}-${i}`}>
                      <span className="mono">{String(e.severity)}</span> {String(e.summary ?? "").slice(0, 80)}
                    </li>
                  ))}
                </ul>
              </div>
              <div>
                <div className="histcap">Decisions (DB)</div>
                <ul>
                  {(historyCompare?.decisions ?? []).slice(0, 6).map((d, i) => (
                    <li key={`${String(d.severity)}-${String(d.action ?? "").slice(0, 20)}-${i}`}>
                      <span className="mono">{String(d.severity)}</span> {String(d.action ?? "").slice(0, 80)}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
            <div className="sub">
              Threshold trend: {String(historyCompare?.adaptive_threshold_trend?.trend ?? "—")} (
              {String(historyCompare?.adaptive_threshold_trend?.n ?? "0")} samples)
            </div>
          </div>

          <div className="replay">
            <label>Timeline replay</label>
            <input
              type="range"
              min={0}
              max={Math.max(0, replayFrames.length - 1)}
              value={replayIdx}
              disabled={replayFrames.length === 0}
              onChange={(e) => setReplayIdx(Number(e.target.value))}
            />
            <div className="row" style={{ marginTop: 8 }}>
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={replayEnabled}
                  disabled={replayFrames.length === 0}
                  onChange={(e) => {
                    const on = e.target.checked;
                    setReplayEnabled(on);
                    if (on && replayFrames.length) setReplayIdx(replayFrames.length - 1);
                  }}
                />
                Enable replay mode
              </label>
            </div>
          </div>

          <div className="correlations">
            {(snap?.correlations ?? []).slice(0, 4).map((c, idx) => (
              <div key={String(c.title ?? idx)} className="alert">
                <div className="t">{String(c.title ?? "Correlation")}</div>
                <div className="b">{String(c.details ?? "")}</div>
              </div>
            ))}
            {(snap?.correlations ?? []).length === 0 ? (
              <div style={{ color: "var(--muted)", fontSize: 12 }}>No correlation hits.</div>
            ) : null}
          </div>

          <div className="footer">
            Public, lawful feeds only. Unavailable sources remain empty and are never replaced with simulated
            objects. Configure approved AIS and webcam providers plus Cesium Ion for production coverage.
          </div>
        </div>
      </div>
      <FloatingAiRobot />
    </div>
  );
}
