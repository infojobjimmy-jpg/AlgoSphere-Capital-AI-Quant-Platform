import * as Cesium from "cesium";
import "cesium/Build/Cesium/Widgets/widgets.css";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import MemberAccessPanel from "./components/subscriptions/MemberAccessPanel";
import SubscriptionPanel from "./components/subscriptions/SubscriptionPanel";
import AccountPanel from "./components/subscriptions/AccountPanel";
import SocialPanel from "./components/social/SocialPanel";

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

type GpsMode = "car" | "truck";

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

function availabilityCount(value: number, lang: "fr" | "en"): React.ReactNode {
  return value > 0 ? value : <span className="source-unavailable">{lang === "fr" ? "Indisponible" : "Unavailable"}</span>;
}

function publicGeospatialText(value: unknown): string {
  return String(value ?? "")
    .replace(/maritime demo/gi, "maritime tracks")
    .replace(/démo maritime/gi, "pistes maritimes");
}

function isPublicGeospatialTrace(value: unknown): boolean {
  return !/(?:\btrade\b|trade[_-]|\btrading\b|\bbroker\b|\bportfolio\b|\bmarket hub\b|\bpaper execution\b)/i.test(String(value ?? ""));
}

export default function App() {
  const [lang, setLang] = useState<"fr" | "en">("fr");
  const [showSubscriptions, setShowSubscriptions] = useState(false);
  const [showMemberAccess, setShowMemberAccess] = useState(false);
  const [showAccount, setShowAccount] = useState(false);
  const [showSocial, setShowSocial] = useState(false);
  const [authConfigured, setAuthConfigured] = useState(false);
  const [ownerAccessConfigured, setOwnerAccessConfigured] = useState(false);
  const [member, setMember] = useState<Record<string, unknown> | null>(null);
  const [selectedFeature, setSelectedFeature] = useState<{ kind: string; data: Record<string, unknown> } | null>(null);
  const [showGpsPanel, setShowGpsPanel] = useState(false);
  const [gpsMode, setGpsMode] = useState<GpsMode>("car");
  const [gpsStatus, setGpsStatus] = useState<"idle" | "locating" | "ready" | "denied">("idle");
  const [truckProfile, setTruckProfile] = useState({
    heightM: "4.15",
    widthM: "2.60",
    lengthM: "21.0",
    weightKg: "36000",
    axles: "5",
    hazmat: false,
  });
  const [currentPosition, setCurrentPosition] = useState<{ lat: number; lon: number } | null>(null);
  const [destination, setDestination] = useState("");
  const [routeStatus, setRouteStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [routeSummary, setRouteSummary] = useState<{ distanceKm: number; durationMin: number; destination: string; warning?: string } | null>(null);
  const [roadStatus, setRoadStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [roadCounts, setRoadCounts] = useState({ cameras: 0, inspections: 0, restAreas: 0, events: 0 });
  const hostRef = useRef<HTMLDivElement | null>(null);
  const viewerRef = useRef<Cesium.Viewer | null>(null);
  const layersRef = useRef<{
    aircraft: Cesium.PointPrimitiveCollection;
    ships: Cesium.PointPrimitiveCollection;
    sats: Cesium.PointPrimitiveCollection;
    wx: Cesium.PointPrimitiveCollection;
    storms: Cesium.PointPrimitiveCollection;
    cams: Cesium.PointPrimitiveCollection;
    gps: Cesium.PointPrimitiveCollection;
    road: Cesium.PointPrimitiveCollection;
    route: Cesium.PolylineCollection;
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
  const [showStorms, setShowStorms] = useState(true);
  const [showCams, setShowCams] = useState(true);
  const [showHeat, setShowHeat] = useState(true);
  const [shipClass, setShipClass] = useState("all");
  const [shipCountry, setShipCountry] = useState("all");
  const [aircraftClass, setAircraftClass] = useState("all");
  const [satCountry, setSatCountry] = useState("all");
  const [satFunction, setSatFunction] = useState("all");
  const [satOrbit, setSatOrbit] = useState("all");

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
  const [leftPct, setLeftPct] = useState(66);
  const [fullscreen, setFullscreen] = useState<null | "globe" | "lab">(null);
  const dragRef = useRef<{ startX: number; startPct: number; width: number } | null>(null);

  const snap = useMemo(() => {
    if (replayEnabled && replayFrames.length) {
      const idx = Cesium.Math.clamp(replayIdx, 0, replayFrames.length - 1);
      return replayFrames[idx] ?? null;
    }
    return liveSnap;
  }, [liveSnap, replayFrames, replayEnabled, replayIdx]);

  useEffect(() => {
    let active = true;
    fetch(`${apiBase}/auth/status`, { credentials: "same-origin" })
      .then((response) => response.json())
      .then((payload) => {
        if (!active) return;
        setAuthConfigured(Boolean(payload.configured));
        setOwnerAccessConfigured(Boolean(payload.owner_access_configured));
        setMember(payload.authenticated ? (payload.member ?? {}) : null);
      })
      .catch(() => {
        if (active) setAuthConfigured(false);
      });
    return () => { active = false; };
  }, []);

  const counts = useMemo(() => {
    const layers = snap?.layers ?? {};
    return {
      aircraft: (layers.aircraft ?? []).length,
      satellites: (layers.satellites ?? []).length,
      ships: (layers.ships ?? []).length,
      cameras: (layers.cameras ?? []).length,
      weather: (layers.weather ?? []).length,
      storms: (layers.storms ?? []).length,
      updated: String(snap?.meta?.updated_at ?? "—"),
    };
  }, [snap]);

  const unavailableSources = useMemo(() => {
    if (!snap) return [] as string[];
    return [
      [counts.aircraft, lang === "fr" ? "avions" : "aircraft"],
      [counts.ships, lang === "fr" ? "navires" : "vessels"],
      [counts.weather, lang === "fr" ? "météo" : "weather"],
      [counts.cameras, lang === "fr" ? "caméras" : "cameras"],
    ].filter(([count]) => Number(count) === 0).map(([, label]) => String(label));
  }, [counts, lang, snap]);

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
        url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        credit: new Cesium.Credit("Earth imagery © Esri and imagery partners", false),
      }),
    );

    const primitives = viewer.scene.primitives;
    const aircraft = primitives.add(new Cesium.PointPrimitiveCollection());
    const ships = primitives.add(new Cesium.PointPrimitiveCollection());
    const sats = primitives.add(new Cesium.PointPrimitiveCollection());
    const wx = primitives.add(new Cesium.PointPrimitiveCollection());
    const storms = primitives.add(new Cesium.PointPrimitiveCollection());
    const cams = primitives.add(new Cesium.PointPrimitiveCollection());
    const gps = primitives.add(new Cesium.PointPrimitiveCollection());
    const road = primitives.add(new Cesium.PointPrimitiveCollection());
    const route = primitives.add(new Cesium.PolylineCollection());
    const trails = primitives.add(new Cesium.PolylineCollection());

    layersRef.current = { aircraft, ships, sats, wx, storms, cams, gps, road, route, trails, heat: viewer.entities };

    viewer.camera.setView({
      destination: Cesium.Cartesian3.fromDegrees(-15, 25, 18_000_000),
    });

    viewerRef.current = viewer;
    const clickHandler = new Cesium.ScreenSpaceEventHandler(viewer.scene.canvas);
    clickHandler.setInputAction((movement: { position: Cesium.Cartesian2 }) => {
      const picked = viewer.scene.pick(movement.position) as { id?: unknown; primitive?: { id?: unknown } } | undefined;
      const value = picked?.id ?? picked?.primitive?.id;
      if (value && typeof value === "object" && "kind" in value && "data" in value) {
        setSelectedFeature(value as { kind: string; data: Record<string, unknown> });
      }
    }, Cesium.ScreenSpaceEventType.LEFT_CLICK);
    return () => {
      clickHandler.destroy();
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
    L.storms.removeAll();
    L.cams.removeAll();
    L.trails.removeAll();
    L.heat.removeAll();

    const layers = snap.layers ?? {};

    if (showAircraft) {
      const mem = trailMemRef.current;
      const alive = new Set<string>();
      for (const a of layers.aircraft ?? []) {
        if (aircraftClass !== "all" && String(a.aircraft_class ?? "other") !== aircraftClass) continue;
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
          id: { kind: "aircraft", data: a },
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
        if (shipClass !== "all" && String(s.ship_class ?? "other") !== shipClass) continue;
        if (shipCountry !== "all" && String(s.country ?? "Other") !== shipCountry) continue;
        const lat = Number(s.lat);
        const lon = Number(s.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.ships.add({
          id: { kind: "ship", data: s },
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 5),
          color: Cesium.Color.fromCssColorString("#7af8d6").withAlpha(0.9),
          pixelSize: 3,
        });
      }
    }

    if (showSats) {
      for (const s of layers.satellites ?? []) {
        if (satCountry !== "all" && String(s.country ?? "Other") !== satCountry) continue;
        if (satFunction !== "all" && String(s.function ?? "Other") !== satFunction) continue;
        if (satOrbit !== "all" && String(s.orbit ?? "Other") !== satOrbit) continue;
        const lat = Number(s.lat);
        const lon = Number(s.lon);
        const altKm = Number(s.alt_km ?? 400);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        const h = Math.max(250_000, altKm * 1000);
        L.sats.add({
          id: { kind: "satellite", data: s },
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
          id: { kind: "weather", data: w },
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 25_000),
          color: colorForTempC(t),
          pixelSize: 10,
        });
      }
    }

    if (showStorms) {
      for (const storm of layers.storms ?? []) {
        const lat = Number(storm.lat);
        const lon = Number(storm.lon);
        const wind = Number(storm.wind_kn ?? 0);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.storms.add({
          id: { kind: "storm", data: storm },
          position: Cesium.Cartesian3.fromDegrees(lon, lat, 55_000),
          color: wind >= 64 ? Cesium.Color.fromCssColorString("#ff477e") : Cesium.Color.fromCssColorString("#ffb703"),
          pixelSize: 13 + Cesium.Math.clamp(wind / 20, 0, 8),
          outlineColor: Cesium.Color.WHITE.withAlpha(0.85),
          outlineWidth: 2,
        });
      }
    }

    if (showCams) {
      for (const c of layers.cameras ?? []) {
        const lat = Number(c.lat);
        const lon = Number(c.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        L.cams.add({
          id: { kind: "camera", data: c },
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
  }, [snap, showAircraft, showSats, showShips, showWeather, showStorms, showCams, showHeat, aircraftClass, shipClass, shipCountry, satCountry, satFunction, satOrbit]);

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

  const locateUser = useCallback(() => {
    if (!navigator.geolocation) {
      setGpsStatus("denied");
      return;
    }
    setGpsStatus("locating");
    navigator.geolocation.getCurrentPosition(
      ({ coords }) => {
        const viewer = viewerRef.current;
        if (!viewer) return;
        const gps = layersRef.current?.gps;
        if (!gps) return;
        gps.removeAll();
        gps.add({
          position: Cesium.Cartesian3.fromDegrees(coords.longitude, coords.latitude, 50),
          pixelSize: 15,
          color: Cesium.Color.fromCssColorString("#7af8d6"),
          outlineColor: Cesium.Color.WHITE,
          outlineWidth: 3,
        });
        viewer.camera.flyTo({
          destination: Cesium.Cartesian3.fromDegrees(coords.longitude, coords.latitude, 450_000),
          duration: 1.8,
        });
        setCurrentPosition({ lat: coords.latitude, lon: coords.longitude });
        setGpsStatus("ready");
      },
      () => setGpsStatus("denied"),
      { enableHighAccuracy: true, timeout: 12_000, maximumAge: 60_000 },
    );
  }, [lang]);

  const calculateNavigation = useCallback(async () => {
    if (authConfigured && !member) {
      setShowMemberAccess(true);
      return;
    }
    if (!currentPosition) {
      locateUser();
      setRouteStatus("error");
      return;
    }
    if (destination.trim().length < 3) {
      setRouteStatus("error");
      return;
    }
    setRouteStatus("loading");
    setRouteSummary(null);
    try {
      const searchResponse = await fetch(`${apiBase}/navigation/search?q=${encodeURIComponent(destination.trim())}`);
      const searchPayload = await searchResponse.json();
      const match = searchPayload.results?.[0];
      if (!searchResponse.ok || !match) throw new Error("Destination not found");
      const params = new URLSearchParams({
        origin_lat: String(currentPosition.lat),
        origin_lon: String(currentPosition.lon),
        destination_lat: String(match.lat),
        destination_lon: String(match.lon),
        mode: gpsMode,
      });
      const routeResponse = await fetch(`${apiBase}/navigation/route?${params.toString()}`);
      const routePayload = await routeResponse.json();
      if (!routeResponse.ok || !routePayload.geometry?.coordinates) throw new Error("Route unavailable");
      const points = routePayload.geometry.coordinates.map(([lon, lat]: [number, number]) =>
        Cesium.Cartesian3.fromDegrees(lon, lat, 80),
      );
      const routeLayer = layersRef.current?.route;
      routeLayer?.removeAll();
      routeLayer?.add({
        positions: points,
        width: 5,
        material: Cesium.Material.fromType("Color", { color: Cesium.Color.fromCssColorString("#7af8d6") }),
      });
      const viewer = viewerRef.current;
      if (viewer && points.length) {
        viewer.camera.flyTo({
          destination: Cesium.Cartesian3.fromDegrees(
            (currentPosition.lon + Number(match.lon)) / 2,
            (currentPosition.lat + Number(match.lat)) / 2,
            Math.max(250_000, Number(routePayload.distance_m || 0) * 1.7),
          ),
          duration: 1.8,
        });
        viewer.scene.requestRender();
      }
      setRouteSummary({
        distanceKm: Number(routePayload.distance_m || 0) / 1000,
        durationMin: Number(routePayload.duration_s || 0) / 60,
        destination: String(match.label || destination),
        warning: routePayload.warning || undefined,
      });
      setRouteStatus("ready");
    } catch {
      setRouteStatus("error");
    }
  }, [authConfigured, currentPosition, destination, gpsMode, locateUser, member]);

  useEffect(() => {
    if (!currentPosition || (authConfigured && !member)) return;
    let active = true;
    setRoadStatus("loading");
    const load = async () => {
      try {
        const params = new URLSearchParams({
          lat: String(currentPosition.lat),
          lon: String(currentPosition.lon),
          radius_km: "150",
        });
        const response = await fetch(`${apiBase}/navigation/nearby?${params.toString()}`);
        const payload = await response.json();
        if (!response.ok) throw new Error("Road data unavailable");
        if (!active) return;
        const layers = payload.layers ?? {};
        const cameras = Array.isArray(layers.cameras) ? layers.cameras : [];
        const inspections = Array.isArray(layers.inspection_stations) ? layers.inspection_stations : [];
        const restAreas = Array.isArray(layers.truck_rest_areas) ? layers.truck_rest_areas : [];
        const events = Array.isArray(layers.events) ? layers.events : [];
        setRoadCounts({ cameras: cameras.length, inspections: inspections.length, restAreas: restAreas.length, events: events.length });
        const road = layersRef.current?.road;
        road?.removeAll();
        const addRows = (rows: Array<Record<string, unknown>>, color: string, size: number) => {
          rows.slice(0, 150).forEach((item) => {
            const lat = Number(item.Latitude ?? item.latitude ?? item.lat);
            const lon = Number(item.Longitude ?? item.longitude ?? item.lon);
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
            road?.add({
              position: Cesium.Cartesian3.fromDegrees(lon, lat, 70),
              pixelSize: size,
              color: Cesium.Color.fromCssColorString(color),
              outlineColor: Cesium.Color.fromCssColorString("#07101d"),
              outlineWidth: 2,
            });
          });
        };
        addRows(cameras, "#4fd1ff", 8);
        addRows(inspections, "#ffd166", 11);
        addRows(restAreas, "#7af8d6", 9);
        addRows(events, "#ff6b7a", 10);
        viewerRef.current?.scene.requestRender();
        setRoadStatus("ready");
      } catch {
        if (active) setRoadStatus("error");
      }
    };
    void load();
    return () => { active = false; };
  }, [authConfigured, currentPosition, member]);

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
          <div className="gaios-brand">ALGOSPHERE <span>GLOBAL</span></div>
          <button type="button" className="gaios-subscribe-btn" onClick={() => setShowSubscriptions(true)}>
            {lang === "fr" ? "Abonnements" : "Pricing"}
          </button>
          <button type="button" className="gaios-member-btn" onClick={() => member ? setShowAccount(true) : setShowMemberAccess(true)}>
            {member ? (lang === "fr" ? "Mon compte" : "My account") : (lang === "fr" ? "Accès membre" : "Member access")}
          </button>
          <button type="button" className="gaios-social-btn" onClick={() => member ? setShowSocial(true) : setShowMemberAccess(true)}>
            {lang === "fr" ? "Communauté" : "Community"}
          </button>
          <button type="button" className="gaios-lang-btn" onClick={() => setLang((value) => value === "fr" ? "en" : "fr")}>
            {lang === "fr" ? "EN" : "FR"}
          </button>
          <button type="button" className="gaios-gps-btn" onClick={() => setShowGpsPanel(true)}>
            {gpsStatus === "locating"
              ? (lang === "fr" ? "Localisation…" : "Locating…")
              : gpsStatus === "ready"
                ? (lang === "fr" ? "GPS / Navigation" : "GPS / Navigation")
                : (lang === "fr" ? "GPS / Navigation" : "GPS / Navigation")}
          </button>
          {gpsStatus === "denied" ? (
            <span className="gps-error">{lang === "fr" ? "Autorisez la localisation dans le navigateur." : "Allow location access in your browser."}</span>
          ) : null}
          <button
            type="button"
            className={fullscreen === "globe" ? "gaios-fs-btn gaios-fs-on" : "gaios-fs-btn"}
            onClick={() => setFullscreen((f) => (f === "globe" ? null : "globe"))}
          >
            {fullscreen === "globe" ? (lang === "fr" ? "Quitter le plein écran" : "Exit fullscreen") : (lang === "fr" ? "Globe plein écran" : "Globe fullscreen")}
          </button>
          <button
            type="button"
            className={fullscreen === "lab" ? "gaios-fs-btn gaios-fs-on" : "gaios-fs-btn"}
            onClick={() => setFullscreen((f) => (f === "lab" ? null : "lab"))}
          >
            {fullscreen === "lab" ? (lang === "fr" ? "Quitter le plein écran" : "Exit fullscreen") : (lang === "fr" ? "Intelligence plein écran" : "Intelligence fullscreen")}
          </button>
        </div>
        <div className={`gaios-panel gaios-panel--dock ${authConfigured && !member ? "is-premium-locked" : ""}`}>
          <div className="gaios-title">
            <h1>ALGOSPHERE GLOBAL</h1>
            <span>{lang === "fr" ? "Intelligence géospatiale unifiée · données mondiales vérifiées" : "Unified geospatial intelligence · verified global data"}</span>
          </div>

          <div className="grid">
            {unavailableSources.length ? (
              <div className="source-status-banner" role="status">
                <strong>{lang === "fr" ? "Certaines sources sont temporairement indisponibles" : "Some sources are temporarily unavailable"}</strong>
                <span>{unavailableSources.join(", ")}. {lang === "fr" ? "Les autres couches continuent de fonctionner; aucune donnée simulée ne remplace la source." : "Other layers remain available; simulated data never replaces the source."}</span>
              </div>
            ) : null}
            <div className="stat">
              <label>{lang === "fr" ? "Avions" : "Aircraft"}</label>
              <strong>{availabilityCount(counts.aircraft, lang)}</strong>
            </div>
            <div className="stat">
              <label>Satellites</label>
              <strong>{availabilityCount(counts.satellites, lang)}</strong>
            </div>
            <div className="stat">
              <label>{lang === "fr" ? "Navires" : "Ships"}</label>
              <strong>{availabilityCount(counts.ships, lang)}</strong>
            </div>
            <div className="stat">
              <label>{lang === "fr" ? "Météo" : "Weather"}</label>
              <strong>{availabilityCount(counts.weather, lang)}</strong>
            </div>
            <div className="stat">
              <label>{lang === "fr" ? "Tempêtes actives" : "Active storms"}</label>
              <strong>{counts.storms}</strong>
            </div>
            <div className="stat">
              <label>{lang === "fr" ? "Caméras" : "Cameras"}</label>
              <strong>{availabilityCount(counts.cameras, lang)}</strong>
            </div>
            <div className="stat">
              <label>Updated</label>
              <strong style={{ fontSize: 12, color: "var(--muted)" }}>{counts.updated}</strong>
            </div>
          </div>

          <div className="row">
            <label className="toggle">
              <input type="checkbox" checked={showAircraft} onChange={(e) => setShowAircraft(e.target.checked)} />
              {lang === "fr" ? "Avions" : "Aircraft"}
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showSats} onChange={(e) => setShowSats(e.target.checked)} />
              Satellites
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showShips} onChange={(e) => setShowShips(e.target.checked)} />
              {lang === "fr" ? "Navires" : "Ships"}
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showWeather} onChange={(e) => setShowWeather(e.target.checked)} />
              {lang === "fr" ? "Météo" : "Weather"}
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showStorms} onChange={(e) => setShowStorms(e.target.checked)} />
              {lang === "fr" ? "Tempêtes et ouragans" : "Storms & hurricanes"}
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showCams} onChange={(e) => setShowCams(e.target.checked)} />
              {lang === "fr" ? "Caméras" : "Cameras"}
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showHeat} onChange={(e) => setShowHeat(e.target.checked)} />
              {lang === "fr" ? "Zones d’activité" : "Activity zones"}
            </label>
          </div>
          <div className={`layer-filters ${authConfigured && !member ? "layer-filters--locked" : ""}`} onClick={() => { if (authConfigured && !member) setShowSubscriptions(true); }}>
            <label>{lang === "fr" ? "Navires" : "Ships"}<select value={shipClass} disabled={authConfigured && !member} onChange={(e) => setShipClass(e.target.value)}><option value="all">{lang === "fr" ? "Tous les types" : "All types"}</option><option value="cargo">Cargo</option><option value="tanker">{lang === "fr" ? "Pétrolier" : "Tanker"}</option><option value="passenger">{lang === "fr" ? "Passagers" : "Passenger"}</option><option value="fishing">{lang === "fr" ? "Pêche" : "Fishing"}</option><option value="government">{lang === "fr" ? "Gouvernemental" : "Government"}</option></select></label>
            <label>{lang === "fr" ? "Pavillon" : "Flag"}<select value={shipCountry} disabled={authConfigured && !member} onChange={(e) => setShipCountry(e.target.value)}><option value="all">{lang === "fr" ? "Tous les pays" : "All countries"}</option><option value="Canada">Canada</option><option value="USA">USA</option></select></label>
            <label>{lang === "fr" ? "Avions" : "Aircraft"}<select value={aircraftClass} disabled={authConfigured && !member} onChange={(e) => setAircraftClass(e.target.value)}><option value="all">{lang === "fr" ? "Tous les types" : "All types"}</option><option value="commercial">Commercial</option><option value="cargo">Cargo</option><option value="private">{lang === "fr" ? "Privé" : "Private"}</option><option value="emergency">{lang === "fr" ? "Urgence" : "Emergency"}</option><option value="government">{lang === "fr" ? "Gouvernemental" : "Government"}</option></select></label>
            <label>{lang === "fr" ? "Satellite · pays" : "Satellite · country"}<input disabled={authConfigured && !member} value={satCountry === "all" ? "" : satCountry} placeholder={lang === "fr" ? "Tous" : "All"} onChange={(e) => setSatCountry(e.target.value.trim() || "all")} /></label>
            <label>{lang === "fr" ? "Fonction" : "Function"}<input disabled={authConfigured && !member} value={satFunction === "all" ? "" : satFunction} placeholder={lang === "fr" ? "Toutes" : "All"} onChange={(e) => setSatFunction(e.target.value.trim() || "all")} /></label>
            <label>{lang === "fr" ? "Orbite" : "Orbit"}<select value={satOrbit} disabled={authConfigured && !member} onChange={(e) => setSatOrbit(e.target.value)}><option value="all">{lang === "fr" ? "Toutes" : "All"}</option><option value="LEO">LEO</option><option value="MEO">MEO</option><option value="GEO">GEO</option></select></label>
            {authConfigured && !member ? <strong>{lang === "fr" ? "Filtres premium — abonnement requis" : "Premium filters — subscription required"}</strong> : null}
          </div>

          <div className="customer-home">
            <button type="button" className="customer-action" onClick={() => setShowGpsPanel(true)}>
              <span className="customer-action-icon">⌖</span>
              <span>
                <strong>{lang === "fr" ? "Navigation GPS" : "GPS navigation"}</strong>
                <small>{lang === "fr" ? "Auto, camion, routes et services" : "Car, truck, routes and services"}</small>
              </span>
            </button>
            <button type="button" className="customer-action" onClick={() => setFullscreen("globe")}>
              <span className="customer-action-icon">◎</span>
              <span>
                <strong>{lang === "fr" ? "Explorer le monde" : "Explore the world"}</strong>
                <small>{lang === "fr" ? "Globe plein écran et couches en direct" : "Fullscreen globe and live layers"}</small>
              </span>
            </button>
            <button type="button" className="customer-action" onClick={() => member ? setShowAccount(true) : setShowSubscriptions(true)}>
              <span className="customer-action-icon">★</span>
              <span>
                <strong>{member ? (lang === "fr" ? "Mon abonnement" : "My membership") : (lang === "fr" ? "Débloquer AlgoSphere" : "Unlock AlgoSphere")}</strong>
                <small>{lang === "fr" ? "Favoris, alertes et historique" : "Favorites, alerts and history"}</small>
              </span>
            </button>
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
                  <span className="inteltxt">{publicGeospatialText(it.text)}</span>
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
              {((snap?.meta?.reasoning_trace as string[] | undefined) ?? []).filter(isPublicGeospatialTrace).map((ln, idx) => (
                <div key={`${idx}-${ln.slice(0, 20)}`} className="traceln">
                  {publicGeospatialText(ln)}
                </div>
              ))}
              {((snap?.meta?.reasoning_trace as string[] | undefined) ?? []).filter(isPublicGeospatialTrace).length === 0 ? (
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
      {showGpsPanel ? (
        <div className="gps-backdrop" role="presentation" onMouseDown={() => setShowGpsPanel(false)}>
          <section
            className="gps-panel"
            role="dialog"
            aria-modal="true"
            aria-label={lang === "fr" ? "Navigation GPS" : "GPS navigation"}
            onMouseDown={(event) => event.stopPropagation()}
          >
            <button type="button" className="subscription-close" onClick={() => setShowGpsPanel(false)} aria-label={lang === "fr" ? "Fermer" : "Close"}>×</button>
            <div className="subscription-kicker">ALGOSPHERE NAV</div>
            <h2>{lang === "fr" ? "GPS Canada–États-Unis" : "Canada–United States GPS"}</h2>
            <p className="subscription-lead">
              {lang === "fr"
                ? "Navigation quotidienne avec un profil camion supplémentaire. Le globe et toutes les couches géospatiales restent disponibles."
                : "Everyday navigation with an additional truck profile. The globe and all geospatial layers remain available."}
            </p>

            <div className="gps-mode-tabs">
              <button type="button" className={gpsMode === "car" ? "is-active" : ""} onClick={() => setGpsMode("car")}>
                {lang === "fr" ? "Auto / normal" : "Car / standard"}
              </button>
              <button type="button" className={gpsMode === "truck" ? "is-active" : ""} onClick={() => setGpsMode("truck")}>
                {lang === "fr" ? "Camion" : "Truck"}
              </button>
            </div>

            <div className="gps-actions">
              <button type="button" className="subscription-cta" onClick={locateUser} disabled={gpsStatus === "locating"}>
                {gpsStatus === "locating"
                  ? (lang === "fr" ? "Localisation…" : "Locating…")
                  : (lang === "fr" ? "Afficher ma position sur le globe" : "Show my position on the globe")}
              </button>
            </div>

            <label className="gps-destination">
              {lang === "fr" ? "Destination (Canada ou États-Unis)" : "Destination (Canada or United States)"}
              <input
                value={destination}
                onChange={(event) => setDestination(event.target.value)}
                placeholder={lang === "fr" ? "Adresse, ville ou lieu" : "Address, city or place"}
              />
            </label>
            <button type="button" className="subscription-cta gps-route-btn" onClick={calculateNavigation} disabled={routeStatus === "loading"}>
              {routeStatus === "loading"
                ? (lang === "fr" ? "Calcul de l’itinéraire…" : "Calculating route…")
                : (lang === "fr" ? "Calculer et afficher l’itinéraire" : "Calculate and show route")}
            </button>
            {routeStatus === "error" ? (
              <p className="member-error">
                {lang === "fr" ? "Activez votre position et vérifiez la destination." : "Enable your position and check the destination."}
              </p>
            ) : null}
            {routeSummary ? (
              <div className="gps-route-summary">
                <strong>{routeSummary.distanceKm.toFixed(1)} km · {Math.round(routeSummary.durationMin)} min</strong>
                <span>{routeSummary.destination}</span>
                {routeSummary.warning ? <em>{lang === "fr" ? "Profil camion consultatif : respectez toujours la signalisation." : routeSummary.warning}</em> : null}
              </div>
            ) : null}
            <div className="gps-road-status">
              <strong>
                {roadStatus === "loading"
                  ? (lang === "fr" ? "Chargement des services routiers…" : "Loading road services…")
                  : (lang === "fr" ? "Services près de votre position" : "Services near your position")}
              </strong>
              <span>📷 {roadCounts.cameras} · ⚖ {roadCounts.inspections} · 🅿 {roadCounts.restAreas} · ⚠ {roadCounts.events}</span>
              {roadStatus === "error" ? <em>{lang === "fr" ? "Source routière temporairement indisponible." : "Road source temporarily unavailable."}</em> : null}
            </div>

            {gpsMode === "car" ? (
              <div className="gps-feature-grid">
                <article><strong>{lang === "fr" ? "Navigation normale" : "Standard navigation"}</strong><span>{lang === "fr" ? "Auto, moto et déplacements personnels." : "Car, motorcycle and personal travel."}</span></article>
                <article><strong>{lang === "fr" ? "Trafic et météo" : "Traffic and weather"}</strong><span>{lang === "fr" ? "Incidents, travaux, conditions et caméras autorisées." : "Incidents, roadwork, conditions and approved cameras."}</span></article>
                <article><strong>{lang === "fr" ? "Favoris et alertes" : "Favorites and alerts"}</strong><span>{lang === "fr" ? "Enregistrement des lieux et avis importants." : "Saved places and important notices."}</span></article>
              </div>
            ) : (
              <>
                <div className="truck-profile-grid">
                  <label>{lang === "fr" ? "Hauteur (m)" : "Height (m)"}<input value={truckProfile.heightM} onChange={(e) => setTruckProfile({ ...truckProfile, heightM: e.target.value })} inputMode="decimal" /></label>
                  <label>{lang === "fr" ? "Largeur (m)" : "Width (m)"}<input value={truckProfile.widthM} onChange={(e) => setTruckProfile({ ...truckProfile, widthM: e.target.value })} inputMode="decimal" /></label>
                  <label>{lang === "fr" ? "Longueur (m)" : "Length (m)"}<input value={truckProfile.lengthM} onChange={(e) => setTruckProfile({ ...truckProfile, lengthM: e.target.value })} inputMode="decimal" /></label>
                  <label>{lang === "fr" ? "Poids total (kg)" : "Gross weight (kg)"}<input value={truckProfile.weightKg} onChange={(e) => setTruckProfile({ ...truckProfile, weightKg: e.target.value })} inputMode="numeric" /></label>
                  <label>{lang === "fr" ? "Essieux" : "Axles"}<input value={truckProfile.axles} onChange={(e) => setTruckProfile({ ...truckProfile, axles: e.target.value })} inputMode="numeric" /></label>
                  <label className="truck-check"><input type="checkbox" checked={truckProfile.hazmat} onChange={(e) => setTruckProfile({ ...truckProfile, hazmat: e.target.checked })} />{lang === "fr" ? "Matières dangereuses" : "Hazardous materials"}</label>
                </div>
                <div className="gps-feature-grid">
                  <article><strong>{lang === "fr" ? "Gabarit et poids" : "Clearance and weight"}</strong><span>{lang === "fr" ? "Ponts bas, limites de charge et routes interdites." : "Low bridges, load limits and restricted roads."}</span></article>
                  <article><strong>{lang === "fr" ? "Balances et frontières" : "Scales and borders"}</strong><span>{lang === "fr" ? "Postes d’inspection et attentes commerciales Canada–USA." : "Inspection stations and Canada–US commercial waits."}</span></article>
                  <article><strong>{lang === "fr" ? "Services camion" : "Truck services"}</strong><span>{lang === "fr" ? "Stationnements, haltes, carburant et conditions routières." : "Parking, rest areas, fuel and road conditions."}</span></article>
                </div>
              </>
            )}
            <p className="gps-safety">
              {lang === "fr"
                ? "Déploiement progressif avec données publiques vérifiées. Les panneaux routiers et consignes officielles demeurent prioritaires."
                : "Progressive rollout using verified public data. Posted road signs and official directions always take priority."}
            </p>
          </section>
        </div>
      ) : null}
      {showMemberAccess ? (
        <MemberAccessPanel
          lang={lang}
          configured={authConfigured}
          ownerAccessConfigured={ownerAccessConfigured}
          onAuthenticated={setMember}
          onClose={() => setShowMemberAccess(false)}
          onSubscribe={() => {
            setShowMemberAccess(false);
            setShowSubscriptions(true);
          }}
        />
      ) : null}
      {selectedFeature ? (
        <div className="feature-backdrop" role="presentation" onMouseDown={() => setSelectedFeature(null)}>
          <section className="feature-card" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
            <button type="button" className="subscription-close" onClick={() => setSelectedFeature(null)}>×</button>
            <div className="subscription-kicker">{selectedFeature.kind.toUpperCase()}</div>
            <h2>{String(selectedFeature.data.label || selectedFeature.data.callsign || selectedFeature.data.registration || selectedFeature.data.id || (lang === "fr" ? "Information en direct" : "Live information"))}</h2>
            {selectedFeature.kind === "camera" && selectedFeature.data.preview_url ? (
              <img className="feature-camera-preview" src={String(selectedFeature.data.preview_url)} alt={String(selectedFeature.data.label || "Camera")} />
            ) : null}
            {selectedFeature.kind === "camera" && selectedFeature.data.stream_url ? (
              <iframe className="feature-camera-frame" src={String(selectedFeature.data.stream_url)} title={String(selectedFeature.data.label || "Live camera")} allow="autoplay; fullscreen" loading="lazy" />
            ) : null}
            <dl className="feature-details">
              {Object.entries(selectedFeature.data)
                .filter(([key, value]) => value !== null && value !== undefined && !["preview_url", "stream_url", "info_url", "importance", "license"].includes(key))
                .slice(0, 18)
                .map(([key, value]) => <div key={key}><dt>{key.replaceAll("_", " ")}</dt><dd>{typeof value === "boolean" ? (value ? (lang === "fr" ? "Oui" : "Yes") : "No") : String(value)}</dd></div>)}
            </dl>
            {selectedFeature.kind === "camera" && selectedFeature.data.info_url ? (
              <a className="subscription-cta feature-open-link" href={String(selectedFeature.data.info_url)} target="_blank" rel="noreferrer">
                {lang === "fr" ? "Ouvrir la caméra chez le fournisseur" : "Open camera at provider"}
              </a>
            ) : null}
            <p className="feature-source">{lang === "fr" ? "Source" : "Source"} : {String(selectedFeature.data.source || "AlgoSphere")}</p>
          </section>
        </div>
      ) : null}
      {showAccount && member ? <AccountPanel lang={lang} member={member} onClose={() => setShowAccount(false)} onLogout={() => { setMember(null); setShowAccount(false); }} /> : null}
      {showSocial && member ? <SocialPanel
        lang={lang}
        currentPosition={currentPosition}
        onLocate={locateUser}
        onFocusPerson={(lat, lon) => {
          layersRef.current?.gps.add({
            position: Cesium.Cartesian3.fromDegrees(lon, lat, 180),
            pixelSize: 17,
            color: Cesium.Color.fromCssColorString("#66f2d5"),
            outlineColor: Cesium.Color.WHITE,
            outlineWidth: 3,
          });
          viewerRef.current?.camera.flyTo({
            destination: Cesium.Cartesian3.fromDegrees(lon, lat, 1800000),
            duration: 1.5,
          });
          setShowSocial(false);
        }}
        onClose={() => setShowSocial(false)}
      /> : null}
      {showSubscriptions ? <SubscriptionPanel lang={lang} onClose={() => setShowSubscriptions(false)} /> : null}
    </div>
  );
}
