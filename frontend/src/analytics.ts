const CONSENT_KEY = "algosphere_analytics_consent";
const SESSION_KEY = "algosphere_analytics_session";
const VISITOR_SESSION_KEY = "acap_vsid";
const VISITOR_ID_KEY = "acap_vid";
const HEARTBEAT_INTERVAL_MS = 25_000;

export type ConsentValue = "accepted" | "declined" | null;

export function analyticsConsent(): ConsentValue {
  const value = localStorage.getItem(CONSENT_KEY);
  return value === "accepted" || value === "declined" ? value : null;
}

export function setAnalyticsConsent(value: Exclude<ConsentValue, null>): void {
  localStorage.setItem(CONSENT_KEY, value);
}

function sessionId(): string {
  let value = sessionStorage.getItem(SESSION_KEY);
  if (!value) {
    value = crypto.randomUUID();
    sessionStorage.setItem(SESSION_KEY, value);
  }
  return value;
}

function visitorSessionId(): string {
  let id = sessionStorage.getItem(VISITOR_SESSION_KEY);
  if (!id) {
    id = crypto.randomUUID().replace(/-/g, "");
    sessionStorage.setItem(VISITOR_SESSION_KEY, id);
  }
  return id;
}

function visitorId(): string {
  if (analyticsConsent() !== "accepted") return "";
  let id = localStorage.getItem(VISITOR_ID_KEY);
  if (!id) {
    id = crypto.randomUUID().replace(/-/g, "");
    localStorage.setItem(VISITOR_ID_KEY, id);
  }
  return id;
}

function deviceType(): string {
  const ua = navigator.userAgent.toLowerCase();
  if (/mobile|android|iphone|ipod/.test(ua)) return "mobile";
  if (/ipad|tablet/.test(ua)) return "tablet";
  return "desktop";
}

function campaignProperties(): Record<string, string> {
  const params = new URLSearchParams(window.location.search);
  const current = ["utm_source", "utm_medium", "utm_campaign", "utm_content"].reduce<Record<string, string>>((result, key) => {
    const value = params.get(key);
    if (value) result[key] = value.slice(0, 120);
    return result;
  }, {});
  if (Object.keys(current).length) sessionStorage.setItem("algosphere_campaign", JSON.stringify(current));
  try {
    return JSON.parse(sessionStorage.getItem("algosphere_campaign") || "{}") as Record<string, string>;
  } catch {
    return {};
  }
}

function referrerDomain(): string {
  if (!document.referrer) return "";
  try { return new URL(document.referrer).hostname; } catch { return ""; }
}

export function trackEvent(event: string, options: { plan?: string; properties?: Record<string, string | number | boolean | null> } = {}): void {
  if (analyticsConsent() !== "accepted") return;
  void fetch("/api/analytics/event", {
    method: "POST",
    credentials: "same-origin",
    keepalive: true,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event,
      path: window.location.pathname,
      plan: options.plan,
      session_id: sessionId(),
      properties: { ...campaignProperties(), ...(options.properties ?? {}) },
    }),
  }).catch(() => undefined);
}

// ---------------------------------------------------------------------------
// Heartbeat / presence
// ---------------------------------------------------------------------------

let _heartbeatTimer: ReturnType<typeof setInterval> | null = null;

export function sendHeartbeat(page?: string): void {
  const campaign = campaignProperties();
  void fetch("/api/analytics/heartbeat", {
    method: "POST",
    credentials: "same-origin",
    keepalive: true,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      visitor_session_id: visitorSessionId(),
      visitor_id: visitorId(),
      page: page || window.location.pathname,
      device_type: deviceType(),
      referrer_domain: referrerDomain(),
      utm_source: campaign.utm_source || "",
      utm_campaign: campaign.utm_campaign || "",
    }),
  }).catch(() => undefined);
}

export function initHeartbeat(getPage: () => string): void {
  sendHeartbeat(getPage());
  if (_heartbeatTimer !== null) clearInterval(_heartbeatTimer);
  _heartbeatTimer = setInterval(() => sendHeartbeat(getPage()), HEARTBEAT_INTERVAL_MS);
}

export function stopHeartbeat(): void {
  if (_heartbeatTimer !== null) {
    clearInterval(_heartbeatTimer);
    _heartbeatTimer = null;
  }
}
