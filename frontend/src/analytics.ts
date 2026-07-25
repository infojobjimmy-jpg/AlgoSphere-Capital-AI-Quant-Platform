const CONSENT_KEY = "algosphere_analytics_consent";
const SESSION_KEY = "algosphere_analytics_session";

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
      properties: options.properties ?? {},
    }),
  }).catch(() => undefined);
}
