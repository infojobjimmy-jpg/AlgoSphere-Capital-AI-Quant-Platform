import { useEffect, useState } from "react";

type Props = {
  lang: "fr" | "en";
  member: Record<string, unknown>;
  onClose: () => void;
  onLogout: () => void;
};

type Preferences = { favorites: Array<{ name: string }>; alerts: Array<{ name: string; enabled: boolean }> };
type AnalyticsSummary = {
  totals: { visitors_7d: number; checkout_clicks_7d: number; checkout_rate_7d: number; logins_7d: number; visitors_30d: number };
  campaigns: Array<{ campaign: string; source: string; visitors: number; checkout_clicks: number }>;
};

export default function AccountPanel({ lang, member, onClose, onLogout }: Props) {
  const fr = lang === "fr";
  const [preferences, setPreferences] = useState<Preferences>({ favorites: [], alerts: [] });
  const [favorite, setFavorite] = useState("");
  const [alertName, setAlertName] = useState("");
  const [saved, setSaved] = useState(false);
  const [currentOwnerCode, setCurrentOwnerCode] = useState("");
  const [newOwnerCode, setNewOwnerCode] = useState("");
  const [ownerMessage, setOwnerMessage] = useState("");
  const [analytics, setAnalytics] = useState<AnalyticsSummary | null>(null);

  useEffect(() => {
    fetch("/api/account/preferences", { credentials: "same-origin" })
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((value) => setPreferences({
        favorites: Array.isArray(value.favorites) ? value.favorites : [],
        alerts: Array.isArray(value.alerts) ? value.alerts : [],
      }))
      .catch(() => undefined);
  }, []);

  useEffect(() => {
    if (member.role !== "owner") return;
    fetch("/api/analytics/summary", { credentials: "same-origin" })
      .then((response) => response.ok ? response.json() : Promise.reject())
      .then((payload) => setAnalytics(payload as AnalyticsSummary))
      .catch(() => undefined);
  }, [member.role]);

  const save = async (next: Preferences) => {
    setPreferences(next);
    const response = await fetch("/api/account/preferences", {
      method: "POST", credentials: "same-origin",
      headers: { "Content-Type": "application/json" }, body: JSON.stringify(next),
    });
    setSaved(response.ok);
  };

  const logout = async () => {
    await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
    onLogout();
  };

  const changeOwnerCode = async () => {
    setOwnerMessage("");
    const response = await fetch("/api/auth/owner/change-code", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ current_code: currentOwnerCode, new_code: newOwnerCode }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      setOwnerMessage(String(payload.detail || (fr ? "Changement impossible." : "Unable to change code.")));
      return;
    }
    setOwnerMessage(fr ? "Code changé. Reconnectez-vous avec le nouveau code." : "Code changed. Sign in again with the new code.");
    setCurrentOwnerCode("");
    setNewOwnerCode("");
    onLogout();
  };

  return (
    <div className="gps-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="member-panel account-panel" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <button type="button" className="subscription-close" onClick={onClose}>×</button>
        <div className="subscription-kicker">ALGOSPHERE MEMBER</div>
        <h2>{fr ? "Mon compte" : "My account"}</h2>
        <div className="account-summary">
          <span><small>{fr ? "Forfait" : "Plan"}</small><strong>{member.role === "owner" ? (fr ? "AlgoSphere Global — Propriétaire" : "AlgoSphere Global — Owner") : String(member.product_name || member.product_id || "AlgoSphere")}</strong></span>
          <span><small>{fr ? "État" : "Status"}</small><strong>{String(member.status || "active")}</strong></span>
          <span><small>{fr ? "Expiration / renouvellement" : "Expiration / renewal"}</small><strong>{member.role === "owner" ? (fr ? "Accès propriétaire" : "Owner access") : member.expires_at ? new Date(String(member.expires_at)).toLocaleDateString(fr ? "fr-CA" : "en-US") : (fr ? "Selon Whop" : "Managed by Whop")}</strong></span>
        </div>
        <h3>{fr ? "Mes favoris" : "My favorites"}</h3>
        <div className="account-add"><input value={favorite} onChange={(e) => setFavorite(e.target.value)} placeholder={fr ? "Lieu, navire, avion…" : "Place, ship, aircraft…"} /><button onClick={() => { if (favorite.trim()) { void save({ ...preferences, favorites: [...preferences.favorites, { name: favorite.trim() }] }); setFavorite(""); } }}>+</button></div>
        <div className="account-chips">{preferences.favorites.map((item, index) => <button key={`${item.name}-${index}`} onClick={() => void save({ ...preferences, favorites: preferences.favorites.filter((_, i) => i !== index) })}>{item.name} ×</button>)}</div>
        <h3>{fr ? "Mes alertes personnalisées" : "My custom alerts"}</h3>
        <div className="account-add"><input value={alertName} onChange={(e) => setAlertName(e.target.value)} placeholder={fr ? "Ex. cargo près de Montréal" : "E.g. cargo near Montreal"} /><button onClick={() => { if (alertName.trim()) { void save({ ...preferences, alerts: [...preferences.alerts, { name: alertName.trim(), enabled: true }] }); setAlertName(""); } }}>+</button></div>
        <div className="account-alerts">{preferences.alerts.map((item, index) => <label key={`${item.name}-${index}`}><input type="checkbox" checked={item.enabled} onChange={(e) => { const alerts = [...preferences.alerts]; alerts[index] = { ...item, enabled: e.target.checked }; void save({ ...preferences, alerts }); }} />{item.name}<button onClick={() => void save({ ...preferences, alerts: preferences.alerts.filter((_, i) => i !== index) })}>×</button></label>)}</div>
        {saved ? <p className="account-saved">{fr ? "Enregistré." : "Saved."}</p> : null}
        {member.role === "owner" ? (
          <>
          <section className="owner-analytics">
            <h3>{fr ? "Campagne pilote" : "Pilot campaign"}</h3>
            <div className="owner-metrics">
              <span><small>{fr ? "Visiteurs · 7 j" : "Visitors · 7d"}</small><strong>{analytics?.totals.visitors_7d ?? "—"}</strong></span>
              <span><small>{fr ? "Clics vers Whop" : "Whop clicks"}</small><strong>{analytics?.totals.checkout_clicks_7d ?? "—"}</strong></span>
              <span><small>{fr ? "Taux de clic" : "Click rate"}</small><strong>{analytics ? `${analytics.totals.checkout_rate_7d}%` : "—"}</strong></span>
              <span><small>{fr ? "Connexions" : "Logins"}</small><strong>{analytics?.totals.logins_7d ?? "—"}</strong></span>
            </div>
            {analytics?.campaigns.length ? (
              <div className="campaign-list">{analytics.campaigns.map((row) => (
                <span key={`${row.source}-${row.campaign}`}><b>{row.campaign}</b><small>{row.source} · {row.visitors} {fr ? "visites" : "visits"} · {row.checkout_clicks} clics</small></span>
              ))}</div>
            ) : <p>{fr ? "Les résultats apparaîtront ici dès les premières visites consenties." : "Results will appear here after the first consented visits."}</p>}
          </section>
          <section className="owner-security">
            <h3>{fr ? "Sécurité propriétaire" : "Owner security"}</h3>
            <p>{fr ? "Utilisez au moins 16 caractères uniques. Après le changement, l’ancien code ne permettra plus de nouvelle connexion." : "Use at least 16 unique characters. After the change, the old code can no longer start a new session."}</p>
            <input type="password" autoComplete="current-password" value={currentOwnerCode} onChange={(e) => setCurrentOwnerCode(e.target.value)} placeholder={fr ? "Code actuel" : "Current code"} />
            <input type="password" autoComplete="new-password" value={newOwnerCode} onChange={(e) => setNewOwnerCode(e.target.value)} placeholder={fr ? "Nouveau code" : "New code"} />
            <button className="subscription-cta" type="button" disabled={currentOwnerCode.length < 12 || newOwnerCode.length < 16} onClick={() => void changeOwnerCode()}>{fr ? "Changer le code" : "Change code"}</button>
            {ownerMessage ? <p className="account-saved">{ownerMessage}</p> : null}
          </section>
          </>
        ) : null}
        <div className="account-actions">
          <a className="subscription-cta" href={String(member.manage_url || "https://whop.com/hub")} target="_blank" rel="noreferrer">{fr ? "Gérer sur Whop" : "Manage on Whop"}</a>
          <button className="member-subscribe-link" onClick={() => void logout()}>{fr ? "Se déconnecter" : "Sign out"}</button>
        </div>
      </section>
    </div>
  );
}
