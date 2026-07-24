import { useEffect, useState } from "react";

type Props = {
  lang: "fr" | "en";
  member: Record<string, unknown>;
  onClose: () => void;
  onLogout: () => void;
};

type Preferences = { favorites: Array<{ name: string }>; alerts: Array<{ name: string; enabled: boolean }> };

export default function AccountPanel({ lang, member, onClose, onLogout }: Props) {
  const fr = lang === "fr";
  const [preferences, setPreferences] = useState<Preferences>({ favorites: [], alerts: [] });
  const [favorite, setFavorite] = useState("");
  const [alertName, setAlertName] = useState("");
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    fetch("/api/account/preferences", { credentials: "same-origin" })
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((value) => setPreferences({
        favorites: Array.isArray(value.favorites) ? value.favorites : [],
        alerts: Array.isArray(value.alerts) ? value.alerts : [],
      }))
      .catch(() => undefined);
  }, []);

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

  return (
    <div className="gps-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="member-panel account-panel" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <button type="button" className="subscription-close" onClick={onClose}>×</button>
        <div className="subscription-kicker">ALGOSPHERE MEMBER</div>
        <h2>{fr ? "Mon compte" : "My account"}</h2>
        <div className="account-summary">
          <span><small>{fr ? "Forfait" : "Plan"}</small><strong>{String(member.product_name || member.product_id || "AlgoSphere")}</strong></span>
          <span><small>{fr ? "État" : "Status"}</small><strong>{String(member.status || "active")}</strong></span>
          <span><small>{fr ? "Expiration / renouvellement" : "Expiration / renewal"}</small><strong>{member.expires_at ? new Date(String(member.expires_at)).toLocaleDateString() : (fr ? "Selon Whop" : "Managed by Whop")}</strong></span>
        </div>
        <h3>{fr ? "Mes favoris" : "My favorites"}</h3>
        <div className="account-add"><input value={favorite} onChange={(e) => setFavorite(e.target.value)} placeholder={fr ? "Lieu, navire, avion…" : "Place, ship, aircraft…"} /><button onClick={() => { if (favorite.trim()) { void save({ ...preferences, favorites: [...preferences.favorites, { name: favorite.trim() }] }); setFavorite(""); } }}>+</button></div>
        <div className="account-chips">{preferences.favorites.map((item, index) => <button key={`${item.name}-${index}`} onClick={() => void save({ ...preferences, favorites: preferences.favorites.filter((_, i) => i !== index) })}>{item.name} ×</button>)}</div>
        <h3>{fr ? "Mes alertes personnalisées" : "My custom alerts"}</h3>
        <div className="account-add"><input value={alertName} onChange={(e) => setAlertName(e.target.value)} placeholder={fr ? "Ex. cargo près de Montréal" : "E.g. cargo near Montreal"} /><button onClick={() => { if (alertName.trim()) { void save({ ...preferences, alerts: [...preferences.alerts, { name: alertName.trim(), enabled: true }] }); setAlertName(""); } }}>+</button></div>
        <div className="account-alerts">{preferences.alerts.map((item, index) => <label key={`${item.name}-${index}`}><input type="checkbox" checked={item.enabled} onChange={(e) => { const alerts = [...preferences.alerts]; alerts[index] = { ...item, enabled: e.target.checked }; void save({ ...preferences, alerts }); }} />{item.name}<button onClick={() => void save({ ...preferences, alerts: preferences.alerts.filter((_, i) => i !== index) })}>×</button></label>)}</div>
        {saved ? <p className="account-saved">{fr ? "Enregistré." : "Saved."}</p> : null}
        <div className="account-actions">
          <a className="subscription-cta" href={String(member.manage_url || "https://whop.com/hub")} target="_blank" rel="noreferrer">{fr ? "Gérer sur Whop" : "Manage on Whop"}</a>
          <button className="member-subscribe-link" onClick={() => void logout()}>{fr ? "Se déconnecter" : "Sign out"}</button>
        </div>
      </section>
    </div>
  );
}
