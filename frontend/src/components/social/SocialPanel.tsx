import { FormEvent, useEffect, useState } from "react";
import { Language } from "../../i18n";

type Row = Record<string, unknown>;
type Props = {
  lang: Language;
  currentPosition: { lat: number; lon: number } | null;
  onLocate: () => void;
  onFocusPerson: (lat: number, lon: number) => void;
  onClose: () => void;
};

export default function SocialPanel({ lang, currentPosition, onLocate, onFocusPerson, onClose }: Props) {
  const fr = lang === "fr";
  const relationshipLabel = (value: unknown) => {
    const key = String(value || "");
    if (!fr) return key;
    return ({ friends: "amis", family: "famille", dating: "rencontres", community: "communauté" } as Record<string, string>)[key] || key;
  };
  const [data, setData] = useState<Record<string, any> | null>(null);
  const [tab, setTab] = useState<"people" | "chat" | "profile">("people");
  const [message, setMessage] = useState("");
  const [invite, setInvite] = useState("");
  const [error, setError] = useState("");
  const [locationConsent, setLocationConsent] = useState<"idle" | "pending" | "awaiting_gps">("idle");

  const load = async () => {
    const response = await fetch("/api/social/overview", { credentials: "same-origin" });
    if (!response.ok) throw new Error("social");
    setData(await response.json());
  };
  useEffect(() => { void load().catch(() => setError(fr ? "Le salon est temporairement indisponible." : "Community is temporarily unavailable.")); }, []);
  useEffect(() => {
    if (tab !== "chat") return;
    const timer = window.setInterval(() => void load().catch(() => {}), 5000);
    return () => window.clearInterval(timer);
  }, [tab]);

  const post = async (url: string, body: unknown) => {
    const response = await fetch(url, { method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!response.ok) throw new Error(await response.text());
    await load();
  };
  const send = async (event: FormEvent) => {
    event.preventDefault();
    if (!message.trim()) return;
    try { await post("/api/social/messages", { room: "global", content: message }); setMessage(""); } catch { setError(fr ? "Attendez un instant avant de republier." : "Please wait before posting again."); }
  };
  const connect = async (event: FormEvent) => {
    event.preventDefault();
    try { await post("/api/social/connect", { invite_code: invite, label: "friends" }); setInvite(""); } catch { setError(fr ? "Code d’invitation introuvable." : "Invite code not found."); }
  };
  // Opens consent dialog; if GPS not yet available, requests it first.
  const requestLocationConsent = () => {
    if (!currentPosition) {
      setLocationConsent("awaiting_gps");
      onLocate();
    } else {
      setLocationConsent("pending");
    }
  };

  // Called when GPS becomes available while awaiting for dialog.
  const prevPosition = data; // use data as stable non-null sentinel
  if (locationConsent === "awaiting_gps" && currentPosition) {
    setLocationConsent("pending");
  }

  const confirmPresence = async (precision: "approximate" | "precise") => {
    setLocationConsent("idle");
    if (!currentPosition) return;
    await post("/api/social/presence", {
      enabled: true,
      lat: currentPosition.lat,
      lon: currentPosition.lon,
      consent_precision: precision,
    });
  };

  const stopPresence = async () => {
    await post("/api/social/presence", { enabled: false, consent_precision: "none", lat: null, lon: null });
  };

  const deletePresence = async () => {
    await fetch("/api/social/presence", { method: "DELETE", credentials: "same-origin" });
    await load();
  };

  void prevPosition; // suppress unused warning
  const saveProfile = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    await post("/api/social/profile", {
      display_name: form.get("display_name"), avatar: form.get("avatar"), bio: form.get("bio"),
      city: form.get("city"), country: form.get("country"), intent: form.get("intent"),
      discoverable: form.get("discoverable") === "on",
    });
  };
  const profile = data?.profile ?? {};
  const badge = Number(profile.points ?? 0) >= 150 ? (fr ? "Navigateur" : "Navigator")
    : Number(profile.points ?? 0) >= 50 ? (fr ? "Connecteur" : "Connector")
    : Number(profile.points ?? 0) >= 10 ? (fr ? "Explorateur" : "Explorer")
    : (fr ? "Nouveau" : "New");

  return (
    <div className="social-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="social-panel" role="dialog" aria-modal="true" aria-label={fr ? "Communauté AlgoSphere" : "AlgoSphere community"} onMouseDown={(e) => e.stopPropagation()}>
        <button type="button" className="subscription-close" onClick={onClose}>×</button>
        <header className="social-head"><div><span>ALGOSPHERE SOCIAL</span><h2>{fr ? "Retrouvez votre monde" : "Find your world"}</h2></div><div className="social-score"><b>{String(profile.avatar ?? "🧭")}</b><span>{profile.points ?? 0} pts · {badge}</span></div></header>
        <p className="social-consent">{fr ? "Votre position n’est jamais partagée sans votre action. Seuls vos contacts acceptés peuvent la voir." : "Your location is never shared without your action. Only accepted contacts can see it."}</p>
        <nav className="social-tabs">
          <button className={tab === "people" ? "active" : ""} onClick={() => setTab("people")}>{fr ? "Proches" : "People"}</button>
          <button className={tab === "chat" ? "active" : ""} onClick={() => setTab("chat")}>{fr ? "Salon mondial" : "Global chat"}</button>
          <button className={tab === "profile" ? "active" : ""} onClick={() => setTab("profile")}>{fr ? "Avatar et profil" : "Avatar & profile"}</button>
        </nav>
        {error ? <p className="member-error">{error}</p> : null}
        {!data ? <div className="social-loading">{fr ? "Connexion à la communauté…" : "Connecting to the community…"}</div> : null}
        {data && tab === "people" ? <div className="social-people">
          <div className="social-invite"><div><small>{fr ? "VOTRE CODE PRIVÉ" : "YOUR PRIVATE CODE"}</small><strong>{profile.invite_code}</strong><p>{fr ? "Envoyez ce code à votre famille, vos amis ou votre partenaire." : "Send this code to family, friends or your partner."}</p></div><form onSubmit={connect}><input value={invite} onChange={(e) => setInvite(e.target.value.toUpperCase())} placeholder={fr ? "Entrer un code" : "Enter a code"} /><button>{fr ? "Ajouter" : "Add"}</button></form></div>
          <div className="social-location">
            <div>
              <strong>📍 {fr ? "Présence sur le globe" : "Globe presence"}</strong>
              <p>{fr ? "Seuls vos contacts acceptés voient votre position. Elle expire après 24 h." : "Only accepted contacts see your position. It expires after 24 h."}</p>
            </div>
            {locationConsent === "pending" ? (
              <div className="social-consent-dialog" role="dialog" aria-modal="false">
                <p className="social-consent-heading">{fr ? "Avant de partager votre position" : "Before sharing your position"}</p>
                <ul className="social-consent-list">
                  <li>{fr ? "Données : latitude et longitude de votre appareil" : "Data: your device's latitude and longitude"}</li>
                  <li>{fr ? "Visible par : vos contacts acceptés uniquement" : "Visible to: your accepted contacts only"}</li>
                  <li>{fr ? "Position approximative : arrondie à ~50 km — recommandé" : "Approximate: rounded to ~50 km — recommended"}</li>
                  <li>{fr ? "Position précise : coordonnées exactes — uniquement si nécessaire" : "Precise: exact coordinates — only if needed"}</li>
                  <li>{fr ? "Durée : 24 heures, non renouvelée automatiquement" : "Duration: 24 hours, not automatically renewed"}</li>
                  <li>{fr ? "Retrait : bouton « Arrêter » ou « Supprimer mes données »" : "Withdrawal: 'Stop' button or 'Delete my data'"}</li>
                </ul>
                <p className="social-consent-legal">
                  {fr ? "⚖ EFVP requise — À VALIDER JURIDIQUEMENT avant toute activation en production." : "⚖ PIA required — REQUIRES LEGAL REVIEW before production activation."}
                </p>
                <div className="social-consent-actions">
                  <button type="button" className="quiet" onClick={() => setLocationConsent("idle")}>{fr ? "Ne pas partager" : "Do not share"}</button>
                  <button type="button" onClick={() => void confirmPresence("approximate")}>{fr ? "Position approximative (~50 km)" : "Approximate (~50 km)"}</button>
                  <button type="button" className="quiet" onClick={() => void confirmPresence("precise")}>{fr ? "Position précise" : "Precise position"}</button>
                </div>
              </div>
            ) : locationConsent === "awaiting_gps" ? (
              <p className="social-gps-wait">{fr ? "En attente du GPS…" : "Waiting for GPS…"}</p>
            ) : (data?.my_presence?.enabled) ? (
              <div className="social-presence-active">
                <span className="social-presence-badge">
                  {data.my_presence.precision === "approximate"
                    ? (fr ? "Présence active — approximative" : "Active — approximate")
                    : (fr ? "Présence active — précise" : "Active — precise")}
                </span>
                <div className="social-presence-controls">
                  <button type="button" className="quiet" onClick={() => void stopPresence()}>{fr ? "Arrêter le partage" : "Stop sharing"}</button>
                  <button type="button" className="quiet social-delete-btn" onClick={() => void deletePresence()}>{fr ? "Supprimer mes données" : "Delete my data"}</button>
                </div>
              </div>
            ) : (
              <div className="social-presence-controls">
                <button type="button" onClick={requestLocationConsent}>
                  {locationConsent === "awaiting_gps"
                    ? (fr ? "En attente…" : "Waiting…")
                    : currentPosition
                      ? (fr ? "Partager ma position" : "Share my position")
                      : (fr ? "Activer le GPS" : "Enable GPS")}
                </button>
              </div>
            )}
          </div>
          <h3>{fr ? "Mes proches" : "My people"}</h3>
          <div className="social-cards">{(data.connections as Row[]).map((person) => <article key={String(person.member_id)}><b>{String(person.avatar)}</b><div><strong>{String(person.display_name)}</strong><span>{relationshipLabel(person.label)} · {String(person.city || person.country || "")}</span>{person.lat ? <button className="social-focus" onClick={() => onFocusPerson(Number(person.lat), Number(person.lon))}>◎ {fr ? "Voir sur le globe" : "View on globe"}</button> : null}</div></article>)}{!data.connections.length ? <p className="social-empty">{fr ? "Ajoutez un proche avec son code privé." : "Add someone using their private code."}</p> : null}</div>
          <h3>{fr ? "Découvrir la communauté" : "Discover the community"}</h3>
          <div className="social-cards">{(data.discovery as Row[]).map((person) => <article key={String(person.member_id)}><b>{String(person.avatar)}</b><div><strong>{String(person.display_name)}</strong><span>{String(person.city || "")} {String(person.country || "")}</span><small>{relationshipLabel(person.intent)} · {String(person.points)} pts</small></div></article>)}</div>
        </div> : null}
        {data && tab === "chat" ? <div className="social-chat"><div className="social-messages">{(data.messages as Row[]).map((item) => <article key={String(item.id)}><b>{String(item.avatar)}</b><div><strong>{String(item.display_name)} <small>{String(item.points)} pts</small></strong><p>{String(item.content)}</p></div></article>)}</div><form onSubmit={send}><input value={message} maxLength={500} onChange={(e) => setMessage(e.target.value)} placeholder={fr ? "Écrire au salon mondial…" : "Message the global room…"} /><button>{fr ? "Envoyer" : "Send"}</button></form></div> : null}
        {data && tab === "profile" ? <form className="social-profile" onSubmit={saveProfile}>
          <label>{fr ? "Avatar" : "Avatar"}<div className="avatar-picker">{(data.avatars as string[]).map((avatar) => <label key={avatar}><input type="radio" name="avatar" value={avatar} defaultChecked={avatar === profile.avatar} /><span>{avatar}</span></label>)}</div></label>
          <label>{fr ? "Nom affiché" : "Display name"}<input name="display_name" defaultValue={profile.display_name} minLength={2} maxLength={40} required /></label>
          <label>Bio<textarea name="bio" defaultValue={profile.bio} maxLength={240} /></label>
          <div className="social-form-row"><label>{fr ? "Ville" : "City"}<input name="city" defaultValue={profile.city} /></label><label>{fr ? "Pays" : "Country"}<input name="country" defaultValue={profile.country} /></label></div>
          <label>{fr ? "Je souhaite rencontrer" : "I want to connect with"}<select name="intent" defaultValue={profile.intent}><option value="family">{fr ? "Famille" : "Family"}</option><option value="friends">{fr ? "Amis" : "Friends"}</option><option value="community">{fr ? "Communauté" : "Community"}</option><option value="dating">{fr ? "Rencontres" : "Dating"}</option></select></label>
          {profile.intent === "dating" || data?.profile?.intent === "dating" ? <p className="social-consent" style={{marginTop:4}}>{fr ? "⚖ L'intention « Rencontres » implique des données sensibles. Une validation juridique est requise pour activer cette option en production (Loi 25, EFVP)." : "⚖ The 'Dating' intent involves sensitive data. Legal review is required before enabling this in production (Law 25, PIA)."}</p> : null}
          <label className="social-check"><input type="checkbox" name="discoverable" defaultChecked={Boolean(profile.discoverable)} />{fr ? "Me rendre visible dans la communauté (jamais ma position précise)" : "Let the community discover me (never my precise location)"}</label>
          <button className="subscription-cta">{fr ? "Enregistrer mon profil" : "Save profile"}</button>
          <p className="social-rewards">{fr ? "Gagnez des points en participant et en ajoutant de vrais contacts. Les objets de collection numériques arriveront dans une phase ultérieure." : "Earn points by participating and adding real contacts. Digital collectibles will arrive in a later phase."}</p>
        </form> : null}
      </section>
    </div>
  );
}
