import { useState } from "react";
import { trackEvent } from "../../analytics";
import { Language } from "../../i18n";

type Props = {
  lang: Language;
  configured: boolean;
  ownerAccessConfigured: boolean;
  onAuthenticated: (member: Record<string, unknown>) => void;
  onClose: () => void;
  onSubscribe: () => void;
};

export default function MemberAccessPanel({ lang, configured, ownerAccessConfigured, onAuthenticated, onClose, onSubscribe }: Props) {
  const fr = lang === "fr";
  const [licenseKey, setLicenseKey] = useState("");
  const [ownerMode, setOwnerMode] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const validate = async () => {
    setBusy(true);
    setError("");
    try {
      const response = await fetch(ownerMode ? "/api/auth/owner" : "/api/auth/license", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(ownerMode ? { access_code: licenseKey.trim() } : { license_key: licenseKey.trim() }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.detail || "Validation failed"));
      trackEvent("member_login", { properties: { access: ownerMode ? "owner" : "whop" } });
      onAuthenticated(payload.member || {});
      onClose();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : (fr ? "Validation impossible." : "Validation failed."));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="gps-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="member-panel" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <button type="button" className="subscription-close" onClick={onClose} aria-label={fr ? "Fermer" : "Close"}>×</button>
        <div className="subscription-kicker">ALGOSPHERE MEMBER</div>
        <h2>{fr ? "Accès membre" : "Member access"}</h2>
        <p className="subscription-lead">
          {ownerMode
            ? (fr ? "Entrez votre code propriétaire privé. Il demeure uniquement sur le serveur." : "Enter your private owner code. It remains on the server only.")
            : fr
            ? "Utilisez la clé de licence de votre abonnement Whop. Elle est vérifiée directement par le serveur et n’est jamais conservée dans le navigateur."
            : "Use the license key from your Whop membership. It is verified by the server and never stored in your browser."}
        </p>
        {(ownerMode ? ownerAccessConfigured : configured) ? (
          <>
            <label className="member-license-label">
              {ownerMode ? (fr ? "Code propriétaire" : "Owner code") : (fr ? "Clé de licence Whop" : "Whop license key")}
              <input type="password" value={licenseKey} onChange={(event) => setLicenseKey(event.target.value)} autoComplete="off" />
            </label>
            {error ? <p className="member-error">{error}</p> : null}
            <button type="button" className="subscription-cta" disabled={busy || licenseKey.trim().length < 3} onClick={validate}>
              {busy ? (fr ? "Vérification…" : "Checking…") : ownerMode ? (fr ? "Ouvrir mon accès propriétaire" : "Open owner access") : (fr ? "Valider mon abonnement" : "Validate my subscription")}
            </button>
          </>
        ) : (
          <p className="member-error">
            {fr ? "La validation Whop est temporairement indisponible." : "Whop validation is temporarily unavailable."}
          </p>
        )}
        {ownerAccessConfigured ? (
          <button type="button" className="member-subscribe-link" onClick={() => { setOwnerMode((value) => !value); setLicenseKey(""); setError(""); }}>
            {ownerMode ? (fr ? "Retour à l’accès membre" : "Back to member access") : (fr ? "Je suis le propriétaire" : "I am the owner")}
          </button>
        ) : null}
        <button type="button" className="member-subscribe-link" onClick={onSubscribe}>
          {fr ? "Je n’ai pas encore d’abonnement" : "I do not have a subscription yet"}
        </button>
      </section>
    </div>
  );
}
