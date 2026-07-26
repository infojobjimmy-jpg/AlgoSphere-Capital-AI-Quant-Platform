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
  const [ownerPermanentMode, setOwnerPermanentMode] = useState(false);
  const [otpSent, setOtpSent] = useState(false);
  const [emailHint, setEmailHint] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [rememberMe, setRememberMe] = useState(true);

  const requestOwnerCode = async () => {
    setBusy(true);
    setError("");
    try {
      const response = await fetch("/api/auth/owner/request-code", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify({}),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.detail || "Email delivery failed"));
      setOtpSent(true);
      setEmailHint(String(payload.email_hint || ""));
      setLicenseKey("");
      trackEvent("owner_code_requested", { properties: { delivery: "email" } });
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : (fr ? "Envoi impossible." : "Unable to send the code."));
    } finally {
      setBusy(false);
    }
  };

  const validate = async () => {
    setBusy(true);
    setError("");
    try {
      let endpoint = "/api/auth/license";
      let body: Record<string, unknown> = { license_key: licenseKey.trim(), remember_me: rememberMe };
      let access = "whop";

      if (ownerMode && ownerPermanentMode) {
        endpoint = "/api/auth/owner";
        body = { access_code: licenseKey.trim(), remember_me: rememberMe };
        access = "owner_permanent";
      } else if (ownerMode) {
        endpoint = "/api/auth/owner/verify-code";
        body = { code: licenseKey.trim(), remember_me: rememberMe };
        access = "owner_email_otp";
      }

      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(body),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.detail || "Validation failed"));
      trackEvent("member_login", { properties: { access, remember_me: String(rememberMe) } });
      onAuthenticated(payload.member || {});
      onClose();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : (fr ? "Validation impossible." : "Validation failed."));
    } finally {
      setBusy(false);
    }
  };

  const toggleOwnerMode = async () => {
    const nextOwnerMode = !ownerMode;
    setOwnerMode(nextOwnerMode);
    setOwnerPermanentMode(false);
    setOtpSent(false);
    setEmailHint("");
    setLicenseKey("");
    setError("");
    if (nextOwnerMode) {
      await requestOwnerCode();
    }
  };

  const ownerOtpReady = /^\d{6}$/.test(licenseKey.trim());
  const permanentReady = licenseKey.trim().length >= 12;
  const memberReady = licenseKey.trim().length >= 3;

  const rememberMeCheckbox = (
    <label className="member-remember">
      <input type="checkbox" checked={rememberMe} onChange={(e) => setRememberMe(e.target.checked)} />
      {fr ? "Se souvenir de moi (30 jours)" : "Remember me (30 days)"}
    </label>
  );

  return (
    <div className="gps-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="member-panel" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <button type="button" className="subscription-close" onClick={onClose} aria-label={fr ? "Fermer" : "Close"}>×</button>
        <div className="subscription-kicker">ALGOSPHERE MEMBER</div>
        <h2>{ownerMode ? (fr ? "Accès propriétaire" : "Owner access") : (fr ? "Accès membre" : "Member access")}</h2>

        {!ownerMode ? (
          <>
            <p className="subscription-lead">
              {fr
                ? "Utilisez la clé de licence de votre abonnement Whop. Elle est vérifiée directement par le serveur et n'est jamais conservée dans le navigateur."
                : "Use the license key from your Whop membership. It is verified by the server and never stored in your browser."}
            </p>
            {configured ? (
              <>
                <label className="member-license-label">
                  {fr ? "Clé de licence Whop" : "Whop license key"}
                  <input type="password" value={licenseKey} onChange={(event) => setLicenseKey(event.target.value)} autoComplete="off" />
                </label>
                {rememberMeCheckbox}
                {error ? <p className="member-error">{error}</p> : null}
                <button type="button" className="subscription-cta" disabled={busy || !memberReady} onClick={validate}>
                  {busy ? (fr ? "Vérification…" : "Checking…") : (fr ? "Valider mon abonnement" : "Validate my subscription")}
                </button>
              </>
            ) : (
              <p className="member-error">
                {fr ? "La validation Whop est temporairement indisponible." : "Whop validation is temporarily unavailable."}
              </p>
            )}
          </>
        ) : ownerPermanentMode ? (
          <>
            <p className="subscription-lead">
              {fr ? "Utilisez votre ancien code propriétaire permanent." : "Use your previous permanent owner code."}
            </p>
            <label className="member-license-label">
              {fr ? "Code propriétaire permanent" : "Permanent owner code"}
              <input type="password" value={licenseKey} onChange={(event) => setLicenseKey(event.target.value)} autoComplete="off" />
            </label>
            {rememberMeCheckbox}
            {error ? <p className="member-error">{error}</p> : null}
            <button type="button" className="subscription-cta" disabled={busy || !permanentReady} onClick={validate}>
              {busy ? (fr ? "Vérification…" : "Checking…") : (fr ? "Ouvrir mon accès" : "Open owner access")}
            </button>
            <button type="button" className="member-subscribe-link" onClick={() => { setOwnerPermanentMode(false); setOtpSent(false); setLicenseKey(""); setError(""); void requestOwnerCode(); }}>
              {fr ? "Recevoir plutôt un code par courriel" : "Send me an email code instead"}
            </button>
          </>
        ) : (
          <>
            <p className="subscription-lead">
              {otpSent
                ? (fr
                  ? `Un code temporaire a été envoyé à ${emailHint || "votre courriel"}. Il expire dans 10 minutes.`
                  : `A temporary code was sent to ${emailHint || "your email"}. It expires in 10 minutes.`)
                : busy
                  ? (fr ? "Envoi automatique du code à votre courriel…" : "Automatically sending the code to your email…")
                  : (fr
                    ? "L'envoi automatique n'a pas réussi. Vous pouvez réessayer ou utiliser votre ancien code permanent."
                    : "Automatic delivery did not succeed. You can try again or use your permanent code.")}
            </p>

            {!otpSent ? (
              <button type="button" className="subscription-cta" disabled={busy} onClick={requestOwnerCode}>
                {busy ? (fr ? "Envoi…" : "Sending…") : (fr ? "Réessayer l'envoi" : "Try sending again")}
              </button>
            ) : (
              <>
                <label className="member-license-label">
                  {fr ? "Code temporaire à 6 chiffres" : "6-digit temporary code"}
                  <input
                    type="text"
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    maxLength={6}
                    value={licenseKey}
                    onChange={(event) => setLicenseKey(event.target.value.replace(/\D/g, "").slice(0, 6))}
                  />
                </label>
                {rememberMeCheckbox}
                <button type="button" className="subscription-cta" disabled={busy || !ownerOtpReady} onClick={validate}>
                  {busy ? (fr ? "Vérification…" : "Checking…") : (fr ? "Ouvrir mon accès" : "Open owner access")}
                </button>
                <button type="button" className="member-subscribe-link" disabled={busy} onClick={requestOwnerCode}>
                  {fr ? "Renvoyer un nouveau code" : "Send another code"}
                </button>
              </>
            )}

            {error ? <p className="member-error">{error}</p> : null}
            <button type="button" className="member-subscribe-link" onClick={() => { setOwnerPermanentMode(true); setOtpSent(false); setLicenseKey(""); setError(""); }}>
              {fr ? "Utiliser mon ancien code permanent" : "Use my permanent code"}
            </button>
          </>
        )}

        {ownerAccessConfigured ? (
          <button type="button" className="member-subscribe-link" onClick={() => { void toggleOwnerMode(); }}>
            {ownerMode ? (fr ? "Retour à l'accès membre" : "Back to member access") : (fr ? "Je suis le propriétaire" : "I am the owner")}
          </button>
        ) : null}
        <button type="button" className="member-subscribe-link" onClick={onSubscribe}>
          {fr ? "Je n'ai pas encore d'abonnement" : "I do not have a subscription yet"}
        </button>
      </section>
    </div>
  );
}
