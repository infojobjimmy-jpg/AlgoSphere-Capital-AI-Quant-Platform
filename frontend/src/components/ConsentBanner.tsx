import { useEffect, useState } from "react";
import { analyticsConsent, setAnalyticsConsent, trackEvent } from "../analytics";

export default function ConsentBanner() {
  const [choice, setChoice] = useState(analyticsConsent());
  const [lang, setLang] = useState<"fr" | "en">(() => localStorage.getItem("algosphere_lang") === "en" ? "en" : "fr");
  const fr = lang === "fr";

  useEffect(() => {
    const update = (event: Event) => setLang((event as CustomEvent<"fr" | "en">).detail);
    window.addEventListener("algosphere-language-change", update);
    return () => window.removeEventListener("algosphere-language-change", update);
  }, []);

  useEffect(() => {
    if (choice === "accepted") trackEvent(window.location.pathname === "/app" ? "app_open" : "page_view");
  }, [choice]);

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      const target = event.target instanceof Element ? event.target.closest("a") : null;
      const href = target?.getAttribute("href") ?? "";
      if (!href.includes("whop.com/algosphere-ia-lab/")) return;
      const plan = href.includes("explorer") ? "explorer" : href.includes("business") ? "business" : href.includes("pro") ? "pro" : "contact";
      trackEvent("checkout_click", { plan });
    };
    document.addEventListener("click", handleClick, true);
    return () => document.removeEventListener("click", handleClick, true);
  }, []);

  if (choice) return null;
  return (
    <aside className="consent-banner" role="dialog" aria-label={fr ? "Choix de confidentialité" : "Privacy choice"}>
      <p>
        {fr
          ? "Avec votre accord, nous mesurons les visites et les clics vers l’achat pour améliorer AlgoSphere. Aucun profil publicitaire et aucune donnée de paiement."
          : "With your permission, we measure visits and checkout clicks to improve AlgoSphere. No advertising profile or payment data."}
        {" "}<a href={fr ? "/confidentialite" : "/privacy"}>{fr ? "En savoir plus" : "Learn more"}</a>
      </p>
      <div>
        <button type="button" onClick={() => { setAnalyticsConsent("declined"); setChoice("declined"); }}>{fr ? "Refuser" : "Decline"}</button>
        <button className="consent-accept" type="button" onClick={() => { setAnalyticsConsent("accepted"); setChoice("accepted"); }}>{fr ? "Accepter" : "Accept"}</button>
      </div>
    </aside>
  );
}
