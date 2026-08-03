import { useEffect, useState } from "react";
import "./marketing.css";
import LanguageSelect from "./components/LanguageSelect";
import { Language, savedLanguage, useInterfaceTranslation } from "./i18n";
import { initHeartbeat, trackEvent } from "./analytics";

type Lang = Language;

const whop = {
  explorer: "https://whop.com/algosphere-ia-lab/algosphere-explorer-founder",
  pro: "https://whop.com/algosphere-ia-lab/algosphere-pro-founder",
  business: "https://whop.com/algosphere-ia-lab/algosphere-business-pilot",
};

const legalCopy = {
  privacy: {
    fr: {
      title: "Politique de confidentialité",
      intro: "Dernière mise à jour technique : 3 août 2026 — À VALIDER JURIDIQUEMENT avant toute publication officielle. Ce document est un texte de travail préliminaire; il ne constitue pas un avis juridique final.",
      sections: [
        ["Responsable du traitement", "AlgoSphere Global exploite ce service d’intelligence géospatiale. Nom légal complet, adresse et coordonnées du responsable de la protection des renseignements personnels : INFORMATION LÉGALE REQUISE — à compléter et à valider par un juriste avant la publication. Pour toute question : page AlgoSphere sur Whop (lien en bas de page)."],
        ["Renseignements traités", "Nous pouvons traiter, selon vos choix : (1) Données de service — clé de licence et statut d’abonnement transmis par Whop.com, données de session de connexion, préférences enregistrées (favoris, alertes). Nous ne recevons ni ne conservons vos données de carte bancaire. (2) Mesure d’audience — avec votre consentement uniquement : pages consultées, événements d’utilisation, source de la visite, type d’appareil. Un identifiant de visite pseudonymisé est stocké. (3) Présence sociale — avec votre consentement explicite uniquement : votre position géographique si vous activez la fonctionnalité de présence dans le module social. Cette information n’est partagée qu’avec les contacts que vous avez vous-même ajoutés."],
        ["Finalités et bases de traitement", "Exécution du service (base : exécution du contrat) : authentification, vérification de l’abonnement, conservation des préférences. Mesure d’audience (base : consentement — retirable en tout temps) : amélioration du service, mesure des conversions. Présence sociale (base : consentement explicite) : fonctionnalité optionnelle de partage de localisation entre membres. Sécurité et prévention des abus (base : intérêt légitime) : journaux temporaires, limiteurs de débit."],
        ["Transferts hors Québec / hors Canada", "Whop.com (États-Unis) traite les paiements, les abonnements et la validation des licences. Ce transfert est soumis à la Loi modernisant des dispositions législatives en matière de protection des renseignements personnels (Loi 25, Québec) et à la LPRPDE. Une évaluation des facteurs relatifs à la vie privée (EFVP) est requise pour ce transfert — INFORMATION LÉGALE REQUISE. Nous ne vendons aucun renseignement personnel à des tiers."],
        ["Fournisseurs de services tiers", "Whop.com (États-Unis) : abonnements et paiements. Cesium ion : affichage de la carte 3D. Fournisseurs de données géospatiales : couches approuvées (OpenSky, AISstream, Open-Meteo, etc.). Chaque fournisseur applique sa propre politique de confidentialité."],
        ["Conservation", "Sessions de visite non authentifiées : 90 jours. Sessions de membres identifiés : 2 ans ou jusqu’à la suppression du compte. Journaux de sécurité temporaires : 30 jours. Durées définitives : INFORMATION LÉGALE REQUISE — à formaliser dans une politique de conservation validée."],
        ["Vos droits (Loi 25 / LPRPDE)", "Vous avez le droit d’accéder à vos renseignements personnels, de les faire corriger ou supprimer, d’en recevoir une copie (portabilité), et de retirer votre consentement en tout temps sans préjudice rétroactif. Pour exercer ces droits : page AlgoSphere sur Whop. Processus formel et délais de réponse : INFORMATION LÉGALE REQUISE avant publication."],
        ["Préférences de confidentialité", "Vous pouvez accepter ou refuser la mesure d’audience lors de votre première visite. Pour modifier votre choix à tout moment, cliquez sur « Préférences de confidentialité » dans le pied de page de n’importe quelle page AlgoSphere Global."],
        ["Sécurité", "Nous utilisons HTTPS, des cookies de session sécurisés (HttpOnly, Secure, SameSite=Lax), une authentification à code temporaire pour l’accès propriétaire, et des limiteurs de débit. Aucune transmission sur Internet ne peut être garantie sans risque."],
        ["Mises à jour", "Cette politique peut être mise à jour. La date de dernière modification est indiquée en haut de la page. Les changements importants seront signalés aux abonnés. Consultez régulièrement cette page."],
      ],
    },
    en: {
      title: "Privacy policy",
      intro: "Technical last updated: August 3, 2026 — REQUIRES LEGAL REVIEW before any official publication. This is a preliminary working document, not a final legal opinion.",
      sections: [
        ["Data controller", "AlgoSphere Global operates this geospatial intelligence service. Full legal name, address and privacy officer contact: LEGAL INFORMATION REQUIRED — to be completed and validated by legal counsel before publication. For questions: AlgoSphere page on Whop (link at the bottom)."],
        ["Information we process", "Depending on your choices, we may process: (1) Service data — license key and subscription status from Whop.com, session and authentication data, saved preferences (favorites, alerts). We do not receive or store payment-card details. (2) Audience measurement — with your consent only: pages viewed, usage events, visit source, device type. A pseudonymous visit identifier is stored. (3) Social presence — with your explicit consent only: your geographic position if you enable the presence feature in the social module. This is shared only with contacts you have added yourself."],
        ["Purposes and legal bases", "Service delivery (basis: contract performance): authentication, subscription verification, preferences. Audience measurement (basis: consent — withdrawable at any time): service improvement, conversion measurement. Social presence (basis: explicit consent): optional location-sharing feature between members. Security and abuse prevention (basis: legitimate interest): temporary logs, rate limiting."],
        ["Cross-border transfers", "Whop.com (United States) processes payments, subscriptions and license validation. This transfer is subject to Quebec Law 25 and PIPEDA. A Privacy Impact Assessment (PIA) is required for this transfer — LEGAL INFORMATION REQUIRED. We do not sell personal information to third parties."],
        ["Third-party service providers", "Whop.com (United States): subscriptions and payments. Cesium ion: 3D map display. Geospatial data providers: approved layers (OpenSky, AISstream, Open-Meteo, etc.). Each provider operates under its own privacy policy."],
        ["Retention", "Non-authenticated visit sessions: 90 days. Identified member sessions: 2 years or until account deletion. Temporary security logs: 30 days. Final retention periods: LEGAL INFORMATION REQUIRED — to be formalized in a validated retention schedule."],
        ["Your rights (Law 25 / PIPEDA)", "You have the right to access, correct or delete your personal information, receive a copy (portability), and withdraw consent at any time without retroactive penalty. To exercise these rights: AlgoSphere page on Whop. Formal process and response timelines: LEGAL INFORMATION REQUIRED before publication."],
        ["Privacy preferences", "You may accept or decline audience measurement on your first visit. To change your choice at any time, click ‘Privacy preferences’ in the footer of any AlgoSphere Global page."],
        ["Security", "We use HTTPS, secure session cookies (HttpOnly, Secure, SameSite=Lax), temporary-code authentication for owner access, and rate limiting. No Internet transmission can be guaranteed risk-free."],
        ["Updates", "This policy may be updated. The last modification date is shown at the top of the page. Material changes will be communicated to subscribers. Check this page regularly."],
      ],
    },
  },
  terms: {
    fr: {
      title: "Conditions d’utilisation",
      intro: "Dernière mise à jour : 25 juillet 2026",
      sections: [
        ["Nature du service", "AlgoSphere Global est un outil de visualisation et d’intelligence géospatiale. Les données peuvent être retardées, incomplètes ou temporairement indisponibles."],
        ["Utilisation permise", "Vous acceptez d’utiliser le service légalement, de ne pas contourner les contrôles d’accès et de ne pas revendre, aspirer ou redistribuer les données sans autorisation."],
        ["Sécurité et décisions", "Le service ne remplace pas les avis officiels, les systèmes certifiés de navigation, les autorités aériennes ou maritimes, ni les services d’urgence. Vérifiez toute information critique auprès d’une source officielle."],
        ["Compte et abonnement", "L’accès premium dépend d’un abonnement Whop valide. Vous êtes responsable de la confidentialité de votre clé de licence et de toute utilisation de votre compte."],
        ["Disponibilité", "Nous pouvons faire évoluer, suspendre ou retirer une source ou une fonction pour des raisons techniques, légales, de sécurité ou de fournisseur."],
      ],
    },
    en: {
      title: "Terms of use",
      intro: "Last updated: July 25, 2026",
      sections: [
        ["Service scope", "AlgoSphere Global is a geospatial visualization and intelligence tool. Data may be delayed, incomplete or temporarily unavailable."],
        ["Acceptable use", "You agree to use the service lawfully, not bypass access controls, and not resell, scrape or redistribute data without authorization."],
        ["Safety and decisions", "The service does not replace official notices, certified navigation systems, aviation or maritime authorities, or emergency services. Verify critical information with an official source."],
        ["Account and subscription", "Premium access requires a valid Whop subscription. You are responsible for protecting your license key and account activity."],
        ["Availability", "We may change, suspend or remove a source or feature for technical, legal, security or provider-related reasons."],
      ],
    },
  },
  refunds: {
    fr: {
      title: "Annulation et remboursement",
      intro: "Dernière mise à jour : 25 juillet 2026",
      sections: [
        ["Essai et facturation", "Les offres admissibles comprennent un essai de 7 jours, tel qu’indiqué au moment de l’achat sur Whop. À la fin de l’essai, l’abonnement est facturé selon le forfait choisi, sauf annulation avant l’échéance."],
        ["Annulation", "Vous pouvez annuler depuis votre espace Whop. L’annulation empêche le prochain renouvellement; l’accès demeure généralement actif jusqu’à la fin de la période déjà payée."],
        ["Demandes de remboursement", "Les demandes sont évaluées selon les conditions affichées au moment de l’achat et les politiques applicables de Whop. Communiquez rapidement avec nous via Whop en indiquant l’adresse liée à l’achat et la raison de la demande."],
        ["Indisponibilité", "Une interruption brève ou la disponibilité variable d’une source externe ne garantit pas automatiquement un remboursement. Une panne prolongée du service principal sera examinée de façon raisonnable au cas par cas."],
      ],
    },
    en: {
      title: "Cancellation and refund policy",
      intro: "Last updated: July 25, 2026",
      sections: [
        ["Trial and billing", "Eligible plans include a 7-day trial when shown at checkout on Whop. Unless cancelled before the deadline, the selected subscription is billed when the trial ends."],
        ["Cancellation", "You may cancel from your Whop account. Cancellation prevents the next renewal; access generally remains active through the paid period."],
        ["Refund requests", "Requests are reviewed under the terms shown at purchase and applicable Whop policies. Contact us promptly through Whop with the purchase email and reason for the request."],
        ["Availability", "A brief interruption or variable availability of an external data source does not automatically guarantee a refund. A prolonged outage of the core service will be reviewed reasonably case by case."],
      ],
    },
  },
} as const;

function LegalPage({ page, lang, setLang }: { page: keyof typeof legalCopy; lang: Lang; setLang: (lang: Lang) => void }) {
  const copy = legalCopy[page][lang === "fr" ? "fr" : "en"];
  return (
    <main className="mkt legal-page">
      <header className="mkt-nav">
        <a className="mkt-brand" href="/"><span>ALGOSPHERE</span> GLOBAL</a>
        <LanguageSelect className="mkt-lang" value={lang} onChange={setLang} />
      </header>
      <article className="legal-card">
        <a className="legal-back" href="/">← {lang === "fr" ? "Retour à l’accueil" : "Back to home"}</a>
        <p className="mkt-eyebrow">ALGOSPHERE GLOBAL</p>
        <h1>{copy.title}</h1>
        <p className="legal-date">{copy.intro}</p>
        {copy.sections.map(([heading, body]) => <section key={heading}><h2>{heading}</h2><p>{body}</p></section>)}
        <p className="legal-contact">{lang === "fr" ? "Questions :" : "Questions:"} <a href="https://whop.com/algosphere-ia-lab/" target="_blank" rel="noreferrer">AlgoSphere sur Whop</a></p>
      </article>
    </main>
  );
}

export default function MarketingApp() {
  const [lang, setLang] = useState<Lang>(savedLanguage);
  useInterfaceTranslation(lang);
  const path = window.location.pathname.replace(/\/+$/, "") || "/";
  const legalPage = path === "/confidentialite" || path === "/privacy" ? "privacy"
    : path === "/conditions" || path === "/terms" ? "terms"
    : path === "/remboursement" || path === "/refunds" ? "refunds"
    : null;

  useEffect(() => {
    localStorage.setItem("algosphere_lang", lang);
    document.documentElement.lang = lang === "fr" ? "fr-CA" : lang;
    window.dispatchEvent(new CustomEvent("algosphere-language-change", { detail: lang }));
  }, [lang]);

  useEffect(() => {
    trackEvent("landing_page_view", { properties: { lang } });
    initHeartbeat(() => window.location.pathname);
  }, []);
  if (legalPage) return <LegalPage page={legalPage} lang={lang} setLang={setLang} />;

  const fr = lang === "fr";
  return (
    <main className="mkt">
      <header className="mkt-nav">
        <a className="mkt-brand" href="/"><span>ALGOSPHERE</span> GLOBAL</a>
        <nav>
          <a href="#capacites">{fr ? "Capacités" : "Capabilities"}</a>
          <a href="#abonnements">{fr ? "Abonnements" : "Pricing"}</a>
          <a className="mkt-nav-app" href="/app" onClick={() => trackEvent("primary_cta_click", { properties: { cta: "nav_open_globe", lang } })}>{fr ? "Ouvrir le globe" : "Open globe"}</a>
          <LanguageSelect className="mkt-lang" value={lang} onChange={setLang} />
        </nav>
      </header>

      <section className="mkt-hero">
        <img src="/ads/algosphere-global-campaign.png" alt="" aria-hidden="true" />
        <div className="mkt-hero-shade" />
        <div className="mkt-hero-copy">
          <p className="mkt-eyebrow">{fr ? "INTELLIGENCE GÉOSPATIALE MONDIALE" : "GLOBAL GEOSPATIAL INTELLIGENCE"}</p>
          <h1>{fr ? <>Voyez le monde.<br/><span>Anticipez la suite.</span></> : <>See the world.<br/><span>Anticipate what’s next.</span></>}</h1>
          <p>{fr
            ? "Avions, satellites, navires, météo, tempêtes et caméras réunis sur un globe 3D vivant — dans une seule vue."
            : "Aircraft, satellites, vessels, weather, storms and cameras brought together on one living 3D globe."}</p>
          <div className="mkt-actions">
            <a className="mkt-primary" href="/app" onClick={() => trackEvent("primary_cta_click", { properties: { cta: "hero_explore", lang } })}>{fr ? "Explorer le globe en direct" : "Explore the live globe"}</a>
            <a className="mkt-secondary" href="#abonnements">{fr ? "Voir les abonnements" : "View pricing"}</a>
          </div>
          <small>{fr ? "Accès public immédiat · fonctions avancées avec abonnement" : "Instant public access · advanced tools with membership"}</small>
        </div>
      </section>

      <section className="mkt-trust">
        {[
          ["6", fr ? "couches mondiales" : "global layers"],
          ["3D", fr ? "globe interactif" : "interactive globe"],
          ["24/7", fr ? "veille continue" : "continuous awareness"],
          [fr ? "7 jours" : "7 days", fr ? "d’essai admissible" : "eligible trial"],
        ].map(([value, label]) => <div key={label}><strong>{value}</strong><span>{label}</span></div>)}
      </section>

      <section className="mkt-section mkt-problem">
        <p className="mkt-eyebrow">{fr ? "UNE IMAGE COMPLÈTE" : "ONE COMPLETE PICTURE"}</p>
        <h2>{fr ? "Le monde bouge sur plusieurs couches. Vous ne devriez pas avoir à ouvrir six outils." : "The world moves across many layers. You shouldn’t need six tools to see it."}</h2>
        <p>{fr
          ? "AlgoSphere rassemble les mouvements et conditions qui façonnent une région afin de réduire le bruit et accélérer la compréhension."
          : "AlgoSphere unifies the movements and conditions shaping a region, reducing noise and accelerating understanding."}</p>
      </section>

      <section id="capacites" className="mkt-section">
        <div className="mkt-section-head">
          <div><p className="mkt-eyebrow">{fr ? "COUVERTURE UNIFIÉE" : "UNIFIED COVERAGE"}</p><h2>{fr ? "Une fenêtre sur l’activité mondiale" : "One window into global activity"}</h2></div>
          <a href="/app" onClick={() => trackEvent("primary_cta_click", { properties: { cta: "capabilities_live_data", lang } })}>{fr ? "Voir les données en direct →" : "See live data →"}</a>
        </div>
        <div className="mkt-grid">
          {[
            ["✈", fr ? "Avions" : "Aircraft", fr ? "Mouvements aériens et trajectoires observables." : "Observable air movements and trajectories."],
            ["◈", fr ? "Satellites" : "Satellites", fr ? "Orbites, pays et fonctions dans le même contexte." : "Orbits, countries and functions in one context."],
            ["≋", fr ? "Navires" : "Vessels", fr ? "Activité maritime issue de sources autorisées." : "Maritime activity from approved sources."],
            ["☁", fr ? "Météo" : "Weather", fr ? "Conditions et températures autour des événements." : "Conditions and temperatures around events."],
            ["◎", fr ? "Tempêtes" : "Storms", fr ? "Systèmes actifs et zones d’influence visibles." : "Active systems and visible impact areas."],
            ["▣", fr ? "Caméras" : "Cameras", fr ? "Points visuels approuvés lorsque disponibles." : "Approved visual points when available."],
          ].map(([icon, title, desc]) => <article key={title}><i>{icon}</i><h3>{title}</h3><p>{desc}</p></article>)}
        </div>
      </section>

      <section className="mkt-section mkt-usecases">
        <p className="mkt-eyebrow">{fr ? "POUR VOIR PLUS CLAIR" : "BUILT FOR CLARITY"}</p>
        <h2>{fr ? "Du signal brut à une compréhension exploitable" : "From raw signals to useful awareness"}</h2>
        <div className="mkt-use-grid">
          <article><b>01</b><h3>{fr ? "Surveiller une région" : "Monitor a region"}</h3><p>{fr ? "Observez plusieurs domaines sans perdre le contexte géographique." : "Observe multiple domains without losing geographic context."}</p></article>
          <article><b>02</b><h3>{fr ? "Comprendre un événement" : "Understand an event"}</h3><p>{fr ? "Comparez mouvements, météo et zones d’activité dans une seule vue." : "Compare movements, weather and activity zones in one view."}</p></article>
          <article><b>03</b><h3>{fr ? "Revenir sur l’essentiel" : "Return to what matters"}</h3><p>{fr ? "Les favoris, alertes et historiques premium vous aident à suivre ce qui compte." : "Premium favorites, alerts and history help you track what matters."}</p></article>
        </div>
      </section>

      <section id="abonnements" className="mkt-section mkt-pricing">
        <div className="mkt-center"><p className="mkt-eyebrow">{fr ? "PRIX FONDATEURS" : "FOUNDER PRICING"}</p><h2>{fr ? "Commencez par voir. Passez à l’anticipation." : "Start by seeing. Move toward anticipation."}</h2><p>{fr ? "Essai de 7 jours sur les offres admissibles. Prix en dollars US." : "7-day trial on eligible plans. Prices in US dollars."}</p></div>
        <div className="mkt-price-grid">
          <article><span>EXPLORER</span><h3>39 $<small>/{fr ? "mois" : "month"}</small></h3><ul><li>{fr ? "Données mondiales en direct" : "Live global data"}</li><li>{fr ? "Filtres et favoris" : "Filters and favorites"}</li><li>{fr ? "Historique 24 heures" : "24-hour history"}</li></ul><a href={whop.explorer} target="_blank" rel="noreferrer">{fr ? "Commencer l’essai" : "Start trial"}</a></article>
          <article className="featured"><em>{fr ? "LE PLUS POPULAIRE" : "MOST POPULAR"}</em><span>PRO</span><h3>99 $<small>/{fr ? "mois" : "month"}</small></h3><ul><li>{fr ? "Alertes personnalisées" : "Custom alerts"}</li><li>{fr ? "Replay et zones surveillées" : "Replay and watch zones"}</li><li>{fr ? "Analyses d’intelligence" : "Intelligence analysis"}</li></ul><a href={whop.pro} target="_blank" rel="noreferrer">{fr ? "Commencer l’essai" : "Start trial"}</a></article>
          <article><span>BUSINESS</span><h3>399 $<small>/{fr ? "mois" : "month"}</small></h3><ul><li>{fr ? "Accès pour une équipe" : "Team access"}</li><li>{fr ? "Rapports et exportation" : "Reports and exports"}</li><li>{fr ? "Assistance prioritaire" : "Priority support"}</li></ul><a href={whop.business} target="_blank" rel="noreferrer">{fr ? "Démarrer le pilote" : "Start pilot"}</a></article>
        </div>
        <p className="mkt-checkout-legal">
          {fr ? "Avant l’achat, consultez nos " : "Before purchasing, review our "}
          <a href={fr ? "/conditions" : "/terms"}>{fr ? "Conditions" : "Terms"}</a>,{" "}
          <a href={fr ? "/confidentialite" : "/privacy"}>{fr ? "Politique de confidentialité" : "Privacy policy"}</a>{" "}
          {fr ? "et notre " : "and "}
          <a href={fr ? "/remboursement" : "/refunds"}>{fr ? "Politique de remboursement" : "Refund policy"}</a>.
        </p>
      </section>

      <section className="mkt-final">
        <p className="mkt-eyebrow">ALGOSPHERE GLOBAL</p>
        <h2>{fr ? "Le monde est déjà en mouvement." : "The world is already moving."}</h2>
        <p>{fr ? "Ouvrez le globe et voyez ce qui se passe maintenant." : "Open the globe and see what is happening now."}</p>
        <a className="mkt-primary" href="/app" onClick={() => trackEvent("primary_cta_click", { properties: { cta: "footer_explore", lang } })}>{fr ? "Explorer gratuitement" : "Explore for free"}</a>
      </section>

      <footer className="mkt-footer">
        <a className="mkt-brand" href="/"><span>ALGOSPHERE</span> GLOBAL</a>
        <p>{fr ? "Intelligence géospatiale mondiale. Les données externes peuvent être retardées ou temporairement indisponibles." : "Global geospatial intelligence. External data may be delayed or temporarily unavailable."}</p>
        <nav>
          <a href={fr ? "/confidentialite" : "/privacy"}>{fr ? "Confidentialité" : "Privacy"}</a>
          <a href={fr ? "/conditions" : "/terms"}>{fr ? "Conditions" : "Terms"}</a>
          <a href={fr ? "/remboursement" : "/refunds"}>{fr ? "Remboursement" : "Refunds"}</a>
          <a href="https://whop.com/algosphere-ia-lab/" target="_blank" rel="noreferrer">Contact</a>
          <button type="button" className="mkt-footer-privacy-btn" onClick={() => window.dispatchEvent(new CustomEvent("algosphere-open-privacy-settings"))}>
            {fr ? "Préférences de confidentialité" : "Privacy preferences"}
          </button>
        </nav>
        <small>© 2026 AlgoSphere Global</small>
      </footer>
    </main>
  );
}
