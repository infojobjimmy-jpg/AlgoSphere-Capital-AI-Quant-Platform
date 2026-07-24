type Lang = "fr" | "en";

type Props = {
  lang: Lang;
  onClose: () => void;
};

const plans = [
  {
    id: "explorer",
    fr: { name: "Explorer", price: "9,99 $ CA", features: ["Données mondiales en direct", "Filtres et favoris", "Historique 24 heures"] },
    en: { name: "Explorer", price: "CA$9.99", features: ["Live global data", "Filters and favourites", "24-hour history"] },
  },
  {
    id: "pro",
    featured: true,
    fr: { name: "Pro", price: "24,99 $ CA", features: ["Alertes personnalisées", "Replay et zones surveillées", "Analyses d’intelligence"] },
    en: { name: "Pro", price: "CA$24.99", features: ["Custom alerts", "Replay and watch zones", "Intelligence analysis"] },
  },
  {
    id: "business",
    fr: { name: "Business", price: "99 $ CA", features: ["Accès pour une équipe", "Rapports et exportation", "Assistance prioritaire"] },
    en: { name: "Business", price: "CA$99", features: ["Team access", "Reports and exports", "Priority support"] },
  },
];

export default function SubscriptionPanel({ lang, onClose }: Props) {
  const checkoutUrls: Record<string, string | undefined> = {
    explorer:
      import.meta.env.VITE_WHOP_EXPLORER_URL ||
      "https://whop.com/algosphere-ia-lab/algosphere-explorer-founder",
    pro:
      import.meta.env.VITE_WHOP_PRO_URL ||
      "https://whop.com/algosphere-ia-lab/algosphere-pro-founder",
    business:
      import.meta.env.VITE_WHOP_BUSINESS_URL ||
      "https://whop.com/algosphere-ia-lab/algosphere-business-pilot",
  };
  const fr = lang === "fr";

  return (
    <div className="subscription-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="subscription-panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="subscription-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <button className="subscription-close" type="button" onClick={onClose} aria-label={fr ? "Fermer" : "Close"}>
          ×
        </button>
        <img
          className="subscription-campaign"
          src="/ads/algosphere-global-campaign.png"
          alt={fr ? "AlgoSphere Global — intelligence géospatiale mondiale" : "AlgoSphere Global — global geospatial intelligence"}
        />
        <div className="subscription-kicker">{fr ? "PRIX FONDATEURS · PLACES LIMITÉES" : "FOUNDING PRICES · LIMITED AVAILABILITY"}</div>
        <h2 id="subscription-title">{fr ? "Passez de l’observation à l’anticipation" : "Move from observation to anticipation"}</h2>
        <p className="subscription-lead">
          {fr
            ? "Les premiers membres conservent leur prix tant que leur abonnement demeure actif. Essai de 7 jours."
            : "Founding members keep their price while their subscription remains active. 7-day trial."}
        </p>
        <div className="subscription-grid">
          {plans.map((plan) => {
            const copy = plan[lang];
            const checkoutUrl = checkoutUrls[plan.id];
            return (
              <article key={plan.id} className={`subscription-card ${plan.featured ? "is-featured" : ""}`}>
                {plan.featured ? <span className="subscription-badge">{fr ? "LE PLUS POPULAIRE" : "MOST POPULAR"}</span> : null}
                <h3>{copy.name}</h3>
                <div className="subscription-price">{copy.price}<small>/{fr ? "mois" : "month"}</small></div>
                <ul>{copy.features.map((feature) => <li key={feature}>{feature}</li>)}</ul>
                {checkoutUrl ? (
                  <a className="subscription-cta" href={checkoutUrl} target="_blank" rel="noreferrer">
                    {fr ? "Commencer l’essai" : "Start free trial"}
                  </a>
                ) : (
                  <button className="subscription-cta" type="button" disabled>
                    {fr ? "Lien Whop bientôt disponible" : "Whop link coming soon"}
                  </button>
                )}
              </article>
            );
          })}
        </div>
        <div className="subscription-enterprise">
          <strong>{fr ? "Entreprise" : "Enterprise"}</strong>
          <span>{fr ? "À partir de 1 500 $ US/mois · accès personnalisé, API et déploiement d’équipe. Vente sur devis." : "From US$1,500/month · custom access, API and team deployment. Contact for a quote."}</span>
          <a className="subscription-enterprise-link" href="https://whop.com/algosphere-ia-lab/" target="_blank" rel="noreferrer">
            {fr ? "Nous contacter" : "Contact us"}
          </a>
        </div>
      </section>
    </div>
  );
}
