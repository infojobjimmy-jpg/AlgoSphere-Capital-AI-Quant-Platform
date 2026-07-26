import { useEffect, useRef, useState } from "react";
import { Language } from "../../i18n";

type LiveVisitor = {
  visitor_session_id: string;
  type: string;
  username: string | null;
  plan: string | null;
  page: string;
  last_seen_ago_s: number;
  device_type: string;
  utm_source: string | null;
  utm_campaign: string | null;
};

type LiveData = {
  total_online: number;
  members_online: number;
  anonymous_online: number;
  visitors: LiveVisitor[];
};

type HistorySession = {
  id: number;
  visitor_session_id: string;
  type: string;
  username: string | null;
  plan: string | null;
  first_seen: string | null;
  last_seen: string | null;
  duration_label: string;
  page_views: number;
  landing_page: string | null;
  utm_source: string | null;
  utm_campaign: string | null;
  device_type: string | null;
  checkout_clicked: boolean;
  login_completed: boolean;
  converted_to_member: boolean;
};

type HistoryData = {
  total: number;
  page: number;
  per_page: number;
  pages: number;
  today: Record<string, number>;
  sessions: HistorySession[];
};

type Props = { lang: Language };

export default function VisitorsTab({ lang }: Props) {
  const fr = lang === "fr";
  const [live, setLive] = useState<LiveData | null>(null);
  const [history, setHistory] = useState<HistoryData | null>(null);
  const [histPage, setHistPage] = useState(1);
  const [filterAuth, setFilterAuth] = useState("");
  const [filterPlan, setFilterPlan] = useState("");
  const [filterSource, setFilterSource] = useState("");
  const [filterConverted, setFilterConverted] = useState("");
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchLive = () => {
    fetch("/api/analytics/live-visitors", { credentials: "same-origin" })
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((d) => setLive(d as LiveData))
      .catch(() => undefined);
  };

  const fetchHistory = (p = histPage) => {
    const params = new URLSearchParams({
      page: String(p),
      per_page: "20",
      ...(search ? { search } : {}),
      ...(filterAuth ? { authenticated: filterAuth } : {}),
      ...(filterPlan ? { plan: filterPlan } : {}),
      ...(filterSource ? { utm_source: filterSource } : {}),
      ...(filterConverted ? { converted: filterConverted } : {}),
    });
    fetch(`/api/analytics/visitor-history?${params}`, { credentials: "same-origin" })
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((d) => setHistory(d as HistoryData))
      .catch(() => undefined);
  };

  useEffect(() => {
    fetchLive();
    fetchHistory(1);
    timerRef.current = setInterval(fetchLive, 10_000);
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setHistPage(1);
    fetchHistory(1);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterAuth, filterPlan, filterSource, filterConverted, search]);

  useEffect(() => {
    fetchHistory(histPage);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [histPage]);

  const exportCsv = () => {
    const params = new URLSearchParams({
      format: "csv", page: "1", per_page: "1000",
      ...(filterAuth ? { authenticated: filterAuth } : {}),
      ...(filterPlan ? { plan: filterPlan } : {}),
      ...(filterSource ? { utm_source: filterSource } : {}),
      ...(filterConverted ? { converted: filterConverted } : {}),
    });
    window.open(`/api/analytics/visitor-history?${params}`, "_blank");
  };

  const agoLabel = (s: number) => {
    if (s < 60) return fr ? `il y a ${s}s` : `${s}s ago`;
    return fr ? `il y a ${Math.floor(s / 60)}m` : `${Math.floor(s / 60)}m ago`;
  };

  const today = history?.today ?? {};

  return (
    <div className="visitors-tab">
      {/* ── En ligne maintenant ── */}
      <section className="visitors-live">
        <h3>{fr ? "En ligne maintenant" : "Online now"}</h3>
        <div className="visitors-counters">
          <span><strong className="visitors-big">{live?.total_online ?? "—"}</strong><small>{fr ? "total" : "total"}</small></span>
          <span><strong>{live?.members_online ?? "—"}</strong><small>{fr ? "membres" : "members"}</small></span>
          <span><strong>{live?.anonymous_online ?? "—"}</strong><small>{fr ? "anonymes" : "anonymous"}</small></span>
        </div>
        {live && live.visitors.length > 0 ? (
          <div className="visitors-live-table-wrap">
            <table className="visitors-table">
              <thead>
                <tr>
                  <th>{fr ? "Type" : "Type"}</th>
                  <th>{fr ? "Utilisateur / ID" : "User / ID"}</th>
                  <th>{fr ? "Forfait" : "Plan"}</th>
                  <th>{fr ? "Page" : "Page"}</th>
                  <th>{fr ? "Appareil" : "Device"}</th>
                  <th>{fr ? "Source" : "Source"}</th>
                  <th>{fr ? "Vu" : "Seen"}</th>
                </tr>
              </thead>
              <tbody>
                {live.visitors.map((v, i) => (
                  <tr key={i} className={v.type === "Membre identifié" ? "row-member" : "row-anon"}>
                    <td><span className={`presence-badge ${v.type === "Membre identifié" ? "badge-member" : "badge-anon"}`}>{v.type}</span></td>
                    <td>{v.username || v.visitor_session_id}</td>
                    <td style={{ textTransform: "capitalize" }}>{v.plan || "—"}</td>
                    <td title={v.page}>{v.page.slice(0, 24)}</td>
                    <td>{v.device_type}</td>
                    <td>{v.utm_source || "—"}</td>
                    <td>{agoLabel(v.last_seen_ago_s)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="visitors-empty">{fr ? "Aucun visiteur actif." : "No active visitors."}</p>
        )}
      </section>

      {/* ── Résumé du jour ── */}
      <section className="visitors-summary">
        <h3>{fr ? "Aujourd'hui" : "Today"}</h3>
        <div className="owner-metrics">
          <span><small>{fr ? "Nouveaux" : "New"}</small><strong>{today.new_today ?? "—"}</strong></span>
          <span><small>{fr ? "De retour" : "Returning"}</small><strong>{today.returning_today ?? "—"}</strong></span>
          <span><small>{fr ? "Membres" : "Members"}</small><strong>{today.auth_today ?? "—"}</strong></span>
          <span><small>{fr ? "Clics Whop" : "Whop clicks"}</small><strong>{today.checkout_today ?? "—"}</strong></span>
          <span><small>{fr ? "Conversions" : "Converted"}</small><strong>{today.converted_today ?? "—"}</strong></span>
          <span><small>{fr ? "Total 7 j" : "Total 7d"}</small><strong>{today.total_7d ?? "—"}</strong></span>
          <span><small>{fr ? "Total 30 j" : "Total 30d"}</small><strong>{today.total_30d ?? "—"}</strong></span>
          <span><small>{fr ? "Durée moy. 7 j" : "Avg dur. 7d"}</small><strong>{today.avg_duration_7d ? `${today.avg_duration_7d}s` : "—"}</strong></span>
        </div>
      </section>

      {/* ── Historique ── */}
      <section className="visitors-history">
        <div className="visitors-history-header">
          <h3>{fr ? "Historique" : "History"}</h3>
          <button type="button" className="member-subscribe-link" onClick={exportCsv}>
            {fr ? "Exporter CSV" : "Export CSV"}
          </button>
        </div>
        <div className="visitors-filters">
          <input
            placeholder={fr ? "Rechercher…" : "Search…"}
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") setSearch(searchInput); }}
          />
          <select value={filterAuth} onChange={(e) => setFilterAuth(e.target.value)}>
            <option value="">{fr ? "Tous" : "All"}</option>
            <option value="true">{fr ? "Membres" : "Members"}</option>
            <option value="false">{fr ? "Anonymes" : "Anonymous"}</option>
          </select>
          <select value={filterPlan} onChange={(e) => setFilterPlan(e.target.value)}>
            <option value="">{fr ? "Tous les forfaits" : "All plans"}</option>
            <option value="explorer">Explorer</option>
            <option value="pro">Pro</option>
            <option value="business">Business</option>
          </select>
          <select value={filterConverted} onChange={(e) => setFilterConverted(e.target.value)}>
            <option value="">{fr ? "Toutes les conversions" : "All conversions"}</option>
            <option value="true">{fr ? "Convertis" : "Converted"}</option>
            <option value="false">{fr ? "Non convertis" : "Not converted"}</option>
          </select>
        </div>
        {history ? (
          <>
            <div className="visitors-live-table-wrap">
              <table className="visitors-table">
                <thead>
                  <tr>
                    <th>{fr ? "Type" : "Type"}</th>
                    <th>{fr ? "Utilisateur" : "User"}</th>
                    <th>{fr ? "Forfait" : "Plan"}</th>
                    <th>{fr ? "Première visite" : "First seen"}</th>
                    <th>{fr ? "Dernière activité" : "Last seen"}</th>
                    <th>{fr ? "Durée" : "Duration"}</th>
                    <th>{fr ? "Pages" : "Pages"}</th>
                    <th>{fr ? "Source" : "Source"}</th>
                    <th>{fr ? "Converti" : "Converted"}</th>
                  </tr>
                </thead>
                <tbody>
                  {history.sessions.map((s) => (
                    <tr key={s.id} className={s.type === "Membre identifié" ? "row-member" : "row-anon"}>
                      <td><span className={`presence-badge ${s.type === "Membre identifié" ? "badge-member" : "badge-anon"}`}>{s.type}</span></td>
                      <td>{s.username || s.visitor_session_id}</td>
                      <td style={{ textTransform: "capitalize" }}>{s.plan || "—"}</td>
                      <td>{s.first_seen ? new Date(s.first_seen).toLocaleString(fr ? "fr-CA" : "en-US") : "—"}</td>
                      <td>{s.last_seen ? new Date(s.last_seen).toLocaleString(fr ? "fr-CA" : "en-US") : "—"}</td>
                      <td>{s.duration_label}</td>
                      <td>{s.page_views}</td>
                      <td>{s.utm_source || "—"}</td>
                      <td>{s.converted_to_member ? "✓" : ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="visitors-pagination">
              <button type="button" disabled={histPage <= 1} onClick={() => setHistPage(histPage - 1)}>←</button>
              <span>{fr ? `Page ${histPage} / ${history.pages}` : `Page ${histPage} of ${history.pages}`}</span>
              <button type="button" disabled={histPage >= history.pages} onClick={() => setHistPage(histPage + 1)}>→</button>
              <small>{fr ? `${history.total} sessions` : `${history.total} sessions`}</small>
            </div>
          </>
        ) : (
          <p className="visitors-empty">{fr ? "Chargement…" : "Loading…"}</p>
        )}
      </section>
    </div>
  );
}
