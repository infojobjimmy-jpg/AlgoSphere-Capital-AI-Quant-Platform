import { useEffect } from "react";

export type Language = "fr" | "en" | "es" | "pt" | "de" | "it";
export const languages: Array<{ code: Language; label: string }> = [
  { code: "fr", label: "FR" }, { code: "en", label: "EN" }, { code: "es", label: "ES" },
  { code: "pt", label: "PT" }, { code: "de", label: "DE" }, { code: "it", label: "IT" },
];

export function savedLanguage(): Language {
  const value = localStorage.getItem("algosphere_lang");
  return languages.some((item) => item.code === value) ? value as Language : "fr";
}

const dictionaries: Partial<Record<Language, Record<string, string>>> = {
  es: {
    "Open globe": "Abrir el globo", "Capabilities": "Capacidades", "Pricing": "Precios",
    "GLOBAL GEOSPATIAL INTELLIGENCE": "INTELIGENCIA GEOESPACIAL GLOBAL",
    "See the world.": "Mira el mundo.", "Anticipate what’s next.": "Anticipa lo que sigue.",
    "Aircraft, satellites, vessels, weather, storms and cameras brought together on one living 3D globe.": "Aviones, satélites, barcos, clima, tormentas y cámaras reunidos en un globo 3D vivo.",
    "Explore the live globe": "Explorar el globo en vivo", "View pricing": "Ver precios",
    "Instant public access · advanced tools with membership": "Acceso público inmediato · herramientas avanzadas con suscripción",
    "global layers": "capas globales", "interactive globe": "globo interactivo", "continuous awareness": "vigilancia continua",
    "7 days": "7 días", "eligible trial": "de prueba elegible", "ONE COMPLETE PICTURE": "UNA IMAGEN COMPLETA",
    "The world moves across many layers. You shouldn’t need six tools to see it.": "El mundo se mueve en muchas capas. No deberías necesitar seis herramientas para verlo.",
    "AlgoSphere unifies the movements and conditions shaping a region, reducing noise and accelerating understanding.": "AlgoSphere unifica los movimientos y las condiciones de una región, reduce el ruido y acelera la comprensión.",
    "UNIFIED COVERAGE": "COBERTURA UNIFICADA", "One window into global activity": "Una ventana a la actividad global",
    "See live data →": "Ver datos en vivo →", "Aircraft": "Aviones", "Vessels": "Barcos", "Weather": "Clima",
    "Storms": "Tormentas", "Cameras": "Cámaras", "Observable air movements and trajectories.": "Movimientos aéreos y trayectorias observables.",
    "Orbits, countries and functions in one context.": "Órbitas, países y funciones en un mismo contexto.",
    "Maritime activity from approved sources.": "Actividad marítima de fuentes autorizadas.",
    "Conditions and temperatures around events.": "Condiciones y temperaturas alrededor de los eventos.",
    "Active systems and visible impact areas.": "Sistemas activos y zonas de impacto visibles.",
    "Approved visual points when available.": "Puntos visuales autorizados cuando están disponibles.",
    "BUILT FOR CLARITY": "DISEÑADO PARA LA CLARIDAD", "From raw signals to useful awareness": "De señales brutas a una comprensión útil",
    "Monitor a region": "Vigilar una región", "Understand an event": "Comprender un evento", "Return to what matters": "Volver a lo esencial",
    "FOUNDER PRICING": "PRECIOS FUNDADORES", "Start by seeing. Move toward anticipation.": "Empieza observando. Avanza hacia la anticipación.",
    "7-day trial on eligible plans. Prices in US dollars.": "Prueba de 7 días en planes elegibles. Precios en dólares estadounidenses.",
    "Live global data": "Datos globales en vivo", "Filters and favorites": "Filtros y favoritos", "24-hour history": "Historial de 24 horas",
    "Start trial": "Iniciar prueba", "MOST POPULAR": "MÁS POPULAR", "Custom alerts": "Alertas personalizadas",
    "Replay and watch zones": "Repetición y zonas vigiladas", "Intelligence analysis": "Análisis de inteligencia",
    "Team access": "Acceso para equipos", "Reports and exports": "Informes y exportaciones", "Priority support": "Soporte prioritario",
    "Start pilot": "Iniciar piloto", "/month": "/mes", "Before purchasing, review our": "Antes de comprar, consulta nuestras",
    "Terms": "Condiciones", "Privacy policy": "Política de privacidad", "and": "y", "Refund policy": "Política de reembolso",
    "The world is already moving.": "El mundo ya está en movimiento.", "Open the globe and see what is happening now.": "Abre el globo y mira lo que ocurre ahora.",
    "Explore for free": "Explorar gratis", "Privacy": "Privacidad", "Refunds": "Reembolsos",
    "Member access": "Acceso de miembros", "Community": "Comunidad", "GPS navigation": "Navegación GPS",
    "Unified geospatial intelligence · verified global data": "Inteligencia geoespacial unificada · datos globales verificados",
    "Active storms": "Tormentas activas", "Updated": "Actualizado", "Ships": "Barcos", "Activity zones": "Zonas de actividad",
    "All types": "Todos los tipos", "All countries": "Todos los países", "All": "Todos", "Private": "Privado",
    "Emergency": "Emergencia", "Government": "Gobierno", "Passenger": "Pasajeros", "Fishing": "Pesca",
    "Premium filters — subscription required": "Filtros premium — suscripción requerida",
    "Car, truck, routes and services": "Auto, camión, rutas y servicios", "Explore the world": "Explorar el mundo",
    "Fullscreen globe and live layers": "Globo a pantalla completa y capas en vivo", "Unlock AlgoSphere": "Desbloquear AlgoSphere",
    "Favorites, alerts and history": "Favoritos, alertas e historial", "No active anomaly alerts.": "No hay alertas de anomalías activas.",
    "Canada–United States GPS": "GPS Canadá–Estados Unidos", "Everyday navigation with an additional truck profile. The globe and all geospatial layers remain available.": "Navegación diaria con un perfil adicional para camiones. El globo y todas las capas siguen disponibles.",
    "Car / standard": "Auto / normal", "Truck": "Camión", "Show my position on the globe": "Mostrar mi posición en el globo",
    "Destination (Canada or United States)": "Destino (Canadá o Estados Unidos)", "Address, city or place": "Dirección, ciudad o lugar",
    "Calculate and show route": "Calcular y mostrar la ruta", "Services near your position": "Servicios cerca de tu posición",
    "Standard navigation": "Navegación normal", "Traffic and weather": "Tráfico y clima", "Favorites and alerts": "Favoritos y alertas",
    "Hazardous materials": "Materiales peligrosos", "Scales and borders": "Básculas y fronteras", "Truck services": "Servicios para camiones",
    "My account": "Mi cuenta", "My membership": "Mi suscripción", "Plan": "Plan", "Status": "Estado",
    "Expiration / renewal": "Vencimiento / renovación", "Owner access": "Acceso del propietario", "My favorites": "Mis favoritos",
    "My custom alerts": "Mis alertas personalizadas", "Pilot campaign": "Campaña piloto", "Visitors · 7d": "Visitantes · 7 d",
    "Whop clicks": "Clics a Whop", "Click rate": "Tasa de clics", "Logins": "Conexiones", "Owner security": "Seguridad del propietario",
    "Current code": "Código actual", "New code": "Código nuevo", "Change code": "Cambiar código", "Sign out": "Cerrar sesión",
    "Find your world": "Encuentra tu mundo", "New": "Nuevo", "People": "Personas", "Global chat": "Chat global",
    "Avatar & profile": "Avatar y perfil", "YOUR PRIVATE CODE": "TU CÓDIGO PRIVADO", "Enter a code": "Ingresa un código",
    "Add": "Agregar", "Globe presence": "Presencia en el globo", "Enable GPS": "Activar GPS", "Stop": "Detener",
    "My people": "Mis contactos", "Discover the community": "Descubrir la comunidad", "Display name": "Nombre visible",
    "City": "Ciudad", "Country": "País", "Family": "Familia", "Friends": "Amigos", "Dating": "Citas",
    "Save profile": "Guardar perfil", "Close": "Cerrar", "Accept": "Aceptar", "Decline": "Rechazar", "Learn more": "Más información",
  },
  pt: {
    "Open globe": "Abrir o globo", "Capabilities": "Recursos", "Pricing": "Preços",
    "GLOBAL GEOSPATIAL INTELLIGENCE": "INTELIGÊNCIA GEOESPACIAL GLOBAL",
    "See the world.": "Veja o mundo.", "Anticipate what’s next.": "Antecipe o que vem a seguir.",
    "Aircraft, satellites, vessels, weather, storms and cameras brought together on one living 3D globe.": "Aviões, satélites, embarcações, clima, tempestades e câmeras reunidos em um globo 3D vivo.",
    "Explore the live globe": "Explorar o globo ao vivo", "View pricing": "Ver preços",
    "Instant public access · advanced tools with membership": "Acesso público imediato · ferramentas avançadas com assinatura",
    "global layers": "camadas globais", "interactive globe": "globo interativo", "continuous awareness": "monitoramento contínuo",
    "7 days": "7 dias", "eligible trial": "de teste elegível", "ONE COMPLETE PICTURE": "UMA VISÃO COMPLETA",
    "The world moves across many layers. You shouldn’t need six tools to see it.": "O mundo se move em várias camadas. Você não deveria precisar de seis ferramentas para enxergá-lo.",
    "UNIFIED COVERAGE": "COBERTURA UNIFICADA", "One window into global activity": "Uma janela para a atividade global",
    "See live data →": "Ver dados ao vivo →", "Aircraft": "Aviões", "Vessels": "Embarcações", "Weather": "Clima",
    "Storms": "Tempestades", "Cameras": "Câmeras", "Observable air movements and trajectories.": "Movimentos aéreos e trajetórias observáveis.",
    "Maritime activity from approved sources.": "Atividade marítima de fontes autorizadas.",
    "Conditions and temperatures around events.": "Condições e temperaturas ao redor dos eventos.",
    "BUILT FOR CLARITY": "CRIADO PARA DAR CLAREZA", "From raw signals to useful awareness": "De sinais brutos a uma visão útil",
    "Monitor a region": "Monitorar uma região", "Understand an event": "Entender um evento", "Return to what matters": "Voltar ao essencial",
    "FOUNDER PRICING": "PREÇOS DE FUNDADOR", "Start by seeing. Move toward anticipation.": "Comece observando. Avance para a antecipação.",
    "7-day trial on eligible plans. Prices in US dollars.": "Teste de 7 dias nos planos elegíveis. Preços em dólares americanos.",
    "Live global data": "Dados globais ao vivo", "Filters and favorites": "Filtros e favoritos", "24-hour history": "Histórico de 24 horas",
    "Start trial": "Iniciar teste", "MOST POPULAR": "MAIS POPULAR", "Custom alerts": "Alertas personalizados",
    "Replay and watch zones": "Replay e zonas monitoradas", "Intelligence analysis": "Análises de inteligência",
    "Team access": "Acesso para equipe", "Reports and exports": "Relatórios e exportações", "Priority support": "Suporte prioritário",
    "Start pilot": "Iniciar piloto", "/month": "/mês", "Terms": "Termos", "Privacy policy": "Política de privacidade",
    "Refund policy": "Política de reembolso", "The world is already moving.": "O mundo já está em movimento.",
    "Open the globe and see what is happening now.": "Abra o globo e veja o que está acontecendo agora.",
    "Explore for free": "Explorar grátis", "Privacy": "Privacidade", "Refunds": "Reembolsos",
    "Member access": "Acesso de membro", "Community": "Comunidade", "GPS navigation": "Navegação GPS",
    "Unified geospatial intelligence · verified global data": "Inteligência geoespacial unificada · dados globais verificados",
    "Active storms": "Tempestades ativas", "Updated": "Atualizado", "Ships": "Embarcações", "Activity zones": "Zonas de atividade",
    "All types": "Todos os tipos", "All countries": "Todos os países", "All": "Todos", "Private": "Privado",
    "Emergency": "Emergência", "Government": "Governo", "Passenger": "Passageiros", "Fishing": "Pesca",
    "Premium filters — subscription required": "Filtros premium — assinatura necessária",
    "Car, truck, routes and services": "Carro, caminhão, rotas e serviços", "Explore the world": "Explorar o mundo",
    "Fullscreen globe and live layers": "Globo em tela cheia e camadas ao vivo", "Unlock AlgoSphere": "Desbloquear AlgoSphere",
    "Favorites, alerts and history": "Favoritos, alertas e histórico", "No active anomaly alerts.": "Nenhum alerta de anomalia ativo.",
    "Canada–United States GPS": "GPS Canadá–Estados Unidos", "Car / standard": "Carro / normal", "Truck": "Caminhão",
    "Show my position on the globe": "Mostrar minha posição no globo", "Destination (Canada or United States)": "Destino (Canadá ou Estados Unidos)",
    "Address, city or place": "Endereço, cidade ou local", "Calculate and show route": "Calcular e mostrar rota",
    "Services near your position": "Serviços perto da sua posição", "Standard navigation": "Navegação normal",
    "Traffic and weather": "Trânsito e clima", "Favorites and alerts": "Favoritos e alertas",
    "Hazardous materials": "Materiais perigosos", "Scales and borders": "Balanças e fronteiras", "Truck services": "Serviços para caminhões",
    "My account": "Minha conta", "My membership": "Minha assinatura", "Status": "Status", "Expiration / renewal": "Validade / renovação",
    "Owner access": "Acesso do proprietário", "My favorites": "Meus favoritos", "My custom alerts": "Meus alertas personalizados",
    "Pilot campaign": "Campanha piloto", "Visitors · 7d": "Visitantes · 7 d", "Whop clicks": "Cliques no Whop",
    "Click rate": "Taxa de cliques", "Logins": "Entradas", "Owner security": "Segurança do proprietário",
    "Current code": "Código atual", "New code": "Novo código", "Change code": "Alterar código", "Sign out": "Sair",
    "Find your world": "Encontre seu mundo", "New": "Novo", "People": "Pessoas", "Global chat": "Chat global",
    "Avatar & profile": "Avatar e perfil", "YOUR PRIVATE CODE": "SEU CÓDIGO PRIVADO", "Enter a code": "Digite um código",
    "Add": "Adicionar", "Globe presence": "Presença no globo", "Enable GPS": "Ativar GPS", "Stop": "Parar",
    "My people": "Meus contatos", "Discover the community": "Descobrir a comunidade", "Display name": "Nome de exibição",
    "City": "Cidade", "Country": "País", "Family": "Família", "Friends": "Amigos", "Dating": "Relacionamentos",
    "Save profile": "Salvar perfil", "Close": "Fechar", "Accept": "Aceitar", "Decline": "Recusar", "Learn more": "Saiba mais",
  },
  de: {
    "Open globe": "Globus öffnen", "Capabilities": "Funktionen", "Pricing": "Preise",
    "GLOBAL GEOSPATIAL INTELLIGENCE": "GLOBALE GEOINTELLIGENZ", "See the world.": "Sieh die Welt.",
    "Anticipate what’s next.": "Erkenne, was als Nächstes kommt.",
    "Aircraft, satellites, vessels, weather, storms and cameras brought together on one living 3D globe.": "Flugzeuge, Satelliten, Schiffe, Wetter, Stürme und Kameras auf einem lebendigen 3D-Globus.",
    "Explore the live globe": "Live-Globus erkunden", "View pricing": "Preise ansehen",
    "Instant public access · advanced tools with membership": "Sofortiger öffentlicher Zugang · erweiterte Werkzeuge mit Mitgliedschaft",
    "global layers": "globale Ebenen", "interactive globe": "interaktiver Globus", "continuous awareness": "kontinuierliche Beobachtung",
    "7 days": "7 Tage", "eligible trial": "berechtigter Testzeitraum", "ONE COMPLETE PICTURE": "EIN VOLLSTÄNDIGES BILD",
    "The world moves across many layers. You shouldn’t need six tools to see it.": "Die Welt bewegt sich auf vielen Ebenen. Du solltest keine sechs Werkzeuge brauchen, um sie zu sehen.",
    "UNIFIED COVERAGE": "VEREINTE ABDECKUNG", "One window into global activity": "Ein Fenster zur globalen Aktivität",
    "See live data →": "Live-Daten ansehen →", "Aircraft": "Flugzeuge", "Vessels": "Schiffe", "Weather": "Wetter",
    "Storms": "Stürme", "Cameras": "Kameras", "Observable air movements and trajectories.": "Beobachtbare Flugbewegungen und Flugbahnen.",
    "Maritime activity from approved sources.": "Maritime Aktivität aus zugelassenen Quellen.",
    "Conditions and temperatures around events.": "Bedingungen und Temperaturen rund um Ereignisse.",
    "BUILT FOR CLARITY": "FÜR KLARHEIT ENTWICKELT", "From raw signals to useful awareness": "Von Rohsignalen zu verwertbarer Übersicht",
    "Monitor a region": "Eine Region überwachen", "Understand an event": "Ein Ereignis verstehen", "Return to what matters": "Zum Wesentlichen zurückkehren",
    "FOUNDER PRICING": "GRÜNDERPREISE", "Start by seeing. Move toward anticipation.": "Beginne mit dem Sehen. Gehe zur Vorausschau über.",
    "7-day trial on eligible plans. Prices in US dollars.": "7-tägiger Test bei berechtigten Tarifen. Preise in US-Dollar.",
    "Live global data": "Globale Live-Daten", "Filters and favorites": "Filter und Favoriten", "24-hour history": "24-Stunden-Verlauf",
    "Start trial": "Test starten", "MOST POPULAR": "AM BELIEBTESTEN", "Custom alerts": "Benutzerdefinierte Alarme",
    "Replay and watch zones": "Wiedergabe und Überwachungszonen", "Intelligence analysis": "Intelligence-Analysen",
    "Team access": "Teamzugang", "Reports and exports": "Berichte und Exporte", "Priority support": "Priorisierter Support",
    "Start pilot": "Pilot starten", "/month": "/Monat", "Terms": "Bedingungen", "Privacy policy": "Datenschutzrichtlinie",
    "Refund policy": "Erstattungsrichtlinie", "The world is already moving.": "Die Welt ist bereits in Bewegung.",
    "Open the globe and see what is happening now.": "Öffne den Globus und sieh, was gerade geschieht.",
    "Explore for free": "Kostenlos erkunden", "Privacy": "Datenschutz", "Refunds": "Erstattungen",
    "Member access": "Mitgliederzugang", "Community": "Community", "GPS navigation": "GPS-Navigation",
    "Unified geospatial intelligence · verified global data": "Vereinte Geointelligenz · verifizierte globale Daten",
    "Active storms": "Aktive Stürme", "Updated": "Aktualisiert", "Ships": "Schiffe", "Activity zones": "Aktivitätszonen",
    "All types": "Alle Typen", "All countries": "Alle Länder", "All": "Alle", "Private": "Privat",
    "Emergency": "Notfall", "Government": "Regierung", "Passenger": "Passagiere", "Fishing": "Fischerei",
    "Premium filters — subscription required": "Premium-Filter — Abonnement erforderlich",
    "Car, truck, routes and services": "Auto, Lkw, Routen und Dienste", "Explore the world": "Die Welt erkunden",
    "Fullscreen globe and live layers": "Vollbild-Globus und Live-Ebenen", "Unlock AlgoSphere": "AlgoSphere freischalten",
    "Favorites, alerts and history": "Favoriten, Alarme und Verlauf", "No active anomaly alerts.": "Keine aktiven Anomaliealarme.",
    "Canada–United States GPS": "GPS Kanada–USA", "Car / standard": "Auto / Standard", "Truck": "Lkw",
    "Show my position on the globe": "Meine Position auf dem Globus anzeigen", "Destination (Canada or United States)": "Ziel (Kanada oder USA)",
    "Address, city or place": "Adresse, Stadt oder Ort", "Calculate and show route": "Route berechnen und anzeigen",
    "Services near your position": "Dienste in deiner Nähe", "Standard navigation": "Standardnavigation",
    "Traffic and weather": "Verkehr und Wetter", "Favorites and alerts": "Favoriten und Alarme",
    "Hazardous materials": "Gefahrgut", "Scales and borders": "Waagen und Grenzen", "Truck services": "Lkw-Dienste",
    "My account": "Mein Konto", "My membership": "Meine Mitgliedschaft", "Status": "Status",
    "Expiration / renewal": "Ablauf / Verlängerung", "Owner access": "Inhaberzugang", "My favorites": "Meine Favoriten",
    "My custom alerts": "Meine benutzerdefinierten Alarme", "Pilot campaign": "Pilotkampagne", "Visitors · 7d": "Besucher · 7 T.",
    "Whop clicks": "Whop-Klicks", "Click rate": "Klickrate", "Logins": "Anmeldungen", "Owner security": "Inhabersicherheit",
    "Current code": "Aktueller Code", "New code": "Neuer Code", "Change code": "Code ändern", "Sign out": "Abmelden",
    "Find your world": "Finde deine Welt", "New": "Neu", "People": "Kontakte", "Global chat": "Globaler Chat",
    "Avatar & profile": "Avatar und Profil", "YOUR PRIVATE CODE": "DEIN PRIVATER CODE", "Enter a code": "Code eingeben",
    "Add": "Hinzufügen", "Globe presence": "Globus-Präsenz", "Enable GPS": "GPS aktivieren", "Stop": "Stoppen",
    "My people": "Meine Kontakte", "Discover the community": "Community entdecken", "Display name": "Anzeigename",
    "City": "Stadt", "Country": "Land", "Family": "Familie", "Friends": "Freunde", "Dating": "Partnersuche",
    "Save profile": "Profil speichern", "Close": "Schließen", "Accept": "Akzeptieren", "Decline": "Ablehnen", "Learn more": "Mehr erfahren",
  },
  it: {
    "Open globe": "Apri il globo", "Capabilities": "Funzionalità", "Pricing": "Prezzi",
    "GLOBAL GEOSPATIAL INTELLIGENCE": "INTELLIGENCE GEOSPAZIALE GLOBALE", "See the world.": "Guarda il mondo.",
    "Anticipate what’s next.": "Anticipa ciò che viene dopo.",
    "Aircraft, satellites, vessels, weather, storms and cameras brought together on one living 3D globe.": "Aerei, satelliti, navi, meteo, tempeste e telecamere riuniti in un globo 3D vivo.",
    "Explore the live globe": "Esplora il globo in diretta", "View pricing": "Vedi i prezzi",
    "Instant public access · advanced tools with membership": "Accesso pubblico immediato · strumenti avanzati con abbonamento",
    "global layers": "livelli globali", "interactive globe": "globo interattivo", "continuous awareness": "monitoraggio continuo",
    "7 days": "7 giorni", "eligible trial": "di prova idonea", "ONE COMPLETE PICTURE": "UN QUADRO COMPLETO",
    "The world moves across many layers. You shouldn’t need six tools to see it.": "Il mondo si muove su molti livelli. Non dovresti aver bisogno di sei strumenti per vederlo.",
    "UNIFIED COVERAGE": "COPERTURA UNIFICATA", "One window into global activity": "Una finestra sull’attività globale",
    "See live data →": "Vedi i dati in diretta →", "Aircraft": "Aerei", "Vessels": "Navi", "Weather": "Meteo",
    "Storms": "Tempeste", "Cameras": "Telecamere", "Observable air movements and trajectories.": "Movimenti aerei e traiettorie osservabili.",
    "Maritime activity from approved sources.": "Attività marittima da fonti autorizzate.",
    "Conditions and temperatures around events.": "Condizioni e temperature attorno agli eventi.",
    "BUILT FOR CLARITY": "CREATO PER LA CHIAREZZA", "From raw signals to useful awareness": "Dai segnali grezzi a una visione utile",
    "Monitor a region": "Monitora una regione", "Understand an event": "Comprendi un evento", "Return to what matters": "Torna a ciò che conta",
    "FOUNDER PRICING": "PREZZI FONDATORI", "Start by seeing. Move toward anticipation.": "Inizia osservando. Passa all’anticipazione.",
    "7-day trial on eligible plans. Prices in US dollars.": "Prova di 7 giorni sui piani idonei. Prezzi in dollari USA.",
    "Live global data": "Dati globali in diretta", "Filters and favorites": "Filtri e preferiti", "24-hour history": "Cronologia di 24 ore",
    "Start trial": "Inizia la prova", "MOST POPULAR": "PIÙ POPOLARE", "Custom alerts": "Avvisi personalizzati",
    "Replay and watch zones": "Replay e zone monitorate", "Intelligence analysis": "Analisi di intelligence",
    "Team access": "Accesso per team", "Reports and exports": "Report ed esportazioni", "Priority support": "Supporto prioritario",
    "Start pilot": "Avvia il progetto pilota", "/month": "/mese", "Terms": "Condizioni", "Privacy policy": "Informativa sulla privacy",
    "Refund policy": "Politica di rimborso", "The world is already moving.": "Il mondo è già in movimento.",
    "Open the globe and see what is happening now.": "Apri il globo e guarda cosa sta succedendo ora.",
    "Explore for free": "Esplora gratis", "Privacy": "Privacy", "Refunds": "Rimborsi",
    "Member access": "Accesso membri", "Community": "Comunità", "GPS navigation": "Navigazione GPS",
    "Unified geospatial intelligence · verified global data": "Intelligence geospaziale unificata · dati globali verificati",
    "Active storms": "Tempeste attive", "Updated": "Aggiornato", "Ships": "Navi", "Activity zones": "Zone di attività",
    "All types": "Tutti i tipi", "All countries": "Tutti i Paesi", "All": "Tutti", "Private": "Privato",
    "Emergency": "Emergenza", "Government": "Governo", "Passenger": "Passeggeri", "Fishing": "Pesca",
    "Premium filters — subscription required": "Filtri premium — abbonamento richiesto",
    "Car, truck, routes and services": "Auto, camion, percorsi e servizi", "Explore the world": "Esplora il mondo",
    "Fullscreen globe and live layers": "Globo a schermo intero e livelli in diretta", "Unlock AlgoSphere": "Sblocca AlgoSphere",
    "Favorites, alerts and history": "Preferiti, avvisi e cronologia", "No active anomaly alerts.": "Nessun avviso di anomalia attivo.",
    "Canada–United States GPS": "GPS Canada–Stati Uniti", "Car / standard": "Auto / standard", "Truck": "Camion",
    "Show my position on the globe": "Mostra la mia posizione sul globo", "Destination (Canada or United States)": "Destinazione (Canada o Stati Uniti)",
    "Address, city or place": "Indirizzo, città o luogo", "Calculate and show route": "Calcola e mostra il percorso",
    "Services near your position": "Servizi vicino alla tua posizione", "Standard navigation": "Navigazione standard",
    "Traffic and weather": "Traffico e meteo", "Favorites and alerts": "Preferiti e avvisi",
    "Hazardous materials": "Materiali pericolosi", "Scales and borders": "Pese e frontiere", "Truck services": "Servizi per camion",
    "My account": "Il mio account", "My membership": "Il mio abbonamento", "Status": "Stato",
    "Expiration / renewal": "Scadenza / rinnovo", "Owner access": "Accesso proprietario", "My favorites": "I miei preferiti",
    "My custom alerts": "I miei avvisi personalizzati", "Pilot campaign": "Campagna pilota", "Visitors · 7d": "Visitatori · 7 g",
    "Whop clicks": "Clic su Whop", "Click rate": "Tasso di clic", "Logins": "Accessi", "Owner security": "Sicurezza proprietario",
    "Current code": "Codice attuale", "New code": "Nuovo codice", "Change code": "Cambia codice", "Sign out": "Esci",
    "Find your world": "Trova il tuo mondo", "New": "Nuovo", "People": "Persone", "Global chat": "Chat globale",
    "Avatar & profile": "Avatar e profilo", "YOUR PRIVATE CODE": "IL TUO CODICE PRIVATO", "Enter a code": "Inserisci un codice",
    "Add": "Aggiungi", "Globe presence": "Presenza sul globo", "Enable GPS": "Attiva GPS", "Stop": "Interrompi",
    "My people": "I miei contatti", "Discover the community": "Scopri la comunità", "Display name": "Nome visualizzato",
    "City": "Città", "Country": "Paese", "Family": "Famiglia", "Friends": "Amici", "Dating": "Incontri",
    "Save profile": "Salva profilo", "Close": "Chiudi", "Accept": "Accetta", "Decline": "Rifiuta", "Learn more": "Scopri di più",
  },
};

function translateValue(value: string, lang: Language): string {
  if (lang === "fr" || lang === "en") return value;
  const dictionary = dictionaries[lang] || {};
  let normalized = value;
  for (const table of Object.values(dictionaries)) {
    for (const [english, translated] of Object.entries(table || {})) normalized = normalized.replaceAll(translated, english);
  }
  let result = normalized;
  for (const [english, translated] of Object.entries(dictionary)) result = result.replaceAll(english, translated);
  return result;
}

function translateTree(root: ParentNode, lang: Language): void {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let node: Node | null;
  while ((node = walker.nextNode())) {
    if (node.parentElement?.closest("script,style")) continue;
    const value = node.nodeValue || "";
    const translated = translateValue(value, lang);
    if (translated !== value) node.nodeValue = translated;
  }
  root.querySelectorAll<HTMLElement>("[placeholder],[aria-label],[title]").forEach((element) => {
    ["placeholder", "aria-label", "title"].forEach((attribute) => {
      const value = element.getAttribute(attribute);
      if (value) element.setAttribute(attribute, translateValue(value, lang));
    });
  });
}

export function useInterfaceTranslation(lang: Language): void {
  useEffect(() => {
    if (lang === "fr" || lang === "en") return;
    const apply = () => translateTree(document.body, lang);
    apply();
    const observer = new MutationObserver(() => apply());
    observer.observe(document.body, { childList: true, subtree: true, characterData: true });
    return () => observer.disconnect();
  }, [lang]);
}
