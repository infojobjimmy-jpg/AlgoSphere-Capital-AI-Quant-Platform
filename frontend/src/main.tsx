import React, { Suspense, lazy } from "react";
import ReactDOM from "react-dom/client";
import ConsentBanner from "./components/ConsentBanner";
import "./index.css";

const App = lazy(() => import("./App"));
const MarketingApp = lazy(() => import("./MarketingApp"));

const productRoutes = ["/app", "/globe", "/navigation"];
const isProductRoute = productRoutes.some((route) => window.location.pathname.startsWith(route));

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Suspense fallback={<div style={{ background: "#041018", minHeight: "100vh" }} />}>
      {isProductRoute ? <App /> : <MarketingApp />}
    </Suspense>
    <ConsentBanner />
  </React.StrictMode>,
);

if ("serviceWorker" in navigator && import.meta.env.PROD) {
  window.addEventListener("load", () => {
    void navigator.serviceWorker.register("/sw.js");
  });
}
