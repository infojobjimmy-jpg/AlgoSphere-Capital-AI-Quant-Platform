import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import MarketingApp from "./MarketingApp";
import ConsentBanner from "./components/ConsentBanner";
import "./index.css";

const productRoutes = ["/app", "/globe", "/navigation"];
const isProductRoute = productRoutes.some((route) => window.location.pathname.startsWith(route));

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {isProductRoute ? <App /> : <MarketingApp />}
    <ConsentBanner />
  </React.StrictMode>,
);

if ("serviceWorker" in navigator && import.meta.env.PROD) {
  window.addEventListener("load", () => {
    void navigator.serviceWorker.register("/sw.js");
  });
}
