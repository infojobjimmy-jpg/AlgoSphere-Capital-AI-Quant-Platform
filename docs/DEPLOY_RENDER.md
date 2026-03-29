# Deploy AlgoSphere Streamlit (Investor Landing) on Render

This deploys **only the Streamlit dashboard** as a public **read-only allocator surface**.  
**Admin cockpit**, **Client dashboard**, **Landing**, and **Pricing** are hidden when `ALGOSPHERE_PUBLIC_SURFACE=investor`.

Trading and execution are **not** started by this service; the UI uses **HTTP GET** to your API if configured.

## Prerequisites

- GitHub (or GitLab) repository containing this project (code pushed; `render.yaml` at **repo root**).
- Optional: a separate **FastAPI** service URL if you want live metrics (otherwise snapshot cards may show unavailable).

### If you see `404` and `x-render-routing: no-server`

That response means **Render has no web service bound to that hostname** (nothing deployed yet, wrong URL, or service deleted). Creating or applying the Blueprint **from your account** is required; the YAML file in Git does not deploy by itself.

## Steps (Render dashboard) — required once

1. Sign in at [Render](https://render.com) and connect your Git provider.
2. **New +** → **Blueprint** → choose the GitHub/GitLab repo that contains this project → confirm **`render.yaml`** at the **repository root** → **Apply** (creates the service defined in the file).
3. In the service settings, confirm **Root Directory** is **empty** (repository root). If you used **Web Service** manually instead of Blueprint, use the same **Build** / **Start** commands as in `render.yaml`.
4. Under **Environment**, set:
   - `ALGO_SPHERE_API_URL` — full base URL of your API, e.g. `https://algosphere-api.onrender.com` (no trailing slash). **Required** for data from a cloud API; if omitted, the app defaults to `127.0.0.1:8000` (will not work from Render’s servers).
   - `ALGOSPHERE_PRIVATE_INVESTOR_PASSWORD` / `ALGOSPHERE_PRIVATE_PARTNER_PASSWORD` — strong secrets if you expose **Investor (private)** / **Partner (private)** on the public internet.
5. Deploy. Wait for build + start.
6. Open the **URL** shown on the service page, e.g. `https://algosphere-investor-dashboard.onrender.com`.

## Free tier notes

- The service **spins down** after inactivity; first load after sleep can take ~30–60 seconds.
- The URL is whatever Render assigns (`https://<service-name>.onrender.com`); you can rename the service to adjust the subdomain (if available).

## Local smoke test (public surface)

From the repository root:

```bash
set PYTHONPATH=.
set ALGOSPHERE_PUBLIC_SURFACE=investor
set ALGO_SPHERE_API_URL=http://127.0.0.1:8000
streamlit run frontend/dashboard.py --server.port 8501
```

You should see only **Investor Landing**, **Investor Dashboard**, and private investor/partner modes — no Admin.

## Security

- This configuration does **not** expose Streamlit **admin** or **client** retail modes.
- Private routes still rely on **passwords**; use long random secrets on any public URL.
- Do not commit `.env` or API keys to the repository.
