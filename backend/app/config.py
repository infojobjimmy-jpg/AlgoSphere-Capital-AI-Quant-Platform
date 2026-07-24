from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_slug: str = "acap"
    product_name: str = "Algosphere Capital"
    database_url: str = "postgresql+asyncpg://gaios:gaios@localhost:5432/gaios"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap: str = "localhost:9094"
    kafka_telemetry_topic: str = "acap.telemetry"
    chroma_host: str = "localhost"
    chroma_port: int = 8001
    openai_api_key: str | None = None
    discovery_seed_urls: str = (
        "https://registry.opendata.aws/,https://catalog.data.gov/dataset"
    )
    webcam_registry_path: str = "config/webcams.example.json"
    log_level: str = "INFO"
    public_geospatial_mode: bool = True
    api_admin_key: str | None = None
    cors_allowed_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    aisstream_api_key: str | None = None
    windy_webcams_api_key: str | None = None
    tavily_api_key: str | None = None
    serpapi_key: str | None = None
    camera_probe_interval_sec: float = 90.0
    ws_compress_snapshots: bool = False
    h3_aircraft_resolution: int = 4
    fusion_event_db_write: bool = True
    decision_db_write: bool = True
    action_hooks_json: str = ""
    autonomy_chroma_memory: bool = True
    meta_learning_enabled: bool = True
    meta_learning_evolution_interval_sec: float = 180.0
    meta_learning_sandbox_promote_min_lift: float = 0.03
    meta_learning_min_rank_samples: int = 3
    evolutionary_enabled: bool = True
    evolutionary_variants_per_round: int = 10
    evolutionary_seed_fingerprints: int = 2
    evolutionary_min_composite: float = 0.48
    evolutionary_promote_min_lift: float = 0.02
    coingecko_api_key: str | None = None
    twelve_data_api_key: str | None = None
    finnhub_api_key: str | None = None
    alpha_vantage_api_key: str | None = None
    oanda_api_token: str | None = None
    oanda_account_id: str | None = None
    oanda_base_url: str = "https://api-fxpractice.oanda.com"
    trading_max_risk_per_trade_pct: float = 1.0
    trading_default_stop_pct: float = 1.5
    trading_global_drawdown_kill_pct: float = 15.0
    trading_capital_floor_pct: float = 70.0
    trading_emergency_stop_dd_pct: float = 25.0
    trading_kill_switch: bool = False
    paper_capital_usd: float = 100_000.0
    trading_mode: str = "paper"  # paper | live
    trading_live_broker: str = "mt5"  # mt5 | ctrader
    mt5_bridge_url: str | None = None
    mt5_api_token: str | None = None
    ctrader_base_url: str | None = None
    ctrader_access_token: str | None = None
    ctrader_account_id: str | None = None
    trading_admin_api_key: str | None = None  # set TRADING_ADMIN_API_KEY in env; kill-switch/mode require it
    self_code_enabled: bool = False
    self_code_dry_run: bool = True
    self_code_workspace_root: str = "/app"
    self_code_baseline_command: str = "python -m compileall -q app"
    self_code_candidate_command: str = "python -m compileall -q app"
    self_code_command_timeout_sec: int = 180
    self_code_min_improvement: float = 0.02

    def redis_snapshot_key(self) -> str:
        return f"{self.app_slug}:snapshot:latest"

    def redis_timeline_key(self) -> str:
        return f"{self.app_slug}:timeline"

    def redis_hist_aircraft_key(self) -> str:
        return f"{self.app_slug}:hist:aircraft"

    def redis_hist_ships_key(self) -> str:
        return f"{self.app_slug}:hist:ships"

    def redis_trail_key(self, icao: str) -> str:
        return f"{self.app_slug}:trail:{icao}"

    def redis_learn_thresholds_key(self) -> str:
        return f"{self.app_slug}:learn:thresholds"

    def redis_evolution_guidance_key(self) -> str:
        return f"{self.app_slug}:meta:evolution:guidance"

    def redis_trading_kill_key(self) -> str:
        return f"{self.app_slug}:trading:kill"

    def redis_trading_mode_key(self) -> str:
        return f"{self.app_slug}:trading:mode"

    def redis_live_positions_key(self) -> str:
        return f"{self.app_slug}:live:positions"

    def redis_live_account_key(self) -> str:
        return f"{self.app_slug}:live:account"


settings = Settings()
