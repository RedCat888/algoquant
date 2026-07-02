use anyhow::{Context, Result};
use notify::{Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub alpaca: AlpacaConfig,
    pub crypto: CryptoConfig,
    pub finnhub: FinnhubConfig,
    pub nats: NatsConfig,
    pub redis: RedisConfig,
    pub questdb: QuestDbConfig,
    pub signals: SignalsConfig,
    pub risk: RiskConfig,
    pub strategies: StrategiesConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    pub log_level: String,
    pub paper_trading: bool,
    /// Seconds to wait after startup before allowing any trades.
    /// During warmup, data is ingested and signals are computed
    /// but not acted upon. This prevents garbage signals from
    /// insufficient data.
    pub warmup_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlpacaConfig {
    pub api_key: String,
    pub api_secret: String,
    pub base_url: String,
    pub data_ws_url: String,
    pub trading_ws_url: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CryptoConfig {
    pub enabled: bool,
    pub ws_url: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuestDbConfig {
    pub enabled: bool,
    pub ilp_address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FinnhubConfig {
    pub enabled: bool,
    pub api_key: String,
    /// Poll interval in seconds for the REST news endpoint.
    pub poll_interval_secs: u64,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SignalsConfig {
    pub cross_asset: CrossAssetSignalConfig,
    pub technical: TechnicalSignalConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrossAssetSignalConfig {
    pub enabled: bool,
    pub vol_window_secs: u64,
    pub min_ticks: usize,
    pub z_score_threshold: f64,
    pub correlation_window_secs: u64,
    /// Minimum seconds between signal emissions to prevent spam.
    pub signal_cooldown_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TechnicalSignalConfig {
    pub enabled: bool,
    pub rsi_period: usize,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub vwap_deviation_threshold: f64,
    pub volume_spike_multiplier: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position_pct: f64,
    pub max_total_exposure_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_open_orders: usize,
    pub min_trade_interval_secs: u64,
    /// Maximum notional value ($) of any single order.
    pub max_order_notional: f64,
    /// Daily loss limit as fraction of equity. Separate from drawdown circuit breaker.
    pub daily_loss_limit_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategiesConfig {
    pub crypto_vol_leads_equity: CryptoVolLeadsEquityConfig,
    pub mean_reversion: MeanReversionConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CryptoVolLeadsEquityConfig {
    pub enabled: bool,
    pub crypto_symbol: String,
    pub equity_symbols: Vec<String>,
    pub position_size_pct: f64,
    pub vol_spike_threshold: f64,
    pub hold_duration_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MeanReversionConfig {
    pub enabled: bool,
    pub symbols: Vec<String>,
    /// RSI below this triggers buy.
    pub rsi_entry_threshold: f64,
    /// RSI above this triggers exit.
    pub rsi_exit_threshold: f64,
    pub position_size_pct: f64,
    pub max_hold_secs: u64,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config from {}", path.display()))?;
        let mut config: AppConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config TOML")?;

        if let Ok(key) = std::env::var("ALPACA_API_KEY") {
            config.alpaca.api_key = key;
        }
        if let Ok(secret) = std::env::var("ALPACA_API_SECRET") {
            config.alpaca.api_secret = secret;
        }
        if let Ok(url) = std::env::var("NATS_URL") {
            config.nats.url = url;
        }
        if let Ok(url) = std::env::var("REDIS_URL") {
            config.redis.url = url;
        }
        if let Ok(key) = std::env::var("FINNHUB_API_KEY") {
            config.finnhub.api_key = key;
        }

        Ok(config)
    }
}

pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    pub rx: watch::Receiver<Arc<AppConfig>>,
}

impl ConfigWatcher {
    pub fn new(config_path: PathBuf) -> Result<Self> {
        let initial = AppConfig::load(&config_path)?;
        let (tx, rx) = watch::channel(Arc::new(initial));

        let watch_path = config_path.clone();
        let mut watcher =
            notify::recommended_watcher(move |res: Result<NotifyEvent, notify::Error>| {
                match res {
                    Ok(event) if matches!(event.kind, EventKind::Modify(_)) => {
                        info!("Config file changed, reloading...");
                        match AppConfig::load(&watch_path) {
                            Ok(new_config) => {
                                let _ = tx.send(Arc::new(new_config));
                                info!("Config reloaded successfully");
                            }
                            Err(e) => {
                                error!("Failed to reload config: {e}");
                            }
                        }
                    }
                    Err(e) => error!("Config watcher error: {e}"),
                    _ => {}
                }
            })?;

        watcher.watch(&config_path, RecursiveMode::NonRecursive)?;

        Ok(Self {
            _watcher: watcher,
            rx,
        })
    }
}
