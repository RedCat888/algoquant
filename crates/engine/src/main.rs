use algoquant_core::config::{AppConfig, ConfigWatcher};
use algoquant_core::events::Event;
use algoquant_core::types::*;
use rust_decimal::prelude::ToPrimitive;
use algoquant_execution::{AlpacaClient, OrderManager};
use algoquant_ingestion::{AlpacaFeed, CryptoFeed, FinnhubFeed, Normalizer};
use algoquant_risk::{RiskDecision, RiskManager};
use algoquant_signals::SignalEngine;
use algoquant_storage::QuestDbWriter;
use algoquant_strategy::{CryptoVolLeadsEquity, MeanReversion, Strategy};

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("========================================");
    info!("  AlgoQuant Trading Engine v0.2.0");
    info!("  Cross-Asset Algorithmic Trading");
    info!("========================================");

    let config_path = PathBuf::from("config/default.toml");
    let config = AppConfig::load(&config_path).context("Failed to load config")?;

    if config.alpaca.api_key.is_empty() || config.alpaca.api_secret.is_empty() {
        anyhow::bail!("Alpaca API credentials not set — set ALPACA_API_KEY and ALPACA_API_SECRET env vars");
    }

    info!(
        paper = config.general.paper_trading,
        warmup_secs = config.general.warmup_secs,
        alpaca_symbols = ?config.alpaca.symbols,
        crypto_symbols = ?config.crypto.symbols,
        finnhub = config.finnhub.enabled,
        "Configuration loaded"
    );

    let config_watcher = ConfigWatcher::new(config_path.clone())
        .context("Failed to start config watcher")?;
    let _config_rx = config_watcher.rx;

    // === Execution client ===
    let alpaca_client = Arc::new(AlpacaClient::new(&config.alpaca));
    let order_manager = Arc::new(OrderManager::new());

    // === Connect and sync state ===
    let startup_portfolio = alpaca_client
        .get_account()
        .await
        .context("Failed to connect to Alpaca on startup")?;

    info!(
        equity = %startup_portfolio.equity,
        cash = %startup_portfolio.cash,
        buying_power = %startup_portfolio.buying_power,
        positions = startup_portfolio.positions.len(),
        "Connected to Alpaca"
    );

    if !startup_portfolio.positions.is_empty() {
        warn!(
            count = startup_portfolio.positions.len(),
            "Existing positions found on Alpaca"
        );
        for pos in &startup_portfolio.positions {
            info!(
                symbol = %pos.symbol,
                qty = %pos.quantity,
                side = ?pos.side,
                unrealized_pnl = %pos.unrealized_pnl,
                "  Existing position"
            );
        }
    }

    // === Risk Manager ===
    let risk_manager = Arc::new(RiskManager::new(config.risk.clone()));
    risk_manager.update_portfolio(&startup_portfolio);
    for pos in &startup_portfolio.positions {
        if let Some(price) = pos.current_price.to_f64() {
            risk_manager.update_price(&pos.symbol, price);
        }
    }

    // === QuestDB ===
    let questdb: Option<Arc<QuestDbWriter>> = if config.questdb.enabled {
        let writer = Arc::new(QuestDbWriter::new(&config.questdb.ilp_address));
        match writer.connect() {
            Ok(()) => {
                writer.write_portfolio(&startup_portfolio);
                Some(writer)
            }
            Err(e) => {
                warn!("QuestDB not available: {e} — continuing without persistence");
                None
            }
        }
    } else {
        None
    };

    // === Channels ===
    const TICK_BUFFER: usize = 50_000;
    const EVENT_BUFFER: usize = 50_000;
    const SIGNAL_BUFFER: usize = 1_000;
    const NEWS_BUFFER: usize = 500;

    let (tick_tx, tick_rx) = mpsc::channel::<Tick>(TICK_BUFFER);
    let (event_tx, event_rx) = mpsc::channel::<Event>(EVENT_BUFFER);
    let (signal_tx, signal_rx) = mpsc::channel::<SignalValue>(SIGNAL_BUFFER);
    let (news_tx, mut news_rx) = mpsc::channel::<NewsArticle>(NEWS_BUFFER);

    // === Data Ingestion ===
    let alpaca_feed = AlpacaFeed::new(config.alpaca.clone(), tick_tx.clone());
    let crypto_feed = CryptoFeed::new(config.crypto.clone(), config.alpaca.clone(), tick_tx.clone());
    let finnhub_feed = FinnhubFeed::new(config.finnhub.clone(), news_tx);
    drop(tick_tx);

    // === Normalizer ===
    let normalizer = Normalizer::new(tick_rx, event_tx.clone());

    // === Signal Engine ===
    let signal_engine = SignalEngine::new(
        &config.signals,
        event_rx,
        signal_tx,
        config.general.warmup_secs,
    );

    // === Strategies ===
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(CryptoVolLeadsEquity::new(
            config.strategies.crypto_vol_leads_equity.clone(),
        )),
        Box::new(MeanReversion::new(config.strategies.mean_reversion.clone())),
    ];

    let strategy_names: Vec<&str> = strategies.iter().map(|s| s.name()).collect();
    info!(strategies = ?strategy_names, "Strategies loaded");
    info!(
        warmup_secs = config.general.warmup_secs,
        "Pipeline starting — signals suppressed during warmup"
    );

    // === Spawn all tasks ===
    let alpaca_handle = tokio::spawn(async move {
        if let Err(e) = alpaca_feed.run().await {
            error!("Alpaca feed fatal error: {e}");
        }
    });

    let crypto_handle = tokio::spawn(async move {
        if let Err(e) = crypto_feed.run().await {
            error!("Crypto feed fatal error: {e}");
        }
    });

    let finnhub_handle = tokio::spawn(async move {
        if let Err(e) = finnhub_feed.run().await {
            error!("Finnhub feed fatal error: {e}");
        }
    });

    // Bridge news articles into the event bus
    let news_event_tx = event_tx.clone();
    let news_questdb = questdb.clone();
    let news_bridge_handle = tokio::spawn(async move {
        while let Some(article) = news_rx.recv().await {
            if let Some(ref db) = news_questdb {
                db.write_news(&article);
            }
            let event = Event::News(article);
            if news_event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
    drop(event_tx);

    let normalizer_handle = tokio::spawn(async move {
        normalizer.run().await;
    });

    let signal_handle = tokio::spawn(async move {
        signal_engine.run().await;
    });

    let strategy_client = alpaca_client.clone();
    let strategy_risk = risk_manager.clone();
    let strategy_orders = order_manager.clone();
    let strategy_questdb = questdb.clone();
    let strategy_handle = tokio::spawn(async move {
        run_strategy_loop(
            strategies,
            signal_rx,
            strategy_client,
            strategy_risk,
            strategy_orders,
            strategy_questdb,
        )
        .await;
    });

    let monitor_client = alpaca_client.clone();
    let monitor_risk = risk_manager.clone();
    let monitor_questdb = questdb.clone();
    let monitor_handle = tokio::spawn(async move {
        run_portfolio_monitor(monitor_client, monitor_risk, monitor_questdb).await;
    });

    info!("All systems go. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, cleaning up...");

    alpaca_handle.abort();
    crypto_handle.abort();
    finnhub_handle.abort();
    news_bridge_handle.abort();
    normalizer_handle.abort();
    signal_handle.abort();
    strategy_handle.abort();
    monitor_handle.abort();

    info!("AlgoQuant engine stopped.");
    Ok(())
}

async fn run_strategy_loop(
    mut strategies: Vec<Box<dyn Strategy>>,
    mut signal_rx: mpsc::Receiver<SignalValue>,
    client: Arc<AlpacaClient>,
    risk: Arc<RiskManager>,
    orders: Arc<OrderManager>,
    questdb: Option<Arc<QuestDbWriter>>,
) {
    let names: Vec<String> = strategies.iter().map(|s| s.name().to_string()).collect();
    info!(strategies = ?names, "Strategy runner started");

    while let Some(signal) = signal_rx.recv().await {
        info!(
            signal = %signal.name,
            direction = signal.direction,
            confidence = format!("{:.3}", signal.confidence),
            value = format!("{:.3}", signal.value),
            symbols = ?signal.symbols.iter().map(|s| &s.0).collect::<Vec<_>>(),
            "Signal received"
        );

        if let Some(ref db) = questdb {
            db.write_signal(&signal);
        }

        let portfolio = match client.get_account().await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to get portfolio state: {e}");
                continue;
            }
        };

        risk.update_portfolio(&portfolio);
        for pos in &portfolio.positions {
            if let Some(price) = pos.current_price.to_f64() {
                risk.update_price(&pos.symbol, price);
            }
        }

        for strategy in &mut strategies {
            if !strategy.is_enabled() {
                continue;
            }

            let proposed_orders = strategy.on_signal(&signal, &portfolio);

            for order in proposed_orders {
                let open_count = orders.open_order_count().await;
                let decision = risk.evaluate_order(&order, &portfolio, open_count);

                match decision {
                    RiskDecision::Approved => {
                        info!(
                            strategy = strategy.name(),
                            symbol = %order.symbol,
                            side = ?order.side,
                            qty = %order.quantity,
                            "Order APPROVED"
                        );
                        submit_order(&client, &risk, &orders, &questdb, order).await;
                    }
                    RiskDecision::Rejected(reason) => {
                        warn!(
                            strategy = strategy.name(),
                            symbol = %order.symbol,
                            reason = %reason,
                            "Order REJECTED"
                        );
                    }
                    RiskDecision::ReducedSize {
                        new_quantity,
                        reason,
                    } => {
                        info!(
                            strategy = strategy.name(),
                            symbol = %order.symbol,
                            original_qty = %order.quantity,
                            new_qty = %new_quantity,
                            reason = %reason,
                            "Order RESIZED"
                        );
                        let mut adjusted = order.clone();
                        adjusted.quantity = new_quantity;
                        submit_order(&client, &risk, &orders, &questdb, adjusted).await;
                    }
                }
            }
        }
    }

    info!("Strategy runner stopped");
}

async fn submit_order(
    client: &AlpacaClient,
    risk: &RiskManager,
    orders: &OrderManager,
    questdb: &Option<Arc<QuestDbWriter>>,
    order: Order,
) {
    match client.submit_order(&order).await {
        Ok(submitted) => {
            risk.record_trade(&submitted.symbol);
            if let Some(ref db) = questdb {
                db.write_trade(&submitted);
            }
            orders.track(submitted).await;
        }
        Err(e) => error!(
            symbol = %order.symbol,
            error = %e,
            "Order submission failed"
        ),
    }
}

async fn run_portfolio_monitor(
    client: Arc<AlpacaClient>,
    risk: Arc<RiskManager>,
    questdb: Option<Arc<QuestDbWriter>>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        match client.get_account().await {
            Ok(portfolio) => {
                info!(
                    equity = %portfolio.equity,
                    cash = %portfolio.cash,
                    positions = portfolio.positions.len(),
                    circuit_breaker = risk.is_circuit_breaker_tripped(),
                    "Portfolio snapshot"
                );

                for pos in &portfolio.positions {
                    if let Some(price) = pos.current_price.to_f64() {
                        risk.update_price(&pos.symbol, price);
                    }
                }

                risk.update_portfolio(&portfolio);

                if let Some(ref db) = questdb {
                    db.write_portfolio(&portfolio);
                }
            }
            Err(e) => {
                warn!("Portfolio monitor: {e}");
            }
        }
    }
}
