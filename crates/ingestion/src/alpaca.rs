use algoquant_core::config::AlpacaConfig;
use algoquant_core::types::{AssetClass, Exchange, Symbol, Tick};
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Serialize)]
struct AuthMessage {
    action: String,
    key: String,
    secret: String,
}

#[derive(Debug, Serialize)]
struct SubscribeMessage {
    action: String,
    trades: Vec<String>,
    quotes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AlpacaWsMessage {
    #[serde(rename = "T")]
    msg_type: String,
    #[serde(rename = "S")]
    symbol: Option<String>,
    #[serde(rename = "p")]
    price: Option<f64>,
    #[serde(rename = "s")]
    size: Option<f64>,
    #[serde(rename = "t")]
    timestamp: Option<String>,
    #[serde(rename = "bp")]
    bid_price: Option<f64>,
    #[serde(rename = "ap")]
    ask_price: Option<f64>,
    #[serde(rename = "bs")]
    bid_size: Option<f64>,
    #[serde(rename = "as")]
    ask_size: Option<f64>,
    msg: Option<String>,
}

pub struct AlpacaFeed {
    config: AlpacaConfig,
    tx: mpsc::Sender<Tick>,
}

impl AlpacaFeed {
    pub fn new(config: AlpacaConfig, tx: mpsc::Sender<Tick>) -> Self {
        Self { config, tx }
    }

    pub async fn run(&self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        loop {
            match self.connect_and_stream().await {
                Ok(()) => {
                    info!("Alpaca WebSocket closed normally");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("connection limit") {
                        backoff = backoff.max(Duration::from_secs(30));
                    }
                    error!("Alpaca feed: {e}");
                }
            }

            warn!("Reconnecting Alpaca feed in {backoff:?}...");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        let url = &self.config.data_ws_url;
        info!("Connecting to Alpaca data stream: {url}");

        let (ws_stream, _) = connect_async(url)
            .await
            .context("Failed to connect to Alpaca WebSocket")?;

        let (mut write, mut read) = ws_stream.split();

        if let Some(msg) = read.next().await {
            let msg = msg?;
            info!("Alpaca welcome: {}", msg.to_text().unwrap_or("binary"));
        }

        let auth = AuthMessage {
            action: "auth".to_string(),
            key: self.config.api_key.clone(),
            secret: self.config.api_secret.clone(),
        };
        write
            .send(Message::Text(serde_json::to_string(&auth)?.into()))
            .await?;

        if let Some(msg) = read.next().await {
            let msg = msg?;
            let text = msg.to_text().unwrap_or("binary");
            if text.contains("\"error\"") {
                let _ = write.send(Message::Close(None)).await;
                if text.contains("connection limit") {
                    anyhow::bail!("Alpaca connection limit exceeded — stale session may exist");
                }
                anyhow::bail!("Alpaca auth failed: {text}");
            }
            info!("Alpaca authenticated");
        }

        let sub = SubscribeMessage {
            action: "subscribe".to_string(),
            trades: self.config.symbols.clone(),
            quotes: self.config.symbols.clone(),
        };
        write
            .send(Message::Text(serde_json::to_string(&sub)?.into()))
            .await?;

        info!(symbols = ?self.config.symbols, "Alpaca subscribed");

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if let Message::Text(text) = msg {
                self.process_message(&text).await;
            }
        }

        Ok(())
    }

    async fn process_message(&self, text: &str) {
        let messages: Vec<AlpacaWsMessage> = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to parse Alpaca message: {e}");
                return;
            }
        };

        for msg in messages {
            match msg.msg_type.as_str() {
                "t" => {
                    if let (Some(sym), Some(price)) = (&msg.symbol, msg.price) {
                        let tick = Tick {
                            symbol: Symbol::new(sym),
                            asset_class: AssetClass::Equity,
                            exchange: Exchange::Alpaca,
                            price: Decimal::from_str(&price.to_string())
                                .unwrap_or(Decimal::ZERO),
                            bid: None,
                            ask: None,
                            volume: msg
                                .size
                                .map(|s| Decimal::from_str(&s.to_string()).unwrap_or(Decimal::ZERO)),
                            timestamp: msg
                                .timestamp
                                .as_ref()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or_else(Utc::now),
                            received_at: Utc::now(),
                        };
                        let _ = self.tx.send(tick).await;
                    }
                }
                "q" => {
                    if let (Some(sym), Some(bid), Some(ask)) =
                        (&msg.symbol, msg.bid_price, msg.ask_price)
                    {
                        let mid = (bid + ask) / 2.0;
                        let tick = Tick {
                            symbol: Symbol::new(sym),
                            asset_class: AssetClass::Equity,
                            exchange: Exchange::Alpaca,
                            price: Decimal::from_str(&mid.to_string())
                                .unwrap_or(Decimal::ZERO),
                            bid: Some(
                                Decimal::from_str(&bid.to_string()).unwrap_or(Decimal::ZERO),
                            ),
                            ask: Some(
                                Decimal::from_str(&ask.to_string()).unwrap_or(Decimal::ZERO),
                            ),
                            volume: None,
                            timestamp: msg
                                .timestamp
                                .as_ref()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or_else(Utc::now),
                            received_at: Utc::now(),
                        };
                        let _ = self.tx.send(tick).await;
                    }
                }
                "success" | "subscription" => {}
                other => {
                    tracing::trace!("Unhandled Alpaca message type: {other}");
                }
            }
        }
    }
}
