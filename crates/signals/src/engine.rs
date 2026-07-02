use algoquant_core::config::SignalsConfig;
use algoquant_core::events::Event;
use algoquant_core::types::SignalValue;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::cross_asset::CrossAssetAnalyzer;
use crate::sentiment::SentimentAnalyzer;
use crate::technical::TechnicalAnalyzer;

pub struct SignalEngine {
    cross_asset: Arc<CrossAssetAnalyzer>,
    technical: Arc<TechnicalAnalyzer>,
    sentiment: Arc<SentimentAnalyzer>,
    event_rx: mpsc::Receiver<Event>,
    signal_tx: mpsc::Sender<SignalValue>,
    warmup_until: DateTime<Utc>,
}

impl SignalEngine {
    pub fn new(
        config: &SignalsConfig,
        event_rx: mpsc::Receiver<Event>,
        signal_tx: mpsc::Sender<SignalValue>,
        warmup_secs: u64,
    ) -> Self {
        let cross_asset = Arc::new(CrossAssetAnalyzer::new(
            config.cross_asset.vol_window_secs,
            config.cross_asset.min_ticks,
            config.cross_asset.z_score_threshold,
            config.cross_asset.correlation_window_secs,
            config.cross_asset.signal_cooldown_secs,
        ));

        let technical = Arc::new(TechnicalAnalyzer::new(
            config.technical.rsi_period,
            config.technical.rsi_overbought,
            config.technical.rsi_oversold,
            config.technical.vwap_deviation_threshold,
            config.technical.volume_spike_multiplier,
        ));

        let sentiment = Arc::new(SentimentAnalyzer::new());

        let warmup_until = Utc::now() + chrono::Duration::seconds(warmup_secs as i64);

        Self {
            cross_asset,
            technical,
            sentiment,
            event_rx,
            signal_tx,
            warmup_until,
        }
    }

    pub fn cross_asset_analyzer(&self) -> Arc<CrossAssetAnalyzer> {
        self.cross_asset.clone()
    }

    pub fn technical_analyzer(&self) -> Arc<TechnicalAnalyzer> {
        self.technical.clone()
    }

    pub async fn run(mut self) {
        info!(
            warmup_until = %self.warmup_until.format("%H:%M:%S UTC"),
            "Signal engine started"
        );

        let mut event_count: u64 = 0;
        let mut signal_count: u64 = 0;
        let mut suppressed_count: u64 = 0;
        let mut warmup_logged = false;

        while let Some(event) = self.event_rx.recv().await {
            event_count += 1;
            let now = Utc::now();
            let in_warmup = now < self.warmup_until;

            if in_warmup && !warmup_logged && event_count % 5000 == 0 {
                let remaining = (self.warmup_until - now).num_seconds();
                info!(
                    remaining_secs = remaining,
                    events = event_count,
                    "Warmup in progress — signals suppressed"
                );
            }

            if !in_warmup && !warmup_logged {
                warmup_logged = true;
                info!(
                    events_during_warmup = event_count,
                    suppressed_signals = suppressed_count,
                    "Warmup complete — signals now active"
                );
            }

            let mut signals: Vec<SignalValue> = Vec::new();

            match &event {
                Event::Tick(tick) => {
                    if let Some(sig) = self.cross_asset.on_tick(tick) {
                        signals.push(sig);
                    }
                    if let Some(sig) = self.technical.on_tick(tick) {
                        signals.push(sig);
                    }
                }
                Event::News(article) => {
                    let news_signals = self.sentiment.on_news(article);
                    signals.extend(news_signals);
                }
                _ => {}
            }

            for signal in signals {
                if in_warmup {
                    suppressed_count += 1;
                    continue;
                }
                signal_count += 1;
                if self.signal_tx.send(signal).await.is_err() {
                    warn!("Signal channel closed");
                    return;
                }
            }

            if event_count % 10000 == 0 {
                info!(
                    events = event_count,
                    signals_emitted = signal_count,
                    suppressed = suppressed_count,
                    warmup = in_warmup,
                    "Signal engine progress"
                );
            }
        }

        info!("Signal engine finished — {event_count} events, {signal_count} signals");
    }
}
