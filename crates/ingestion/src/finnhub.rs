use algoquant_core::config::FinnhubConfig;
use algoquant_core::types::{NewsArticle, Symbol};
use anyhow::Result;
use chrono::{NaiveDate, Utc};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct FinnhubNewsItem {
    id: u64,
    headline: String,
    summary: String,
    source: String,
    url: String,
    related: String,
    datetime: i64,
}

/// Polls Finnhub REST API for company news and produces NewsArticle events.
/// Uses keyword-based sentiment scoring — fast and deterministic.
pub struct FinnhubFeed {
    config: FinnhubConfig,
    tx: mpsc::Sender<NewsArticle>,
    seen_ids: Mutex<HashSet<u64>>,
    client: reqwest::Client,
}

impl FinnhubFeed {
    pub fn new(config: FinnhubConfig, tx: mpsc::Sender<NewsArticle>) -> Self {
        Self {
            config,
            tx,
            seen_ids: Mutex::new(HashSet::new()),
            client: reqwest::Client::new(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        if !self.config.enabled {
            info!("Finnhub news feed disabled");
            std::future::pending::<()>().await;
            return Ok(());
        }

        if self.config.api_key.is_empty() {
            warn!("Finnhub API key not set — news feed disabled");
            std::future::pending::<()>().await;
            return Ok(());
        }

        info!(
            symbols = ?self.config.symbols,
            poll_secs = self.config.poll_interval_secs,
            "Finnhub news feed started"
        );

        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(self.config.poll_interval_secs));

        loop {
            interval.tick().await;
            self.poll_news().await;
        }
    }

    async fn poll_news(&self) {
        let today = Utc::now().date_naive();
        let from = today - chrono::Duration::days(1);

        for symbol in &self.config.symbols {
            match self.fetch_company_news(symbol, from, today).await {
                Ok(articles) => {
                    for article in articles {
                        if self.tx.send(article).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    warn!(symbol = symbol, error = %e, "Finnhub fetch failed");
                }
            }
            // Rate limit: ~30ms between requests stays well under 60/min
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn fetch_company_news(
        &self,
        symbol: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<NewsArticle>> {
        let url = format!(
            "https://finnhub.io/api/v1/company-news?symbol={}&from={}&to={}&token={}",
            symbol,
            from.format("%Y-%m-%d"),
            to.format("%Y-%m-%d"),
            self.config.api_key,
        );

        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("Finnhub returned {}", resp.status());
        }

        let items: Vec<FinnhubNewsItem> = resp.json().await?;
        let mut articles = Vec::new();

        let mut seen = self.seen_ids.lock().unwrap();
        for item in items {
            if seen.contains(&item.id) {
                continue;
            }
            seen.insert(item.id);

            let symbols: Vec<Symbol> = item
                .related
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| Symbol::new(s.trim()))
                .collect();

            let sentiment = score_sentiment(&item.headline, &item.summary);

            articles.push(NewsArticle {
                id: item.id.to_string(),
                title: item.headline,
                summary: Some(item.summary),
                source: item.source,
                url: Some(item.url),
                symbols,
                sentiment_score: Some(sentiment),
                published_at: chrono::DateTime::from_timestamp(item.datetime, 0)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            });
        }

        if !articles.is_empty() {
            info!(
                symbol = symbol,
                count = articles.len(),
                "Finnhub: new articles"
            );
        }

        // Cap the seen set to prevent unbounded growth
        if seen.len() > 50_000 {
            seen.clear();
        }

        Ok(articles)
    }
}

/// Keyword-based sentiment scoring. Returns -1.0 to 1.0.
///
/// This is intentionally simple and fast. It's not trying to be an NLP model --
/// it's trying to detect extreme sentiment (crash, surge, layoffs, record earnings)
/// that a keyword approach can catch reliably. Subtle sentiment is noise anyway.
fn score_sentiment(headline: &str, summary: &str) -> f64 {
    let text = format!("{} {}", headline.to_lowercase(), summary.to_lowercase());

    let strong_negative = [
        "crash", "plunge", "plummet", "collapse", "bankrupt", "fraud",
        "default", "layoff", "recall", "investigation", "indictment",
        "downgrade", "sell-off", "selloff", "panic", "recession",
        "bear market", "liquidation", "warning", "miss", "shortfall",
        "cuts guidance", "lowers guidance", "disappointing",
    ];

    let mild_negative = [
        "decline", "drop", "fall", "loss", "weak", "concern", "risk",
        "volatility", "uncertainty", "slow", "below", "pressure",
        "cut", "lower", "miss", "delayed",
    ];

    let strong_positive = [
        "surge", "soar", "record", "breakthrough", "beat", "exceeds",
        "upgrade", "bullish", "rally", "all-time high", "acquisition",
        "partnership", "approval", "launches", "innovation", "growth",
        "raises guidance", "strong earnings", "blowout",
    ];

    let mild_positive = [
        "gain", "rise", "up", "improve", "positive", "optimistic",
        "recovery", "rebound", "above", "strong", "momentum",
        "buy", "outperform", "higher",
    ];

    let mut score = 0.0;
    let mut hits = 0;

    for kw in &strong_negative {
        if text.contains(kw) {
            score -= 1.0;
            hits += 1;
        }
    }
    for kw in &mild_negative {
        if text.contains(kw) {
            score -= 0.4;
            hits += 1;
        }
    }
    for kw in &strong_positive {
        if text.contains(kw) {
            score += 1.0;
            hits += 1;
        }
    }
    for kw in &mild_positive {
        if text.contains(kw) {
            score += 0.4;
            hits += 1;
        }
    }

    if hits == 0 {
        return 0.0;
    }

    (score / hits as f64).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strong_negative_sentiment() {
        let s = score_sentiment("Markets crash on recession fears", "The stock market plunged today");
        assert!(s < -0.5, "Expected strong negative, got {s}");
    }

    #[test]
    fn test_strong_positive_sentiment() {
        let s = score_sentiment("NVDA beats earnings, stock surges to record", "Revenue growth exceeds expectations");
        assert!(s > 0.5, "Expected strong positive, got {s}");
    }

    #[test]
    fn test_neutral_sentiment() {
        let s = score_sentiment("Company reports quarterly results", "The company filed standard regulatory documents");
        assert!(s.abs() < 0.3, "Expected neutral, got {s}");
    }

    #[test]
    fn test_mixed_sentiment() {
        let s = score_sentiment("Stock rises despite concerns", "Positive earnings but weak guidance");
        // Mixed signals should land near zero
        assert!(s.abs() < 0.6, "Expected mild/mixed, got {s}");
    }
}
