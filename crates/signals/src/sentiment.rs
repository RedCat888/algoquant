use algoquant_core::types::{NewsArticle, SignalValue, Symbol};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use std::collections::VecDeque;
use tracing::info;

/// Aggregates news sentiment per symbol over a rolling window.
/// Only emits a signal when sentiment is strongly directional
/// (many articles agreeing) — isolated noise is ignored.
pub struct SentimentAnalyzer {
    articles: DashMap<Symbol, VecDeque<ScoredArticle>>,
    window_secs: i64,
    min_articles: usize,
    threshold: f64,
}

struct ScoredArticle {
    score: f64,
    received_at: DateTime<Utc>,
}

impl SentimentAnalyzer {
    pub fn new() -> Self {
        Self {
            articles: DashMap::new(),
            window_secs: 3600,
            min_articles: 3,
            threshold: 0.4,
        }
    }

    pub fn on_news(&self, article: &NewsArticle) -> Vec<SignalValue> {
        let score = article.sentiment_score.unwrap_or(0.0);
        if score.abs() < 0.1 {
            return vec![];
        }

        let now = Utc::now();
        let cutoff = now - Duration::seconds(self.window_secs);
        let mut signals = Vec::new();

        for symbol in &article.symbols {
            let mut window = self
                .articles
                .entry(symbol.clone())
                .or_insert_with(VecDeque::new);
            window.push_back(ScoredArticle {
                score,
                received_at: now,
            });
            while window.front().map_or(false, |a| a.received_at < cutoff) {
                window.pop_front();
            }

            if window.len() >= self.min_articles {
                let scores: Vec<f64> = window.iter().map(|a| a.score).collect();
                let avg = scores.iter().sum::<f64>() / scores.len() as f64;

                if avg.abs() >= self.threshold {
                    info!(
                        symbol = %symbol,
                        avg_sentiment = format!("{avg:.3}"),
                        articles = scores.len(),
                        "News sentiment signal"
                    );
                    signals.push(SignalValue {
                        name: "news_sentiment".to_string(),
                        value: avg,
                        direction: avg.signum(),
                        confidence: avg.abs().min(1.0),
                        symbols: vec![symbol.clone()],
                        timestamp: now,
                        metadata: serde_json::json!({
                            "avg_sentiment": avg,
                            "article_count": scores.len(),
                            "latest_title": article.title,
                            "source": article.source,
                        }),
                    });
                }
            }
        }

        signals
    }
}
