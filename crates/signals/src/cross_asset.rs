use algoquant_core::types::{AssetClass, SignalValue, Symbol, Tick};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use rust_decimal::prelude::ToPrimitive;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

/// Tracks realized volatility across asset classes and detects divergences.
///
/// The real signal is not "is crypto vol high?" but "is crypto vol high
/// RELATIVE to equity vol?" If both are spiking, that's just a volatile
/// market. If crypto vol is spiking and equity vol is calm, that's the
/// leading indicator — crypto is pricing in risk that equities haven't yet.
pub struct CrossAssetAnalyzer {
    price_windows: DashMap<Symbol, VecDeque<PricePoint>>,
    vol_cache: DashMap<Symbol, VecDeque<VolPoint>>,
    vol_window_secs: i64,
    min_ticks: usize,
    z_score_threshold: f64,
    correlation_window_secs: i64,
    signal_cooldown_secs: i64,
    last_signal_epoch: AtomicU64,
    #[allow(dead_code)]
    started_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PricePoint {
    price: f64,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct VolPoint {
    vol: f64,
    timestamp: DateTime<Utc>,
}

impl CrossAssetAnalyzer {
    pub fn new(
        vol_window_secs: u64,
        min_ticks: usize,
        z_score_threshold: f64,
        correlation_window_secs: u64,
        signal_cooldown_secs: u64,
    ) -> Self {
        Self {
            price_windows: DashMap::new(),
            vol_cache: DashMap::new(),
            vol_window_secs: vol_window_secs as i64,
            min_ticks,
            z_score_threshold,
            correlation_window_secs: correlation_window_secs as i64,
            signal_cooldown_secs: signal_cooldown_secs as i64,
            last_signal_epoch: AtomicU64::new(0),
            started_at: Utc::now(),
        }
    }

    pub fn on_tick(&self, tick: &Tick) -> Option<SignalValue> {
        let price = tick.price.to_f64()?;
        if price <= 0.0 {
            return None;
        }

        let now = tick.timestamp;
        let cutoff = now - Duration::seconds(self.vol_window_secs);

        let mut window = self
            .price_windows
            .entry(tick.symbol.clone())
            .or_insert_with(VecDeque::new);
        window.push_back(PricePoint {
            price,
            timestamp: now,
        });
        while window.front().map_or(false, |p| p.timestamp < cutoff) {
            window.pop_front();
        }

        if window.len() < self.min_ticks {
            return None;
        }

        let vol = compute_realized_vol(&window);
        drop(window);

        let vol_cutoff = now - Duration::seconds(self.correlation_window_secs);
        let mut vol_series = self
            .vol_cache
            .entry(tick.symbol.clone())
            .or_insert_with(VecDeque::new);
        vol_series.push_back(VolPoint {
            vol,
            timestamp: now,
        });
        while vol_series.front().map_or(false, |v| v.timestamp < vol_cutoff) {
            vol_series.pop_front();
        }
        drop(vol_series);

        if tick.asset_class != AssetClass::Crypto {
            return None;
        }

        self.evaluate_divergence(&tick.symbol, vol, now)
    }

    fn evaluate_divergence(
        &self,
        crypto_symbol: &Symbol,
        current_crypto_vol: f64,
        now: DateTime<Utc>,
    ) -> Option<SignalValue> {
        // Signal cooldown
        let last = self.last_signal_epoch.load(Ordering::Relaxed);
        let now_epoch = now.timestamp() as u64;
        if last > 0 && (now_epoch.saturating_sub(last)) < self.signal_cooldown_secs as u64 {
            return None;
        }

        let crypto_vol_series = self.vol_cache.get(crypto_symbol)?;
        if crypto_vol_series.len() < 30 {
            return None;
        }

        // Z-score of crypto vol relative to its own recent history
        let crypto_vols: Vec<f64> = crypto_vol_series.iter().map(|v| v.vol).collect();
        let crypto_mean = mean(&crypto_vols);
        let crypto_std = std_dev(&crypto_vols, crypto_mean);
        if crypto_std < 1e-12 {
            return None;
        }
        let crypto_z = (current_crypto_vol - crypto_mean) / crypto_std;
        drop(crypto_vol_series);

        // Collect equity vols and compute an aggregate equity vol z-score
        let mut equity_vols_now: Vec<f64> = Vec::new();
        let mut equity_z_scores: Vec<f64> = Vec::new();

        for entry in self.vol_cache.iter() {
            let sym = entry.key().0.as_str();
            // Identify equity symbols: no "/" (not crypto like BTC/USD)
            if sym.contains('/') {
                continue;
            }
            let series = entry.value();
            if series.len() < 20 {
                continue;
            }
            let vols: Vec<f64> = series.iter().map(|v| v.vol).collect();
            let m = mean(&vols);
            let s = std_dev(&vols, m);
            if s < 1e-12 {
                continue;
            }
            if let Some(last_vol) = series.back() {
                let z = (last_vol.vol - m) / s;
                equity_vols_now.push(last_vol.vol);
                equity_z_scores.push(z);
            }
        }

        if equity_z_scores.is_empty() {
            debug!("No equity vol data yet, skipping divergence check");
            return None;
        }

        let avg_equity_z = mean(&equity_z_scores);

        // The divergence: crypto vol z-score minus equity vol z-score.
        // High positive = crypto is stressed, equities are calm = leading signal.
        // If both are spiking (both z > 2), it's not a divergence, it's priced in.
        let divergence = crypto_z - avg_equity_z;

        debug!(
            crypto = %crypto_symbol,
            crypto_z = format!("{crypto_z:.3}"),
            equity_z = format!("{avg_equity_z:.3}"),
            divergence = format!("{divergence:.3}"),
            "Cross-asset vol divergence"
        );

        if divergence.abs() < self.z_score_threshold {
            return None;
        }

        // Reject if equity vol is already elevated — the move is priced in
        if avg_equity_z > 1.5 && crypto_z > 1.5 {
            debug!(
                "Both crypto and equity vol elevated (z={crypto_z:.2}, eq_z={avg_equity_z:.2}) — no divergence"
            );
            return None;
        }

        self.last_signal_epoch
            .store(now_epoch, Ordering::Relaxed);

        let direction = if divergence > 0.0 { -1.0 } else { 1.0 };
        let confidence = (divergence.abs() / (self.z_score_threshold * 2.0)).min(1.0);

        info!(
            crypto = %crypto_symbol,
            crypto_z = format!("{crypto_z:.3}"),
            equity_z = format!("{avg_equity_z:.3}"),
            divergence = format!("{divergence:.3}"),
            direction = direction,
            confidence = format!("{confidence:.3}"),
            "CROSS-ASSET DIVERGENCE SIGNAL"
        );

        Some(SignalValue {
            name: "crypto_vol_leads_equity".to_string(),
            value: divergence,
            direction,
            confidence,
            symbols: vec![crypto_symbol.clone()],
            timestamp: now,
            metadata: serde_json::json!({
                "crypto_z_score": crypto_z,
                "equity_z_score_avg": avg_equity_z,
                "divergence": divergence,
                "crypto_vol": current_crypto_vol,
                "crypto_vol_mean": crypto_mean,
                "equity_z_scores": equity_z_scores,
                "num_equity_symbols": equity_z_scores.len(),
            }),
        })
    }

    pub fn get_vol(&self, symbol: &Symbol) -> Option<f64> {
        self.vol_cache.get(symbol)?.back().map(|v| v.vol)
    }
}

fn compute_realized_vol(window: &VecDeque<PricePoint>) -> f64 {
    if window.len() < 2 {
        return 0.0;
    }
    let log_returns: Vec<f64> = window
        .iter()
        .zip(window.iter().skip(1))
        .filter(|(prev, curr)| prev.price > 0.0 && curr.price > 0.0)
        .map(|(prev, curr)| (curr.price / prev.price).ln())
        .collect();
    if log_returns.is_empty() {
        return 0.0;
    }
    let m = mean(&log_returns);
    let variance = log_returns.iter().map(|r| (r - m).powi(2)).sum::<f64>() / log_returns.len() as f64;
    variance.sqrt()
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_realized_vol_basic() {
        let mut window = VecDeque::new();
        let base = Utc::now();
        // Constant prices → zero vol
        for i in 0..100 {
            window.push_back(PricePoint {
                price: 100.0,
                timestamp: base + Duration::seconds(i),
            });
        }
        let vol = compute_realized_vol(&window);
        assert!(vol < 1e-10, "Constant prices should have ~zero vol, got {vol}");
    }

    #[test]
    fn test_compute_realized_vol_with_movement() {
        let mut window = VecDeque::new();
        let base = Utc::now();
        for i in 0..100 {
            let price = 100.0 + (i as f64 * 0.1).sin() * 2.0;
            window.push_back(PricePoint {
                price,
                timestamp: base + Duration::seconds(i),
            });
        }
        let vol = compute_realized_vol(&window);
        assert!(vol > 0.0, "Oscillating prices should have positive vol");
        assert!(vol < 1.0, "Vol should be reasonable, got {vol}");
    }

    #[test]
    fn test_mean_and_std_dev() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let m = mean(&values);
        assert!((m - 5.0).abs() < 1e-10);
        let s = std_dev(&values, m);
        assert!((s - 2.0).abs() < 0.1, "Expected std ~2.0, got {s}");
    }

    #[test]
    fn test_z_score_threshold() {
        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 5.0];
        let m = mean(&values);
        let s = std_dev(&values, m);
        let z = (5.0 - m) / s;
        assert!(z > 2.0, "Outlier should have z > 2, got {z}");
    }
}
