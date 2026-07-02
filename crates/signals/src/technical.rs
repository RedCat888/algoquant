use algoquant_core::types::{AssetClass, SignalValue, Symbol, Tick};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use rust_decimal::prelude::ToPrimitive;
use std::collections::VecDeque;
use tracing::info;

/// Computes RSI, VWAP deviation, and volume anomaly for each symbol.
///
/// Key design decision: RSI is computed from 1-minute bar closes, not raw ticks.
/// Raw tick RSI is noise — it flips between 0 and 100 within milliseconds as
/// individual trades arrive. By bucketing into 1-minute bars first, we get the
/// same RSI behavior that a human trader would see on a 1-min chart.
pub struct TechnicalAnalyzer {
    /// Raw tick accumulator per symbol, used to build 1-min bars.
    tick_accumulators: DashMap<Symbol, TickAccumulator>,
    /// 1-minute bar history per symbol, used for RSI and VWAP.
    bar_histories: DashMap<Symbol, VecDeque<MiniBar>>,
    /// Last signal emission time per symbol to prevent spam.
    last_signal_time: DashMap<Symbol, DateTime<Utc>>,
    rsi_period: usize,
    rsi_overbought: f64,
    rsi_oversold: f64,
    vwap_deviation_threshold: f64,
    volume_spike_multiplier: f64,
}

#[derive(Debug, Clone)]
struct TickAccumulator {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    count: u32,
    bar_start: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct MiniBar {
    close: f64,
    vwap: f64,
    volume: f64,
    timestamp: DateTime<Utc>,
}

impl TechnicalAnalyzer {
    pub fn new(
        rsi_period: usize,
        rsi_overbought: f64,
        rsi_oversold: f64,
        vwap_deviation_threshold: f64,
        volume_spike_multiplier: f64,
    ) -> Self {
        Self {
            tick_accumulators: DashMap::new(),
            bar_histories: DashMap::new(),
            last_signal_time: DashMap::new(),
            rsi_period,
            rsi_overbought,
            rsi_oversold,
            vwap_deviation_threshold,
            volume_spike_multiplier,
        }
    }

    pub fn on_tick(&self, tick: &Tick) -> Option<SignalValue> {
        let price = tick.price.to_f64()?;
        if price <= 0.0 {
            return None;
        }

        if tick.asset_class != AssetClass::Equity {
            return None;
        }

        let volume = tick.volume.and_then(|v| v.to_f64()).unwrap_or(0.0);
        let now = tick.timestamp;

        let completed_bar = self.accumulate_tick(&tick.symbol, price, volume, now);

        if let Some(bar) = completed_bar {
            self.on_bar_close(&tick.symbol, bar, now)
        } else {
            None
        }
    }

    /// Accumulate ticks into 1-minute bars. Returns Some(bar) when a bar completes.
    fn accumulate_tick(
        &self,
        symbol: &Symbol,
        price: f64,
        volume: f64,
        now: DateTime<Utc>,
    ) -> Option<MiniBar> {
        let bar_duration = Duration::seconds(60);
        let mut acc = self
            .tick_accumulators
            .entry(symbol.clone())
            .or_insert_with(|| TickAccumulator {
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0.0,
                count: 0,
                bar_start: now,
            });

        let elapsed = now - acc.bar_start;
        if elapsed >= bar_duration && acc.count > 0 {
            let completed = MiniBar {
                close: acc.close,
                vwap: if acc.volume > 0.0 {
                    acc.close
                } else {
                    (acc.open + acc.high + acc.low + acc.close) / 4.0
                },
                volume: acc.volume,
                timestamp: acc.bar_start + bar_duration,
            };

            // Start new bar
            acc.open = price;
            acc.high = price;
            acc.low = price;
            acc.close = price;
            acc.volume = volume;
            acc.count = 1;
            acc.bar_start = now;

            return Some(completed);
        }

        // Update current bar
        acc.high = acc.high.max(price);
        acc.low = acc.low.min(price);
        acc.close = price;
        acc.volume += volume;
        acc.count += 1;

        None
    }

    fn on_bar_close(
        &self,
        symbol: &Symbol,
        bar: MiniBar,
        now: DateTime<Utc>,
    ) -> Option<SignalValue> {
        let cutoff = now - Duration::seconds(7200); // 2-hour history
        let mut history = self
            .bar_histories
            .entry(symbol.clone())
            .or_insert_with(VecDeque::new);
        history.push_back(bar);
        while history.front().map_or(false, |b| b.timestamp < cutoff) {
            history.pop_front();
        }

        // Need enough bars for RSI
        if history.len() < self.rsi_period + 5 {
            return None;
        }

        // Per-symbol cooldown: at most one signal per 2 minutes
        if let Some(last) = self.last_signal_time.get(symbol) {
            if (now - *last).num_seconds() < 120 {
                return None;
            }
        }

        let rsi = self.compute_rsi(&history)?;
        let (vwap, vwap_dev) = self.compute_vwap(&history);
        let vol_ratio = self.compute_volume_ratio(&history);

        // RSI extreme signals
        if rsi <= self.rsi_oversold {
            let strength = (self.rsi_oversold - rsi) / self.rsi_oversold;
            self.last_signal_time.insert(symbol.clone(), now);
            info!(
                symbol = %symbol,
                rsi = format!("{rsi:.1}"),
                bars = history.len(),
                "RSI oversold (1-min bars)"
            );
            return Some(SignalValue {
                name: "rsi_oversold".to_string(),
                value: rsi,
                direction: 1.0,
                confidence: strength.min(1.0),
                symbols: vec![symbol.clone()],
                timestamp: now,
                metadata: serde_json::json!({
                    "rsi": rsi,
                    "vwap_deviation": vwap_dev,
                    "volume_ratio": vol_ratio,
                    "bars": history.len(),
                }),
            });
        }

        if rsi >= self.rsi_overbought {
            let strength = (rsi - self.rsi_overbought) / (100.0 - self.rsi_overbought);
            self.last_signal_time.insert(symbol.clone(), now);
            info!(
                symbol = %symbol,
                rsi = format!("{rsi:.1}"),
                bars = history.len(),
                "RSI overbought (1-min bars)"
            );
            return Some(SignalValue {
                name: "rsi_overbought".to_string(),
                value: rsi,
                direction: -1.0,
                confidence: strength.min(1.0),
                symbols: vec![symbol.clone()],
                timestamp: now,
                metadata: serde_json::json!({
                    "rsi": rsi,
                    "vwap_deviation": vwap_dev,
                    "volume_ratio": vol_ratio,
                    "bars": history.len(),
                }),
            });
        }

        // Volume spike + VWAP breakout
        if let (Some(vd), Some(vr)) = (vwap_dev, vol_ratio) {
            if vr >= self.volume_spike_multiplier && vd.abs() >= self.vwap_deviation_threshold {
                let direction = if vd > 0.0 { 1.0 } else { -1.0 };
                let confidence = ((vr / self.volume_spike_multiplier) * 0.5).min(1.0);
                self.last_signal_time.insert(symbol.clone(), now);
                info!(
                    symbol = %symbol,
                    volume_ratio = format!("{vr:.1}x"),
                    vwap_dev = format!("{vd:.4}"),
                    "Volume spike + VWAP breakout"
                );
                return Some(SignalValue {
                    name: "volume_vwap_breakout".to_string(),
                    value: vr,
                    direction,
                    confidence,
                    symbols: vec![symbol.clone()],
                    timestamp: now,
                    metadata: serde_json::json!({
                        "volume_ratio": vr,
                        "vwap_deviation": vd,
                        "rsi": rsi,
                    }),
                });
            }
        }

        None
    }

    fn compute_rsi(&self, history: &VecDeque<MiniBar>) -> Option<f64> {
        if history.len() < self.rsi_period + 1 {
            return None;
        }

        let closes: Vec<f64> = history.iter().map(|b| b.close).collect();
        let n = closes.len();
        let lookback = &closes[n.saturating_sub(self.rsi_period + 1)..];

        let mut gains = 0.0;
        let mut losses = 0.0;
        let mut count = 0;

        for pair in lookback.windows(2) {
            let change = pair[1] - pair[0];
            if change > 0.0 {
                gains += change;
            } else {
                losses += change.abs();
            }
            count += 1;
        }

        if count == 0 {
            return None;
        }

        let avg_gain = gains / count as f64;
        let avg_loss = losses / count as f64;

        if avg_loss < 1e-12 {
            return Some(100.0);
        }

        let rs = avg_gain / avg_loss;
        Some(100.0 - (100.0 / (1.0 + rs)))
    }

    fn compute_vwap(&self, history: &VecDeque<MiniBar>) -> (Option<f64>, Option<f64>) {
        let mut cum_pv = 0.0;
        let mut cum_vol = 0.0;

        for bar in history.iter() {
            if bar.volume > 0.0 {
                cum_pv += bar.close * bar.volume;
                cum_vol += bar.volume;
            }
        }

        if cum_vol < 1e-12 {
            return (None, None);
        }

        let vwap = cum_pv / cum_vol;
        let current_price = history.back().map(|b| b.close).unwrap_or(0.0);
        if current_price <= 0.0 {
            return (Some(vwap), None);
        }
        let deviation = (current_price - vwap) / vwap;
        (Some(vwap), Some(deviation))
    }

    fn compute_volume_ratio(&self, history: &VecDeque<MiniBar>) -> Option<f64> {
        let volumes: Vec<f64> = history.iter().filter(|b| b.volume > 0.0).map(|b| b.volume).collect();
        if volumes.len() < 5 {
            return None;
        }
        let avg = volumes.iter().sum::<f64>() / volumes.len() as f64;
        if avg < 1e-12 {
            return None;
        }
        let current = volumes.last()?;
        Some(current / avg)
    }

    pub fn get_rsi(&self, symbol: &Symbol) -> Option<f64> {
        let history = self.bar_histories.get(symbol)?;
        self.compute_rsi(&history)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bar(close: f64, volume: f64, secs_offset: i64) -> MiniBar {
        MiniBar {
            close,
            vwap: close,
            volume,
            timestamp: Utc::now() + Duration::seconds(secs_offset),
        }
    }

    #[test]
    fn test_rsi_overbought() {
        let analyzer = TechnicalAnalyzer::new(14, 70.0, 30.0, 0.02, 3.0);
        let mut history = VecDeque::new();
        for i in 0..20 {
            history.push_back(make_bar(100.0 + i as f64, 1000.0, i * 60));
        }
        let rsi = analyzer.compute_rsi(&history).unwrap();
        assert!(rsi > 90.0, "Steadily rising bars should have RSI > 90, got {rsi}");
    }

    #[test]
    fn test_rsi_oversold() {
        let analyzer = TechnicalAnalyzer::new(14, 70.0, 30.0, 0.02, 3.0);
        let mut history = VecDeque::new();
        for i in 0..20 {
            history.push_back(make_bar(100.0 - i as f64, 1000.0, i * 60));
        }
        let rsi = analyzer.compute_rsi(&history).unwrap();
        assert!(rsi < 10.0, "Steadily falling bars should have RSI < 10, got {rsi}");
    }

    #[test]
    fn test_rsi_neutral() {
        let analyzer = TechnicalAnalyzer::new(14, 70.0, 30.0, 0.02, 3.0);
        let mut history = VecDeque::new();
        for i in 0..20 {
            let close = 100.0 + if i % 2 == 0 { 1.0 } else { -1.0 };
            history.push_back(make_bar(close, 1000.0, i * 60));
        }
        let rsi = analyzer.compute_rsi(&history).unwrap();
        assert!(rsi > 40.0 && rsi < 60.0, "Alternating prices should have RSI ~50, got {rsi}");
    }

    #[test]
    fn test_vwap_computation() {
        let analyzer = TechnicalAnalyzer::new(14, 70.0, 30.0, 0.02, 3.0);
        let mut history = VecDeque::new();
        for i in 0..10 {
            history.push_back(make_bar(100.0, 100.0, i * 60));
        }
        let (vwap, dev) = analyzer.compute_vwap(&history);
        assert!((vwap.unwrap() - 100.0).abs() < 0.01);
        assert!(dev.unwrap().abs() < 0.001, "Zero deviation expected for constant prices");
    }
}
