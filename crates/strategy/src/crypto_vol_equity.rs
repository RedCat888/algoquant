use algoquant_core::config::CryptoVolLeadsEquityConfig;
use algoquant_core::types::*;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::info;

use crate::framework::Strategy;

/// When BTC realized volatility spikes (high z-score), this strategy
/// interprets it as an early risk-off signal and reduces equity exposure
/// or opens short hedges on broad market ETFs.
///
/// The thesis: crypto markets trade 24/7 and react faster to global risk
/// events. By the time US equity markets open (or during the trading day),
/// the risk-off move often hasn't fully priced in yet.
pub struct CryptoVolLeadsEquity {
    config: CryptoVolLeadsEquityConfig,
    /// Track when we entered hedging positions so we can time exits.
    active_hedges: HashMap<Symbol, HedgeEntry>,
}

#[allow(dead_code)]
struct HedgeEntry {
    entered_at: DateTime<Utc>,
    quantity: Decimal,
    signal_z_score: f64,
}

impl CryptoVolLeadsEquity {
    pub fn new(config: CryptoVolLeadsEquityConfig) -> Self {
        Self {
            config,
            active_hedges: HashMap::new(),
        }
    }

    fn should_exit_hedge(&self, symbol: &Symbol, now: DateTime<Utc>) -> bool {
        if let Some(hedge) = self.active_hedges.get(symbol) {
            let held_for = (now - hedge.entered_at).num_seconds();
            held_for >= self.config.hold_duration_secs as i64
        } else {
            false
        }
    }
}

impl Strategy for CryptoVolLeadsEquity {
    fn name(&self) -> &str {
        "crypto_vol_leads_equity"
    }

    fn on_signal(&mut self, signal: &SignalValue, portfolio: &PortfolioState) -> Vec<Order> {
        if signal.name != "crypto_vol_leads_equity" {
            return vec![];
        }

        let now = signal.timestamp;
        let mut orders = Vec::new();

        // Check if we should exit any existing hedges
        let symbols_to_exit: Vec<Symbol> = self
            .active_hedges
            .keys()
            .filter(|s| self.should_exit_hedge(s, now))
            .cloned()
            .collect();

        for symbol in symbols_to_exit {
            if let Some(hedge) = self.active_hedges.remove(&symbol) {
                info!(
                    symbol = %symbol,
                    held_secs = (now - hedge.entered_at).num_seconds(),
                    "Exiting hedge position"
                );
                orders.push(Order {
                    id: uuid::Uuid::new_v4().to_string(),
                    symbol: symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Market,
                    quantity: hedge.quantity,
                    limit_price: None,
                    stop_price: None,
                    time_in_force: TimeInForce::Day,
                    status: OrderStatus::Pending,
                    filled_qty: Decimal::ZERO,
                    filled_avg_price: None,
                    strategy_id: self.name().to_string(),
                    created_at: now,
                    updated_at: now,
                });
            }
        }

        // Only enter new hedges if signal direction is negative (risk-off)
        if signal.direction >= 0.0 {
            return orders;
        }

        let z_score = signal.value;
        if z_score.abs() < self.config.vol_spike_threshold {
            return orders;
        }

        let buying_power = portfolio.buying_power.to_f64().unwrap_or(0.0);
        let trade_amount = buying_power * self.config.position_size_pct;

        for equity_sym in &self.config.equity_symbols {
            let symbol = Symbol::new(equity_sym);

            // Skip if we already have an active hedge
            if self.active_hedges.contains_key(&symbol) {
                continue;
            }

            // Estimate share count from current positions or recent price
            let current_price = portfolio
                .positions
                .iter()
                .find(|p| p.symbol == symbol)
                .map(|p| p.current_price.to_f64().unwrap_or(100.0))
                .unwrap_or(100.0);

            if current_price <= 0.0 {
                continue;
            }

            let qty_f64 = (trade_amount / current_price).floor();
            if qty_f64 < 1.0 {
                continue;
            }
            let quantity = Decimal::from_f64_retain(qty_f64).unwrap_or(dec!(1));

            info!(
                symbol = %symbol,
                qty = %quantity,
                z_score = format!("{z_score:.3}"),
                confidence = format!("{:.3}", signal.confidence),
                "Opening hedge — crypto vol spike detected"
            );

            self.active_hedges.insert(
                symbol.clone(),
                HedgeEntry {
                    entered_at: now,
                    quantity,
                    signal_z_score: z_score,
                },
            );

            orders.push(Order {
                id: uuid::Uuid::new_v4().to_string(),
                symbol,
                side: OrderSide::Sell,
                order_type: OrderType::Market,
                quantity,
                limit_price: None,
                stop_price: None,
                time_in_force: TimeInForce::Day,
                status: OrderStatus::Pending,
                filled_qty: Decimal::ZERO,
                filled_avg_price: None,
                strategy_id: self.name().to_string(),
                created_at: now,
                updated_at: now,
            });
        }

        orders
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}
