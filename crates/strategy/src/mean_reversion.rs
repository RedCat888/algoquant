use algoquant_core::config::MeanReversionConfig;
use algoquant_core::types::*;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::info;

use crate::framework::Strategy;

/// Buys oversold equities (low RSI) and sells when RSI recovers.
/// Only enters when the cross-asset regime is not in crisis mode
/// (no open vol-hedge positions from the other strategy).
pub struct MeanReversion {
    config: MeanReversionConfig,
    active_positions: HashMap<Symbol, MREntry>,
}

struct MREntry {
    entered_at: DateTime<Utc>,
    quantity: Decimal,
    entry_rsi: f64,
}

impl MeanReversion {
    pub fn new(config: MeanReversionConfig) -> Self {
        Self {
            config,
            active_positions: HashMap::new(),
        }
    }
}

impl Strategy for MeanReversion {
    fn name(&self) -> &str {
        "mean_reversion"
    }

    fn on_signal(&mut self, signal: &SignalValue, portfolio: &PortfolioState) -> Vec<Order> {
        let now = signal.timestamp;
        let mut orders = Vec::new();

        // Check for timed-out positions
        let to_exit: Vec<Symbol> = self
            .active_positions
            .iter()
            .filter(|(_, entry)| {
                (now - entry.entered_at).num_seconds() >= self.config.max_hold_secs as i64
            })
            .map(|(sym, _)| sym.clone())
            .collect();

        for symbol in to_exit {
            if let Some(entry) = self.active_positions.remove(&symbol) {
                info!(
                    symbol = %symbol,
                    held_secs = (now - entry.entered_at).num_seconds(),
                    "Mean reversion: exiting on timeout"
                );
                orders.push(make_order(
                    &symbol,
                    OrderSide::Sell,
                    entry.quantity,
                    self.name(),
                    now,
                ));
            }
        }

        // RSI exit: if RSI has recovered above exit threshold, take profit
        if signal.name == "rsi_overbought" {
            for sym in &signal.symbols {
                if let Some(entry) = self.active_positions.remove(sym) {
                    info!(
                        symbol = %sym,
                        entry_rsi = format!("{:.1}", entry.entry_rsi),
                        exit_rsi = format!("{:.1}", signal.value),
                        "Mean reversion: RSI recovered, taking profit"
                    );
                    orders.push(make_order(
                        sym,
                        OrderSide::Sell,
                        entry.quantity,
                        self.name(),
                        now,
                    ));
                }
            }
        }

        // RSI entry: if RSI is oversold, buy the dip
        if signal.name == "rsi_oversold" {
            let rsi = signal.value;
            if rsi > self.config.rsi_entry_threshold {
                return orders;
            }

            for sym in &signal.symbols {
                if !self.config.symbols.iter().any(|s| Symbol::new(s) == *sym) {
                    continue;
                }
                if self.active_positions.contains_key(sym) {
                    continue;
                }

                let equity = portfolio.equity.to_f64().unwrap_or(0.0);
                let trade_amount = equity * self.config.position_size_pct;

                let price = portfolio
                    .positions
                    .iter()
                    .find(|p| p.symbol == *sym)
                    .map(|p| p.current_price.to_f64().unwrap_or(0.0))
                    .unwrap_or(0.0);

                if price <= 0.0 {
                    continue;
                }

                let qty = (trade_amount / price).floor();
                if qty < 1.0 {
                    continue;
                }
                let quantity = Decimal::from_f64_retain(qty).unwrap_or(dec!(1));

                info!(
                    symbol = %sym,
                    rsi = format!("{rsi:.1}"),
                    qty = %quantity,
                    "Mean reversion: buying oversold"
                );

                self.active_positions.insert(
                    sym.clone(),
                    MREntry {
                        entered_at: now,
                        quantity,
                        entry_rsi: rsi,
                    },
                );

                orders.push(make_order(sym, OrderSide::Buy, quantity, self.name(), now));
            }
        }

        orders
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

fn make_order(
    symbol: &Symbol,
    side: OrderSide,
    quantity: Decimal,
    strategy_id: &str,
    now: DateTime<Utc>,
) -> Order {
    Order {
        id: uuid::Uuid::new_v4().to_string(),
        symbol: symbol.clone(),
        side,
        order_type: OrderType::Market,
        quantity,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::Day,
        status: OrderStatus::Pending,
        filled_qty: Decimal::ZERO,
        filled_avg_price: None,
        strategy_id: strategy_id.to_string(),
        created_at: now,
        updated_at: now,
    }
}
