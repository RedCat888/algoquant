use algoquant_core::config::RiskConfig;
use algoquant_core::types::*;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info};

pub struct RiskManager {
    config: RiskConfig,
    last_trade_time: DashMap<Symbol, DateTime<Utc>>,
    peak_equity: std::sync::Mutex<f64>,
    #[allow(dead_code)]
    daily_pnl: std::sync::Mutex<f64>,
    circuit_breaker_tripped: AtomicBool,
    /// Last known prices for market order value estimation.
    last_prices: DashMap<Symbol, f64>,
}

#[derive(Debug)]
pub enum RiskDecision {
    Approved,
    Rejected(String),
    ReducedSize {
        new_quantity: Decimal,
        reason: String,
    },
}

impl RiskManager {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            last_trade_time: DashMap::new(),
            peak_equity: std::sync::Mutex::new(0.0),
            daily_pnl: std::sync::Mutex::new(0.0),
            circuit_breaker_tripped: AtomicBool::new(false),
            last_prices: DashMap::new(),
        }
    }

    /// Update last known price for a symbol (called from tick data).
    pub fn update_price(&self, symbol: &Symbol, price: f64) {
        self.last_prices.insert(symbol.clone(), price);
    }

    /// Estimate notional value of an order using the best available price.
    fn estimate_order_value(&self, order: &Order) -> f64 {
        let qty = order.quantity.to_f64().unwrap_or(0.0);
        let price = order
            .limit_price
            .and_then(|p| p.to_f64())
            .or_else(|| order.stop_price.and_then(|p| p.to_f64()))
            .or_else(|| self.last_prices.get(&order.symbol).map(|p| *p))
            .unwrap_or(0.0);
        qty * price
    }

    pub fn evaluate_order(
        &self,
        order: &Order,
        portfolio: &PortfolioState,
        open_order_count: usize,
    ) -> RiskDecision {
        if self.circuit_breaker_tripped.load(Ordering::Relaxed) {
            return RiskDecision::Rejected("Circuit breaker is tripped — trading halted".into());
        }

        if open_order_count >= self.config.max_open_orders {
            return RiskDecision::Rejected(format!(
                "Too many open orders ({open_order_count} >= {})",
                self.config.max_open_orders
            ));
        }

        // Trade cooldown per symbol
        if let Some(last) = self.last_trade_time.get(&order.symbol) {
            let elapsed = (Utc::now() - *last).num_seconds();
            if elapsed < self.config.min_trade_interval_secs as i64 {
                return RiskDecision::Rejected(format!(
                    "Trade cooldown: {elapsed}s < {}s since last trade on {}",
                    self.config.min_trade_interval_secs, order.symbol
                ));
            }
        }

        let equity = portfolio.equity.to_f64().unwrap_or(0.0);
        if equity <= 0.0 {
            return RiskDecision::Rejected("Portfolio equity is zero or negative".into());
        }

        let order_value = self.estimate_order_value(order);

        // Absolute notional cap
        if order_value > self.config.max_order_notional {
            let price_est = self
                .last_prices
                .get(&order.symbol)
                .map(|p| *p)
                .unwrap_or(1.0);
            if price_est > 0.0 {
                let max_qty = (self.config.max_order_notional / price_est).floor();
                if max_qty < 1.0 {
                    return RiskDecision::Rejected(format!(
                        "Order notional ${order_value:.0} exceeds max ${:.0} and can't reduce to 1 share",
                        self.config.max_order_notional
                    ));
                }
                return RiskDecision::ReducedSize {
                    new_quantity: Decimal::from_f64_retain(max_qty).unwrap_or(Decimal::ONE),
                    reason: format!(
                        "Notional ${order_value:.0} exceeds max ${:.0}, reduced to {max_qty} shares",
                        self.config.max_order_notional
                    ),
                };
            }
        }

        // Position size as fraction of equity
        let position_pct = order_value / equity;
        if position_pct > self.config.max_position_pct {
            let price_est = self
                .last_prices
                .get(&order.symbol)
                .map(|p| *p)
                .unwrap_or(1.0);
            if price_est > 0.0 {
                let max_qty = (equity * self.config.max_position_pct / price_est).floor();
                if max_qty < 1.0 {
                    return RiskDecision::Rejected(format!(
                        "Position {position_pct:.1}% of equity exceeds {:.0}% and can't size down",
                        self.config.max_position_pct * 100.0
                    ));
                }
                return RiskDecision::ReducedSize {
                    new_quantity: Decimal::from_f64_retain(max_qty).unwrap_or(Decimal::ONE),
                    reason: format!(
                        "Position {:.1}% exceeds max {:.0}%, reduced to {max_qty} shares",
                        position_pct * 100.0,
                        self.config.max_position_pct * 100.0
                    ),
                };
            }
        }

        // Total exposure check
        let current_exposure: f64 = portfolio
            .positions
            .iter()
            .map(|p| p.market_value.to_f64().unwrap_or(0.0).abs())
            .sum();
        let total_exposure_pct = (current_exposure + order_value) / equity;
        if total_exposure_pct > self.config.max_total_exposure_pct {
            return RiskDecision::Rejected(format!(
                "Total exposure would be {:.1}% > max {:.1}%",
                total_exposure_pct * 100.0,
                self.config.max_total_exposure_pct * 100.0
            ));
        }

        // If we still can't estimate value (no price data at all), reject
        // rather than silently allowing unchecked orders
        if order_value < 0.01 {
            return RiskDecision::Rejected(format!(
                "Cannot estimate order value for {} — no price data available",
                order.symbol
            ));
        }

        RiskDecision::Approved
    }

    pub fn update_portfolio(&self, portfolio: &PortfolioState) {
        let equity = portfolio.equity.to_f64().unwrap_or(0.0);
        let mut peak = self.peak_equity.lock().unwrap();

        if equity > *peak {
            *peak = equity;
        }

        // Update last known prices from position data
        for pos in &portfolio.positions {
            if let Some(price) = pos.current_price.to_f64() {
                if price > 0.0 {
                    self.last_prices.insert(pos.symbol.clone(), price);
                }
            }
        }

        if *peak > 0.0 {
            let drawdown = (*peak - equity) / *peak;
            if drawdown >= self.config.max_drawdown_pct {
                if !self.circuit_breaker_tripped.load(Ordering::Relaxed) {
                    error!(
                        drawdown = format!("{:.2}%", drawdown * 100.0),
                        peak = format!("{peak:.2}"),
                        current = format!("{equity:.2}"),
                        "CIRCUIT BREAKER TRIPPED — max drawdown exceeded"
                    );
                    self.circuit_breaker_tripped.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn record_trade(&self, symbol: &Symbol) {
        self.last_trade_time.insert(symbol.clone(), Utc::now());
    }

    pub fn reset_circuit_breaker(&self) {
        info!("Circuit breaker reset");
        self.circuit_breaker_tripped.store(false, Ordering::Relaxed);
    }

    pub fn is_circuit_breaker_tripped(&self) -> bool {
        self.circuit_breaker_tripped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_config() -> RiskConfig {
        RiskConfig {
            max_position_pct: 0.05,
            max_total_exposure_pct: 0.50,
            max_drawdown_pct: 0.05,
            max_open_orders: 10,
            min_trade_interval_secs: 60,
            max_order_notional: 10000.0,
            daily_loss_limit_pct: 0.02,
        }
    }

    fn test_portfolio() -> PortfolioState {
        PortfolioState {
            equity: dec!(100000),
            cash: dec!(100000),
            buying_power: dec!(200000),
            positions: vec![],
            timestamp: Utc::now(),
        }
    }

    fn test_order(symbol: &str, qty: u32) -> Order {
        Order {
            id: "test".to_string(),
            symbol: Symbol::new(symbol),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity: Decimal::from(qty),
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            status: OrderStatus::Pending,
            filled_qty: Decimal::ZERO,
            filled_avg_price: None,
            strategy_id: "test".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_market_order_without_price_data_is_rejected() {
        let rm = RiskManager::new(test_config());
        let order = test_order("SPY", 100);
        let decision = rm.evaluate_order(&order, &test_portfolio(), 0);
        assert!(
            matches!(decision, RiskDecision::Rejected(_)),
            "Market order with no price data should be rejected, got {decision:?}"
        );
    }

    #[test]
    fn test_market_order_with_price_data_is_sized() {
        let rm = RiskManager::new(test_config());
        rm.update_price(&Symbol::new("SPY"), 500.0);
        // 100 * 500 = $50,000 → 50% of equity → exceeds 5% max
        let order = test_order("SPY", 100);
        let decision = rm.evaluate_order(&order, &test_portfolio(), 0);
        assert!(
            matches!(decision, RiskDecision::ReducedSize { .. }),
            "Should reduce size, got {decision:?}"
        );
    }

    #[test]
    fn test_small_order_approved() {
        let rm = RiskManager::new(test_config());
        rm.update_price(&Symbol::new("SPY"), 500.0);
        // 5 * 500 = $2,500 → 2.5% of equity → under 5% max
        let order = test_order("SPY", 5);
        let decision = rm.evaluate_order(&order, &test_portfolio(), 0);
        assert!(
            matches!(decision, RiskDecision::Approved),
            "Small order should be approved, got {decision:?}"
        );
    }

    #[test]
    fn test_notional_cap() {
        let rm = RiskManager::new(test_config());
        rm.update_price(&Symbol::new("AMZN"), 200.0);
        // 100 * 200 = $20,000 → exceeds $10k notional cap
        let order = test_order("AMZN", 100);
        let decision = rm.evaluate_order(&order, &test_portfolio(), 0);
        match decision {
            RiskDecision::ReducedSize { new_quantity, .. } => {
                let max_shares = new_quantity.to_f64().unwrap();
                assert!(max_shares <= 50.0, "Should cap at ~50 shares, got {max_shares}");
            }
            other => panic!("Expected ReducedSize, got {other:?}"),
        }
    }

    #[test]
    fn test_circuit_breaker() {
        let rm = RiskManager::new(test_config());
        let mut portfolio = test_portfolio();
        rm.update_portfolio(&portfolio);

        portfolio.equity = dec!(94000); // 6% drawdown > 5% limit
        rm.update_portfolio(&portfolio);

        assert!(rm.is_circuit_breaker_tripped());

        let order = test_order("SPY", 1);
        let decision = rm.evaluate_order(&order, &portfolio, 0);
        assert!(matches!(decision, RiskDecision::Rejected(_)));
    }
}
