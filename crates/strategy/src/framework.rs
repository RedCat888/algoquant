use algoquant_core::types::{Order, PortfolioState, SignalValue};

/// Every strategy implements this trait.
/// Strategies receive signals and portfolio state, and emit order requests.
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn on_signal(&mut self, signal: &SignalValue, portfolio: &PortfolioState) -> Vec<Order>;
    fn is_enabled(&self) -> bool;
}
