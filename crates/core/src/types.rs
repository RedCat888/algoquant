use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into().to_uppercase())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    Equity,
    Crypto,
    Commodity,
    Forex,
    Option,
    Future,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Alpaca,
    Binance,
    Coinbase,
    Polygon,
    Synthetic,
}

/// Normalized price tick from any source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: Symbol,
    pub asset_class: AssetClass,
    pub exchange: Exchange,
    pub price: Decimal,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

/// OHLCV bar aggregated from ticks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub symbol: Symbol,
    pub asset_class: AssetClass,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub vwap: Option<Decimal>,
    pub bar_start: DateTime<Utc>,
    pub bar_end: DateTime<Utc>,
}

/// A computed signal value with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalValue {
    pub name: String,
    pub value: f64,
    /// -1.0 (strong sell) to 1.0 (strong buy), 0.0 = neutral
    pub direction: f64,
    pub confidence: f64,
    pub symbols: Vec<Symbol>,
    pub timestamp: DateTime<Utc>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

/// An order to be submitted (or already submitted) to a broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub limit_price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub time_in_force: TimeInForce,
    pub status: OrderStatus,
    pub filled_qty: Decimal,
    pub filled_avg_price: Option<Decimal>,
    pub strategy_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Current position in an asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub quantity: Decimal,
    pub avg_entry_price: Decimal,
    pub current_price: Decimal,
    pub market_value: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub side: PositionSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
    Flat,
}

/// Account-level portfolio state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioState {
    pub equity: Decimal,
    pub cash: Decimal,
    pub buying_power: Decimal,
    pub positions: Vec<Position>,
    pub timestamp: DateTime<Utc>,
}

/// News article from any source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsArticle {
    pub id: String,
    pub title: String,
    pub summary: Option<String>,
    pub source: String,
    pub url: Option<String>,
    pub symbols: Vec<Symbol>,
    pub sentiment_score: Option<f64>,
    pub published_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}
