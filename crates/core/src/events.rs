use crate::types::{Bar, NewsArticle, Order, PortfolioState, SignalValue, Tick};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Every piece of data flowing through the system is an Event.
/// This is the fundamental unit of the event-driven architecture.
/// The same event types are replayed in backtesting.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    Tick(Tick),
    Bar(Bar),
    Signal(SignalValue),
    OrderRequest(Order),
    OrderUpdate(Order),
    PortfolioUpdate(PortfolioState),
    News(NewsArticle),
    MacroData(MacroDataPoint),
    SystemCommand(SystemCommand),
}

impl Event {
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Event::Tick(t) => t.timestamp,
            Event::Bar(b) => b.bar_end,
            Event::Signal(s) => s.timestamp,
            Event::OrderRequest(o) | Event::OrderUpdate(o) => o.updated_at,
            Event::PortfolioUpdate(p) => p.timestamp,
            Event::News(n) => n.published_at,
            Event::MacroData(m) => m.timestamp,
            Event::SystemCommand(c) => c.timestamp(),
        }
    }

    pub fn nats_subject(&self) -> &'static str {
        match self {
            Event::Tick(_) => "market.tick",
            Event::Bar(_) => "market.bar",
            Event::Signal(_) => "signal",
            Event::OrderRequest(_) => "order.request",
            Event::OrderUpdate(_) => "order.update",
            Event::PortfolioUpdate(_) => "portfolio.update",
            Event::News(_) => "data.news",
            Event::MacroData(_) => "data.macro",
            Event::SystemCommand(_) => "system.command",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroDataPoint {
    pub indicator: String,
    pub value: f64,
    pub previous: Option<f64>,
    pub source: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command")]
pub enum SystemCommand {
    Shutdown { timestamp: DateTime<Utc> },
    ReloadConfig { timestamp: DateTime<Utc> },
    PauseTrading { timestamp: DateTime<Utc> },
    ResumeTrading { timestamp: DateTime<Utc> },
}

impl SystemCommand {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Shutdown { timestamp }
            | Self::ReloadConfig { timestamp }
            | Self::PauseTrading { timestamp }
            | Self::ResumeTrading { timestamp } => *timestamp,
        }
    }
}

/// NATS subjects used throughout the system.
pub mod subjects {
    pub const TICK: &str = "market.tick";
    pub const BAR: &str = "market.bar";
    pub const SIGNAL: &str = "signal";
    pub const ORDER_REQUEST: &str = "order.request";
    pub const ORDER_UPDATE: &str = "order.update";
    pub const PORTFOLIO_UPDATE: &str = "portfolio.update";
    pub const NEWS: &str = "data.news";
    pub const MACRO_DATA: &str = "data.macro";
    pub const SYSTEM_COMMAND: &str = "system.command";

    /// Wildcard to subscribe to all market data.
    pub const ALL_MARKET: &str = "market.>";
    /// Wildcard to subscribe to all events.
    pub const ALL: &str = ">";
}
