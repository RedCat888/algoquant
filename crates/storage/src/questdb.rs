use algoquant_core::types::{NewsArticle, Order, PortfolioState, SignalValue};
use anyhow::{Context, Result};
use rust_decimal::prelude::ToPrimitive;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Mutex;
use tracing::{info, warn};

/// Writes data to QuestDB via the InfluxDB Line Protocol (ILP) over TCP.
/// QuestDB auto-creates tables from ILP messages.
pub struct QuestDbWriter {
    stream: Mutex<Option<TcpStream>>,
    address: String,
}

impl QuestDbWriter {
    pub fn new(address: &str) -> Self {
        Self {
            stream: Mutex::new(None),
            address: address.to_string(),
        }
    }

    pub fn connect(&self) -> Result<()> {
        let stream = TcpStream::connect(&self.address)
            .with_context(|| format!("Failed to connect to QuestDB at {}", self.address))?;
        stream.set_nodelay(true)?;
        *self.stream.lock().unwrap() = Some(stream);
        info!(address = %self.address, "Connected to QuestDB ILP");
        Ok(())
    }

    fn send_line(&self, line: &str) {
        let mut guard = self.stream.lock().unwrap();
        if let Some(ref mut stream) = *guard {
            if let Err(e) = stream.write_all(line.as_bytes()) {
                warn!("QuestDB write failed: {e}, will reconnect");
                *guard = None;
                if let Ok(new_stream) = TcpStream::connect(&self.address) {
                    let _ = new_stream.set_nodelay(true);
                    let _ = new_stream.try_clone().map(|mut s| s.write_all(line.as_bytes()));
                    *guard = Some(new_stream);
                }
            }
        }
    }

    pub fn write_signal(&self, signal: &SignalValue) {
        let ts_nanos = signal.timestamp.timestamp_nanos_opt().unwrap_or(0);
        let symbols = signal
            .symbols
            .iter()
            .map(|s| s.0.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let line = format!(
            "signals,name={},symbols={} value={},direction={},confidence={} {}\n",
            escape_tag(&signal.name),
            escape_tag(&symbols),
            signal.value,
            signal.direction,
            signal.confidence,
            ts_nanos,
        );
        self.send_line(&line);
    }

    pub fn write_trade(&self, order: &Order) {
        let ts_nanos = order.updated_at.timestamp_nanos_opt().unwrap_or(0);
        let side_str = match order.side {
            algoquant_core::types::OrderSide::Buy => "buy",
            algoquant_core::types::OrderSide::Sell => "sell",
        };
        let filled_price = order
            .filled_avg_price
            .and_then(|p| p.to_f64())
            .unwrap_or(0.0);
        let qty = order.quantity.to_f64().unwrap_or(0.0);
        let line = format!(
            "trades,symbol={},side={},strategy={} qty={},filled_price={} {}\n",
            escape_tag(&order.symbol.0),
            side_str,
            escape_tag(&order.strategy_id),
            qty,
            filled_price,
            ts_nanos,
        );
        self.send_line(&line);
    }

    pub fn write_portfolio(&self, portfolio: &PortfolioState) {
        let ts_nanos = portfolio.timestamp.timestamp_nanos_opt().unwrap_or(0);
        let equity = portfolio.equity.to_f64().unwrap_or(0.0);
        let cash = portfolio.cash.to_f64().unwrap_or(0.0);
        let positions = portfolio.positions.len();
        let total_unrealized: f64 = portfolio
            .positions
            .iter()
            .map(|p| p.unrealized_pnl.to_f64().unwrap_or(0.0))
            .sum();
        let line = format!(
            "portfolio equity={},cash={},positions={},unrealized_pnl={} {}\n",
            equity, cash, positions, total_unrealized, ts_nanos,
        );
        self.send_line(&line);
    }

    pub fn write_news(&self, article: &NewsArticle) {
        let ts_nanos = article.published_at.timestamp_nanos_opt().unwrap_or(0);
        let sentiment = article.sentiment_score.unwrap_or(0.0);
        let symbols = article
            .symbols
            .iter()
            .map(|s| s.0.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let line = format!(
            "news,source={},symbols={} sentiment={} {}\n",
            escape_tag(&article.source),
            escape_tag(&symbols),
            sentiment,
            ts_nanos,
        );
        self.send_line(&line);
    }
}

fn escape_tag(s: &str) -> String {
    s.replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace('/', "_")
}
