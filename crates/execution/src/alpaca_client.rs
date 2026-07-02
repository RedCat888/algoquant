use algoquant_core::config::AlpacaConfig;
use algoquant_core::types::*;
use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::info;

pub struct AlpacaClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct AlpacaOrderRequest {
    symbol: String,
    qty: Option<String>,
    notional: Option<String>,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    time_in_force: String,
    limit_price: Option<String>,
    stop_price: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AlpacaOrderResponse {
    id: String,
    status: String,
    symbol: String,
    qty: Option<String>,
    filled_qty: Option<String>,
    filled_avg_price: Option<String>,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct AlpacaPosition {
    symbol: String,
    qty: String,
    avg_entry_price: String,
    current_price: String,
    market_value: String,
    unrealized_pl: String,
    side: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AlpacaAccount {
    equity: String,
    cash: String,
    buying_power: String,
    status: String,
}

impl AlpacaClient {
    pub fn new(config: &AlpacaConfig) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "APCA-API-KEY-ID",
            config.api_key.parse().expect("Invalid API key"),
        );
        headers.insert(
            "APCA-API-SECRET-KEY",
            config.api_secret.parse().expect("Invalid API secret"),
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            http,
            base_url: config.base_url.clone(),
        }
    }

    pub async fn get_account(&self) -> Result<PortfolioState> {
        let resp: AlpacaAccount = self
            .http
            .get(format!("{}/account", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let positions = self.get_positions().await.unwrap_or_default();

        Ok(PortfolioState {
            equity: Decimal::from_str(&resp.equity).unwrap_or(Decimal::ZERO),
            cash: Decimal::from_str(&resp.cash).unwrap_or(Decimal::ZERO),
            buying_power: Decimal::from_str(&resp.buying_power).unwrap_or(Decimal::ZERO),
            positions,
            timestamp: Utc::now(),
        })
    }

    pub async fn get_positions(&self) -> Result<Vec<Position>> {
        let resp: Vec<AlpacaPosition> = self
            .http
            .get(format!("{}/positions", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(resp.into_iter().map(|p| convert_position(p)).collect())
    }

    pub async fn submit_order(&self, order: &Order) -> Result<Order> {
        let side_str = match order.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };
        let type_str = match order.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::Stop => "stop",
            OrderType::StopLimit => "stop_limit",
        };
        let tif_str = match order.time_in_force {
            TimeInForce::Day => "day",
            TimeInForce::Gtc => "gtc",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
        };

        let req = AlpacaOrderRequest {
            symbol: order.symbol.0.clone(),
            qty: Some(order.quantity.to_string()),
            notional: None,
            side: side_str.to_string(),
            order_type: type_str.to_string(),
            time_in_force: tif_str.to_string(),
            limit_price: order.limit_price.map(|p| p.to_string()),
            stop_price: order.stop_price.map(|p| p.to_string()),
        };

        info!(
            symbol = %order.symbol,
            side = side_str,
            qty = %order.quantity,
            "Submitting order to Alpaca"
        );

        let resp: AlpacaOrderResponse = self
            .http
            .post(format!("{}/orders", self.base_url))
            .json(&req)
            .send()
            .await
            .context("Failed to send order to Alpaca")?
            .error_for_status()
            .context("Alpaca rejected order")?
            .json()
            .await
            .context("Failed to parse Alpaca order response")?;

        info!(
            order_id = %resp.id,
            status = %resp.status,
            "Order submitted to Alpaca"
        );

        Ok(convert_order_response(resp, order))
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        self.http
            .delete(format!("{}/orders/{}", self.base_url, order_id))
            .send()
            .await?
            .error_for_status()?;

        info!(order_id = order_id, "Order cancelled");
        Ok(())
    }

    pub async fn cancel_all_orders(&self) -> Result<()> {
        self.http
            .delete(format!("{}/orders", self.base_url))
            .send()
            .await?
            .error_for_status()?;

        info!("All orders cancelled");
        Ok(())
    }

    pub async fn close_all_positions(&self) -> Result<()> {
        self.http
            .delete(format!("{}/positions", self.base_url))
            .send()
            .await?
            .error_for_status()?;

        info!("All positions closed");
        Ok(())
    }
}

fn convert_position(p: AlpacaPosition) -> Position {
    let qty = Decimal::from_str(&p.qty).unwrap_or(Decimal::ZERO);
    let side = if &p.side == "long" {
        PositionSide::Long
    } else {
        PositionSide::Short
    };

    Position {
        symbol: Symbol::new(&p.symbol),
        quantity: qty,
        avg_entry_price: Decimal::from_str(&p.avg_entry_price).unwrap_or(Decimal::ZERO),
        current_price: Decimal::from_str(&p.current_price).unwrap_or(Decimal::ZERO),
        market_value: Decimal::from_str(&p.market_value).unwrap_or(Decimal::ZERO),
        unrealized_pnl: Decimal::from_str(&p.unrealized_pl).unwrap_or(Decimal::ZERO),
        realized_pnl: Decimal::ZERO,
        side,
    }
}

fn convert_order_response(resp: AlpacaOrderResponse, original: &Order) -> Order {
    let status = match resp.status.as_str() {
        "new" | "accepted" => OrderStatus::Submitted,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Pending,
    };

    Order {
        id: resp.id,
        symbol: original.symbol.clone(),
        side: original.side,
        order_type: original.order_type,
        quantity: original.quantity,
        limit_price: original.limit_price,
        stop_price: original.stop_price,
        time_in_force: original.time_in_force,
        status,
        filled_qty: resp
            .filled_qty
            .and_then(|q| Decimal::from_str(&q).ok())
            .unwrap_or(Decimal::ZERO),
        filled_avg_price: resp
            .filled_avg_price
            .and_then(|p| Decimal::from_str(&p).ok()),
        strategy_id: original.strategy_id.clone(),
        created_at: original.created_at,
        updated_at: Utc::now(),
    }
}
