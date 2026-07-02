use algoquant_core::types::{Order, OrderStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Tracks the lifecycle of all orders.
pub struct OrderManager {
    orders: Arc<RwLock<HashMap<String, Order>>>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn track(&self, order: Order) {
        let id = order.id.clone();
        info!(order_id = %id, status = ?order.status, "Tracking order");
        self.orders.write().await.insert(id, order);
    }

    pub async fn update(&self, order: Order) {
        let id = order.id.clone();
        info!(order_id = %id, status = ?order.status, "Order update");
        self.orders.write().await.insert(id, order);
    }

    pub async fn get(&self, order_id: &str) -> Option<Order> {
        self.orders.read().await.get(order_id).cloned()
    }

    pub async fn open_orders(&self) -> Vec<Order> {
        self.orders
            .read()
            .await
            .values()
            .filter(|o| matches!(o.status, OrderStatus::Pending | OrderStatus::Submitted | OrderStatus::PartiallyFilled))
            .cloned()
            .collect()
    }

    pub async fn open_order_count(&self) -> usize {
        self.orders
            .read()
            .await
            .values()
            .filter(|o| matches!(o.status, OrderStatus::Pending | OrderStatus::Submitted | OrderStatus::PartiallyFilled))
            .count()
    }
}
