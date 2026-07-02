use algoquant_core::events::Event;
use algoquant_core::types::Tick;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Receives raw ticks from all feeds and publishes normalized events.
pub struct Normalizer {
    tick_rx: mpsc::Receiver<Tick>,
    event_tx: mpsc::Sender<Event>,
}

impl Normalizer {
    pub fn new(tick_rx: mpsc::Receiver<Tick>, event_tx: mpsc::Sender<Event>) -> Self {
        Self { tick_rx, event_tx }
    }

    pub async fn run(mut self) {
        info!("Normalizer started");
        let mut count: u64 = 0;

        while let Some(tick) = self.tick_rx.recv().await {
            let event = Event::Tick(tick);
            if self.event_tx.send(event).await.is_err() {
                warn!("Event channel closed, normalizer shutting down");
                break;
            }
            count += 1;
            if count % 1000 == 0 {
                info!("Normalizer processed {count} ticks");
            }
        }

        info!("Normalizer finished after {count} ticks");
    }
}
