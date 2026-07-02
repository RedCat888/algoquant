# AlgoQuant

Cross-asset algorithmic trading engine built in Rust.

## Architecture

- **Rust core engine** — real-time data ingestion, signal computation, risk management, order execution
- **Python ML sidecar** (Phase 3) — sentiment analysis, regime detection, alternative data processing
- **Event-driven pipeline** — every market tick, news article, and signal flows as a typed event

## First Strategy: Crypto Vol Leads Equity

BTC realized volatility spikes often precede equity VIX moves by 2-6 hours. Crypto markets trade 24/7 and react faster to global risk events. When crypto vol z-score exceeds the configured threshold, the strategy reduces equity exposure as a hedge.

## Quick Start

### Prerequisites

- Rust 1.75+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Docker & Docker Compose (for NATS, Redis, QuestDB)

### Setup

```bash
# Clone and enter the project
cd algoquant

# Copy env template and fill in your API credentials
cp .env.example .env
# Edit .env with your Alpaca API key and secret

# Start infrastructure services
docker compose up -d

# Build and run
cargo run --release --bin algoquant
```

### Configuration

All parameters are in `config/default.toml` and can be overridden by environment variables.
The config file is hot-reloadable — change parameters while the engine is running.

Key settings:
- `alpaca.symbols` — equity symbols to stream and trade
- `crypto.symbols` — crypto pairs to monitor (Binance format)
- `signals.cross_asset.z_score_threshold` — sensitivity of the vol-spike detector
- `risk.max_drawdown_pct` — circuit breaker threshold
- `strategies.crypto_vol_leads_equity.vol_spike_threshold` — z-score to trigger hedging

## Project Structure

```
crates/
├── core/        — Shared types (Tick, Order, Signal), config, event definitions
├── ingestion/   — WebSocket clients for Alpaca (equities) and Binance (crypto)
├── signals/     — Signal computation: cross-asset vol analyzer, technical indicators
├── strategy/    — Strategy framework + crypto-vol-leads-equity implementation
├── risk/        — Position sizing, exposure limits, max drawdown circuit breaker
├── execution/   — Alpaca REST client for order submission and position management
├── storage/     — Time-series persistence (QuestDB) and state cache (Redis)
├── backtest/    — Event-driven backtester (replays through same signal/strategy code)
└── engine/      — Main binary that wires all components together
```

## Data Pipeline

```
Alpaca WS ─┐
            ├─→ Normalizer ─→ Signal Engine ─→ Strategy ─→ Risk Manager ─→ Execution ─→ Alpaca API
Binance WS ─┘                      ↑
                              ML Sidecar (Python, Phase 3)
```

## Status

In active development (Phase 1). Latest known-good validation: `cargo check` passes, with two warnings in `crates/signals/src/technical.rs` about unused VWAP-related values.

## Roadmap

- **Phase 1** (current) — Core pipeline, Alpaca + Binance ingestion, cross-asset vol signal, paper trading
- **Phase 2** — Cross-asset correlation matrix, commodity signals, QuestDB storage
- **Phase 3** — Python ML sidecar (FinBERT sentiment, HMM regime detection)
- **Phase 4** — Options flow analysis, order book microstructure signals
- **Phase 5** — Event-driven backtester, advanced risk (VaR, Kelly criterion)
- **Phase 6** — Alternative data: vessel tracking, satellite imagery, patent NLP
