use std::time::Duration;

use anyhow::{bail, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    adapters::exchange::{AnyExchangeClient, OrderExecutor, PrivateDataSource},
    domain::Position,
    ports::notifier::NotifierPort,
};

#[async_trait]
pub trait CleanupExchange: OrderExecutor + PrivateDataSource + Sync {
    async fn active_cleanup_positions(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<Vec<Position>> {
        Ok(default_active_positions(positions, min_close_notional))
    }

    async fn submit_limit_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()>;
    async fn submit_market_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()>;
    fn supports_limit_cleanup(&self) -> bool;
}

pub async fn flatten_account_state(
    exchange: &(impl CleanupExchange + Sync),
    notifier: &(impl NotifierPort + Sync),
    phase: &str,
    min_close_notional: Decimal,
) -> Result<()> {
    exchange.cancel_all_orders().await?;

    if exchange.supports_limit_cleanup() {
        close_positions_with_limit_orders(exchange, notifier, phase, min_close_notional).await
    } else {
        Ok(())
    }
}

async fn close_positions_with_limit_orders(
    exchange: &(impl CleanupExchange + Sync),
    notifier: &(impl NotifierPort + Sync),
    phase: &str,
    min_close_notional: Decimal,
) -> Result<()> {
    // Each outer attempt re-prices the close order to the current passive best price.
    // Between re-pricings we poll every 2 s for up to 15 s to give the maker order
    // time to fill before we cancel and reprice.
    const REPRICE_ATTEMPTS: usize = 10;
    const POLL_INTERVAL_MS: u64 = 2_000;
    const POLLS_PER_ATTEMPT: usize = 8; // 8 x 2 s = 16 s wait per reprice round

    for attempt in 1..=REPRICE_ATTEMPTS {
        let positions = exchange.fetch_positions().await?;
        let open_positions = exchange
            .active_cleanup_positions(&positions, min_close_notional)
            .await?;
        if open_positions.is_empty() {
            return Ok(());
        }

        exchange
            .submit_limit_close_orders(&open_positions, min_close_notional)
            .await?;

        // Poll until filled or timeout, then reprice.
        for poll in 1..=POLLS_PER_ATTEMPT {
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            let positions = exchange.fetch_positions().await?;
            let remaining = exchange
                .active_cleanup_positions(&positions, min_close_notional)
                .await?;
            if remaining.is_empty() {
                return Ok(());
            }
        }

        exchange.cancel_all_orders().await?;
        sleep(Duration::from_millis(300)).await;
    }

    let positions = exchange.fetch_positions().await?;
    let residual_positions = exchange
        .active_cleanup_positions(&positions, min_close_notional)
        .await?;
    if residual_positions.is_empty() {
        return Ok(());
    }

    let summary = residual_positions
        .iter()
        .map(|position| format!("{} {}", position.symbol, position.quantity))
        .collect::<Vec<_>>()
        .join(", ");
    warn!(phase, residual = %summary, "limit-close exhausted retries; closing by market order");
    notifier
        .notify(format!(
            "cleanup: limit orders failed after {REPRICE_ATTEMPTS} attempts; closing by market ({phase}): {summary}"
        ))
        .await;

    exchange.cancel_all_orders().await?;
    exchange
        .submit_market_close_orders(&residual_positions, min_close_notional)
        .await?;

    sleep(Duration::from_secs(2)).await;
    let positions = exchange.fetch_positions().await?;
    let still_open = exchange
        .active_cleanup_positions(&positions, min_close_notional)
        .await?;
    if still_open.is_empty() {
        return Ok(());
    }

    let still_summary = still_open
        .iter()
        .map(|position| format!("{} {}", position.symbol, position.quantity))
        .collect::<Vec<_>>()
        .join(", ");
    warn!(phase, residual = %still_summary, "market-close cleanup still has open positions");
    notifier
        .notify(format!(
            "cleanup: market close also failed ({phase}): {still_summary}"
        ))
        .await;
    bail!("cleanup could not close positions even with market orders: {still_summary}")
}

fn default_active_positions(positions: &[Position], min_close_notional: Decimal) -> Vec<Position> {
    positions
        .iter()
        .filter(|position| position.quantity != Decimal::ZERO)
        .filter(|position| {
            let reference_price = position.entry_price.abs();
            reference_price.is_zero()
                || position.quantity.abs() * reference_price >= min_close_notional
        })
        .cloned()
        .collect()
}

#[async_trait]
impl CleanupExchange for AnyExchangeClient {
    async fn active_cleanup_positions(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<Vec<Position>> {
        match self {
            AnyExchangeClient::Grvt(client) => {
                client
                    .active_cleanup_positions(positions, min_close_notional)
                    .await
            }
            AnyExchangeClient::Hibachi(_) | AnyExchangeClient::Extended(_) => {
                Ok(default_active_positions(positions, min_close_notional))
            }
        }
    }

    async fn submit_limit_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()> {
        match self {
            AnyExchangeClient::Grvt(client) => {
                client
                    .submit_limit_close_orders(positions, min_close_notional)
                    .await
            }
            AnyExchangeClient::Hibachi(_) => Ok(()),
            AnyExchangeClient::Extended(_) => Ok(()),
        }
    }

    async fn submit_market_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()> {
        match self {
            AnyExchangeClient::Grvt(client) => {
                client
                    .submit_market_close_orders(positions, min_close_notional)
                    .await
            }
            AnyExchangeClient::Hibachi(_) => Ok(()),
            AnyExchangeClient::Extended(_) => Ok(()),
        }
    }

    fn supports_limit_cleanup(&self) -> bool {
        matches!(self, AnyExchangeClient::Grvt(_))
    }
}
