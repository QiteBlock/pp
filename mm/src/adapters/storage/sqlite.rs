use std::{collections::HashMap, fs, path::Path, str::FromStr, sync::Mutex};

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;

use crate::{
    config::StorageConfig,
    core::state::BotState,
    domain::{Fill, Position},
};

pub struct FillStore {
    connection: Mutex<Connection>,
}

impl FillStore {
    pub fn from_config(config: Option<&StorageConfig>) -> Result<Option<Self>> {
        let Some(config) = config else {
            return Ok(None);
        };
        if !config.enabled {
            return Ok(None);
        }

        let db_path = Path::new(&config.db_path);
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create storage directory {}", parent.display())
                })?;
            }
        }

        let connection = Connection::open(db_path)
            .with_context(|| format!("failed to open fill store at {}", db_path.display()))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price TEXT NOT NULL,
                quantity TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                position_quantity_after TEXT NOT NULL,
                entry_price_after TEXT NOT NULL,
                realized_pnl_after TEXT NOT NULL,
                unrealized_pnl_after TEXT NOT NULL,
                total_pnl_after TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS global_pnl_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                realized_pnl TEXT NOT NULL,
                unrealized_pnl TEXT NOT NULL,
                total_pnl TEXT NOT NULL,
                account_equity TEXT NOT NULL,
                fill_symbol TEXT,
                fill_side TEXT
            );",
        )?;

        Ok(Some(Self {
            connection: Mutex::new(connection),
        }))
    }

    pub fn insert_fill(&self, fill: &Fill, state: &BotState, is_simulated: bool) -> Result<()> {
        let position = state.positions.get(&fill.symbol);
        let position_quantity_after = position
            .map(|position| position.quantity.to_string())
            .unwrap_or_else(|| "0".to_string());
        let entry_price_after = position
            .map(|position| position.entry_price.to_string())
            .unwrap_or_else(|| "0".to_string());
        let realized_pnl_after = position
            .map(|position| position.realized_pnl.to_string())
            .unwrap_or_else(|| "0".to_string());
        let unrealized_pnl_after = position
            .map(|position| position.unrealized_pnl.to_string())
            .unwrap_or_else(|| "0".to_string());

        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO fills (
                ts, symbol, side, price, quantity, is_simulated,
                position_quantity_after, entry_price_after,
                realized_pnl_after, unrealized_pnl_after, total_pnl_after
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                fill.timestamp.to_rfc3339(),
                fill.symbol,
                format!("{:?}", fill.side),
                fill.price.to_string(),
                fill.quantity.to_string(),
                if is_simulated { 1 } else { 0 },
                position_quantity_after,
                entry_price_after,
                realized_pnl_after,
                unrealized_pnl_after,
                state.effective_total_pnl().to_string(),
            ],
        )?;
        self.insert_global_pnl_snapshot_locked(
            &connection,
            fill.timestamp.to_rfc3339(),
            state,
            is_simulated,
            Some(fill.symbol.as_str()),
            Some(format!("{:?}", fill.side)),
        )?;
        Ok(())
    }

    pub fn insert_global_pnl_snapshot(
        &self,
        ts: String,
        state: &BotState,
        is_simulated: bool,
        fill_symbol: Option<&str>,
        fill_side: Option<String>,
    ) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        self.insert_global_pnl_snapshot_locked(
            &connection,
            ts,
            state,
            is_simulated,
            fill_symbol,
            fill_side,
        )
    }

    fn insert_global_pnl_snapshot_locked(
        &self,
        connection: &Connection,
        ts: String,
        state: &BotState,
        is_simulated: bool,
        fill_symbol: Option<&str>,
        fill_side: Option<String>,
    ) -> Result<()> {
        let (realized_pnl, unrealized_pnl) = aggregate_pnl(state);
        connection.execute(
            "INSERT INTO global_pnl_snapshots (
                ts, is_simulated, realized_pnl, unrealized_pnl, total_pnl,
                account_equity, fill_symbol, fill_side
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                ts,
                if is_simulated { 1 } else { 0 },
                realized_pnl.to_string(),
                unrealized_pnl.to_string(),
                state.effective_total_pnl().to_string(),
                state.account_equity.to_string(),
                fill_symbol,
                fill_side,
            ],
        )?;
        Ok(())
    }

    pub fn load_latest_positions(&self, is_simulated: bool) -> Result<Vec<Position>> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT
                symbol,
                position_quantity_after,
                entry_price_after,
                realized_pnl_after
             FROM fills
             WHERE is_simulated = ?1
             ORDER BY id ASC",
        )?;
        let rows = statement.query_map(params![if is_simulated { 1 } else { 0 }], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut latest_by_symbol: HashMap<String, Position> = HashMap::new();
        for row in rows {
            let (symbol, quantity, entry_price, realized_pnl) = row?;
            latest_by_symbol.insert(
                symbol.clone(),
                Position {
                    symbol,
                    quantity: Decimal::from_str(&quantity)?,
                    entry_price: Decimal::from_str(&entry_price)?,
                    realized_pnl: Decimal::from_str(&realized_pnl)?,
                    unrealized_pnl: Decimal::ZERO,
                    pnl_is_authoritative: false,
                    opened_at: None,
                },
            );
        }

        Ok(latest_by_symbol
            .into_values()
            .filter(|position| !position.quantity.is_zero() || !position.realized_pnl.is_zero())
            .collect())
    }
}

fn aggregate_pnl(state: &BotState) -> (Decimal, Decimal) {
    let realized: Decimal = state
        .positions
        .values()
        .map(|position| position.realized_pnl)
        .sum();
    let unrealized: Decimal = state
        .positions
        .values()
        .map(|position| position.unrealized_pnl)
        .sum();
    (realized, unrealized)
}
