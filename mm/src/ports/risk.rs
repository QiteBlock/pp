use anyhow::Result;

use crate::{
    config::{AppConfig, ParsedConfig},
    core::state::BotState,
};

pub trait RiskCheckPort: Send + Sync {
    fn validate(&self, config: &AppConfig, parsed: &ParsedConfig, state: &BotState) -> Result<()>;
}
