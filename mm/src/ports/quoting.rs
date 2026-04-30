use anyhow::Result;

use crate::{
    config::{AppConfig, PairConfig, ParsedConfig},
    core::analytics::tracker::FillTracker,
    domain::{FactorSnapshot, QuoteIntent},
};

pub trait QuoteGeneratorPort: Send + Sync {
    fn generate(
        &self,
        model: &AppConfig,
        parsed: &ParsedConfig,
        pair: &PairConfig,
        factors: &FactorSnapshot,
        fill_tracker: &FillTracker,
    ) -> Result<Vec<QuoteIntent>>;
}
