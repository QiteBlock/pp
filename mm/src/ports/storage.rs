use anyhow::Result;

use crate::domain::Position;

pub trait FillStoragePort: Send + Sync {
    fn load_latest_positions(&self, dry_run: bool) -> Result<Vec<Position>>;
}
