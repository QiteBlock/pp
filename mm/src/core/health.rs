use std::collections::{HashMap, VecDeque};

use tokio::time::{Duration, Instant};

#[derive(Default)]
pub struct HealthTracker {
    ws_discrepancies: VecDeque<Instant>,
    rest_failures: VecDeque<Instant>,
    /// Consecutive quoting cycles that produced zero orders while a position is held.
    consecutive_empty_cycles: usize,
    /// Per-symbol count of cycles where the position exceeded the emergency threshold
    /// but the unwind side produced zero orders.
    pub empty_unwind_cycles: HashMap<String, usize>,
}

impl HealthTracker {
    pub fn record_ws_discrepancy(&mut self) {
        self.ws_discrepancies.push_back(Instant::now());
        self.prune();
    }

    pub fn is_degraded(&self) -> bool {
        self.ws_discrepancies.len() >= 3 || self.rest_failures.len() >= 2
    }

    pub fn ws_discrepancy_count(&self) -> usize {
        self.ws_discrepancies.len()
    }

    pub fn rest_failure_count(&self) -> usize {
        self.rest_failures.len()
    }

    pub fn update_empty_cycles(
        &mut self,
        orders_generated: usize,
        has_position: bool,
        threshold: usize,
    ) -> bool {
        if orders_generated == 0 && has_position {
            self.consecutive_empty_cycles += 1;
            self.consecutive_empty_cycles == threshold
        } else {
            self.consecutive_empty_cycles = 0;
            false
        }
    }

    pub fn update_unwind_cycles(
        &mut self,
        symbol: &str,
        unwind_orders: usize,
        over_threshold: bool,
        emergency_cycles: usize,
    ) -> bool {
        if unwind_orders == 0 && over_threshold {
            let count = self
                .empty_unwind_cycles
                .entry(symbol.to_string())
                .or_insert(0);
            *count += 1;
            *count >= emergency_cycles
        } else {
            self.empty_unwind_cycles.remove(symbol);
            false
        }
    }

    fn prune(&mut self) {
        let horizon = Duration::from_secs(120);
        while self
            .ws_discrepancies
            .front()
            .map(|instant| instant.elapsed() > horizon)
            .unwrap_or(false)
        {
            self.ws_discrepancies.pop_front();
        }
        while self
            .rest_failures
            .front()
            .map(|instant| instant.elapsed() > horizon)
            .unwrap_or(false)
        {
            self.rest_failures.pop_front();
        }
    }
}
