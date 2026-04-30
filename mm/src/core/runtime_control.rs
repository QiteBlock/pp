use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct RuntimeControl {
    paused: AtomicBool,
}

impl RuntimeControl {
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
}
