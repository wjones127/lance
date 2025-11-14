use std::sync::atomic::{AtomicU64, Ordering};

/// Global allocation statistics tracked using atomic operations for thread safety
pub struct AllocationStats {
    pub total_allocations: AtomicU64,
    pub total_deallocations: AtomicU64,
    pub total_bytes_allocated: AtomicU64,
    pub total_bytes_deallocated: AtomicU64,
    pub current_bytes: AtomicU64,
    pub peak_bytes: AtomicU64,
}

impl AllocationStats {
    pub const fn new() -> Self {
        Self {
            total_allocations: AtomicU64::new(0),
            total_deallocations: AtomicU64::new(0),
            total_bytes_allocated: AtomicU64::new(0),
            total_bytes_deallocated: AtomicU64::new(0),
            current_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
        }
    }

    pub fn record_allocation(&self, size: usize) {
        self.total_allocations.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_allocated
            .fetch_add(size as u64, Ordering::Relaxed);

        let current = self.current_bytes.fetch_add(size as u64, Ordering::Relaxed) + size as u64;

        // Update peak if necessary
        let mut peak = self.peak_bytes.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_bytes.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    pub fn record_deallocation(&self, size: usize) {
        self.total_deallocations.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_deallocated
            .fetch_add(size as u64, Ordering::Relaxed);
        self.current_bytes.fetch_sub(size as u64, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.total_allocations.store(0, Ordering::Relaxed);
        self.total_deallocations.store(0, Ordering::Relaxed);
        self.total_bytes_allocated.store(0, Ordering::Relaxed);
        self.total_bytes_deallocated.store(0, Ordering::Relaxed);
        self.current_bytes.store(0, Ordering::Relaxed);
        self.peak_bytes.store(0, Ordering::Relaxed);
    }

    pub fn get_snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_allocations: self.total_allocations.load(Ordering::Relaxed),
            total_deallocations: self.total_deallocations.load(Ordering::Relaxed),
            total_bytes_allocated: self.total_bytes_allocated.load(Ordering::Relaxed),
            total_bytes_deallocated: self.total_bytes_deallocated.load(Ordering::Relaxed),
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
            peak_bytes: self.peak_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub total_allocations: u64,
    pub total_deallocations: u64,
    pub total_bytes_allocated: u64,
    pub total_bytes_deallocated: u64,
    pub current_bytes: u64,
    pub peak_bytes: u64,
}

/// Global statistics instance
pub static STATS: AllocationStats = AllocationStats::new();
