// Copyright 2025 Mach5 Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Token bucket rate limiter for controlling per-worker throughput.
///
/// Supports dynamic rate updates (for phase transitions) and tracks
/// missed operations when the target rate exceeds capacity.
pub struct TokenBucketRateLimiter {
    /// Tokens per second.
    rate: f64,
    /// Current token count.
    tokens: f64,
    /// Maximum burst size (tokens).
    max_tokens: f64,
    /// Last refill time.
    last_refill: Instant,
    /// Operations that couldn't be performed due to rate limiting.
    missed_ops: u64,
}

impl TokenBucketRateLimiter {
    /// Create a new rate limiter with the given rate (ops/sec).
    pub fn new(rate: f64) -> Self {
        let max_tokens = rate.max(1.0); // Allow burst up to 1 second of tokens
        Self {
            rate,
            tokens: max_tokens, // Start full
            max_tokens,
            last_refill: Instant::now(),
            missed_ops: 0,
        }
    }

    /// Update the rate target (e.g., on phase transition).
    pub fn set_rate(&mut self, new_rate: f64) {
        self.rate = new_rate;
        self.max_tokens = new_rate.max(1.0);
        // Don't reset tokens — carry over remaining tokens.
    }

    /// Current rate in ops/sec.
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Wait until a token is available, then consume it.
    /// Returns the number of missed ops since last check and resets the counter.
    pub async fn acquire(&mut self) -> u64 {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            let missed = self.missed_ops;
            self.missed_ops = 0;
            return missed;
        }

        // Calculate wait time for next token.
        if self.rate <= 0.0 {
            // Rate is zero — sleep a long time (phase with no activity).
            sleep(Duration::from_secs(1)).await;
            self.missed_ops += 1;
            let missed = self.missed_ops;
            self.missed_ops = 0;
            return missed;
        }

        let wait_secs = (1.0 - self.tokens) / self.rate;
        sleep(Duration::from_secs_f64(wait_secs)).await;
        self.refill();
        self.tokens -= 1.0;

        let missed = self.missed_ops;
        self.missed_ops = 0;
        missed
    }

    /// Try to acquire a batch of N tokens. Returns the number actually acquired
    /// (may be less than N if not enough tokens).
    pub fn try_acquire_batch(&mut self, n: u64) -> u64 {
        self.refill();
        let available = self.tokens.floor() as u64;
        let acquired = available.min(n);
        self.tokens -= acquired as f64;
        acquired
    }

    /// Reset missed ops counter and return the current value.
    pub fn take_missed_ops(&mut self) -> u64 {
        let missed = self.missed_ops;
        self.missed_ops = 0;
        missed
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.max_tokens);
    }
}
