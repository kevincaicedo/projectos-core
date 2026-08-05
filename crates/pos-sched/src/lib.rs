//! # pos-sched
//!
//! First-class scheduler (F36): SQLite job queue (idempotency keys, priorities, backoff, DLQ, fairness), tz-aware cron with overlap policies, weighted worker classes, capacity windows (F68 later).
//!
//! Skeleton created by m0-s01; filled by m0-s14. Charter: master plan §19.

#![forbid(unsafe_code)]
