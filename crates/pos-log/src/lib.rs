//! # pos-log
//!
//! The L1 substrate: append-only event log, projections applied in the same transaction, snapshots + tail replay, time-travel reads reserved. Append is the only write.
//!
//! Skeleton created by m0-s01; filled by m0-s03. Charter: master plan §19.

#![forbid(unsafe_code)]
