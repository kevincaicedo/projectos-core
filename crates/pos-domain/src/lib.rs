//! # pos-domain
//!
//! Entities, events, projections, why-chain integrity (L1, L2). The typed EventKind vocabulary and the projection apply paths live here.
//!
//! Filled by m0-s03 (v0 kinds). Charter: master plan §19.
//!
//! Everything here is pure over the envelope: bodies are versioned CBOR
//! (`events`), projections return typed row writes that `pos-log`'s apply
//! chokepoint executes (`projections`), and the deterministic synthetic
//! generator (`synthetic`) feeds tests and `pos-bench` the same reproducible
//! corpora.

#![forbid(unsafe_code)]

pub mod events;
pub mod projections;
pub mod synthetic;

pub use events::{
    AccountAuditedBody, DomainDecodeError, DomainEvent, JobCompletedBody, JobEnqueuedBody,
    ModelCallCompletedBody, ProjectCreatedBody, ProjectRenamedBody, RunFinishedBody, RunOutcome,
    RunStartedBody, RunStepCommittedBody,
};
pub use projections::v0_registry;
pub use synthetic::SyntheticEvents;
