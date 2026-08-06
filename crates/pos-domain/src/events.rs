//! The v0 `EventKind` vocabulary (m0-s03): past-tense facts with versioned
//! CBOR bodies. Old events are eternal — a field is never removed or
//! re-typed; evolution adds a `V2` variant beside `V1` and the decoder
//! matches both (event-sourcing skill). Unknown kinds decode to `None` so a
//! newer project opens under an older build instead of failing closed.

use pos_foundation::{AccountId, JobId, ProjectId, RunId};
use pos_log::{Actor, AppendRequest, EntityRef, KindTag, LogError};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A typed decode failure: the kind is known but the body does not parse —
/// real corruption or a forward-versioned body, named per seq by callers.
#[derive(Debug)]
pub struct DomainDecodeError {
    pub kind: &'static str,
    pub reason: String,
}

impl fmt::Display for DomainDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} body did not decode: {}",
            self.kind, self.reason
        )
    }
}

impl std::error::Error for DomainDecodeError {}

/// How a Run ended. Stored as text in projections; the words are UI copy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunOutcome {
    Completed,
    Failed,
    Canceled,
}

impl RunOutcome {
    #[must_use]
    pub const fn as_status_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

// Body enums are externally tagged (`{"V1": {...}}`): the variant name IS
// the version tag STYLE requires, and new versions are new variants.

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProjectCreatedBody {
    V1 {
        project_id: ProjectId,
        name: String,
        template: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProjectRenamedBody {
    V1 { project_id: ProjectId, name: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStartedBody {
    V1 {
        run_id: RunId,
        worker: String,
        trigger: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStepCommittedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        summary: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunFinishedBody {
    V1 {
        run_id: RunId,
        outcome: RunOutcome,
        steps_total: u32,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobEnqueuedBody {
    V1 { job_id: JobId, job_kind: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobCompletedBody {
    V1 { job_id: JobId, attempts: u32 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AccountAuditedBody {
    V1 {
        account_id: AccountId,
        action: String,
        target: String,
    },
}

/// The typed v0 vocabulary. The set grows additively; a tag, once shipped,
/// never changes meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomainEvent {
    ProjectCreated(ProjectCreatedBody),
    ProjectRenamed(ProjectRenamedBody),
    RunStarted(RunStartedBody),
    RunStepCommitted(RunStepCommittedBody),
    RunFinished(RunFinishedBody),
    JobEnqueued(JobEnqueuedBody),
    JobCompleted(JobCompletedBody),
    AccountAudited(AccountAuditedBody),
}

impl DomainEvent {
    #[must_use]
    pub const fn kind_tag(&self) -> &'static str {
        match self {
            Self::ProjectCreated(_) => "ProjectCreated",
            Self::ProjectRenamed(_) => "ProjectRenamed",
            Self::RunStarted(_) => "RunStarted",
            Self::RunStepCommitted(_) => "RunStepCommitted",
            Self::RunFinished(_) => "RunFinished",
            Self::JobEnqueued(_) => "JobEnqueued",
            Self::JobCompleted(_) => "JobCompleted",
            Self::AccountAudited(_) => "AccountAudited",
        }
    }

    /// The L2 refs this event creates/touches — every entity id in the body,
    /// under its fixed domain noun. An event with missing refs orphans
    /// artifacts (event-sourcing skill), so refs derive from the body rather
    /// than trusting each call site.
    #[must_use]
    pub fn refs(&self) -> Vec<EntityRef> {
        match self {
            Self::ProjectCreated(ProjectCreatedBody::V1 { project_id, .. })
            | Self::ProjectRenamed(ProjectRenamedBody::V1 { project_id, .. }) => {
                vec![entity_ref("project", project_id.into_bytes())]
            }
            Self::RunStarted(RunStartedBody::V1 { run_id, .. })
            | Self::RunStepCommitted(RunStepCommittedBody::V1 { run_id, .. })
            | Self::RunFinished(RunFinishedBody::V1 { run_id, .. }) => {
                vec![entity_ref("run", run_id.into_bytes())]
            }
            Self::JobEnqueued(JobEnqueuedBody::V1 { job_id, .. })
            | Self::JobCompleted(JobCompletedBody::V1 { job_id, .. }) => {
                vec![entity_ref("job", job_id.into_bytes())]
            }
            Self::AccountAudited(AccountAuditedBody::V1 { account_id, .. }) => {
                vec![entity_ref("account", account_id.into_bytes())]
            }
        }
    }

    /// Versioned CBOR encoding of the body (the §7.1 `body` column).
    #[must_use]
    pub fn encode_body(&self) -> Vec<u8> {
        let mut body = Vec::new();
        let encoded = match self {
            Self::ProjectCreated(inner) => ciborium::into_writer(inner, &mut body),
            Self::ProjectRenamed(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunStarted(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunStepCommitted(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunFinished(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobEnqueued(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobCompleted(inner) => ciborium::into_writer(inner, &mut body),
            Self::AccountAudited(inner) => ciborium::into_writer(inner, &mut body),
        };
        encoded.expect("CBOR encoding of typed bodies into a Vec cannot fail"); // INVARIANT: bodies contain only owned serde-friendly values and the writer is a Vec.
        body
    }

    /// Decodes a stored event. `Ok(None)` for a kind this build does not
    /// know — forward compatibility, never data loss (the raw event is still
    /// in the log).
    pub fn decode(kind: &KindTag, body: &[u8]) -> Result<Option<Self>, DomainDecodeError> {
        fn read<T: for<'de> Deserialize<'de>>(
            kind: &'static str,
            body: &[u8],
        ) -> Result<T, DomainDecodeError> {
            ciborium::from_reader(body).map_err(|error| DomainDecodeError {
                kind,
                reason: error.to_string(),
            })
        }
        let decoded = match kind.as_str() {
            "ProjectCreated" => Self::ProjectCreated(read("ProjectCreated", body)?),
            "ProjectRenamed" => Self::ProjectRenamed(read("ProjectRenamed", body)?),
            "RunStarted" => Self::RunStarted(read("RunStarted", body)?),
            "RunStepCommitted" => Self::RunStepCommitted(read("RunStepCommitted", body)?),
            "RunFinished" => Self::RunFinished(read("RunFinished", body)?),
            "JobEnqueued" => Self::JobEnqueued(read("JobEnqueued", body)?),
            "JobCompleted" => Self::JobCompleted(read("JobCompleted", body)?),
            "AccountAudited" => Self::AccountAudited(read("AccountAudited", body)?),
            _ => return Ok(None),
        };
        Ok(Some(decoded))
    }

    /// Builds the append request for this fact. The actor is the caller's to
    /// supply — it is never defaulted (event-sourcing skill).
    pub fn into_request(
        self,
        device: pos_foundation::DeviceId,
        actor: Actor,
    ) -> Result<AppendRequest, LogError> {
        Ok(AppendRequest {
            device,
            actor,
            kind: KindTag::new(self.kind_tag())?,
            body: self.encode_body(),
            refs: self.refs(),
        })
    }
}

fn entity_ref(entity: &str, id: [u8; 16]) -> EntityRef {
    EntityRef {
        entity: entity.to_owned(),
        id,
    }
}

#[cfg(test)]
mod tests {
    use super::{DomainEvent, ProjectCreatedBody, RunFinishedBody, RunOutcome};
    use pos_foundation::{ProjectId, RunId};
    use pos_log::KindTag;

    #[test]
    fn bodies_round_trip_and_unknown_kinds_are_none() {
        let event = DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
            project_id: ProjectId::from_bytes([1; 16]),
            name: "Acme Widgets".to_owned(),
            template: "generic".to_owned(),
        });
        let body = event.encode_body();
        let decoded = DomainEvent::decode(&KindTag::new("ProjectCreated").expect("valid"), &body)
            .expect("body decodes");
        assert_eq!(decoded, Some(event));

        let unknown =
            DomainEvent::decode(&KindTag::new("HoloDeckCalibrated").expect("valid"), &body)
                .expect("unknown kinds are not errors");
        assert_eq!(unknown, None);
    }

    #[test]
    fn malformed_bodies_are_typed_errors_naming_the_kind() {
        let error = DomainEvent::decode(
            &KindTag::new("RunFinished").expect("valid"),
            &[0xff, 0x00, 0x01],
        )
        .expect_err("garbage must not decode");
        assert_eq!(error.kind, "RunFinished");
    }

    #[test]
    fn refs_carry_every_entity_the_event_touches() {
        let event = DomainEvent::RunFinished(RunFinishedBody::V1 {
            run_id: RunId::from_bytes([9; 16]),
            outcome: RunOutcome::Canceled,
            steps_total: 4,
        });
        let refs = event.refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].entity, "run");
        assert_eq!(refs[0].id, [9; 16]);
    }
}
