use crate::*;
use pos_foundation::{AccountId, ProjectId, RunId, WorkspaceId};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

const CREDENTIAL_SCOPE_COUNT_MAX: usize = 16;
const CREDENTIAL_LEASE_COUNT_MAX: u16 = 1_024;
const SYNC_FRAME_BATCH_COUNT_MAX: usize = 256;
const SYNC_FRAME_PROJECT_COUNT_MAX: u16 = 4_096;
const REALTIME_MESSAGE_PROJECT_COUNT_MAX: u16 = 1_024;
const WORKER_LEASE_COUNT_MAX: u16 = 64;
const PACK_DISCOVERY_COUNT_MAX: u16 = 256;
const USAGE_RECORD_COUNT_MAX: u32 = 65_536;
const CONNECTOR_TICK_ITEM_COUNT_MAX: u16 = 32;

#[derive(Clone, Copy)]
pub struct LocalControlPlane {
    owner_account_id: AccountId,
    workspace_id: WorkspaceId,
}

impl LocalControlPlane {
    #[must_use]
    pub const fn new(owner_account_id: AccountId, workspace_id: WorkspaceId) -> Self {
        Self {
            owner_account_id,
            workspace_id,
        }
    }

    fn execute_now(&self, request: ControlPlaneRequest) -> ControlPlaneResponse {
        match request {
            ControlPlaneRequest::Authorize {
                account_id,
                workspace_id,
                permission: _,
            } => {
                let workspace_matches = workspace_id == self.workspace_id;
                let account_matches = account_id == self.owner_account_id;
                let reason = if !workspace_matches {
                    AuthorizationReason::WorkspaceMismatch
                } else if !account_matches {
                    AuthorizationReason::AccountIsNotLocalOwner
                } else {
                    AuthorizationReason::LocalOwner
                };
                ControlPlaneResponse::Authorization {
                    allowed: workspace_matches && account_matches,
                    reason,
                }
            }
            ControlPlaneRequest::Entitlements { workspace_id } => {
                ControlPlaneResponse::Entitlements {
                    all_granted: workspace_id == self.workspace_id,
                }
            }
        }
    }
}

impl CapabilityProvider for LocalControlPlane {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::ControlPlane, "LocalControlPlane")
    }
}

impl ControlPlane for LocalControlPlane {
    fn execute(
        &self,
        request: ControlPlaneRequest,
    ) -> ProviderFuture<'_, Result<ControlPlaneResponse, CapabilityError>> {
        Box::pin(async move { Ok(self.execute_now(request)) })
    }
}

#[derive(Clone)]
struct CredentialLease {
    run_id: RunId,
    secret_ref: SecretRef,
    scopes: Vec<CredentialScope>,
    expires_at_ms: u64,
}

pub struct KeychainBroker {
    leases: Mutex<BTreeMap<CredentialLeaseId, CredentialLease>>,
}

impl Default for KeychainBroker {
    fn default() -> Self {
        Self {
            leases: Mutex::new(BTreeMap::new()),
        }
    }
}

impl KeychainBroker {
    fn execute_now(
        &self,
        request: CredentialBrokerRequest,
    ) -> Result<CredentialBrokerResponse, CapabilityError> {
        match request {
            CredentialBrokerRequest::Issue {
                lease_id,
                run_id,
                secret_ref,
                scopes,
                expires_at_ms,
            } => self.issue(lease_id, run_id, secret_ref, scopes, expires_at_ms),
            CredentialBrokerRequest::Renew {
                lease_id,
                expires_at_ms,
            } => self.renew(lease_id, expires_at_ms),
            CredentialBrokerRequest::Revoke { lease_id } => self.revoke(lease_id),
        }
    }

    fn issue(
        &self,
        lease_id: CredentialLeaseId,
        run_id: RunId,
        secret_ref: SecretRef,
        scopes: Vec<CredentialScope>,
        expires_at_ms: u64,
    ) -> Result<CredentialBrokerResponse, CapabilityError> {
        if scopes.is_empty() || scopes.len() > CREDENTIAL_SCOPE_COUNT_MAX {
            return Err(CapabilityError::InvalidRequest {
                field: "scopes",
                reason: "scope count must be between 1 and 16",
            });
        }
        let mut leases = self
            .leases
            .lock()
            .expect("credential lease state is not poisoned"); // INVARIANT: a poisoned broker cannot safely release credentials.
        if leases.contains_key(&lease_id) {
            return Err(CapabilityError::Conflict {
                resource: "credential lease",
            });
        }
        if leases.len() >= usize::from(CREDENTIAL_LEASE_COUNT_MAX) {
            return Err(CapabilityError::ResourceExhausted {
                resource: "credential leases",
                limit: u32::from(CREDENTIAL_LEASE_COUNT_MAX),
            });
        }
        leases.insert(
            lease_id,
            CredentialLease {
                run_id,
                secret_ref,
                scopes,
                expires_at_ms,
            },
        );
        Ok(CredentialBrokerResponse::Issued {
            lease_id,
            expires_at_ms,
        })
    }

    fn renew(
        &self,
        lease_id: CredentialLeaseId,
        expires_at_ms: u64,
    ) -> Result<CredentialBrokerResponse, CapabilityError> {
        let mut leases = self
            .leases
            .lock()
            .expect("credential lease state is not poisoned"); // INVARIANT: a poisoned broker cannot safely release credentials.
        let Some(lease) = leases.get_mut(&lease_id) else {
            return Err(CapabilityError::NotFound {
                resource: "credential lease",
            });
        };
        lease.expires_at_ms = expires_at_ms;
        Ok(CredentialBrokerResponse::Renewed {
            lease_id,
            expires_at_ms,
        })
    }

    fn revoke(
        &self,
        lease_id: CredentialLeaseId,
    ) -> Result<CredentialBrokerResponse, CapabilityError> {
        let mut leases = self
            .leases
            .lock()
            .expect("credential lease state is not poisoned"); // INVARIANT: a poisoned broker cannot safely release credentials.
        let Some(lease) = leases.remove(&lease_id) else {
            return Err(CapabilityError::NotFound {
                resource: "credential lease",
            });
        };
        let _ = (lease.run_id, lease.secret_ref, lease.scopes);
        Ok(CredentialBrokerResponse::Revoked { lease_id })
    }
}

impl CapabilityProvider for KeychainBroker {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::IdentityBroker, "KeychainBroker")
    }
}

impl CredentialBroker for KeychainBroker {
    fn execute(
        &self,
        request: CredentialBrokerRequest,
    ) -> ProviderFuture<'_, Result<CredentialBrokerResponse, CapabilityError>> {
        Box::pin(async move { self.execute_now(request) })
    }
}

#[derive(Default)]
pub struct DirectSync {
    frames: Mutex<BTreeMap<ProjectId, Vec<SyncFrame>>>,
}

impl DirectSync {
    fn push(
        &self,
        project_id: ProjectId,
        frames: Vec<SyncFrame>,
    ) -> Result<SyncTransportResponse, CapabilityError> {
        if frames.is_empty() || frames.len() > SYNC_FRAME_BATCH_COUNT_MAX {
            return Err(CapabilityError::InvalidRequest {
                field: "frames",
                reason: "frame count must be between 1 and 256",
            });
        }
        let mut projects = self
            .frames
            .lock()
            .expect("direct-sync state is not poisoned"); // INVARIANT: poisoned sync state may violate sequence contiguity.
        let existing = projects.entry(project_id).or_default();
        if existing.len() + frames.len() > usize::from(SYNC_FRAME_PROJECT_COUNT_MAX) {
            return Err(CapabilityError::ResourceExhausted {
                resource: "direct-sync frames per project",
                limit: u32::from(SYNC_FRAME_PROJECT_COUNT_MAX),
            });
        }
        let expected_first = existing.last().map_or(1, |frame| frame.seq + 1);
        if frames[0].seq != expected_first
            || frames.windows(2).any(|pair| pair[1].seq != pair[0].seq + 1)
        {
            return Err(CapabilityError::Conflict {
                resource: "sync sequence",
            });
        }
        let watermark = frames.last().map_or(expected_first - 1, |frame| frame.seq);
        existing.extend(frames);
        Ok(SyncTransportResponse::Pushed { watermark })
    }

    fn pull(
        &self,
        project_id: ProjectId,
        after_seq: u64,
        limit: u16,
    ) -> Result<SyncTransportResponse, CapabilityError> {
        if limit == 0 || usize::from(limit) > SYNC_FRAME_BATCH_COUNT_MAX {
            return Err(CapabilityError::InvalidRequest {
                field: "limit",
                reason: "pull limit must be between 1 and 256",
            });
        }
        let projects = self
            .frames
            .lock()
            .expect("direct-sync state is not poisoned"); // INVARIANT: poisoned sync state may violate sequence contiguity.
        let frames = projects.get(&project_id).map_or_else(Vec::new, |existing| {
            existing
                .iter()
                .filter(|frame| frame.seq > after_seq)
                .take(usize::from(limit))
                .cloned()
                .collect()
        });
        let watermark = projects
            .get(&project_id)
            .and_then(|existing| existing.last())
            .map_or(0, |frame| frame.seq);
        Ok(SyncTransportResponse::Pulled { frames, watermark })
    }
}

impl CapabilityProvider for DirectSync {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::SyncTransport, "DirectSync")
    }
}

impl SyncTransport for DirectSync {
    fn execute(
        &self,
        request: SyncTransportRequest,
    ) -> ProviderFuture<'_, Result<SyncTransportResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                SyncTransportRequest::Push { project_id, frames } => self.push(project_id, frames),
                SyncTransportRequest::Pull {
                    project_id,
                    after_seq,
                    limit,
                } => self.pull(project_id, after_seq, limit),
            }
        })
    }
}

#[derive(Default)]
pub struct LocalBus {
    messages: Mutex<BTreeMap<ProjectId, Vec<RealtimeMessage>>>,
}

impl LocalBus {
    fn publish(
        &self,
        project_id: ProjectId,
        payload: CapabilityPayload,
    ) -> Result<RealtimeBusResponse, CapabilityError> {
        let mut projects = self
            .messages
            .lock()
            .expect("local-bus state is not poisoned"); // INVARIANT: poisoned cursor state can duplicate notifications.
        let messages = projects.entry(project_id).or_default();
        if messages.len() >= usize::from(REALTIME_MESSAGE_PROJECT_COUNT_MAX) {
            return Err(CapabilityError::ResourceExhausted {
                resource: "realtime messages per project",
                limit: u32::from(REALTIME_MESSAGE_PROJECT_COUNT_MAX),
            });
        }
        let cursor = messages.last().map_or(1, |message| message.cursor + 1);
        messages.push(RealtimeMessage { cursor, payload });
        Ok(RealtimeBusResponse::Published { cursor })
    }

    fn read(
        &self,
        project_id: ProjectId,
        after_cursor: u64,
        limit: u16,
    ) -> Result<RealtimeBusResponse, CapabilityError> {
        if limit == 0 || limit > REALTIME_MESSAGE_PROJECT_COUNT_MAX {
            return Err(CapabilityError::InvalidRequest {
                field: "limit",
                reason: "read limit must be between 1 and 1024",
            });
        }
        let projects = self
            .messages
            .lock()
            .expect("local-bus state is not poisoned"); // INVARIANT: poisoned cursor state can duplicate notifications.
        let messages = projects.get(&project_id).map_or_else(Vec::new, |existing| {
            existing
                .iter()
                .filter(|message| message.cursor > after_cursor)
                .take(usize::from(limit))
                .cloned()
                .collect()
        });
        Ok(RealtimeBusResponse::Messages { messages })
    }
}

impl CapabilityProvider for LocalBus {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::RealtimeBus, "LocalBus")
    }
}

impl RealtimeBus for LocalBus {
    fn execute(
        &self,
        request: RealtimeBusRequest,
    ) -> ProviderFuture<'_, Result<RealtimeBusResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                RealtimeBusRequest::Publish {
                    project_id,
                    payload,
                } => self.publish(project_id, payload),
                RealtimeBusRequest::Read {
                    project_id,
                    after_cursor,
                    limit,
                } => self.read(project_id, after_cursor, limit),
            }
        })
    }
}

#[derive(Clone, Copy)]
struct WorkerLease {
    run_id: RunId,
    resource_class: WorkerResourceClass,
    expires_at_ms: u64,
}

#[derive(Default)]
pub struct LocalPool {
    leases: Mutex<BTreeMap<WorkerLeaseId, WorkerLease>>,
}

impl LocalPool {
    fn execute_now(
        &self,
        request: WorkerFleetRequest,
    ) -> Result<WorkerFleetResponse, CapabilityError> {
        let mut leases = self
            .leases
            .lock()
            .expect("local-pool state is not poisoned"); // INVARIANT: poisoned lease state can duplicate execution.
        match request {
            WorkerFleetRequest::Acquire {
                lease_id,
                run_id,
                resource_class,
                expires_at_ms,
            } => {
                if leases.contains_key(&lease_id) {
                    return Err(CapabilityError::Conflict {
                        resource: "worker lease",
                    });
                }
                if leases.len() >= usize::from(WORKER_LEASE_COUNT_MAX) {
                    return Err(CapabilityError::ResourceExhausted {
                        resource: "local worker leases",
                        limit: u32::from(WORKER_LEASE_COUNT_MAX),
                    });
                }
                leases.insert(
                    lease_id,
                    WorkerLease {
                        run_id,
                        resource_class,
                        expires_at_ms,
                    },
                );
                Ok(WorkerFleetResponse::Acquired { lease_id })
            }
            WorkerFleetRequest::Release { lease_id } => {
                let Some(lease) = leases.remove(&lease_id) else {
                    return Err(CapabilityError::NotFound {
                        resource: "worker lease",
                    });
                };
                let _ = (lease.run_id, lease.resource_class, lease.expires_at_ms);
                Ok(WorkerFleetResponse::Released { lease_id })
            }
        }
    }
}

impl CapabilityProvider for LocalPool {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::WorkerFleet, "LocalPool")
    }
}

impl WorkerFleet for LocalPool {
    fn execute(
        &self,
        request: WorkerFleetRequest,
    ) -> ProviderFuture<'_, Result<WorkerFleetResponse, CapabilityError>> {
        Box::pin(async move { self.execute_now(request) })
    }
}

pub struct FilePackSource {
    root: PathBuf,
}

impl FilePackSource {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn discover(&self, limit: u16) -> Result<PackSourceResponse, CapabilityError> {
        if limit == 0 || limit > PACK_DISCOVERY_COUNT_MAX {
            return Err(CapabilityError::InvalidRequest {
                field: "limit",
                reason: "pack discovery limit must be between 1 and 256",
            });
        }
        if !self.root.exists() {
            return Ok(PackSourceResponse::Packs { ids: Vec::new() });
        }
        let entries = fs::read_dir(&self.root).map_err(|_| CapabilityError::Io {
            operation: "read local pack directory",
        })?;
        let mut ids = BTreeSet::new();
        for entry in entries.take(usize::from(limit)) {
            let entry = entry.map_err(|_| CapabilityError::Io {
                operation: "read local pack entry",
            })?;
            if !entry
                .file_type()
                .map_err(|_| CapabilityError::Io {
                    operation: "inspect local pack entry",
                })?
                .is_dir()
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Ok(id) = PackId::new(name) {
                ids.insert(id);
            }
        }
        Ok(PackSourceResponse::Packs {
            ids: ids.into_iter().collect(),
        })
    }
}

impl CapabilityProvider for FilePackSource {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::PackSource, "FilePackSource")
    }
}

impl PackSource for FilePackSource {
    fn execute(
        &self,
        request: PackSourceRequest,
    ) -> ProviderFuture<'_, Result<PackSourceResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                PackSourceRequest::Discover { limit } => self.discover(limit),
            }
        })
    }
}

#[derive(Clone)]
pub struct LocalRenderer {
    ffmpeg_available: bool,
}

impl LocalRenderer {
    #[must_use]
    pub const fn new(ffmpeg_available: bool) -> Self {
        Self { ffmpeg_available }
    }
}

impl CapabilityProvider for LocalRenderer {
    fn descriptor(&self) -> CapabilityDescriptor {
        if self.ffmpeg_available {
            local_descriptor(CapabilityId::MediaRender, "LocalRenderer")
        } else {
            unavailable_descriptor(
                CapabilityId::MediaRender,
                "LocalRenderer",
                "ffmpeg is not installed; install it to render media locally",
            )
        }
    }
}

impl MediaRenderer for LocalRenderer {
    fn execute(
        &self,
        request: MediaRendererRequest,
    ) -> ProviderFuture<'_, Result<MediaRendererResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                MediaRendererRequest::Probe => Ok(MediaRendererResponse::Probe {
                    ffmpeg_available: self.ffmpeg_available,
                }),
            }
        })
    }
}

#[derive(Default)]
pub struct NoopMeter {
    records: Mutex<BTreeMap<UsageRecordId, u64>>,
}

impl NoopMeter {
    fn execute_now(
        &self,
        request: BillingMeterRequest,
    ) -> Result<BillingMeterResponse, CapabilityError> {
        let mut records = self
            .records
            .lock()
            .expect("local meter state is not poisoned"); // INVARIANT: poisoned usage state could double-count cost.
        match request {
            BillingMeterRequest::Record(record) => {
                if records.contains_key(&record.id) {
                    return Err(CapabilityError::Conflict {
                        resource: "usage record",
                    });
                }
                let record_count =
                    u32::try_from(records.len()).expect("bounded usage record count fits u32"); // INVARIANT: admission caps records at USAGE_RECORD_COUNT_MAX.
                if record_count >= USAGE_RECORD_COUNT_MAX {
                    return Err(CapabilityError::ResourceExhausted {
                        resource: "local usage records",
                        limit: USAGE_RECORD_COUNT_MAX,
                    });
                }
                records.insert(record.id, record.units);
                Ok(BillingMeterResponse::Recorded { id: record.id })
            }
            BillingMeterRequest::Total => {
                let units = records.values().try_fold(0_u64, |total, units| {
                    total
                        .checked_add(*units)
                        .ok_or(CapabilityError::ResourceExhausted {
                            resource: "local usage total",
                            limit: u32::MAX,
                        })
                })?;
                Ok(BillingMeterResponse::Total {
                    records: u32::try_from(records.len())
                        .expect("bounded usage record count fits u32"), // INVARIANT: admission caps records at USAGE_RECORD_COUNT_MAX.
                    units,
                })
            }
        }
    }
}

impl CapabilityProvider for NoopMeter {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::BillingMeter, "NoopMeter")
    }
}

impl BillingMeter for NoopMeter {
    fn execute(
        &self,
        request: BillingMeterRequest,
    ) -> ProviderFuture<'_, Result<BillingMeterResponse, CapabilityError>> {
        Box::pin(async move { self.execute_now(request) })
    }
}

#[derive(Clone)]
pub struct LocalIngress {
    deployment_reachable: bool,
}

impl LocalIngress {
    #[must_use]
    pub const fn new(deployment_reachable: bool) -> Self {
        Self {
            deployment_reachable,
        }
    }
}

impl CapabilityProvider for LocalIngress {
    fn descriptor(&self) -> CapabilityDescriptor {
        if self.deployment_reachable {
            local_descriptor(CapabilityId::RelayIngress, "LocalIngress")
        } else {
            unavailable_descriptor(
                CapabilityId::RelayIngress,
                "LocalIngress",
                "this deployment has no public ingress; configure a reachable endpoint or polling",
            )
        }
    }
}

impl IngressRelay for LocalIngress {
    fn execute(
        &self,
        request: IngressRelayRequest,
    ) -> ProviderFuture<'_, Result<IngressRelayResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                IngressRelayRequest::Probe => Ok(IngressRelayResponse::Probe {
                    deployment_reachable: self.deployment_reachable,
                }),
            }
        })
    }
}

#[derive(Clone, Copy, Default)]
pub struct LocalConnectorHost;

impl CapabilityProvider for LocalConnectorHost {
    fn descriptor(&self) -> CapabilityDescriptor {
        local_descriptor(CapabilityId::ConnectorHost, "LocalConnectorHost")
    }
}

impl ConnectorHost for LocalConnectorHost {
    fn execute(
        &self,
        request: ConnectorHostRequest,
    ) -> ProviderFuture<'_, Result<ConnectorHostResponse, CapabilityError>> {
        Box::pin(async move {
            match request {
                ConnectorHostRequest::Tick {
                    connector_id,
                    cursor,
                    max_items,
                } => {
                    if connector_id.as_str() != "mock" {
                        return Err(CapabilityError::NotFound {
                            resource: "connector",
                        });
                    }
                    if max_items == 0 || max_items > CONNECTOR_TICK_ITEM_COUNT_MAX {
                        return Err(CapabilityError::InvalidRequest {
                            field: "max_items",
                            reason: "connector tick limit must be between 1 and 32",
                        });
                    }
                    let polled_count = max_items.min(3);
                    let next_cursor = cursor.checked_add(u64::from(polled_count)).ok_or(
                        CapabilityError::ResourceExhausted {
                            resource: "connector cursor",
                            limit: u32::MAX,
                        },
                    )?;
                    Ok(ConnectorHostResponse::Tick {
                        polled_count,
                        next_cursor,
                        host_available: true,
                    })
                }
            }
        })
    }
}

fn local_descriptor(id: CapabilityId, provider_name: &'static str) -> CapabilityDescriptor {
    CapabilityDescriptor {
        id,
        provider_name,
        mode: CapabilityMode::Local,
    }
}

fn unavailable_descriptor(
    id: CapabilityId,
    provider_name: &'static str,
    reason: &'static str,
) -> CapabilityDescriptor {
    let reason = UnavailableReason::new(reason).expect("static unavailable reason is valid"); // INVARIANT: compile-time provider reasons are non-empty and bounded.
    CapabilityDescriptor {
        id,
        provider_name,
        mode: CapabilityMode::Unavailable(reason),
    }
}
