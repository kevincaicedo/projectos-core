use crate::{
    CapabilityError, CapabilityPayload, CapabilityProvider, ConnectorId, CredentialLeaseId, PackId,
    ProviderFuture, SecretRef, UsageRecordId, WorkerLeaseId,
};
use pos_foundation::{AccountId, ProjectId, RunId, WorkspaceId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlPermission {
    ReadProject,
    MutateProject,
    ManageWorkspace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlPlaneRequest {
    Authorize {
        account_id: AccountId,
        workspace_id: WorkspaceId,
        permission: ControlPermission,
    },
    Entitlements {
        workspace_id: WorkspaceId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthorizationReason {
    LocalOwner,
    AccountIsNotLocalOwner,
    WorkspaceMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlPlaneResponse {
    Authorization {
        allowed: bool,
        reason: AuthorizationReason,
    },
    Entitlements {
        all_granted: bool,
    },
}

pub trait ControlPlane: CapabilityProvider {
    fn execute(
        &self,
        request: ControlPlaneRequest,
    ) -> ProviderFuture<'_, Result<ControlPlaneResponse, CapabilityError>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialScope {
    ModelProvider,
    Connector,
    Execution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialBrokerRequest {
    Issue {
        lease_id: CredentialLeaseId,
        run_id: RunId,
        secret_ref: SecretRef,
        scopes: Vec<CredentialScope>,
        expires_at_ms: u64,
    },
    Renew {
        lease_id: CredentialLeaseId,
        expires_at_ms: u64,
    },
    Revoke {
        lease_id: CredentialLeaseId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialBrokerResponse {
    Issued {
        lease_id: CredentialLeaseId,
        expires_at_ms: u64,
    },
    Renewed {
        lease_id: CredentialLeaseId,
        expires_at_ms: u64,
    },
    Revoked {
        lease_id: CredentialLeaseId,
    },
}

pub trait CredentialBroker: CapabilityProvider {
    fn execute(
        &self,
        request: CredentialBrokerRequest,
    ) -> ProviderFuture<'_, Result<CredentialBrokerResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncFrame {
    pub seq: u64,
    pub payload: CapabilityPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SyncTransportRequest {
    Push {
        project_id: ProjectId,
        frames: Vec<SyncFrame>,
    },
    Pull {
        project_id: ProjectId,
        after_seq: u64,
        limit: u16,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SyncTransportResponse {
    Pushed {
        watermark: u64,
    },
    Pulled {
        frames: Vec<SyncFrame>,
        watermark: u64,
    },
}

pub trait SyncTransport: CapabilityProvider {
    fn execute(
        &self,
        request: SyncTransportRequest,
    ) -> ProviderFuture<'_, Result<SyncTransportResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeMessage {
    pub cursor: u64,
    pub payload: CapabilityPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RealtimeBusRequest {
    Publish {
        project_id: ProjectId,
        payload: CapabilityPayload,
    },
    Read {
        project_id: ProjectId,
        after_cursor: u64,
        limit: u16,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RealtimeBusResponse {
    Published { cursor: u64 },
    Messages { messages: Vec<RealtimeMessage> },
}

pub trait RealtimeBus: CapabilityProvider {
    fn execute(
        &self,
        request: RealtimeBusRequest,
    ) -> ProviderFuture<'_, Result<RealtimeBusResponse, CapabilityError>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerResourceClass {
    Interactive,
    Background,
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerFleetRequest {
    Acquire {
        lease_id: WorkerLeaseId,
        run_id: RunId,
        resource_class: WorkerResourceClass,
        expires_at_ms: u64,
    },
    Release {
        lease_id: WorkerLeaseId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerFleetResponse {
    Acquired { lease_id: WorkerLeaseId },
    Released { lease_id: WorkerLeaseId },
}

pub trait WorkerFleet: CapabilityProvider {
    fn execute(
        &self,
        request: WorkerFleetRequest,
    ) -> ProviderFuture<'_, Result<WorkerFleetResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PackSourceRequest {
    Discover { limit: u16 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PackSourceResponse {
    Packs { ids: Vec<PackId> },
}

pub trait PackSource: CapabilityProvider {
    fn execute(
        &self,
        request: PackSourceRequest,
    ) -> ProviderFuture<'_, Result<PackSourceResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MediaRendererRequest {
    Probe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MediaRendererResponse {
    Probe { ffmpeg_available: bool },
}

pub trait MediaRenderer: CapabilityProvider {
    fn execute(
        &self,
        request: MediaRendererRequest,
    ) -> ProviderFuture<'_, Result<MediaRendererResponse, CapabilityError>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UsageRecord {
    pub id: UsageRecordId,
    pub units: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BillingMeterRequest {
    Record(UsageRecord),
    Total,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BillingMeterResponse {
    Recorded { id: UsageRecordId },
    Total { records: u32, units: u64 },
}

pub trait BillingMeter: CapabilityProvider {
    fn execute(
        &self,
        request: BillingMeterRequest,
    ) -> ProviderFuture<'_, Result<BillingMeterResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IngressRelayRequest {
    Probe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IngressRelayResponse {
    Probe { deployment_reachable: bool },
}

pub trait IngressRelay: CapabilityProvider {
    fn execute(
        &self,
        request: IngressRelayRequest,
    ) -> ProviderFuture<'_, Result<IngressRelayResponse, CapabilityError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorHostRequest {
    Tick {
        connector_id: ConnectorId,
        cursor: u64,
        max_items: u16,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorHostResponse {
    Tick {
        polled_count: u16,
        next_cursor: u64,
        host_available: bool,
    },
}

pub trait ConnectorHost: CapabilityProvider {
    fn execute(
        &self,
        request: ConnectorHostRequest,
    ) -> ProviderFuture<'_, Result<ConnectorHostResponse, CapabilityError>>;
}
