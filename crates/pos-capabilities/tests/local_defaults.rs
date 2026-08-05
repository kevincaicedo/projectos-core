//! Public-build contract tests for every local capability provider.

#![forbid(unsafe_code)]

use pos_capabilities::{
    BillingMeterRequest, BillingMeterResponse, CapabilityError, CapabilityId, CapabilityMode,
    CapabilityPayload, CapabilityRegistry, ConnectorHostRequest, ConnectorHostResponse,
    ControlPermission, ControlPlaneRequest, ControlPlaneResponse, CredentialBrokerRequest,
    CredentialBrokerResponse, CredentialLeaseId, CredentialScope, IngressRelayRequest,
    IngressRelayResponse, LocalCapabilityConfig, MediaRendererRequest, MediaRendererResponse,
    PackSourceRequest, PackSourceResponse, ProviderFuture, RealtimeBusRequest, RealtimeBusResponse,
    SecretRef, SyncFrame, SyncTransportRequest, SyncTransportResponse, UsageRecord, UsageRecordId,
    WorkerFleetRequest, WorkerFleetResponse, WorkerLeaseId, WorkerResourceClass,
};
use pos_foundation::{AccountId, ProjectId, RunId, WorkspaceId};
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

const READY_POLL_COUNT_MAX: u8 = 4;

fn registry() -> CapabilityRegistry {
    CapabilityRegistry::local(LocalCapabilityConfig {
        owner_account_id: AccountId::from_bytes([1; 16]),
        workspace_id: WorkspaceId::from_bytes([2; 16]),
        pack_root: PathBuf::from("path-that-does-not-exist-in-the-test-checkout"),
        ffmpeg_available: false,
        ingress_reachable: false,
    })
}

fn block_on_ready<T>(future: ProviderFuture<'_, T>) -> T {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..READY_POLL_COUNT_MAX {
        if let Poll::Ready(output) = Future::poll(future.as_mut(), &mut context) {
            return output;
        }
    }
    panic!("local provider fixture did not complete within its bounded ready-poll budget");
}

#[test]
fn local_registry_installs_all_ten_sockets_with_honest_states() {
    let registry = registry();
    let descriptors = registry.descriptors();
    assert_eq!(descriptors.len(), CapabilityId::COUNT);
    for (expected, descriptor) in CapabilityId::ALL.into_iter().zip(descriptors) {
        assert_eq!(descriptor.id, expected);
        assert!(!descriptor.provider_name.is_empty());
    }
    assert!(matches!(
        registry.media_renderer().descriptor().mode,
        CapabilityMode::Unavailable(_)
    ));
    assert!(matches!(
        registry.ingress_relay().descriptor().mode,
        CapabilityMode::Unavailable(_)
    ));
}

#[test]
fn control_plane_is_single_owner_and_single_workspace() {
    let registry = registry();
    let response = block_on_ready(registry.control_plane().execute(
        ControlPlaneRequest::Authorize {
            account_id: AccountId::from_bytes([1; 16]),
            workspace_id: WorkspaceId::from_bytes([2; 16]),
            permission: ControlPermission::ManageWorkspace,
        },
    ))
    .expect("local authorization is operational");
    assert!(matches!(
        response,
        ControlPlaneResponse::Authorization { allowed: true, .. }
    ));
}

#[test]
fn keychain_broker_issues_and_revokes_reference_only_lease() {
    let registry = registry();
    let lease_id = CredentialLeaseId::from_bytes([3; 16]);
    let response = block_on_ready(registry.credential_broker().execute(
        CredentialBrokerRequest::Issue {
            lease_id,
            run_id: RunId::from_bytes([4; 16]),
            secret_ref:
                SecretRef::new("keychain:model/openai").expect("fixture reference is valid"),
            scopes: vec![CredentialScope::ModelProvider],
            expires_at_ms: 10_000,
        },
    ))
    .expect("local reference lease is operational");
    assert!(matches!(
        response,
        CredentialBrokerResponse::Issued { lease_id: id, .. } if id == lease_id
    ));
    let revoked = block_on_ready(
        registry
            .credential_broker()
            .execute(CredentialBrokerRequest::Revoke { lease_id }),
    )
    .expect("local reference lease can be revoked");
    assert_eq!(revoked, CredentialBrokerResponse::Revoked { lease_id });
}

#[test]
fn direct_sync_round_trips_contiguous_bounded_frames() {
    let registry = registry();
    let project_id = ProjectId::from_bytes([5; 16]);
    let frame = SyncFrame {
        seq: 1,
        payload: CapabilityPayload::new(b"frame".to_vec()).expect("fixture is bounded"),
    };
    let pushed = block_on_ready(
        registry
            .sync_transport()
            .execute(SyncTransportRequest::Push {
                project_id,
                frames: vec![frame.clone()],
            }),
    )
    .expect("direct sync push works");
    assert_eq!(pushed, SyncTransportResponse::Pushed { watermark: 1 });
    let pulled = block_on_ready(
        registry
            .sync_transport()
            .execute(SyncTransportRequest::Pull {
                project_id,
                after_seq: 0,
                limit: 1,
            }),
    )
    .expect("direct sync pull works");
    assert_eq!(
        pulled,
        SyncTransportResponse::Pulled {
            frames: vec![frame],
            watermark: 1,
        }
    );
}

#[test]
fn realtime_bus_publishes_and_reads_in_cursor_order() {
    let registry = registry();
    let project_id = ProjectId::from_bytes([6; 16]);
    let published = block_on_ready(
        registry
            .realtime_bus()
            .execute(RealtimeBusRequest::Publish {
                project_id,
                payload: CapabilityPayload::new(b"message".to_vec()).expect("fixture is bounded"),
            }),
    )
    .expect("local publish works");
    assert_eq!(published, RealtimeBusResponse::Published { cursor: 1 });
    let read = block_on_ready(registry.realtime_bus().execute(RealtimeBusRequest::Read {
        project_id,
        after_cursor: 0,
        limit: 1,
    }))
    .expect("local read works");
    assert!(matches!(
        read,
        RealtimeBusResponse::Messages { messages } if messages.len() == 1 && messages[0].cursor == 1
    ));
}

#[test]
fn local_pool_owns_a_bounded_lease_lifecycle() {
    let registry = registry();
    let lease_id = WorkerLeaseId::from_bytes([7; 16]);
    let acquired = block_on_ready(
        registry
            .worker_fleet()
            .execute(WorkerFleetRequest::Acquire {
                lease_id,
                run_id: RunId::from_bytes([8; 16]),
                resource_class: WorkerResourceClass::Interactive,
                expires_at_ms: 20_000,
            }),
    )
    .expect("local lease works");
    assert_eq!(acquired, WorkerFleetResponse::Acquired { lease_id });
    let released = block_on_ready(
        registry
            .worker_fleet()
            .execute(WorkerFleetRequest::Release { lease_id }),
    )
    .expect("local release works");
    assert_eq!(released, WorkerFleetResponse::Released { lease_id });
}

#[test]
fn local_pack_media_meter_and_ingress_defaults_report_real_state() {
    let registry = registry();
    let packs = block_on_ready(
        registry
            .pack_source()
            .execute(PackSourceRequest::Discover { limit: 10 }),
    )
    .expect("missing pack directory is an honest empty state");
    assert_eq!(packs, PackSourceResponse::Packs { ids: Vec::new() });

    let media = block_on_ready(
        registry
            .media_renderer()
            .execute(MediaRendererRequest::Probe),
    )
    .expect("media probe works");
    assert_eq!(
        media,
        MediaRendererResponse::Probe {
            ffmpeg_available: false,
        }
    );

    let usage_id = UsageRecordId::from_bytes([9; 16]);
    let recorded = block_on_ready(
        registry
            .billing_meter()
            .execute(BillingMeterRequest::Record(UsageRecord {
                id: usage_id,
                units: 12,
            })),
    )
    .expect("local usage row works");
    assert_eq!(recorded, BillingMeterResponse::Recorded { id: usage_id });

    let ingress = block_on_ready(registry.ingress_relay().execute(IngressRelayRequest::Probe))
        .expect("local ingress probe works");
    assert_eq!(
        ingress,
        IngressRelayResponse::Probe {
            deployment_reachable: false,
        }
    );
}

#[test]
fn connector_host_runs_one_bounded_mock_tick_and_refuses_oversize() {
    let registry = registry();
    let connector_id = pos_capabilities::ConnectorId::new("mock").expect("fixture id is valid");
    let response = block_on_ready(
        registry
            .connector_host()
            .execute(ConnectorHostRequest::Tick {
                connector_id: connector_id.clone(),
                cursor: 7,
                max_items: 8,
            }),
    )
    .expect("bounded mock tick works");
    assert_eq!(
        response,
        ConnectorHostResponse::Tick {
            polled_count: 3,
            next_cursor: 10,
            host_available: true,
        }
    );

    let error = block_on_ready(
        registry
            .connector_host()
            .execute(ConnectorHostRequest::Tick {
                connector_id,
                cursor: 0,
                max_items: 33,
            }),
    )
    .expect_err("oversize tick must refuse, never truncate");
    assert!(matches!(
        error,
        CapabilityError::InvalidRequest {
            field: "max_items",
            ..
        }
    ));
}
