//! # pos-agents
//!
//! The provider-neutral native agent harness (F49): runs, ledger-before-effect steps (L7), hard budgets (L8), tool/runtime registries with capability gates (L5) and taint plumbing (L6), roster charters, pack loader.
//!
//! Skeleton created by m0-s01; filled by m0-s12/s13. Charter: master plan §19.

#![forbid(unsafe_code)]

mod budget;
mod echo;
mod harness;
mod roster;
mod runtime;
mod tools;

pub use budget::{
    BudgetConfigError, BudgetExceeded, RUN_RETRY_COUNT_MAX, RUN_STEP_COUNT_MAX,
    RUN_TOOL_CALL_COUNT_MAX,
};
pub use echo::{
    ECHO_AGENT_NAME, ECHO_MODEL_TOOL_ID, EchoAgent, EchoError, EchoFaultPlan, EchoFaultPoint,
    echo_charter, echo_marker, echo_tool_grants, echo_tool_registry,
};
pub use harness::{
    ArtifactReport, HarnessError, RUN_LINEAGE_DEPTH_MAX, RunHarness, RunStartSpec, StepPlan,
    StepPreparation, ToolEffectReport, ValidationReport,
};
pub use roster::{RosterCharter, RosterRegistry};
pub use runtime::{
    RUNTIME_REGISTRY_COUNT_MAX, RuntimeAuthState, RuntimeControlCapabilities, RuntimeDescriptor,
    RuntimeHealth, RuntimeId, RuntimeRegistry, RuntimeRegistryError,
};
pub use tools::{
    AuthorizationContext, AuthorizationError, AuthorizedToolCall, AutonomyLevel, CapabilityScope,
    GateReceipt, RUN_TOOL_GRANT_COUNT_MAX, RunToolGrants, TOOL_INPUT_BYTES_MAX,
    TOOL_REGISTRY_COUNT_MAX, ToolCallRequest, ToolDescriptor, ToolEffectClass, ToolGrantMode,
    ToolId, ToolPolicyMode, ToolRegistry, ToolRegistryError,
};
