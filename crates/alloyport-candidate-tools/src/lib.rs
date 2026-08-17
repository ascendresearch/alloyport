//! Candidate submission, immutable materialization, and independent Source Gate tools.

mod build_tool;
mod correctness_tool;
mod gate_result;
mod gateway;
mod materialization;
mod record;
mod record_git;
mod record_stream;
mod reference;

pub use build_tool::{
    CandidateBuildToolConfig, CandidateBuildToolConfigError, READ_BUILD_DIAGNOSTICS_TOOL,
    REQUEST_ASCEND_BUILD_TOOL,
};
pub use correctness_tool::{CandidateCorrectnessToolConfig, REQUEST_REDUCTION_CORRECTNESS_TOOL};
pub use gateway::{
    CandidateToolConfig, CandidateToolGateway, REQUEST_SOURCE_GATE_TOOL,
    SUBMIT_CANDIDATE_BUNDLE_TOOL,
};
pub use materialization::{CandidateMaterialization, CandidateMaterializationError};
pub use record::{CandidateRecordError, collect_candidate_record};
pub use record_git::{CandidateRecord, RecordedCommit, write_candidate_record};
pub use record_stream::{
    RECORD_BRANCH, RecordStreamError, RecordedCandidate, RecordedFile, RecordedGate,
    RecordedGateOutcome, candidate_ref, fast_import_stream,
};
pub use reference::{READ_REFERENCE_TOOL, ReferenceCorpus, ReferenceStatus};

#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
