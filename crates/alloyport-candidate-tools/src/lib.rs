//! Candidate submission, immutable materialization, and independent Source Gate tools.

mod gateway;
mod materialization;

pub use gateway::{
    CandidateToolConfig, CandidateToolGateway, REQUEST_SOURCE_GATE_TOOL,
    SUBMIT_CANDIDATE_BUNDLE_TOOL,
};
pub use materialization::{CandidateMaterialization, CandidateMaterializationError};

#[cfg(test)]
mod tests;
