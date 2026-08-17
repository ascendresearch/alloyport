//! Turning a defect the model can read into a correction turn instead of a dead migration.
//!
//! Split out of `gateway.rs` for the module-size limit. Every rule here is one a paid run already
//! paid for: a citation that resolves to nothing, an instrument that could end a run, and arguments
//! that stopped mid-string because a response ran out of room.

use super::submit::{SUBMIT_CANDIDATE_BUNDLE_ARGUMENT_CONTRACT, SubmitCandidateBundleArguments};
use super::{
    CandidateToolGateway, REQUEST_SOURCE_GATE_TOOL, SOURCE_GATE_ARGUMENT_CONTRACT,
    SUBMIT_CANDIDATE_BUNDLE_TOOL, SourceGateArguments, adapter_error, ingest_bytes,
};
use crate::build_tool::{READ_BUILD_DIAGNOSTICS_TOOL, REQUEST_ASCEND_BUILD_TOOL};
use crate::correctness_tool::REQUEST_REDUCTION_CORRECTNESS_TOOL;
use crate::reference::READ_REFERENCE_TOOL;
use alloyport_core::{
    GatewayToolCall, ToolGatewayError, ToolGatewayOutcome, ToolInvocation, ToolOperationStatus,
};

impl CandidateToolGateway {
    /// Checks that what a decodable call names actually exists, without touching anything.
    ///
    /// `check_arguments` only proves the JSON decodes. A live migration then died on
    /// `read_reference` naming a document one letter off from a real one, because a referent that
    /// does not resolve became an adapter error. That is a defect the model can see and correct, so
    /// it belongs here, where the reducer records it as terminal `RejectedAsInvalid` and the
    /// Episode continues.
    pub(super) fn check_referents(
        &self,
        call: &GatewayToolCall,
    ) -> Result<(), (String, &'static str)> {
        let (Some(corpus), READ_REFERENCE_TOOL) = (self.reference.as_ref(), call.name.as_str())
        else {
            return Ok(());
        };
        let Ok(arguments) =
            serde_json::from_slice::<crate::reference::ReadReferenceArguments>(&call.raw_arguments)
        else {
            return Ok(());
        };
        let Some(document) = arguments.document else {
            return Ok(());
        };
        if corpus.contains(&document) {
            return Ok(());
        }
        Err((
            format!(
                "no reference document named {document}; the corpus holds {}",
                corpus.document_ids().join(", ")
            ),
            crate::reference::READ_REFERENCE_ARGUMENT_CONTRACT,
        ))
    }

    /// Turns "you named the wrong thing" into a correction turn, for every tool.
    ///
    /// This is the general form of the per-site nets that preceded it. A `Citation` error means the
    /// model pointed at something this migration cannot resolve, which it can fix by pointing
    /// somewhere else; `Adapter` keeps its durable semantics because a broken store is not the
    /// model's to correct. Separating the two is the whole difference between a correction turn and
    /// a dead migration, and they were one variant while three paid runs died on the wrong side of
    /// it.
    pub(super) fn recover_citations(
        &self,
        request: &ToolInvocation,
        outcome: Result<ToolGatewayOutcome, ToolGatewayError>,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let Err(ToolGatewayError::Citation(message)) = outcome else {
            return outcome;
        };
        self.publish_recoverable(
            &request.call.name,
            &message,
            "nothing was built, run, or changed. Reissue this call naming a value an earlier tool \
             result gave you.",
        )
    }

    /// Keeps an instrument from ever ending a migration.
    ///
    /// An instrument is `ReadOnly` and cannot satisfy a subtask — it grants no authority — so it
    /// must not hold the power to terminate the run either. Design 0040 made model-authored input
    /// defects recoverable and Design 0042 extended that to citations; both fixed the sites they
    /// were looking at. This is the same rule enforced where it cannot be missed again: every
    /// adapter failure reaching a model through an instrument comes back as a readable result.
    pub(super) fn as_instrument(
        &self,
        request: &ToolInvocation,
        outcome: Result<ToolGatewayOutcome, ToolGatewayError>,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let Err(ToolGatewayError::Adapter(message)) = outcome else {
            return outcome;
        };
        self.publish_recoverable(
            &request.call.name,
            &message,
            "this tool only reports; nothing was built, run, or changed. Correct the arguments \
             and reissue, or continue without it.",
        )
    }

    /// Publishes a readable refusal and returns it as a terminal-but-recoverable tool result.
    pub(super) fn publish_recoverable(
        &self,
        tool: &str,
        reason: &str,
        guidance: &str,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let explanation = serde_json::json!({
            "rejected": true,
            "tool": tool,
            "reason": reason,
            "recoverable": true,
            "guidance": guidance,
        });
        let bytes = serde_json::to_vec(&explanation)
            .map_err(|error| adapter_error(format!("cannot encode rejection: {error}")))?;
        let artifact = ingest_bytes(self.artifacts.as_ref(), &bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status: ToolOperationStatus::CandidateFailed,
            result_digest: artifact.digest,
            receipt_digests: Vec::new(),
            satisfies_subtask: false,
        })
    }
}

/// Says what to do about a rejection, not only that there was one.
///
/// A serde message is accurate and useless on its own. "EOF while parsing a string at line 1
/// column 7687" is what a model sees when it ran out of output tokens mid-bundle, and it spent a
/// turn re-sending the same oversized call. The two cases worth naming are the two that happened:
/// output that stopped mid-string, and a field name that is nearly right.
pub(super) fn guidance_for(tool: &str, reason: &str) -> String {
    let truncated = reason.contains("EOF while parsing");
    if truncated && tool == SUBMIT_CANDIDATE_BUNDLE_TOOL {
        return "your arguments stop part-way through a string, which is what a response that ran \
                out of room looks like. Nothing was affected. Send less in one call: set \
                inherit_from_manifest_digest to a manifest an earlier submission returned and \
                include only the files that change."
            .to_owned();
    }
    if truncated {
        return "your arguments stop part-way through a string, which is what a response that ran \
                out of room looks like. Nothing was affected. Reissue this call with a shorter \
                argument."
            .to_owned();
    }
    if reason.contains("unknown field") {
        return "a field name is not one this tool accepts; the accepted names are listed in \
                expected_arguments. Nothing was affected. Reissue with the exact names."
            .to_owned();
    }
    "the arguments were not decodable; no candidate, build, or run was affected. Reissue this call \
     with corrected arguments."
        .to_owned()
}

/// Decodes the arguments of one call without touching the workspace or any worker.
pub(super) fn check_arguments(name: &str, raw: &[u8]) -> Result<(), (String, &'static str)> {
    let decoded = match name {
        SUBMIT_CANDIDATE_BUNDLE_TOOL => {
            serde_json::from_slice::<SubmitCandidateBundleArguments>(raw).map(|_| ())
        }
        REQUEST_SOURCE_GATE_TOOL => serde_json::from_slice::<SourceGateArguments>(raw).map(|_| ()),
        REQUEST_ASCEND_BUILD_TOOL => crate::build_tool::check_ascend_build_arguments(raw),
        READ_BUILD_DIAGNOSTICS_TOOL => {
            crate::build_tool::check_read_build_diagnostics_arguments(raw)
        }
        READ_REFERENCE_TOOL => crate::reference::check_read_reference_arguments(raw),
        REQUEST_REDUCTION_CORRECTNESS_TOOL => {
            crate::correctness_tool::check_reduction_correctness_arguments(raw)
        }
        _ => return Err((format!("unknown tool {name}"), "")),
    };
    decoded.map_err(|error| (error.to_string(), argument_contract(name)))
}

pub(super) fn argument_contract(name: &str) -> &'static str {
    match name {
        SUBMIT_CANDIDATE_BUNDLE_TOOL => SUBMIT_CANDIDATE_BUNDLE_ARGUMENT_CONTRACT,
        REQUEST_SOURCE_GATE_TOOL => SOURCE_GATE_ARGUMENT_CONTRACT,
        REQUEST_ASCEND_BUILD_TOOL => crate::build_tool::REQUEST_ASCEND_BUILD_ARGUMENT_CONTRACT,
        READ_BUILD_DIAGNOSTICS_TOOL => crate::build_tool::READ_BUILD_DIAGNOSTICS_ARGUMENT_CONTRACT,
        READ_REFERENCE_TOOL => crate::reference::READ_REFERENCE_ARGUMENT_CONTRACT,
        REQUEST_REDUCTION_CORRECTNESS_TOOL => {
            crate::correctness_tool::REQUEST_REDUCTION_CORRECTNESS_ARGUMENT_CONTRACT
        }
        _ => "",
    }
}
